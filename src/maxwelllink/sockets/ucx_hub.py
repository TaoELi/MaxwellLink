# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
UCX-backed SocketHub implementation.

This module provides :class:`SocketHubUCX`, a synchronous hub with the same
solver-facing API as :class:`maxwelllink.sockets.sockets.SocketHub` while
internally using UCX active messages.
"""

from __future__ import annotations

import asyncio
import gc
import inspect
import os
import socket
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Optional

import numpy as np

from .sockets import am_master
from .ucx_protocol import (
    BYE,
    HELLO,
    STEP_RESPONSE,
    pack_init,
    pack_step_request,
    pack_stop,
    reset_ucx_module,
    unpack_hello,
    unpack_message,
    unpack_step_response,
    load_ucx_module,
    init_ucx_module,
)

_INBOX_EMPTY = object()
_INBOX_EOF = object()


def _scrub_inherited_transport_env() -> None:
    """
    Remove inherited transport pinning before initializing a local UCX listener.

    When MaxwellLink is launched from an HPC login shell, environment variables
    such as ``UCX_NET_DEVICES`` may point at a NIC that does not exist on the
    current host. That is valid for a batch node but breaks local loopback UCX
    listeners. Set ``MXL_UCX_KEEP_TRANSPORT_ENV=1`` to preserve the inherited
    UCX/FI settings.
    """

    keep_transport_env = (
        os.environ.get(
            "MXL_UCX_KEEP_TRANSPORT_ENV",
            os.environ.get("MXL_DRIVER_UCX_KEEP_TRANSPORT_ENV", ""),
        )
        .strip()
        .lower()
        in {"1", "true", "yes", "on"}
    )
    if keep_transport_env:
        return

    for key in list(os.environ.keys()):
        if key.startswith(("UCX_", "FI_")):
            os.environ.pop(key, None)


@dataclass
class _UCXClientState:
    """
    Per-endpoint state tracked by :class:`SocketHubUCX`.

    Attributes
    ----------
    endpoint : object
        UCX endpoint object.
    address : str
        Human-readable peer description.
    molecule_id : int
        Assigned molecule ID, or ``-1`` while unbound.
    inbox : asyncio.Queue
        Queue of inbound non-HELLO control/data messages.
    last_amp : numpy.ndarray or None
        Last source amplitude vector.
    pending_send : bool
        Whether a step request has been dispatched but not yet collected.
    initialized : bool
        Whether INIT has been sent for this endpoint.
    alive : bool
        Connection liveness flag.
    hello_received : bool
        Whether the client has sent HELLO.
    hello_payload : dict
        Parsed HELLO payload.
    extras : dict
        Optional per-client metadata.
    """

    endpoint: Optional[object]
    address: str
    molecule_id: int
    inbox: asyncio.Queue
    last_amp: Optional[np.ndarray] = None
    pending_send: bool = False
    initialized: bool = False
    alive: bool = True
    hello_received: bool = False
    hello_payload: dict = field(default_factory=dict)
    extras: dict = field(default_factory=dict)


class SocketHubUCX:
    """
    UCX server coordinating multiple driver connections with an EM solver.

    The public API intentionally mirrors :class:`SocketHub`:
    ``register_molecule``, ``register_molecule_return_id``, ``all_bound``,
    ``wait_until_bound``, ``step_barrier``, ``graceful_shutdown``, and ``stop``.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = 31415,
        timeout: float = 60.0,
        latency: float = 0.01,
        ifname: Optional[str] = None,
        ucx_options: Optional[dict] = None,
        _ucx_module=None,
    ):
        """
        Initialize the UCX hub.

        Parameters
        ----------
        host : str or None, default: None
            Host/IP address clients should connect to. When omitted, the hub
            resolves a best-effort local address.
        port : int or None, default: 31415
            UCX listener port. Use ``0`` to let the runtime choose one.
        timeout : float, default: 60.0
            Timeout (seconds) used by synchronous hub operations.
        latency : float, default: 0.01
            Poll interval (seconds) while waiting for binds or responses.
        ifname : str or None, optional
            Optional network interface name used when resolving the listener address.
        ucx_options : dict or None, optional
            Optional UCX runtime options passed to the Python UCX binding.
        _ucx_module : module or None, optional
            Private dependency-injection hook used by tests.
        """

        self.timeout = float(timeout)
        self.latency = float(latency)
        self.ifname = ifname
        self.host = host
        self.port = int(port or 0)
        self._where = f"{self.host or '0.0.0.0'}:{self.port}"

        self.clients: Dict[int, _UCXClientState] = {}
        self.addrmap: Dict[str, int] = {}
        self.bound: Dict[int, _UCXClientState] = {}
        self.expected: set[int] = set()
        self.paused = False
        self._inflight = None
        self._molecule_id_counter = 0
        self._stop = False
        self._lock = threading.RLock()

        self._ucx = _ucx_module
        self._ucx_options = dict(ucx_options or {})
        self._listener = None
        self._loop = None
        self._loop_thread = None
        self._loop_ready = threading.Event()
        self._serve_tasks: set[asyncio.Task] = set()

        if am_master():
            _scrub_inherited_transport_env()
            self._ucx = self._ucx or load_ucx_module()
            self._loop = asyncio.new_event_loop()
            self._loop_thread = threading.Thread(
                target=self._event_loop_main,
                daemon=True,
                name="maxwelllink-ucx-hub",
            )
            self._loop_thread.start()
            self._loop_ready.wait()
            self._run_coro(self._start_listener())

    def _event_loop_main(self):
        """
        Background event-loop entry point.
        """

        asyncio.set_event_loop(self._loop)
        self._loop_ready.set()
        self._loop.run_forever()

        pending = asyncio.all_tasks(self._loop)
        for task in pending:
            task.cancel()
        if pending:
            try:
                self._loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
            except Exception:
                pass
        self._loop.close()

    def _run_coro(self, coro):
        """
        Execute a coroutine on the background loop and return its result.

        Parameters
        ----------
        coro : coroutine
            Coroutine to execute.

        Returns
        -------
        any
            Coroutine result.
        """

        if self._loop is None:
            raise RuntimeError("SocketHubUCX is only active on the MPI master rank.")
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return fut.result()

    def _resolve_host(self) -> str:
        """
        Resolve a best-effort host/IP string for clients.

        Returns
        -------
        str
            IP address or host string.
        """

        if self.host not in (None, ""):
            return str(self.host)

        getter = getattr(self._ucx, "get_address", None)
        if callable(getter):
            try:
                if self.ifname:
                    return str(getter(ifname=self.ifname))
                return str(getter())
            except Exception:
                pass

        try:
            return socket.gethostbyname(socket.gethostname())
        except Exception:
            return "127.0.0.1"

    async def _start_listener(self):
        """
        Initialize UCX and create the listener.
        """

        init_ucx_module(self._ucx, self._ucx_options)
        callback = self._serve_client_entry
        try:
            self._listener = self._ucx.create_listener(callback, self.port)
        except TypeError:
            self._listener = self._ucx.create_listener(callback, port=self.port)

        self.port = int(getattr(self._listener, "port", self.port))
        self.host = getattr(self._listener, "ip", None) or self._resolve_host()
        self._where = f"{self.host}:{self.port}"

    async def _call_endpoint_method(self, endpoint, name: str, *args):
        """
        Call one endpoint method, awaiting it when needed.

        Parameters
        ----------
        endpoint : object
            UCX endpoint object.
        name : str
            Method name.
        *args
            Positional arguments for the method.
        """

        method = getattr(endpoint, name, None)
        if method is None:
            raise AttributeError(f"UCX endpoint has no method {name!r}")
        result = method(*args)
        if inspect.isawaitable(result):
            return await result
        return result

    async def _am_send(self, endpoint, payload: bytes):
        """
        Send one active-message payload.

        Parameters
        ----------
        endpoint : object
            UCX endpoint object.
        payload : bytes
            Message bytes to send.
        """

        await self._call_endpoint_method(endpoint, "am_send", memoryview(payload))

    async def _am_recv(self, endpoint) -> bytes:
        """
        Receive one active-message payload.

        Parameters
        ----------
        endpoint : object
            UCX endpoint object.

        Returns
        -------
        bytes
            Received message bytes.
        """

        blob = await self._call_endpoint_method(endpoint, "am_recv")
        return bytes(blob)

    async def _close_endpoint(self, endpoint):
        """
        Close or abort one endpoint.

        Parameters
        ----------
        endpoint : object
            UCX endpoint object.
        """

        if endpoint is None:
            return

        for method_name in ("close", "abort"):
            method = getattr(endpoint, method_name, None)
            if method is None:
                continue
            try:
                result = method()
                if inspect.isawaitable(result):
                    await result
                return
            except Exception:
                continue

    def _log(self, *a):
        """
        Log a message with the ``[SocketHubUCX]`` prefix.
        """

        print("[SocketHubUCX]", *a)

    def _pause(self):
        """
        Pause the hub after a disconnect.
        """

        self.paused = True

    def _resume(self):
        """
        Resume the hub after all requested drivers are bound again.
        """

        self.paused = False

    def _find_free_molecule_id(self) -> int:
        """
        Find and return an available molecule ID.

        Returns
        -------
        int
            A unique molecule ID.
        """

        while True:
            molecule_id = self._molecule_id_counter
            self._molecule_id_counter += 1
            if molecule_id not in self.expected:
                return molecule_id

    def _reset_inflight_for(self, molid: int):
        """
        Reset frozen-barrier state for one molecule ID.

        Parameters
        ----------
        molid : int
            Molecule ID to reset.
        """

        if self._inflight and molid in self._inflight["wants"]:
            self._inflight["sent"][molid] = False
            self._inflight["results"].pop(molid, None)

    def _unique_client_states(self):
        """
        Return unique client-state objects tracked by the hub.

        Returns
        -------
        list
            Unique client states.
        """

        with self._lock:
            unique = []
            seen = set()
            for st in self.clients.values():
                if st is None:
                    continue
                key = id(st)
                if key in seen:
                    continue
                seen.add(key)
                unique.append(st)
            return unique

    async def _mark_dead(self, st: _UCXClientState, molid: Optional[int], reason: str):
        """
        Mark a client dead and clear its binding.

        Parameters
        ----------
        st : _UCXClientState
            Client state.
        molid : int or None
            Molecule ID, if already known.
        reason : str
            Short disconnect reason used for logging.
        """

        if molid is None:
            molid = st.molecule_id
        was_alive = st.alive
        st.alive = False

        with self._lock:
            for key, state in list(self.clients.items()):
                if state is st:
                    self.clients.pop(key, None)

        if molid is not None and molid >= 0:
            with self._lock:
                if self.bound.get(molid) is st:
                    if was_alive:
                        self._log(
                            f"DISCONNECTED ({reason}): mol {molid} from {st.address}"
                        )
                    self.bound[molid] = None
            if not self._stop:
                self._pause()
                self._reset_inflight_for(int(molid))

        try:
            st.inbox.put_nowait(_INBOX_EOF)
        except Exception:
            pass
        endpoint = st.endpoint
        st.endpoint = None
        await self._close_endpoint(endpoint)

    async def _serve_client_entry(self, endpoint):
        """
        Track one accepted-client task until it exits.

        Parameters
        ----------
        endpoint : object
            Accepted UCX endpoint.
        """

        task = asyncio.current_task()
        if task is not None:
            self._serve_tasks.add(task)
        try:
            await self._serve_client(endpoint)
        finally:
            if task is not None:
                self._serve_tasks.discard(task)

    async def _serve_client(self, endpoint):
        """
        Per-client active-message receive loop.

        Parameters
        ----------
        endpoint : object
            Accepted UCX endpoint.
        """

        st_key = id(endpoint)
        st = _UCXClientState(
            endpoint=endpoint,
            address=f"endpoint-{st_key}",
            molecule_id=-1,
            inbox=asyncio.Queue(),
        )

        with self._lock:
            self.clients[st_key] = st

        try:
            while not self._stop:
                blob = await self._am_recv(endpoint)
                message = unpack_message(blob)

                if message.opcode == HELLO:
                    hello = unpack_hello(message.payload)
                    st.hello_payload = hello
                    st.hello_received = True
                    peer = hello.get("hostname") or hello.get("peer") or st.address
                    pid = hello.get("pid")
                    parts = [str(peer)]
                    if pid is not None:
                        parts.append(f"pid={pid}")
                    st.address = " ".join(parts)
                    continue

                if message.opcode == BYE:
                    break

                await st.inbox.put(message)
        except Exception:
            pass
        finally:
            await self._mark_dead(st, st.molecule_id, reason="recv")

    async def _bind_client(
        self, st: _UCXClientState, molid: int, init_payload: dict, st_key: int
    ) -> bool:
        """
        Bind a fresh client to a molecule ID and send INIT.

        Parameters
        ----------
        st : _UCXClientState
            Client state.
        molid : int
            Molecule ID to bind.
        init_payload : dict
            Initialization payload.
        st_key : int
            Temporary key used in ``self.clients``.

        Returns
        -------
        bool
            ``True`` on success, otherwise ``False``.
        """

        with self._lock:
            if (
                self.bound.get(molid) is not None
                or not st.alive
                or not st.hello_received
            ):
                return False

        try:
            await self._am_send(st.endpoint, pack_init(init_payload))
        except Exception:
            await self._mark_dead(st, molid, reason="init")
            return False

        with self._lock:
            if not st.alive:
                return False
            st.initialized = True
            st.molecule_id = molid
            self.bound[molid] = st
            self.addrmap[st.address] = molid
            self.clients[molid] = st
            if st_key != molid:
                self.clients.pop(st_key, None)

        self._log(f"CONNECTED: mol {molid} <- {st.address}")
        self._reset_inflight_for(molid)
        st.pending_send = False
        return True

    async def _progress_binds(self, init_payloads: Dict[int, dict]) -> None:
        """
        Try to bind fresh HELLO-complete clients to requested molecule IDs.

        Parameters
        ----------
        init_payloads : dict[int, dict]
            Mapping from molecule ID to INIT payload.
        """

        with self._lock:
            pending_ids = [
                int(mid)
                for mid in init_payloads.keys()
                if self.bound.get(int(mid)) is None
            ]
            fresh_clients = [
                (k, st)
                for k, st in list(self.clients.items())
                if st is not None
                and st.alive
                and st.molecule_id < 0
                and st.hello_received
            ]

        for st_key, st in fresh_clients:
            if not pending_ids:
                return
            chosen = pending_ids.pop(0)
            payload = dict(init_payloads.get(chosen) or {"molecule_id": chosen})
            payload["molecule_id"] = chosen
            ok = await self._bind_client(st, int(chosen), payload, st_key)
            if not ok:
                pending_ids.insert(0, chosen)

    def register_molecule(self, molecule_id: int) -> None:
        """
        Reserve a slot for one molecule ID.

        Parameters
        ----------
        molecule_id : int
            Molecule ID to reserve.
        """

        with self._lock:
            molecule_id = int(molecule_id)
            if molecule_id in self.expected:
                raise ValueError(f"Molecule ID {molecule_id} already registered!")
            self.expected.add(molecule_id)
            self.bound.setdefault(molecule_id, None)

    def register_molecule_return_id(self) -> int:
        """
        Reserve a slot and return an auto-assigned molecule ID.

        Returns
        -------
        int
            Reserved molecule ID.
        """

        with self._lock:
            molecule_id = self._find_free_molecule_id()
            self.register_molecule(molecule_id)
            return molecule_id

    def all_bound(self, molecule_ids, require_init=True):
        """
        Check whether all requested molecule IDs are bound.

        Parameters
        ----------
        molecule_ids : iterable of int
            Molecule IDs to inspect.
        require_init : bool, default: True
            Also require that INIT has been sent.

        Returns
        -------
        bool
            ``True`` if every requested molecule is available.
        """

        with self._lock:
            for mid in molecule_ids:
                st = self.bound.get(int(mid))
                if st is None or not st.alive:
                    return False
                if require_init and not st.initialized:
                    return False
            return True

    async def _wait_until_bound_async(
        self, init_payloads: dict, require_init=True, timeout=None
    ) -> bool:
        """
        Async implementation of :meth:`wait_until_bound`.

        Parameters
        ----------
        init_payloads : dict[int, dict]
            Mapping from molecule ID to INIT payload.
        require_init : bool, default: True
            Also require initialization.
        timeout : float or None, optional
            Timeout in seconds.

        Returns
        -------
        bool
            ``True`` if all IDs are bound before the deadline.
        """

        wanted = {int(k) for k in init_payloads.keys()}
        payloads = {int(k): dict(v) for k, v in init_payloads.items()}
        deadline = None if timeout is None else (time.time() + float(timeout))

        while True:
            if self.all_bound(wanted, require_init=require_init):
                self._resume()
                return True

            await self._progress_binds(payloads)

            if deadline is not None and time.time() > deadline:
                return False
            await asyncio.sleep(self.latency)

    def wait_until_bound(self, init_payloads: dict, require_init=True, timeout=None):
        """
        Block until all requested molecule IDs are bound.

        Parameters
        ----------
        init_payloads : dict[int, dict]
            Mapping from molecule ID to INIT payload.
        require_init : bool, default: True
            Also require initialization.
        timeout : float or None, optional
            Timeout in seconds.

        Returns
        -------
        bool
            ``True`` on success, otherwise ``False``.
        """

        if not am_master():
            return True
        return self._run_coro(
            self._wait_until_bound_async(
                init_payloads, require_init=require_init, timeout=timeout
            )
        )

    async def _collect_one_response(self, st: _UCXClientState):
        """
        Poll one client inbox for a STEP_RESPONSE message.

        Parameters
        ----------
        st : _UCXClientState
            Client state.

        Returns
        -------
        UCXMessage or None
            A message if available, otherwise ``None``.
        """

        try:
            return st.inbox.get_nowait()
        except asyncio.QueueEmpty:
            return _INBOX_EMPTY

    async def _step_barrier_async(
        self, requests: Dict[int, dict], timeout: Optional[float] = None
    ) -> Dict[int, dict]:
        """
        Async implementation of :meth:`step_barrier`.

        Parameters
        ----------
        requests : dict[int, dict]
            Molecule requests.
        timeout : float or None, optional
            Timeout in seconds.

        Returns
        -------
        dict
            ``molid -> {"amp": ndarray(3,), "extra": bytes}``.
        """

        if self.paused:
            return {}

        deadline = time.time() + (timeout if timeout is not None else self.timeout)

        if self._inflight is None:
            wants = {int(k) for k in requests.keys()}
            self._inflight = {
                "wants": wants,
                "efields": {
                    int(mid): np.asarray(requests[mid]["efield_au"], dtype=float).copy()
                    for mid in wants
                },
                "meta": {int(mid): requests[mid].get("meta", {}) for mid in wants},
                "sent": {int(mid): False for mid in wants},
                "results": {},
            }

        wants = set(self._inflight["wants"])
        current_results = self._inflight["results"]

        if not self.all_bound(wants, require_init=True):
            init_payloads = {
                int(mid): requests.get(mid, {}).get("init") or {"molecule_id": int(mid)}
                for mid in wants
            }
            await self._progress_binds(init_payloads)
            return {}

        snapshot = []
        with self._lock:
            for mid in wants:
                if mid in current_results:
                    continue
                if self._inflight["sent"].get(mid, False):
                    continue
                st = self.bound.get(mid)
                if st is None or not st.alive:
                    self._pause()
                    self._reset_inflight_for(mid)
                    return {}
                snapshot.append(
                    (
                        int(mid),
                        st,
                        self._inflight["efields"][mid],
                        self._inflight["meta"][mid],
                    )
                )

        for mid, st, efield, meta in snapshot:
            try:
                await self._am_send(st.endpoint, pack_step_request(efield))
                st.pending_send = True
                if meta:
                    st.extras.update(meta)
                self._inflight["sent"][mid] = True
            except Exception:
                await self._mark_dead(st, mid, reason="send")
                return {}

        pending_mids = {
            int(mid) for mid in wants if int(mid) not in self._inflight["results"]
        }
        while pending_mids:
            if time.time() > deadline:
                return {}

            got_any = False
            for mid in list(pending_mids):
                with self._lock:
                    st = self.bound.get(mid)
                if st is None or not st.alive:
                    self._pause()
                    self._reset_inflight_for(mid)
                    return {}

                message = await self._collect_one_response(st)
                if message is _INBOX_EMPTY:
                    continue
                got_any = True

                if message is _INBOX_EOF:
                    self._pause()
                    self._reset_inflight_for(mid)
                    return {}

                if message.opcode != STEP_RESPONSE:
                    await self._mark_dead(st, mid, reason="proto")
                    return {}

                try:
                    amp, extra = unpack_step_response(message.payload)
                except Exception:
                    await self._mark_dead(st, mid, reason="decode")
                    return {}

                result = {"amp": amp, "extra": extra}
                self._inflight["results"][mid] = result
                st.last_amp = amp
                st.pending_send = False
                pending_mids.discard(mid)

            if not got_any:
                await asyncio.sleep(self.latency)

        results = {
            int(mid): self._inflight["results"][int(mid)]
            for mid in sorted(self._inflight["wants"])
        }
        self._inflight = None
        return results

    def step_barrier(
        self, requests: Dict[int, dict], timeout: Optional[float] = None
    ) -> Dict[int, dict]:
        """
        Barrier step: dispatch fields and collect source amplitudes.

        Parameters
        ----------
        requests : dict[int, dict]
            Molecule requests.
        timeout : float or None, optional
            Timeout in seconds.

        Returns
        -------
        dict
            ``molid -> {"amp": ndarray(3,), "extra": bytes}``.
        """

        if not am_master():
            return {}
        return self._run_coro(self._step_barrier_async(requests, timeout=timeout))

    async def _graceful_shutdown_async(
        self, reason: Optional[str] = None, wait: float = 2.0
    ):
        """
        Async implementation of :meth:`graceful_shutdown`.

        Parameters
        ----------
        reason : str or None, optional
            Optional shutdown reason.
        wait : float, default: 2.0
            Seconds to wait for clients to disconnect.
        """

        states = [st for st in self._unique_client_states() if st.alive]
        for st in states:
            try:
                await self._am_send(st.endpoint, pack_stop(reason))
            except Exception:
                await self._mark_dead(st, st.molecule_id, reason="stop")

        deadline = time.time() + float(wait)
        while time.time() < deadline:
            if not any(st.alive for st in states):
                return
            await asyncio.sleep(self.latency)

    def graceful_shutdown(self, reason: Optional[str] = None, wait: float = 2.0):
        """
        Politely ask all connected drivers to exit.

        Parameters
        ----------
        reason : str or None, optional
            Optional shutdown reason.
        wait : float, default: 2.0
            Seconds to wait for clean disconnects.
        """

        if not am_master() or self._loop is None:
            return
        self._run_coro(self._graceful_shutdown_async(reason=reason, wait=wait))

    async def _stop_async(self):
        """
        Async implementation of :meth:`stop`.
        """

        self._stop = True

        listener = self._listener
        self._listener = None
        if listener is not None:
            close = getattr(listener, "close", None)
            if callable(close):
                result = close()
                if inspect.isawaitable(result):
                    await result

        await self._graceful_shutdown_async(wait=max(2.0, 10.0 * self.latency))

        for st in self._unique_client_states():
            endpoint = st.endpoint
            st.endpoint = None
            await self._close_endpoint(endpoint)

        pending_tasks = [
            task
            for task in list(self._serve_tasks)
            if task is not asyncio.current_task()
        ]
        if pending_tasks:
            await asyncio.gather(*pending_tasks, return_exceptions=True)

        with self._lock:
            self.clients.clear()
            self.addrmap.clear()
            self._inflight = None
            for molid in list(self.bound.keys()):
                self.bound[molid] = None

        gc.collect()

        reset_ucx_module(self._ucx)

    def stop(self):
        """
        Stop accepting new connections and close active endpoints.
        """

        if not am_master() or self._loop is None:
            return

        try:
            self._run_coro(self._stop_async())
        finally:
            self._loop.call_soon_threadsafe(self._loop.stop)
            if self._loop_thread is not None:
                self._loop_thread.join(timeout=2.0)
