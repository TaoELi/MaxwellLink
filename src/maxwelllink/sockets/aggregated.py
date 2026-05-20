# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
Two-layer socket aggregation for MaxwellLink.

This module adds an opt-in transport layer on top of the existing
``SocketHub`` implementation without modifying the original hub logic.

The new design introduces two roles:

- ``AggregatedSocketHub``: an EM-side hub that keeps the public hub API
  expected by MaxwellLink solvers, but aggregates multiple molecule requests
  into one upstream connection per HPC node.
- ``LocalSocketHubBridge``: a node-local bridge process/thread that talks to
  ``AggregatedSocketHub`` upstream while reusing an ordinary downstream
  :class:`~maxwelllink.sockets.sockets.SocketHub` to fan out work to multiple
  existing Python/socket-only drivers.

This preserves existing ``SocketHub`` behavior while enabling a two-layer
communication topology:

    EM solver -> AggregatedSocketHub ==TCP==> LocalSocketHubBridge
              -> local SocketHub ==TCP/UNIX==> many molecular drivers
"""

from __future__ import annotations

from collections.abc import Iterable
import json
import socket
import time
import threading
from dataclasses import dataclass, field
from typing import Dict, Mapping, Optional

import numpy as np

from .sockets import (
    DT_FLOAT,
    BYE,
    STOP,
    SocketHub,
    _ClientState,
    _SocketClosed,
    _recv_array,
    _recv_bytes,
    _recv_int,
    _recv_msg,
    _send_array,
    _send_bytes,
    _send_int,
    _send_msg,
)

AGGHELLO = b"AGGHELLO"
AGGINIT = b"AGGINIT"
AGGREADY = b"AGGREADY"
AGGSTEP = b"AGGSTEP"
AGGRESULT = b"AGGRESULT"


def _json_dumps_bytes(payload: Mapping) -> bytes:
    """Encode a mapping into compact UTF-8 JSON bytes."""

    return json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _json_loads_bytes(payload: bytes) -> dict:
    """Decode a UTF-8 JSON payload, defaulting empty content to ``{}``."""

    return json.loads(payload.decode("utf-8") or "{}")


def _recv_msg_with_timeout(sock: socket.socket, timeout: float) -> bytes:
    """
    Receive one 12-byte MaxwellLink header using a temporary timeout.

    This is used while discovering fresh bridge clients so the hub can poll
    for their HELLO payload without blocking the whole EM-side wait loop.
    """

    old_timeout = sock.gettimeout()
    try:
        sock.settimeout(timeout)
        return _recv_msg(sock)
    finally:
        sock.settimeout(old_timeout)


def _connect_tcp_with_retry(address: str, port: int, timeout: float) -> socket.socket:
    """Connect to a TCP server with bounded retries."""

    deadline = time.monotonic() + float(timeout)
    delay = 0.05
    last_error = None

    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break

        sock = socket.socket(socket.AF_INET)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except (OSError, AttributeError):
            pass
        sock.settimeout(min(10.0, max(0.25, remaining)))

        try:
            sock.connect((address, port))
            sock.settimeout(timeout)
            return sock
        except (ConnectionRefusedError, TimeoutError, socket.timeout, OSError) as exc:
            last_error = exc
            try:
                sock.close()
            except OSError:
                pass
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(delay, remaining))
            delay = min(delay * 1.5, 1.0)

    raise TimeoutError(
        f"Timed out connecting to aggregated hub at {(address, port)!r}"
    ) from last_error


def _send_aggregate_hello(sock: socket.socket, *, group_id: str) -> None:
    """Send the bridge HELLO banner used by the aggregate protocol."""

    _send_msg(sock, AGGHELLO)
    _send_bytes(sock, _json_dumps_bytes({"group_id": str(group_id), "version": 1}))


def _recv_aggregate_hello(sock: socket.socket) -> dict:
    """Receive and decode a bridge HELLO payload."""

    msg = _recv_msg(sock)
    if msg != AGGHELLO:
        raise RuntimeError(f"Expected {AGGHELLO!r}, got {msg!r}")
    hello = _json_loads_bytes(_recv_bytes(sock))
    group_id = str(hello.get("group_id", "")).strip()
    if not group_id:
        raise RuntimeError("Bridge HELLO is missing a non-empty 'group_id'")
    return hello


def _send_aggregate_init(
    sock: socket.socket,
    *,
    group_id: str,
    init_payloads: Mapping[int, dict],
) -> None:
    """Send group membership plus per-molecule INIT payloads to a bridge."""

    payload = {
        "group_id": str(group_id),
        "molecule_ids": [int(mid) for mid in init_payloads.keys()],
        "init_payloads": {
            str(int(mid)): {
                **dict(init_payloads[mid]),
                "molecule_id": int(mid),
            }
            for mid in init_payloads.keys()
        },
    }
    _send_msg(sock, AGGINIT)
    _send_bytes(sock, _json_dumps_bytes(payload))


def _recv_aggregate_init(sock: socket.socket) -> dict:
    """Receive group initialization data from an aggregated hub."""

    msg = _recv_msg(sock)
    if msg != AGGINIT:
        raise RuntimeError(f"Expected {AGGINIT!r}, got {msg!r}")
    payload = _json_loads_bytes(_recv_bytes(sock))
    payload["group_id"] = str(payload.get("group_id", "")).strip()
    payload["molecule_ids"] = [int(mid) for mid in payload.get("molecule_ids", [])]
    payload["init_payloads"] = {
        int(mid): dict(data)
        for mid, data in payload.get("init_payloads", {}).items()
    }
    return payload


def _send_aggregate_step(
    sock: socket.socket, requests: Mapping[int, Mapping[str, np.ndarray]]
) -> None:
    """Send one grouped fan-out step to a bridge."""

    _send_msg(sock, AGGSTEP)
    _send_int(sock, len(requests))
    for mid, payload in requests.items():
        field = np.asarray(payload["efield_au"], dtype=DT_FLOAT).reshape(3)
        _send_int(sock, int(mid))
        _send_array(sock, field, DT_FLOAT)


def _recv_aggregate_step(sock: socket.socket) -> Dict[int, np.ndarray]:
    """Receive a grouped fan-out step on the bridge side."""

    msg = _recv_msg(sock)
    if msg != AGGSTEP:
        raise RuntimeError(f"Expected {AGGSTEP!r}, got {msg!r}")

    nreq = _recv_int(sock)
    requests: Dict[int, np.ndarray] = {}
    for _ in range(nreq):
        mid = int(_recv_int(sock))
        requests[mid] = _recv_array(sock, (3,), DT_FLOAT).astype(float, copy=False)
    return requests


def _send_aggregate_result(
    sock: socket.socket, responses: Mapping[int, Mapping[str, object]]
) -> None:
    """Send grouped molecule responses back to the aggregated hub."""

    _send_msg(sock, AGGRESULT)
    _send_int(sock, len(responses))
    for mid, payload in responses.items():
        amp = np.asarray(payload["amp"], dtype=DT_FLOAT).reshape(3)
        extra = payload.get("extra", b"")
        if isinstance(extra, str):
            extra = extra.encode("utf-8")
        extra = bytes(extra)
        _send_int(sock, int(mid))
        _send_array(sock, amp, DT_FLOAT)
        _send_bytes(sock, extra)


def _recv_aggregate_result(sock: socket.socket) -> Dict[int, dict]:
    """Receive grouped molecule responses from a bridge."""

    msg = _recv_msg(sock)
    if msg != AGGRESULT:
        raise RuntimeError(f"Expected {AGGRESULT!r}, got {msg!r}")

    nresp = _recv_int(sock)
    responses: Dict[int, dict] = {}
    for _ in range(nresp):
        mid = int(_recv_int(sock))
        amp = _recv_array(sock, (3,), DT_FLOAT).astype(float, copy=False)
        responses[mid] = {"amp": amp, "extra": _recv_bytes(sock)}
    return responses


@dataclass
class _AggregateGroupState:
    """Per-bridge group state tracked by :class:`AggregatedSocketHub`."""

    group_id: str
    molecule_ids: list[int] = field(default_factory=list)
    init_payloads: Dict[int, dict] = field(default_factory=dict)
    bridge: Optional[_ClientState] = None


class AggregatedBridge:
    """
    Convenience handle for one hub-owned local bridge.

    Instances of this class are returned by
    :meth:`AggregatedSocketHub.add_bridge`. They provide a light wrapper around
    :class:`LocalSocketHubBridge` so existing input scripts only need to:

    1. create bridge handles from the hub,
    2. attach molecules to a handle via :meth:`append`, and
    3. launch downstream drivers against ``address``.
    """

    def __init__(
        self,
        *,
        hub: "AggregatedSocketHub",
        group_id: str,
        bridge: "LocalSocketHubBridge",
    ):
        self.hub = hub
        self.group_id = str(group_id)
        self._bridge = bridge

    @property
    def address(self) -> str:
        """Address string downstream UNIX-socket drivers should use."""

        if self._bridge.local_unixsocket is None:
            raise RuntimeError("This convenience bridge does not use a UNIX socket.")
        return self._bridge.local_unixsocket

    @property
    def unixsocket(self) -> Optional[str]:
        """Configured UNIX-socket driver address, if any."""

        return self._bridge.local_unixsocket

    @property
    def unixsocket_path(self) -> Optional[str]:
        """Resolved filesystem path for the local UNIX socket."""

        return self._bridge.local_hub.unixsocket_path

    @property
    def local_endpoint(self) -> dict:
        """Return the downstream endpoint mapping for driver launch code."""

        return dict(self._bridge.local_endpoint)

    def append(self, molecules) -> None:
        """
        Attach one molecule or an iterable of molecules to this bridge group.

        The helper only mutates ``molecule.init_payload["aggregate_group"]`` and
        therefore works with existing ``mxl.Molecule`` / ``SocketMolecule``
        objects without changing solver-side logic.
        """

        if hasattr(molecules, "init_payload"):
            items = [molecules]
        elif isinstance(molecules, Iterable) and not isinstance(
            molecules, (str, bytes, bytearray)
        ):
            items = list(molecules)
        else:
            raise TypeError(
                "append(...) expects one molecule or an iterable of molecules."
            )

        for molecule in items:
            if not hasattr(molecule, "init_payload"):
                raise TypeError(
                    "append(...) received an item without an 'init_payload' attribute."
                )
            molecule_hub = getattr(molecule, "hub", self.hub)
            if molecule_hub is not self.hub:
                raise ValueError(
                    "All molecules attached to an AggregatedBridge must use the same hub."
                )

            payload = molecule.init_payload
            if payload is None:
                payload = {}
                molecule.init_payload = payload
            elif not isinstance(payload, dict):
                payload = dict(payload)
                molecule.init_payload = payload

            previous = payload.get("aggregate_group")
            if previous is not None and str(previous).strip() not in ("", self.group_id):
                raise ValueError(
                    f"Molecule is already assigned to aggregate_group {previous!r}, "
                    f"cannot move it to {self.group_id!r}."
                )
            payload["aggregate_group"] = self.group_id

    def start(self) -> threading.Thread:
        """Start the underlying local bridge thread."""

        return self._bridge.start()

    def stop(self, wait: float = 2.0) -> None:
        """Stop the underlying local bridge."""

        self._bridge.stop(wait=wait)


class AggregatedSocketHub(SocketHub):
    """
    EM-side hub that aggregates multiple molecule requests into one bridge link.

    This class keeps the same public methods used by MaxwellLink solvers
    (``register_molecule_return_id``, ``wait_until_bound``, ``all_bound``,
    ``step_barrier``) while mapping many molecule IDs onto a smaller number of
    bridge connections.

    Molecules are assigned to a bridge group through
    ``init_payload["aggregate_group"]``. All molecules sharing the same group
    are sent together to one :class:`LocalSocketHubBridge`.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = 31415,
        timeout: float = 60.0,
        latency: float = 0.01,
    ):
        super().__init__(
            host=host,
            port=port,
            unixsocket=None,
            timeout=timeout,
            latency=latency,
        )
        self._groups: Dict[str, _AggregateGroupState] = {}
        self._molecule_to_group: Dict[int, str] = {}
        self._bridge_connect_host = (
            "127.0.0.1" if host in (None, "", "0.0.0.0") else str(host)
        )
        self._bridge_connect_port = int(port or 31415)
        self._owned_bridges: list[AggregatedBridge] = []
        self._bridge_counter = 0

    def add_bridge(self, local_unixsocket: str) -> AggregatedBridge:
        """
        Create, start, and return one hub-owned local UNIX-socket bridge.

        This is the convenience entry point intended for minimal edits when
        migrating an existing single-layer ``SocketHub`` script to the new
        two-layer transport.
        """

        unix_name = str(local_unixsocket).strip()
        if not unix_name:
            raise ValueError("local_unixsocket must be a non-empty string.")
        for handle in self._owned_bridges:
            if handle.unixsocket == unix_name:
                raise ValueError(
                    f"A bridge for local unix address {unix_name!r} already exists."
                )

        group_id = f"node-{self._bridge_counter}"
        self._bridge_counter += 1

        bridge = LocalSocketHubBridge(
            group_id=group_id,
            upstream_host=self._bridge_connect_host,
            upstream_port=self._bridge_connect_port,
            timeout=self.timeout,
            latency=self.latency,
            local_unixsocket=unix_name,
        )
        bridge.start()

        handle = AggregatedBridge(hub=self, group_id=group_id, bridge=bridge)
        self._owned_bridges.append(handle)
        self._log(
            f"STARTED: aggregate group {group_id!r} -> unix address {handle.address!r}"
        )
        if handle.unixsocket_path and handle.unixsocket_path != handle.address:
            self._log(f"UNIX PATH: {handle.unixsocket_path}")
        return handle

    def _extract_group_id(self, init_payload: Mapping, molecule_id: int) -> str:
        """Return the aggregate group for one molecule."""

        group_id = init_payload.get("aggregate_group")
        if group_id is None:
            return f"molecule-{int(molecule_id)}"
        group_id = str(group_id).strip()
        if not group_id:
            raise ValueError(
                f"aggregate_group for molecule {int(molecule_id)} must be non-empty"
            )
        return group_id

    def _prepare_groups_locked(self, init_payloads: Mapping[int, dict]) -> None:
        """Build or update aggregate group metadata from solver INIT payloads."""

        for mid, raw_payload in init_payloads.items():
            molid = int(mid)
            payload = {**dict(raw_payload), "molecule_id": molid}
            previous = self._molecule_to_group.get(molid)
            if "aggregate_group" not in payload and previous is not None:
                group_id = previous
            else:
                group_id = self._extract_group_id(payload, molid)
            if previous is not None and previous != group_id:
                raise ValueError(
                    f"Molecule {molid} was already assigned to aggregate_group "
                    f"{previous!r}, cannot reassign it to {group_id!r}."
                )

            self._molecule_to_group[molid] = group_id
            group = self._groups.setdefault(group_id, _AggregateGroupState(group_id))
            group.init_payloads[molid] = payload
            if molid not in group.molecule_ids:
                group.molecule_ids.append(molid)

    def _bind_group_locked(self, group_id: str, st_key, st: _ClientState) -> None:
        """Attach one accepted bridge socket to a configured group."""

        group = self._groups[group_id]
        group.bridge = st
        st.molecule_id = group.molecule_ids[0] if group.molecule_ids else -1
        st.initialized = False
        st.extras["aggregate_group"] = group_id
        self.clients[group_id] = st
        if st_key != group_id:
            self.clients.pop(st_key, None)
        for mid in group.molecule_ids:
            self.bound[mid] = st
        self._log(f"CONNECTED: aggregate group {group_id!r} <- {st.address}")

    def _drop_client_locked(self, st_key, st: _ClientState, reason: str) -> None:
        """Remove a temporary or duplicate bridge client."""

        self.clients.pop(st_key, None)
        self._unregister_sock(st.sock)
        st.alive = False
        try:
            st.sock.close()
        except OSError:
            pass
        self._log(f"DROPPED ({reason}): {st.address}")

    def _mark_group_dead(self, group_id: str, reason: str) -> None:
        """Mark an aggregate group as disconnected and clear all molecule bindings."""

        with self._lock:
            group = self._groups.get(group_id)
            if group is None or group.bridge is None:
                return
            st = group.bridge
            group.bridge = None
            self.clients.pop(group_id, None)
            st.alive = False
            st.initialized = False
            self._unregister_sock(st.sock)
            for mid in group.molecule_ids:
                if self.bound.get(mid) is st:
                    self.bound[mid] = None

        self._log(f"DISCONNECTED ({reason}): aggregate group {group_id!r}")
        try:
            st.sock.close()
        except OSError:
            pass
        self._pause()

    def _try_identify_fresh_clients(self) -> None:
        """
        Poll newly accepted sockets for bridge HELLO messages.

        A bridge sends HELLO immediately after connecting. We keep the read
        timeout short here so one slow client cannot stall the entire hub.
        """

        with self._lock:
            fresh_clients = [
                (st_key, st)
                for st_key, st in list(self.clients.items())
                if st is not None
                and st.alive
                and st.molecule_id < 0
                and "aggregate_group" not in st.extras
            ]

        for st_key, st in fresh_clients:
            try:
                msg = _recv_msg_with_timeout(st.sock, max(self.latency, 0.05))
            except socket.timeout:
                continue
            except (RuntimeError, _SocketClosed, OSError):
                with self._lock:
                    self._drop_client_locked(st_key, st, reason="hello")
                continue

            if msg != AGGHELLO:
                with self._lock:
                    self._drop_client_locked(st_key, st, reason="hello-header")
                continue

            try:
                hello = _json_loads_bytes(_recv_bytes(st.sock))
            except (RuntimeError, _SocketClosed, OSError):
                with self._lock:
                    self._drop_client_locked(st_key, st, reason="hello-payload")
                continue

            group_id = str(hello.get("group_id", "")).strip()
            if not group_id:
                with self._lock:
                    self._drop_client_locked(st_key, st, reason="hello-group")
                continue
            with self._lock:
                st.extras["aggregate_group"] = group_id

    def _progress_group_binds(self) -> None:
        """Bind identified bridge clients to configured groups whenever possible."""

        with self._lock:
            fresh_clients = [
                (st_key, st)
                for st_key, st in list(self.clients.items())
                if st is not None
                and st.alive
                and st.molecule_id < 0
                and "aggregate_group" in st.extras
            ]

            for st_key, st in fresh_clients:
                group_id = st.extras["aggregate_group"]
                group = self._groups.get(group_id)
                if group is None:
                    continue
                if group.bridge is None:
                    self._bind_group_locked(group_id, st_key, st)
                elif group.bridge is not st:
                    self._drop_client_locked(st_key, st, reason="duplicate-group")

    def _initialize_group(self, group_id: str) -> bool:
        """Send AGGINIT to a bound bridge and wait for AGGREADY."""

        with self._lock:
            group = self._groups[group_id]
            st = group.bridge
            init_payloads = dict(group.init_payloads)

        if st is None or not st.alive:
            return False

        try:
            _send_aggregate_init(
                st.sock,
                group_id=group_id,
                init_payloads=init_payloads,
            )
            msg = _recv_msg_with_timeout(st.sock, self.timeout)
            if msg != AGGREADY:
                raise RuntimeError(f"Expected {AGGREADY!r}, got {msg!r}")
        except (socket.timeout, RuntimeError, _SocketClosed, OSError):
            self._mark_group_dead(group_id, reason="init")
            return False

        with self._lock:
            if group.bridge is st and st.alive:
                st.initialized = True
                return True
        return False

    def wait_until_bound(self, init_payloads: dict, require_init=True, timeout=None):
        """
        Wait until all requested molecules are served by initialized bridges.

        Molecules are grouped through ``init_payload["aggregate_group"]`` and each
        group must be backed by exactly one connected bridge.
        """

        wanted = {int(mid) for mid in init_payloads.keys()}
        deadline = time.time() + (
            float(timeout) if timeout is not None else float(self.timeout)
        )
        payloads = {
            int(mid): {**dict(init_payloads[mid]), "molecule_id": int(mid)}
            for mid in init_payloads.keys()
        }

        with self._lock:
            self._prepare_groups_locked(payloads)

        while True:
            if self.all_bound(wanted, require_init=require_init):
                self._resume()
                return True

            self._try_identify_fresh_clients()
            self._progress_group_binds()

            with self._lock:
                groups_needing_init = [
                    group_id
                    for group_id, group in self._groups.items()
                    if any(mid in wanted for mid in group.molecule_ids)
                    and group.bridge is not None
                    and group.bridge.alive
                    and not group.bridge.initialized
                ]

            for group_id in groups_needing_init:
                self._initialize_group(group_id)

            if timeout is not None and time.time() > deadline:
                return False
            time.sleep(self.latency)

    def step_barrier(
        self, requests: Dict[int, dict], timeout: Optional[float] = None
    ) -> Dict[int, dict]:
        """
        Dispatch all requested fields group-by-group and collect grouped replies.

        The caller-facing contract matches ``SocketHub.step_barrier``:
        ``responses[molid]`` contains ``{"amp": ndarray(3,), "extra": bytes}``.
        """

        if self.paused:
            return {}

        wants = {int(mid) for mid in requests.keys()}
        deadline = time.time() + (
            float(timeout) if timeout is not None else float(self.timeout)
        )
        payloads = {
            int(mid): (
                dict(requests[mid].get("init") or {"molecule_id": int(mid)})
            )
            for mid in requests.keys()
        }

        with self._lock:
            self._prepare_groups_locked(payloads)
            if not self.all_bound(wants, require_init=True):
                return {}

            grouped_requests: Dict[str, Dict[int, dict]] = {}
            for mid in wants:
                group_id = self._molecule_to_group[mid]
                grouped_requests.setdefault(group_id, {})[mid] = {
                    "efield_au": np.asarray(requests[mid]["efield_au"], dtype=float)
                }

            group_states = {
                group_id: self._groups[group_id].bridge
                for group_id in grouped_requests.keys()
            }

        for group_id, group_request in grouped_requests.items():
            st = group_states[group_id]
            if st is None or not st.alive or not st.initialized:
                self._pause()
                return {}
            try:
                _send_aggregate_step(st.sock, group_request)
            except (socket.timeout, _SocketClosed, OSError):
                self._mark_group_dead(group_id, reason="send")
                return {}

        responses: Dict[int, dict] = {}
        for group_id, group_request in grouped_requests.items():
            st = group_states[group_id]
            if st is None:
                self._pause()
                return {}

            remaining = deadline - time.time()
            if remaining <= 0.0:
                return {}

            old_timeout = st.sock.gettimeout()
            try:
                st.sock.settimeout(remaining)
                group_responses = _recv_aggregate_result(st.sock)
            except (socket.timeout, RuntimeError, _SocketClosed, OSError):
                self._mark_group_dead(group_id, reason="recv")
                return {}
            finally:
                st.sock.settimeout(old_timeout)

            expected = set(group_request.keys())
            actual = set(group_responses.keys())
            if actual != expected:
                self._mark_group_dead(group_id, reason="protocol")
                raise RuntimeError(
                    f"Aggregate group {group_id!r} returned molecule ids {sorted(actual)}, "
                    f"expected {sorted(expected)}."
                )
            responses.update(group_responses)

        return responses

    def stop(self):
        """
        Stop the aggregate hub and clean up bridge groups coherently.

        The base ``SocketHub.stop()`` assumes one client per molecule, which is
        not true here. This override shuts down each bridge once and clears all
        molecule bindings associated with that bridge.
        """

        owned_bridges = list(self._owned_bridges)
        self._stop = True
        try:
            self.serversock.close()
        except Exception:
            pass

        with self._lock:
            group_clients = [
                (group_id, group.bridge, list(group.molecule_ids))
                for group_id, group in self._groups.items()
                if group.bridge is not None
            ]
            seen = {id(st) for _, st, _ in group_clients}
            other_clients = []
            for key, st in list(self.clients.items()):
                if st is None or id(st) in seen:
                    continue
                seen.add(id(st))
                other_clients.append((key, st))

        for _group_id, st, _molecule_ids in group_clients:
            if not st.alive:
                continue
            try:
                _send_msg(st.sock, STOP)
            except Exception:
                pass

        deadline = time.time() + max(2.0, 10.0 * self.latency)
        while time.time() < deadline:
            remaining_alive = False
            for _group_id, st, _molecule_ids in group_clients:
                if not st.alive:
                    continue
                remaining_alive = True
                try:
                    st.sock.settimeout(self.latency)
                    msg = _recv_msg(st.sock)
                    if msg == BYE:
                        st.alive = False
                except (socket.timeout, _SocketClosed, OSError):
                    continue
            if not remaining_alive:
                break
            time.sleep(self.latency)

        sockets_to_close = []
        with self._lock:
            for group_id, st, molecule_ids in group_clients:
                group = self._groups.get(group_id)
                if group is not None and group.bridge is st:
                    group.bridge = None
                self.clients.pop(group_id, None)
                self._unregister_sock(st.sock)
                st.alive = False
                st.initialized = False
                for mid in molecule_ids:
                    if self.bound.get(mid) is st:
                        self.bound[mid] = None
                sockets_to_close.append((f"aggregate group {group_id!r}", st.sock))

            for key, st in other_clients:
                self.clients.pop(key, None)
                self._unregister_sock(st.sock)
                st.alive = False
                sockets_to_close.append((f"client {key!r}", st.sock))

        for label, sock in sockets_to_close:
            self._log(f"DISCONNECTED: {label}")
            try:
                sock.close()
            except Exception:
                pass

        try:
            self._selector.close()
        except Exception:
            pass

        for handle in owned_bridges:
            try:
                handle.stop(wait=max(2.0, 10.0 * self.latency))
            except Exception:
                pass


class LocalSocketHubBridge:
    """
    Bridge process/thread that fans out aggregate requests to a local SocketHub.

    Upstream:
        one TCP connection to :class:`AggregatedSocketHub`

    Downstream:
        one ordinary :class:`SocketHub` using either TCP or UNIX sockets,
        connected to many existing MaxwellLink socket drivers.
    """

    def __init__(
        self,
        *,
        group_id: str,
        upstream_host: str,
        upstream_port: int,
        timeout: float = 60.0,
        latency: float = 0.01,
        local_host: str = "127.0.0.1",
        local_port: Optional[int] = None,
        local_unixsocket: Optional[str] = None,
    ):
        if not str(group_id).strip():
            raise ValueError("group_id must be a non-empty string")
        if local_unixsocket is None and local_port is None:
            raise ValueError(
                "LocalSocketHubBridge requires either local_unixsocket or local_port."
            )

        self.group_id = str(group_id)
        self.upstream_host = str(upstream_host)
        self.upstream_port = int(upstream_port)
        self.timeout = float(timeout)
        self.latency = float(latency)
        self.local_host = str(local_host)
        self.local_port = int(local_port) if local_port is not None else None
        self.local_unixsocket = local_unixsocket

        self.local_hub = SocketHub(
            host=self.local_host if self.local_unixsocket is None else None,
            port=self.local_port,
            unixsocket=self.local_unixsocket,
            timeout=self.timeout,
            latency=self.latency,
        )

        self._init_payloads: Dict[int, dict] = {}
        self._upstream_sock: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    @property
    def local_endpoint(self) -> dict:
        """Return the downstream socket endpoint local drivers should connect to."""

        if self.local_unixsocket is not None:
            return {"unixsocket": self.local_unixsocket}
        return {"host": self.local_host, "port": self.local_port}

    def _ensure_local_hub_ready(self, init_payloads: Mapping[int, dict]) -> None:
        """Register downstream molecule ids and wait until local drivers bind."""

        for mid in init_payloads.keys():
            try:
                self.local_hub.register_molecule(int(mid))
            except ValueError:
                pass

        ok = self.local_hub.wait_until_bound(
            dict(init_payloads),
            require_init=True,
            timeout=None,
        )
        if not ok:
            raise RuntimeError(
                f"Timed out waiting for local drivers in aggregate group {self.group_id!r}"
            )

    def _handle_group_init(self, payload: dict) -> None:
        """Accept a new group membership assignment from the upstream hub."""

        incoming_group = str(payload.get("group_id", "")).strip()
        if incoming_group != self.group_id:
            raise RuntimeError(
                f"Bridge {self.group_id!r} received AGGINIT for group {incoming_group!r}."
            )

        init_payloads = {
            int(mid): {**dict(data), "molecule_id": int(mid)}
            for mid, data in payload["init_payloads"].items()
        }
        self._ensure_local_hub_ready(init_payloads)
        self._init_payloads = init_payloads

    def _run_local_step(self, efields: Mapping[int, np.ndarray]) -> Dict[int, dict]:
        """Fan out one grouped step to the downstream local hub."""

        requests = {
            int(mid): {
                "efield_au": np.asarray(efields[mid], dtype=float),
                "init": self._init_payloads[int(mid)],
            }
            for mid in efields.keys()
        }

        responses = self.local_hub.step_barrier(requests)
        while not responses:
            self._ensure_local_hub_ready(self._init_payloads)
            responses = self.local_hub.step_barrier(requests)
        return responses

    def run(self) -> None:
        """
        Run the bridge loop until the aggregated hub sends ``STOP`` or disconnects.
        """

        sock = _connect_tcp_with_retry(
            address=self.upstream_host,
            port=self.upstream_port,
            timeout=self.timeout,
        )
        self._upstream_sock = sock
        _send_aggregate_hello(sock, group_id=self.group_id)

        try:
            while not self._stop_event.is_set():
                msg = _recv_msg(sock)

                if msg == AGGINIT:
                    payload = _json_loads_bytes(_recv_bytes(sock))
                    payload["init_payloads"] = {
                        int(mid): dict(data)
                        for mid, data in payload.get("init_payloads", {}).items()
                    }
                    self._handle_group_init(payload)
                    _send_msg(sock, AGGREADY)

                elif msg == AGGSTEP:
                    nreq = _recv_int(sock)
                    efields: Dict[int, np.ndarray] = {}
                    for _ in range(nreq):
                        mid = int(_recv_int(sock))
                        efields[mid] = _recv_array(sock, (3,), DT_FLOAT).astype(
                            float, copy=False
                        )
                    responses = self._run_local_step(efields)
                    _send_aggregate_result(sock, responses)

                elif msg == STOP:
                    try:
                        _send_msg(sock, BYE)
                    except OSError:
                        pass
                    break

                else:
                    raise RuntimeError(f"Unexpected aggregate header: {msg!r}")

        except (socket.timeout, _SocketClosed, OSError):
            pass
        finally:
            try:
                sock.close()
            except OSError:
                pass
            self._upstream_sock = None
            self.local_hub.stop()

    def start(self) -> threading.Thread:
        """Start the bridge loop in a daemon thread and return the thread handle."""

        if self._thread is not None and self._thread.is_alive():
            return self._thread

        self._thread = threading.Thread(target=self.run, daemon=True)
        self._thread.start()
        return self._thread

    def stop(self, wait: float = 2.0) -> None:
        """Stop the bridge loop and close the downstream local hub."""

        self._stop_event.set()

        sock = self._upstream_sock
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass

        if self._thread is not None:
            self._thread.join(timeout=float(wait))

        self.local_hub.stop()


__all__ = [
    "AggregatedBridge",
    "AggregatedSocketHub",
    "LocalSocketHubBridge",
]
