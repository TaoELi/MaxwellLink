# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
Base socket hub for MaxwellLink drivers and servers.

:class:`SocketHub` is the root of the hub hierarchy: a multi-client server
coordinating many driver connections with an FDTD engine over the i-PI-style
protocol (https://ipi-code.org/)::

    SocketHub                                  this module
    ├── AggregatedSocketHub                    aggregated.py (bridge transport)
    │   └── _AggregatedSusceptibilitySocketHubServer
    └── _SusceptibilitySocketHubServer         susceptibility hubs for Meep
                                               (see _meep_hub_base.py)

The wire formats (headers, framed arrays, the step_barrier fast path) live in
``protocol.py`` and are re-exported here for backward compatibility. The MPI
helpers (``am_master``, ``mpi_bcast_from_master``) and the host/port discovery
used by Slurm workflows (``get_available_host_port``) also live here.
"""

from __future__ import annotations

import os
import selectors
import socket
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

import numpy as np


# ----------------------------------------------------------------------
# Wire protocol (moved to protocol.py)
# ----------------------------------------------------------------------
# The byte formats live in protocol.py; every name is re-exported here so
# existing imports (drivers, tests, user scripts) keep working unchanged.
from .protocol import (  # noqa: F401  re-exported for backward compatibility
    BYE,
    DT_FLOAT,
    DT_INT,
    FIELDDATA,
    FORCEREADY,
    GETFORCE,
    GETSOURCE,
    HAVEDATA,
    HEADER_LEN,
    INIT,
    NEEDINIT,
    POSDATA,
    READY,
    SOURCEREADY,
    STATUS,
    STOP,
    _EYE3_BYTES,
    _FIELDDATA_HDR,
    _FLOAT64,
    _GETSOURCE_HDR,
    _INT32,
    _NAT1_BYTES,
    _REPLY_EXTRA_LEN_OFFSET,
    _REPLY_FIXED_LEN,
    _REPLY_FORCES_OFFSET,
    _REPLY_NAT_OFFSET,
    _SEND_FIELD_OFFSET,
    _SEND_TEMPLATE,
    _SEND_TOTAL_LEN,
    _STRUCT_3D,
    _STRUCT_I,
    _SocketClosed,
    _pack_init,
    _pad12,
    _recv_array,
    _recv_bytes,
    _recv_int,
    _recv_msg,
    _recv_posdata,
    _recvall,
    _send_array,
    _send_bytes,
    _send_force_ready,
    _send_int,
    _send_msg,
)

# ======================================================================
# Module-level utilities (host/port discovery and MPI helpers)
# ======================================================================


def get_available_host_port(localhost=True, save_to_file=None) -> Tuple[str, int]:
    """
    Ask the OS for an available localhost TCP port.

    Parameters
    ----------
    localhost : bool, default: True
        If True, bind to the localhost interface ("127.0.0.1"). If False, bind to all interfaces ("0.0.0.0").

    save_to_file : str or None, default: None
        If provided, save the selected host and port to the given file with filename provided by `save_to_file`.
        The first line contains the host, and the second line contains the port.

    Returns
    -------
    tuple
        ``(host, port)`` pair, e.g., ``("127.0.0.1", 34567)``.
    """
    bind_addr = "127.0.0.1" if localhost else "0.0.0.0"
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((bind_addr, 0))
        port = s.getsockname()[1]

    ip = "127.0.0.1"
    if not localhost:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as tmp:
            tmp.connect(("8.8.8.8", 80))
            ip = tmp.getsockname()[0]

    if am_master():
        # save host and port number to a file so mxl_driver can read it
        if save_to_file is not None:
            with open(save_to_file, "w") as f:
                f.write(f"{ip}\n{port}\n")

    return ip, port


def _mpi_comm():
    """
    Return ``MPI.COMM_WORLD`` if ``mpi4py`` is importable, otherwise ``None``.

    Centralizes the optional-dependency import so the MPI helpers below can
    treat "no mpi4py" as a single-process (rank 0) world.
    """

    try:
        from mpi4py import MPI

        return MPI.COMM_WORLD
    except Exception:
        return None


# helper function to determine whether this processor is the MPI master using mpi4py
def am_master():
    """
    Return True if this process is the MPI master rank (rank 0), otherwise False.

    Notes
    -----
    Attempts to import ``mpi4py`` and query ``COMM_WORLD``. If unavailable,
    returns ``True`` by treating the single process as rank 0.
    """

    comm = _mpi_comm()
    rank = comm.Get_rank() if comm is not None else 0
    return rank == 0


# helper function to broadcast a value from master to all MPI ranks
def mpi_bcast_from_master(value):
    """
    Broadcast a Python value from the master rank to all ranks via MPI.

    Parameters
    ----------
    value : any
        The value to broadcast.

    Returns
    -------
    any
        The broadcast value (unchanged when MPI is unavailable).
    """

    comm = _mpi_comm()
    if comm is not None:
        value = comm.bcast(value, root=0)
    return value


# ======================================================================
# Per-client state and the socket hub
# ======================================================================


@dataclass
class _ClientState:
    """
    Dataclass storing per-client state for the socket hub.

    Attributes
    ----------
    sock : socket.socket
        Connected client socket.
    address : str
        Peer address string.
    molecule_id : int
        Bound molecule identifier (``-1`` if unbound).
    last_amp : numpy.ndarray or None
        Last source amplitude vector ``(3,)``.
    pending_send : bool
        Whether a field has been dispatched but not yet committed.
    initialized : bool
        Whether INIT has been completed.
    alive : bool
        Connection liveness flag.
    extras : dict
        Arbitrary metadata associated with the client.
    """

    sock: socket.socket
    address: str
    molecule_id: int
    last_amp: Optional[np.ndarray] = None  # last source amplitude (3,)
    pending_send: bool = False
    initialized: bool = False
    alive: bool = True
    extras: dict = field(default_factory=dict)


class SocketHub:
    """
    Socket server coordinating multiple driver connections with an FDTD engine.

    This server:

    - Accepts and tracks many driver connections.
    - Handles initialization handshakes, field dispatch, and result collection.
    - Provides a barrier-style step to send fields and receive source amplitudes
      from all registered molecules.

    The accept thread starts during ``__init__``; no separate ``start()`` call
    is needed.

    Subclassing contract (followed by every hub in this package):

    may be overridden
        ``_accept_loop`` (custom client classification),
        ``wait_until_bound`` / ``step_barrier`` (custom transport),
        ``stop`` (extra teardown; call ``super().stop()``)
    do not override
        the binding internals (``_progress_binds_locked``,
        ``_bind_client_locked``) and the ``step_barrier`` fast path
        (``_dispatch_field``, ``_read_source_ready``), which share scratch
        buffers and locking assumptions with ``step_barrier`` itself
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = 31415,
        unixsocket: Optional[str] = None,
        timeout: float = 60000.0,
        latency: float = 0.01,
    ):
        """
        Initialize the socket hub.

        Parameters
        ----------
        host : str or None, default: None
            Host address for AF_INET sockets. Ignored when using a UNIX socket.
        port : int or None, default: 31415
            TCP port for AF_INET sockets. Ignored for UNIX sockets.
        unixsocket : str or None, default: None
            Path (or name under ``/tmp/socketmxl_*``) for a UNIX domain socket. When
            provided, ``host`` and ``port`` are ignored.
        timeout : float, default: 60000.0
            Socket timeout (seconds) for client operations.
        latency : float, default: 0.01
            Polling sleep (seconds) between hub sweeps; can be very small for local runs.
        """

        self.unixsocket_path = None
        if am_master():
            if unixsocket:
                self.serversock = socket.socket(socket.AF_UNIX)
                # mirror i-PI's /tmp/ipi_* default when given a name
                if not unixsocket.startswith("/"):
                    unixsocket = f"/tmp/socketmxl_{unixsocket}"
                self.unixsocket_path = unixsocket
                if os.path.exists(self.unixsocket_path):
                    probe = socket.socket(socket.AF_UNIX)
                    try:
                        probe.settimeout(0.25)
                        probe.connect(self.unixsocket_path)
                    except FileNotFoundError:
                        pass
                    except ConnectionRefusedError:
                        try:
                            os.unlink(self.unixsocket_path)
                        except FileNotFoundError:
                            pass
                    else:
                        probe.close()
                        raise RuntimeError(
                            f"Socket path {self.unixsocket_path} already in use"
                        )
                    finally:
                        try:
                            probe.close()
                        except Exception:
                            pass
                self.serversock.bind(unixsocket)
                self._where = unixsocket
            else:
                self.serversock = socket.socket(socket.AF_INET)
                self.serversock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                host = host or ""
                port = port or 31415
                self.serversock.bind((host, port))
                self._where = f"{host}:{port}"

            self.serversock.listen(16384)
            self.serversock.settimeout(0.25)

            self.timeout = float(timeout)
            self.latency = float(latency)

            # key: molecule_id or temp id
            self.clients: Dict[int, _ClientState] = {}

            # peer -> molecule_id
            self.addrmap: Dict[str, int] = {}
            self._stop = False
            self._lock = threading.RLock()
            self._accept_th = threading.Thread(target=self._accept_loop, daemon=True)
            self._accept_th.start()

            # assign a molecular id accumulator
            self._molecule_id_counter = 0

            # Persistent selector — clients are registered on bind, not per step.
            self._selector = selectors.DefaultSelector()

            # Reusable scratch buffers on the hot path:
            #   _scratch_send: the 196-byte FIELDDATA+GETSOURCE blob, with
            #     the 24-byte field window at _SEND_FIELD_OFFSET patched in
            #     place each step via struct.pack_into (no per-step allocation).
            #   _scratch_recv: the 124-byte fixed SOURCEREADY reply, filled
            #     by a single recv_into loop and parsed via struct.
            self._scratch_send = bytearray(_SEND_TEMPLATE)
            self._scratch_recv = bytearray(_REPLY_FIXED_LEN)
            self._scratch_recv_mv = memoryview(self._scratch_recv)

        # molecule_id -> _ClientState (locked client)
        self.bound: Dict[int, _ClientState] = {}

        # molecule ids we expect to serve
        self.expected: set[int] = set()

        # global pause when any driver is down
        self.paused = False

        # holds a frozen barrier until it successfully commits
        self._inflight = None

    def _accept_loop(self):
        """
        Accept-loop thread: accept new connections and register temporary clients.
        """

        while not self._stop:
            try:
                csock, addr = self.serversock.accept()
            except socket.timeout:
                continue
            except OSError:
                break

            # NEW: trim latency and keep long-lived connections healthy
            try:
                csock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                # Only for AF_INET; will raise on AF_UNIX
                csock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except (OSError, AttributeError):
                pass  # AF_UNIX or platform without TCP_NODELAY

            peer = addr if isinstance(addr, str) else f"{addr[0]}:{addr[1]}"
            csock.settimeout(self.timeout)
            st = _ClientState(sock=csock, address=peer, molecule_id=-1)
            with self._lock:
                # temp key: use id(csock) until INIT binds molecule_id
                self.clients[id(csock)] = st

    def _maybe_init_client(self, st: _ClientState, init_payload: dict):
        """
        Send INIT to a client with the given payload and mark it initialized.

        Parameters
        ----------
        st : _ClientState
            Client state to initialize.
        init_payload : dict
            Initialization payload (e.g., contains ``"molecule_id"``).
        """

        _pack_init(st.sock, init_payload)
        st.initialized = True

    def _register_sock(self, sock: socket.socket, molid: int) -> None:
        """
        Register a client's socket with the persistent selector.

        Called once at bind time. If the socket is already registered (for
        example after a rebind/reconnect), we replace the old registration so
        future ``select`` events carry the up-to-date molecule id.

        Parameters
        ----------
        sock : socket.socket
            The client socket.
        molid : int
            Molecule id to attach as the selector ``data`` payload.
        """

        try:
            self._selector.register(sock, selectors.EVENT_READ, data=int(molid))
        except (KeyError, ValueError):
            # Already registered under this fd — swap the data payload.
            try:
                self._selector.unregister(sock)
                self._selector.register(sock, selectors.EVENT_READ, data=int(molid))
            except (KeyError, ValueError, OSError):
                pass
        except OSError:
            pass

    def _unregister_sock(self, sock: socket.socket) -> None:
        """
        Unregister a client socket from the persistent selector.

        Safe to call with a socket that was never registered or has already
        been closed; errors are swallowed so disconnect paths stay simple.

        Parameters
        ----------
        sock : socket.socket
            The client socket.
        """

        try:
            self._selector.unregister(sock)
        except (KeyError, ValueError, OSError):
            pass

    def _mark_dead(
        self,
        st: _ClientState,
        molid: Optional[int] = None,
        reason: Optional[str] = None,
    ) -> bool:
        """
        Mark a client dead, unregister it from the selector, and clear binding.

        This centralizes the bookkeeping that used to be duplicated across the
        STATUS-based sweep and the shutdown paths. It is safe to call from any
        phase and takes ``self._lock`` only briefly for the ``bound`` mutation
        so blocking I/O never runs while the lock is held.

        Parameters
        ----------
        st : _ClientState
            The client whose socket failed.
        molid : int or None, optional
            The molecule id the client was bound to. Falls back to
            ``st.molecule_id`` when ``None``.
        reason : str or None, optional
            Short tag for the disconnect log line (e.g. ``"send"``, ``"recv"``).
            When ``None`` the bare ``DISCONNECTED: ...`` form is logged.

        Returns
        -------
        bool
            ``True`` if a bound molecule was actually released, else ``False``.
        """

        st.alive = False
        self._unregister_sock(st.sock)
        if molid is None:
            molid = st.molecule_id
        if molid is not None and molid >= 0:
            with self._lock:
                if self.bound.get(molid) is st:
                    tag = f" ({reason})" if reason else ""
                    self._log(f"DISCONNECTED{tag}: mol {molid} from {st.address}")
                    self.bound[molid] = None
                    return True
        return False

    def _dispatch_field(
        self, st: _ClientState, blob: "bytes | bytearray | memoryview", meta: dict
    ) -> None:
        """
        Send a pre-packed FIELDDATA+GETSOURCE blob to one client in a single call.

        This is the hot-path send used by :meth:`step_barrier`. The caller is
        responsible for packing the field vector into the shared scratch buffer
        (via ``struct.pack_into``) so a whole group of clients sharing the same
        field can reuse the same blob.

        Parameters
        ----------
        st : _ClientState
            Target client state.
        blob : bytes-like
            Pre-packed 196-byte request buffer.
        meta : dict
            Optional metadata to attach to this send (stored in ``st.extras``).

        Raises
        ------
        _SocketClosed or OSError
            If the client disconnects during send. The caller is responsible
            for calling :meth:`_mark_dead`.
        """

        st.sock.sendall(blob)
        st.pending_send = True
        if meta:
            st.extras.update(meta)

    def _read_source_ready(self, st: _ClientState) -> Tuple[np.ndarray, bytes]:
        """
        Read a SOURCEREADY/FORCEREADY reply into the shared scratch buffer.

        The reply's fixed 124-byte prefix (header, energy, nat, forces, virial,
        extra_len) is drained in a single ``recv_into`` loop into
        ``self._scratch_recv`` and parsed with ``struct.unpack_from`` — no
        numpy temporaries, no per-field ``_recv_array`` calls. Only a single
        3-element ``np.array`` is allocated at the end to carry the amplitude
        back to the caller.

        The shared scratch buffer is safe because :meth:`step_barrier` drains
        selector events serially in the main thread — only one reply is being
        parsed at any given time.

        Parameters
        ----------
        st : _ClientState
            Client whose reply is being drained. Assumes the hub has already
            sent the combined FIELDDATA+GETSOURCE request and the kernel
            reported the socket readable.

        Returns
        -------
        tuple
            ``(amp_vec3, extra_bytes)`` where ``amp_vec3`` is a ``(3,)``
            ``np.ndarray`` and ``extra_bytes`` is the trailing variable blob.

        Raises
        ------
        _SocketClosed or OSError
            If the peer disconnects, the header is not SOURCEREADY, or the
            reported ``nat`` is not the EM-protocol-expected value of 1.
        """

        sock = st.sock
        mv = self._scratch_recv_mv
        n = 0
        while n < _REPLY_FIXED_LEN:
            r = sock.recv_into(mv[n:], _REPLY_FIXED_LEN - n)
            if r == 0:
                raise _SocketClosed("Peer closed")
            n += r

        # Header must be SOURCEREADY (the 12-byte ASCII tag, space-padded).
        if bytes(mv[:HEADER_LEN]).rstrip() != SOURCEREADY:
            raise _SocketClosed(
                f"Expected {SOURCEREADY!r}, got {bytes(mv[:HEADER_LEN]).rstrip()!r}"
            )

        # EM protocol contract: drivers always send nat=1.
        nat = _STRUCT_I.unpack_from(mv, _REPLY_NAT_OFFSET)[0]
        if nat != 1:
            raise _SocketClosed(f"EM fast-path expected nat=1, got nat={nat}")

        fx, fy, fz = _STRUCT_3D.unpack_from(mv, _REPLY_FORCES_OFFSET)
        extra_len = _STRUCT_I.unpack_from(mv, _REPLY_EXTRA_LEN_OFFSET)[0]
        extra = _recvall(sock, extra_len) if extra_len > 0 else b""

        amp = np.array((fx, fy, fz), dtype=float)
        st.last_amp = amp
        st.pending_send = False
        return amp, extra

    def _progress_binds_locked(self, init_payloads: Dict[int, dict]) -> None:
        """
        Drive INIT handshakes for any fresh (unbound) clients.

        Walks ``self.clients`` for entries whose ``molecule_id < 0`` (the temp
        state created by the accept loop) and, for each one, picks an expected
        molecule ID from ``init_payloads`` that is not yet bound and sends
        ``INIT`` directly. This replaces the old STATUS/NEEDINIT round-trip: both
        the Python and LAMMPS drivers accept INIT unconditionally as the first
        message from the hub, so the extra poll is unnecessary.

        Parameters
        ----------
        init_payloads : dict[int, dict]
            Mapping of molecule ID to the INIT payload to send for that ID.

        Notes
        -----
        This method assumes ``self._lock`` is held by the caller.
        """

        pending_ids = [
            int(mid) for mid in init_payloads.keys() if self.bound.get(int(mid)) is None
        ]
        if not pending_ids:
            return
        fresh_clients = [
            (k, st)
            for k, st in list(self.clients.items())
            if st is not None and st.alive and st.molecule_id < 0
        ]
        for st_key, st in fresh_clients:
            if not pending_ids:
                break
            chosen = pending_ids.pop(0)
            payload = init_payloads.get(chosen) or {"molecule_id": chosen}
            payload = {**payload, "molecule_id": chosen}
            try:
                self._bind_client_locked(st, int(chosen), payload, st_key)
            except (socket.timeout, _SocketClosed, OSError):
                st.alive = False
                # put the id back so another fresh client can claim it
                pending_ids.insert(0, chosen)

    def _bind_client_locked(
        self, st: _ClientState, molid: int, init_payload: dict, st_key
    ):
        """
        Bind a client to a molecule ID if available and perform INIT.

        Parameters
        ----------
        st : _ClientState
            Client to bind.
        molid : int
            Molecule ID to bind to.
        init_payload : dict
            INIT payload to send.
        st_key : int
            Temporary key under which the client is stored.

        Returns
        -------
        bool
            ``True`` if binding succeeded, otherwise ``False``.
        """

        if self.bound.get(molid) is None:
            self._maybe_init_client(st, init_payload)
            st.molecule_id = molid
            self.bound[molid] = st
            self.addrmap[st.address] = molid
            self.clients[molid] = st
            if st_key != molid:
                try:
                    del self.clients[st_key]
                except KeyError:
                    pass
            # Register with the persistent selector so Phase B of
            # step_barrier doesn't have to re-register on every call.
            self._register_sock(st.sock, molid)
            address = st.address
            self._log(f"CONNECTED: mol {molid} <- {address}")
            # NEW: this molid is part of a frozen barrier -> force re-dispatch
            self._reset_inflight_for(molid)
            st.pending_send = False  # defensive: this is a fresh socket
            return True
        return False

    def _log(self, *a):
        """
        Log a message with the ``[SocketHub]`` prefix.
        """

        print("[SocketHub]", *a)

    def _pause(self):
        """
        Pause the hub (used when a driver disconnects mid-barrier).
        """

        self.paused = True

    def _resume(self):
        """
        Resume the hub after a pause.
        """

        self.paused = False

    def _reset_inflight_for(self, molid: int):
        """
        Force re-dispatch for ``molid`` in a frozen barrier after reconnect.

        Parameters
        ----------
        molid : int
            Molecule ID to reset in the current barrier state.
        """

        if self._inflight and (molid in self._inflight["wants"]):
            self._inflight["sent"][molid] = False

    def _find_free_molecule_id(self) -> int:
        """
        Find and return an available molecule ID not already registered.

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

    # -------------- public API --------------

    def register_molecule(self, molecule_id: int) -> None:
        """
        Reserve a slot for a given molecule ID (client may connect later).

        Parameters
        ----------
        molecule_id : int
            Molecule ID to register.

        Raises
        ------
        ValueError
            If the molecule ID is already registered.
        """

        with self._lock:
            # If already registered, raising a ValueError
            if molecule_id in self.expected:
                raise ValueError(f"Molecule ID {molecule_id} already registered!")
            # No explicit state needed yet; client binds on INIT.
            self.expected.add(int(molecule_id))
            self.bound.setdefault(int(molecule_id), None)

    def register_molecule_return_id(self) -> int:
        """
        Reserve a slot for a molecule and return an auto-assigned ID.

        Returns
        -------
        int
            The assigned unique molecule ID.
        """

        with self._lock:
            # Find an available molecule_id
            molecule_id = self._find_free_molecule_id()
            self.register_molecule(molecule_id)
            return molecule_id

    def step_barrier(
        self, requests: Dict[int, dict], timeout: Optional[float] = None
    ) -> Dict[int, np.ndarray]:
        """
        Barrier step: dispatch fields and collect source amplitudes from all clients.

        Coordinates sending fields, waiting for results, and jointly committing the
        results once every requested molecule is ready. A frozen barrier is reused if
        a disconnect occurs mid-step.

        Parameters
        ----------
        requests : dict[int, dict]
            Mapping from molecule ID to request dict with keys:
            - ``"efield_au"`` : array-like ``(3,)`` field vector in a.u.
            - ``"meta"`` : dict, optional metadata per send.
            - ``"init"`` : dict, optional INIT payload for first bind.
        timeout : float, optional
            Maximum time (seconds) to wait for the barrier to complete. Defaults to the
            hub's ``timeout`` setting.

        Returns
        -------
        dict[int, dict]
            Mapping ``molid -> {"amp": ndarray(3,), "extra": bytes}``. Returns ``{}``
            when paused, on abort, or if the barrier is incomplete.
        """

        if self.paused:
            return {}

        deadline = time.time() + (timeout if timeout is not None else self.timeout)
        results: Dict[int, dict] = {}

        # If a barrier is already in flight, ignore new 'requests' and reuse the frozen one.
        if self._inflight is None:
            wants = set(int(k) for k in requests.keys())
            self._inflight = {
                "wants": wants,
                "efields": {
                    int(mid): np.asarray(
                        requests[mid]["efield_au"], dtype=DT_FLOAT
                    ).copy()
                    for mid in wants
                },
                "meta": {int(mid): requests[mid].get("meta", {}) for mid in wants},
                "sent": {int(mid): False for mid in wants},
            }
        wants = set(self._inflight["wants"])

        # --- hard gate: do not dispatch fields until everyone is bound ---
        with self._lock:
            if not self.all_bound(wants, require_init=True):
                init_payloads = {
                    int(mid): (
                        requests.get(mid, {}).get("init") or {"molecule_id": int(mid)}
                    )
                    for mid in wants
                }
                self._progress_binds_locked(init_payloads)
                return {}

            # Snapshot the (mid, st, efield, meta) tuples we will send to.
            # Everything below runs without self._lock held, so the accept
            # thread and background bookkeeping cannot be starved by blocking
            # send/recv syscalls.
            snapshot = []
            for mid in wants:
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

        # --- Phase A: pipeline dispatch (FIELDDATA + GETSOURCE in one send) ---
        #
        # We reuse a single 196-byte scratch bytearray for every send; only
        # the 24-byte field window at offset _SEND_FIELD_OFFSET is rewritten
        # via struct.pack_into. Clients sharing an identical field vector
        # (common in Meep runs that dedup by polarization fingerprint) are
        # grouped so we pack once per unique field instead of once per client.
        scratch = self._scratch_send
        groups: Dict[Tuple[float, float, float], list] = {}
        for mid, st, efield, meta in snapshot:
            ef = np.asarray(efield, dtype=DT_FLOAT).reshape(3)
            key = (float(ef[0]), float(ef[1]), float(ef[2]))
            groups.setdefault(key, []).append((mid, st, meta))

        for fkey, members in groups.items():
            _STRUCT_3D.pack_into(scratch, _SEND_FIELD_OFFSET, fkey[0], fkey[1], fkey[2])
            for mid, st, meta in members:
                try:
                    self._dispatch_field(st, scratch, meta)
                    self._inflight["sent"][mid] = True
                except (socket.timeout, _SocketClosed, OSError):
                    self._mark_dead(st, mid, reason="send")
                    self._pause()
                    self._reset_inflight_for(mid)
                    return {}

        # --- Phase B: collect SOURCEREADY replies via the persistent selector ---
        #
        # The selector has every bound client registered (from _bind_client_locked),
        # so we do NOT register per call. Phase B just waits for readable events
        # on the sockets belonging to mids in `pending_mids`, parses their
        # replies via the shared scratch recv buffer, and discards them.
        pending_mids: set[int] = set(int(mid) for mid in wants)
        sel = self._selector
        while pending_mids:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            # Cap the wait so we periodically re-check the deadline.
            events = sel.select(timeout=min(remaining, 1.0))
            if not events:
                continue
            for key, _mask in events:
                mid = key.data
                if mid not in pending_mids:
                    # Spurious wake (stale registration or unrelated driver);
                    # leave it for later and keep draining our own mids.
                    continue
                with self._lock:
                    st = self.bound.get(mid)
                if st is None or not st.alive:
                    pending_mids.discard(mid)
                    self._pause()
                    self._reset_inflight_for(mid)
                    return {}
                try:
                    amp, extra = self._read_source_ready(st)
                    results[mid] = {"amp": amp, "extra": extra}
                    pending_mids.discard(mid)
                except (socket.timeout, _SocketClosed, OSError):
                    self._mark_dead(st, mid, reason="recv")
                    pending_mids.discard(mid)
                    self._pause()
                    self._reset_inflight_for(mid)
                    return {}

        if pending_mids:
            # Timed out waiting for replies; keep the frozen barrier for retry.
            return {}

        # SUCCESS — clear the frozen barrier
        self._inflight = None
        return results

    def all_bound(self, molecule_ids, require_init=True):
        """
        Check if all given molecule IDs are bound (and optionally initialized).

        Parameters
        ----------
        molecule_ids : iterable of int
            Molecule IDs to check.
        require_init : bool, default: True
            Also require that clients completed INIT.

        Returns
        -------
        bool
            ``True`` if all are bound (and initialized if requested), else ``False``.
        """

        with self._lock:
            for mid in molecule_ids:
                st = self.bound.get(int(mid))
                if st is None or not st.alive:
                    return False
                if require_init and not st.initialized:
                    return False
            return True

    def wait_until_bound(self, init_payloads: dict, require_init=True, timeout=None):
        """
        Block until all requested molecule IDs are bound (and optionally initialized).

        Parameters
        ----------
        init_payloads : dict[int, dict]
            Mapping from molecule ID to INIT payload to use on bind.
        require_init : bool, default: True
            Also require that clients completed INIT.
        timeout : float or None, optional
            Maximum time to wait (seconds). Uses hub default if ``None``.

        Returns
        -------
        bool
            ``True`` if all requested IDs became bound within the time limit, else ``False``.
        """

        wanted = {int(k) for k in init_payloads.keys()}
        deadline = time.time() + (timeout if timeout is not None else self.timeout)
        payloads = {int(mid): init_payloads[mid] for mid in init_payloads.keys()}

        while True:
            if self.all_bound(wanted, require_init=require_init):
                self._resume()
                return True

            # Push INIT to any fresh unbound clients. The accept loop has already
            # enqueued them; we no longer use STATUS to probe for NEEDINIT.
            with self._lock:
                pending_ids = {mid for mid in wanted if self.bound.get(mid) is None}
                if pending_ids:
                    sub_payloads = {
                        mid: payloads.get(mid, {"molecule_id": mid})
                        for mid in pending_ids
                    }
                    self._progress_binds_locked(sub_payloads)

            if timeout is not None and time.time() > deadline:
                return False
            time.sleep(self.latency)

    def graceful_shutdown(self, reason: Optional[str] = None, wait: float = 2.0):
        """
        Politely ask all connected drivers to exit and wait briefly for ``BYE``.

        Parameters
        ----------
        reason : str or None, optional
            Optional reason to log for shutdown.
        wait : float, default: 2.0
            Seconds to wait for clean replies.
        """

        with self._lock:
            for st in list(self.clients.values()):
                if not st or not st.alive:
                    continue
                try:
                    _send_msg(st.sock, STOP)
                except Exception:
                    if self._mark_dead(st):
                        self._pause()

        deadline = time.time() + float(wait)
        while time.time() < deadline:
            time.sleep(self.latency)
            with self._lock:
                for st in list(self.clients.values()):
                    if not st or not st.alive:
                        continue
                    try:
                        # Make reads snappy during shutdown
                        st.sock.settimeout(self.latency)
                        msg = _recv_msg(st.sock)
                        if msg == BYE:
                            # Clean close on our side
                            self._mark_dead(st)
                            try:
                                st.sock.shutdown(socket.SHUT_RDWR)
                            except Exception:
                                pass
                            try:
                                st.sock.close()
                            except Exception:
                                pass
                    except (socket.timeout, _SocketClosed, OSError):
                        # Either no message yet or peer closed already; keep sweeping
                        continue

    def stop(self):
        """
        Stop accepting new connections, request clients to exit, and close sockets.

        Also removes the UNIX socket path if one was created.
        """

        # First, stop accepting new connections
        self._stop = True
        try:
            self.serversock.close()
        except Exception:
            pass

        # Then, gracefully end existing sessions
        try:
            self.graceful_shutdown(wait=max(2.0, 10 * self.latency))
        finally:
            with self._lock:
                for st in list(self.clients.values()):
                    self._unregister_sock(st.sock)
                    try:
                        st.sock.close()
                    except Exception:
                        pass
            try:
                self._selector.close()
            except Exception:
                pass

        # if unix socket, remove the path
        if self.unixsocket_path and os.path.exists(self.unixsocket_path):
            os.unlink(self.unixsocket_path)
            print(f"[SocketHub] Unlinked unix socket path {self.unixsocket_path}")
