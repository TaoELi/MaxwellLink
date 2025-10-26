"""
Socket layer for MaxwellLink drivers and servers.

This module implements a lightweight socket protocol inspired by i-PI
(https://ipi-code.org/) and provides:

- **SocketHub**: a multi-client server/poller for coordinating many driver
  connections with an FDTD engine.
- **Protocol constants**: ``STATUS``, ``READY``, ``HAVEDATA``, ``NEEDINIT``,
  ``INIT``, ...
- **EM aliases**: ``FIELDDATA``, ``GETSOURCE``, ``SOURCEREADY`` (1:1 mapping to
  ``POSDATA``/``GETFORCE``/``FORCEREADY``).
- **Low-level helpers**: ``_send_msg``, ``_recv_msg``, ``_send_array``/``_recv_array``,
  etc.
- **Exceptions**: ``_SocketClosed``.
"""

from __future__ import annotations
import socket, struct, json, time, threading, os
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple
import numpy as np

_INT32 = struct.Struct("<i")
_FLOAT64 = struct.Struct("<d")

# Fixed header width (ASCII, space-padded)
HEADER_LEN = 12
# Canonical i-PI message codes
STATUS = b"STATUS"
READY = b"READY"
HAVEDATA = b"HAVEDATA"
NEEDINIT = b"NEEDINIT"
INIT = b"INIT"
POSDATA = b"POSDATA"
GETFORCE = b"GETFORCE"
FORCEREADY = b"FORCEREADY"
STOP = b"STOP"
BYE = b"BYE"

# EM aliases for readability (same wire format)
FIELDDATA = POSDATA
GETSOURCE = GETFORCE
SOURCEREADY = FORCEREADY

# numpy dtypes on the wire (i-PI/ASE use float64 for reals, int32 for counts)
DT_FLOAT = np.float64
DT_INT = np.int32


class _SocketClosed(OSError):
    """
    Exception raised when the peer closes the socket unexpectedly.
    """

    pass


def _pad12(msg: bytes) -> bytes:
    """
    Pad a message to the fixed 12-byte ASCII header width.

    Parameters
    ----------
    msg : bytes
        Message tag to send.

    Returns
    -------
    bytes
        Space-padded header of exactly 12 bytes.

    Raises
    ------
    ValueError
        If ``msg`` exceeds the 12-byte header length.
    """

    if len(msg) > HEADER_LEN:
        raise ValueError("Header too long")
    return msg.ljust(HEADER_LEN, b" ")


def _send_msg(sock: socket.socket, msg: bytes) -> None:
    """
    Send a 12-byte ASCII header (space-padded).

    Parameters
    ----------
    sock : socket.socket
        Connected socket.
    msg : bytes
        Message tag to send (e.g., ``b"STATUS"``).
    """

    sock.sendall(_pad12(msg))


def _recvall(sock: socket.socket, n: int) -> bytes:
    """
    Read exactly ``n`` bytes from a socket.

    Parameters
    ----------
    sock : socket.socket
        Connected socket.
    n : int
        Number of bytes to read.

    Returns
    -------
    bytes
        The data read.

    Raises
    ------
    _SocketClosed
        If the peer closes the connection before all bytes are received.
    """

    """Read exactly n bytes or raise _SocketClosed."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise _SocketClosed("Peer closed")
        buf.extend(chunk)
    return bytes(buf)


def _recv_msg(sock: socket.socket) -> bytes:
    """
    Receive a 12-byte ASCII header.

    Parameters
    ----------
    sock : socket.socket
        Connected socket.

    Returns
    -------
    bytes
        The received header without trailing spaces.
    """

    """Receive 12-byte ASCII header."""
    hdr = _recvall(sock, HEADER_LEN)
    return hdr.rstrip()


def _send_array(sock: socket.socket, arr, dtype) -> None:
    """
    Send a NumPy array over a socket using a contiguous C-order memory view.

    Parameters
    ----------
    sock : socket.socket
        Connected socket.
    arr : array-like
        Array data to send.
    dtype : numpy.dtype
        Data type to cast and send as (e.g., ``np.float64``).
    """

    a = np.asarray(arr, dtype=dtype, order="C")
    sock.sendall(memoryview(a).cast("B"))


def _recv_array(sock: socket.socket, shape, dtype):
    """
    Receive a NumPy array of a given shape and dtype from a socket.

    Parameters
    ----------
    sock : socket.socket
        Connected socket.
    shape : tuple of int
        Expected array shape.
    dtype : numpy.dtype
        Expected dtype (e.g., ``np.float64``).

    Returns
    -------
    numpy.ndarray
        The received array with the specified shape and dtype.

    Raises
    ------
    _SocketClosed
        If the peer closes the connection during the transfer.
    """

    out = np.empty(shape, dtype=dtype, order="C")
    mv = memoryview(out).cast("B")
    need = mv.nbytes
    got = 0
    while got < need:
        r = sock.recv_into(mv[got:], need - got)
        if r == 0:
            raise _SocketClosed("Peer closed")
        got += r
    return out


def _send_int(sock: socket.socket, x: int) -> None:
    """
    Send a 32-bit little-endian integer.

    Parameters
    ----------
    sock : socket.socket
        Connected socket.
    x : int
        Integer value to send.
    """

    sock.sendall(_INT32.pack(int(x)))


def _recv_int(sock: socket.socket) -> int:
    """
    Receive a 32-bit little-endian integer.

    Parameters
    ----------
    sock : socket.socket
        Connected socket.

    Returns
    -------
    int
        The received integer.

    Raises
    ------
    _SocketClosed
        If the peer closes the connection during the transfer.
    """

    buf = bytearray(_INT32.size)
    mv = memoryview(buf)
    got = 0
    while got < _INT32.size:
        r = sock.recv_into(mv[got:], _INT32.size - got)
        if r == 0:
            raise _SocketClosed("Peer closed")
        got += r
    return _INT32.unpack(buf)[0]


def _send_bytes(sock: socket.socket, b: bytes) -> None:
    """
    Send a length-prefixed byte string.

    Parameters
    ----------
    sock : socket.socket
        Connected socket.
    b : bytes
        Byte string to send. The length is sent first as a 32-bit integer.
    """

    _send_int(sock, len(b))
    if len(b):
        sock.sendall(b)


def _recv_bytes(sock: socket.socket) -> bytes:
    """
    Receive a length-prefixed byte string.

    Parameters
    ----------
    sock : socket.socket
        Connected socket.

    Returns
    -------
    bytes
        The received byte string (may be empty).
    """

    n = _recv_int(sock)
    return _recvall(sock, n) if n else b""


# -------- compound payloads (i-PI compatible) --------


def _send_posdata(
    sock: socket.socket, cell_3x3_bohr, invcell_3x3_per_bohr, positions_Nx3_bohr
):
    """
    Send a POSDATA/FIELDDATA block: cell, inverse cell, natoms, positions.

    Parameters
    ----------
    sock : socket.socket
        Connected socket.
    cell_3x3_bohr : array-like, shape (3, 3)
        Lattice vectors (Bohr).
    invcell_3x3_per_bohr : array-like, shape (3, 3)
        Inverse lattice (1/Bohr).
    positions_Nx3_bohr : array-like, shape (N, 3)
        Atomic positions (Bohr).

    Notes
    -----
    For EM use, this is also used to carry field vectors via the positions payload.
    """

    assert np.asarray(cell_3x3_bohr).shape == (3, 3)
    assert np.asarray(invcell_3x3_per_bohr).shape == (3, 3)
    pos = np.asarray(positions_Nx3_bohr, dtype=DT_FLOAT)
    assert pos.ndim == 2 and pos.shape[1] == 3
    _send_msg(sock, POSDATA)
    _send_array(sock, np.asarray(cell_3x3_bohr, dtype=DT_FLOAT).T, DT_FLOAT)
    _send_array(sock, np.asarray(invcell_3x3_per_bohr, dtype=DT_FLOAT).T, DT_FLOAT)
    _send_int(sock, pos.shape[0])
    _send_array(sock, pos, DT_FLOAT)


def _recv_posdata(sock: socket.socket):
    """
    Read a POSDATA/FIELDDATA block.

    Parameters
    ----------
    sock : socket.socket
        Connected socket.

    Returns
    -------
    tuple
        ``(cell, icell, xyz)`` where:

        - ``cell`` : ``(3, 3)`` ndarray (row-major), simulation cell.
        - ``icell`` : ``(3, 3)`` ndarray (row-major), inverse cell.
        - ``xyz`` : ``(nat, 3)`` ndarray of positions (or effective field payload).
    """

    cell = _recv_array(sock, (3, 3), DT_FLOAT).T.copy()
    icell = _recv_array(sock, (3, 3), DT_FLOAT).T.copy()
    nat = _recv_int(sock)
    xyz = _recv_array(sock, (nat, 3), DT_FLOAT)
    return cell, icell, xyz


def _send_force_ready(
    sock: socket.socket,
    energy_ha: float,
    forces_Nx3_ha_per_bohr,
    virial_3x3_ha,
    more: bytes = b"",
):
    """
    Send a FORCEREADY/SOURCEREADY message with energy, forces, virial, and extras.

    Parameters
    ----------
    sock : socket.socket
        Connected socket.
    energy_ha : float
        Total energy (Hartree).
    forces_Nx3_ha_per_bohr : array-like, shape (N, 3)
        Forces (Hartree/Bohr).
    virial_3x3_ha : array-like, shape (3, 3)
        Virial tensor (Hartree).
    more : bytes, optional
        Extra payload (length-prefixed), e.g., JSON metadata.
    """

    _send_msg(sock, FORCEREADY)
    _send_array(sock, np.array([energy_ha], dtype=DT_FLOAT), DT_FLOAT)
    forces = np.asarray(forces_Nx3_ha_per_bohr, dtype=DT_FLOAT)
    assert forces.ndim == 2 and forces.shape[1] == 3
    _send_int(sock, forces.shape[0])
    _send_array(sock, forces, DT_FLOAT)
    _send_array(sock, np.asarray(virial_3x3_ha, dtype=DT_FLOAT).T, DT_FLOAT)
    _send_bytes(sock, more)


def _recv_getforce(sock: socket.socket):
    """
    Receive a FORCEREADY/SOURCEREADY payload after a GETFORCE/GETSOURCE request.

    Parameters
    ----------
    sock : socket.socket
        Connected socket.

    Returns
    -------
    tuple
        ``(energy, forces, virial, extra)`` where ``energy`` is a float, ``forces``
        is an ``(N, 3)`` ndarray, ``virial`` is a ``(3, 3)`` ndarray, and
        ``extra`` is raw bytes.
    """

    e = float(_recv_array(sock, (1,), DT_FLOAT)[0])
    nat = _recv_int(sock)
    frcs = _recv_array(sock, (nat, 3), DT_FLOAT)
    vir = _recv_array(sock, (3, 3), DT_FLOAT).T.copy()
    extra = _recv_bytes(sock)
    return e, frcs, vir, extra


# -------- convenience wrappers for EM (i-PI compatible) --------


def _pack_em_fieldata(
    sock: socket.socket, t_au: float, dt_au: float, efield_au_vec3, meta: dict
):
    """
    Send EM field data encoded as POSDATA with ``natoms=1`` and
    ``positions = [E_x, E_y, E_z]``.

    Parameters
    ----------
    sock : socket.socket
        Connected socket.
    t_au : float
        Current time (a.u.). (Informational; not transmitted in POSDATA.)
    dt_au : float
        Time step (a.u.). (Informational; not transmitted in POSDATA.)
    efield_au_vec3 : array-like, shape (3,)
        Electric field vector ``[E_x, E_y, E_z]`` in a.u.
    meta : dict
        Optional metadata carried alongside in higher-level protocols.
    """

    I = np.eye(3, dtype=DT_FLOAT)
    exyz = np.asarray(efield_au_vec3, dtype=DT_FLOAT).reshape(1, 3)
    _send_posdata(sock, I, I, exyz)
    # meta/time tags can be sent back in SOURCEREADY's extra blob if needed.


def _pack_init(sock: socket.socket, init_dict: dict):
    """
    Send an INIT handshake containing a JSON payload.

    Parameters
    ----------
    sock : socket.socket
        Connected socket.
    init_dict : dict
        Initialization dictionary (e.g., includes ``"molecule_id"``).
    """

    _send_msg(sock, INIT)
    molid = int(init_dict.get("molecule_id", 0))
    _send_int(sock, molid)
    init_bytes = json.dumps(init_dict).encode("utf-8")
    _send_bytes(sock, init_bytes)


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


def get_available_host_port() -> int:
    """
    Ask the OS for an available localhost TCP port.

    Returns
    -------
    tuple
        ``(host, port)`` pair, e.g., ``("127.0.0.1", 34567)``.
    """

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return "127.0.0.1", s.getsockname()[1]


# helper function to determine whether this processor is the MPI master using mpi4py
def am_master():
    """
    Return True if this process is the MPI master rank (rank 0), otherwise False.

    Notes
    -----
    Attempts to import ``mpi4py`` and query ``COMM_WORLD``. If unavailable,
    returns ``True`` by treating the single process as rank 0.
    """

    try:
        from mpi4py import MPI as _MPI

        _COMM = _MPI.COMM_WORLD
        _RANK = _COMM.Get_rank()
    except Exception:
        _COMM = None
        _RANK = 0
    return _RANK == 0


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

    try:
        from mpi4py import MPI as _MPI

        _COMM = _MPI.COMM_WORLD
    except Exception:
        _COMM = None

    if _COMM is not None:
        value = _COMM.bcast(value, root=0)
    return value


class SocketHub:
    """
    Socket server coordinating multiple driver connections with an FDTD engine.

    This server:

    - Accepts and tracks many driver connections.
    - Handles initialization handshakes, field dispatch, and result collection.
    - Provides a barrier-style step to send fields and receive source amplitudes
      from all registered molecules.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = 31415,
        unixsocket: Optional[str] = None,
        timeout: float = 60.0,
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
        timeout : float, default: 60.0
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

            self.serversock.listen(4096)
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

    def _dispatch_field(self, st: _ClientState, efield_au: np.ndarray, meta: dict):
        """
        Dispatch an EM field vector to a client via FIELDDATA/POSDATA.

        Parameters
        ----------
        st : _ClientState
            Target client state.
        efield_au : numpy.ndarray
            Electric field vector ``(3,)`` in a.u.
        meta : dict
            Optional metadata to attach to this send.

        Raises
        ------
        _SocketClosed or OSError
            If the client disconnects during send.
        """

        try:
            _send_msg(st.sock, FIELDDATA)
            I = np.eye(3, dtype=DT_FLOAT)
            _send_array(st.sock, I.T, DT_FLOAT)
            _send_array(st.sock, I.T, DT_FLOAT)
            _send_int(st.sock, 1)
            vec = np.asarray(efield_au, dtype=DT_FLOAT).reshape(1, 3)
            _send_array(st.sock, vec, DT_FLOAT)
            st.pending_send = True
            st.extras.update(meta or {})
        except (socket.timeout, _SocketClosed, OSError):
            st.alive = False
            if st.molecule_id >= 0 and self.bound.get(st.molecule_id) is st:
                self._log(
                    f"DISCONNECTED (send): mol {st.molecule_id} from {st.address}"
                )
                self.bound[st.molecule_id] = None
            raise

    def _query_result(self, st: _ClientState) -> Tuple[np.ndarray, bytes]:
        """
        Request a client's source amplitude and read the READY payload.

        Parameters
        ----------
        st : _ClientState
            Client state to query.

        Returns
        -------
        tuple
            ``(amp_vec3, extra_bytes)`` where ``amp_vec3`` is a ``(3,)`` ndarray and
            ``extra_bytes`` carries auxiliary data.

        Raises
        ------
        _SocketClosed or OSError
            If the client disconnects during the exchange.
        """

        try:
            _send_msg(st.sock, GETSOURCE)
            msg = _recv_msg(st.sock)
            if msg != SOURCEREADY:
                raise _SocketClosed(f"Expected {SOURCEREADY!r}, got {msg!r}")
            e, forces, vir, extra = _recv_getforce(st.sock)
            amp = np.array(forces[0], dtype=float)  # (3,)
            st.last_amp = amp
            st.pending_send = False
            return amp, extra
        except (socket.timeout, _SocketClosed, OSError):
            st.alive = False
            if st.molecule_id >= 0 and self.bound.get(st.molecule_id) is st:
                self._log(
                    f"DISCONNECTED (recv): mol {st.molecule_id} from {st.address}"
                )
                self.bound[st.molecule_id] = None
            raise

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
            self._inflight["ready"][molid] = False

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

        deadline = time.time() + (timeout or self.timeout)
        results: Dict[int, dict] = {}

        # If a barrier is already in flight, ignore new 'requests' and reuse the frozen one.
        if self._inflight is None:
            wants = set(requests.keys())
            self._inflight = {
                "wants": wants,
                "efields": {
                    mid: np.asarray(requests[mid]["efield_au"], dtype=DT_FLOAT).copy()
                    for mid in wants
                },
                "meta": {mid: requests[mid].get("meta", {}) for mid in wants},
                "sent": {mid: False for mid in wants},
                "ready": {mid: False for mid in wants},
            }
        else:
            # Reuse the frozen barrier even if the caller passed different fields
            wants = set(self._inflight["wants"])

        ready = self._inflight["ready"]

        # --- hard gate: do not dispatch fields until everyone is bound ---
        ids = set(int(k) for k in requests.keys())
        with self._lock:
            if not self.all_bound(ids, require_init=True):
                # Try to progress INIT quickly, but DO NOT send FIELDDATA yet
                for st_key, st in list(self.clients.items()):
                    if not st or not st.alive:
                        continue
                    try:
                        _send_msg(st.sock, STATUS)
                        reply = _recv_msg(st.sock)
                    except (socket.timeout, _SocketClosed, OSError):
                        st.alive = False
                        if st.molecule_id >= 0 and self.bound.get(st.molecule_id) is st:
                            self._log(
                                f"DISCONNECTED: mol {st.molecule_id} from {st.address}"
                            )
                            self.bound[st.molecule_id] = None
                            self._pause()
                            # NEW: make the frozen barrier re-send the old field to this molid
                            self._reset_inflight_for(st.molecule_id)
                        continue
                    if reply == NEEDINIT:
                        for mid in ids:
                            if self.bound.get(mid) is None:
                                init_payload = requests.get(mid, {}).get(
                                    "init", {"molecule_id": mid}
                                )
                                self._bind_client_locked(
                                    st, int(mid), init_payload, st_key
                                )
                                break
                return {}  # nothing dispatched; drivers remain idle

        # --- normal step_barrier continues below ---
        aborted = False
        with self._lock:
            # 1. poll and init/dispatch
            for st_key, st in list(self.clients.items()):
                try:
                    _send_msg(st.sock, STATUS)
                    reply = _recv_msg(st.sock)
                except (socket.timeout, _SocketClosed, OSError):
                    st.alive = False
                    if st.molecule_id >= 0 and self.bound.get(st.molecule_id) is st:
                        self._log(
                            f"DISCONNECTED: mol {st.molecule_id} from {st.address}"
                        )
                        self.bound[st.molecule_id] = None
                        self._pause()
                        # NEW: make the frozen barrier re-send the old field to this molid
                        self._reset_inflight_for(st.molecule_id)
                    aborted = True
                    continue
                if reply == NEEDINIT:
                    # Bind this client to the first UNBOUND molecule id present in requests
                    # Preserve the requests' order (Python dicts keep insertion order).
                    chosen = None
                    for mid in requests.keys():
                        if self.bound.get(mid) is None:
                            chosen = mid
                            break
                    # Fallback: allow a rebind if this client was previously bound and crashed/reconnected
                    if (
                        chosen is None
                        and st.molecule_id >= 0
                        and self.bound.get(st.molecule_id) is None
                    ):
                        chosen = st.molecule_id
                    if chosen is None:
                        # Nothing to serve right now; keep the client idle
                        continue
                    init_payload = requests.get(chosen, {}).get(
                        "init", {"molecule_id": chosen}
                    )
                    self._bind_client_locked(st, int(chosen), init_payload, st_key)

                elif reply == READY:
                    molid = st.molecule_id
                    if (
                        molid in self._inflight["wants"]
                        and not st.pending_send
                        and not self._inflight["sent"][molid]
                    ):
                        evec = self._inflight["efields"][molid]
                        meta = self._inflight["meta"][molid]
                        try:
                            self._dispatch_field(st, evec, meta)
                            self._inflight["sent"][molid] = True
                        except Exception:
                            aborted = True
                            break

                elif reply == HAVEDATA:
                    if st.molecule_id in ready:
                        ready[st.molecule_id] = True
                    continue

            # 2. second pass: wait for all pending molecules to finish (barrier)
            wants = set(self._inflight["wants"])

        if aborted:
            # abort this barrier; caller will enter the pause path
            return {}

        while time.time() < deadline and (wants - set(results.keys())):
            time.sleep(self.latency)

            with self._lock:
                # Always sweep over ALL clients, not just molecule-ids in 'wants'
                for st_key, st in list(self.clients.items()):
                    if not st or not st.alive:
                        continue
                    try:
                        _send_msg(st.sock, STATUS)
                        reply = _recv_msg(st.sock)
                    except (socket.timeout, _SocketClosed, OSError):
                        st.alive = False
                        if st.molecule_id >= 0 and self.bound.get(st.molecule_id) is st:
                            self._log(
                                f"DISCONNECTED: mol {st.molecule_id} from {st.address}"
                            )
                            self.bound[st.molecule_id] = None
                            self._pause()
                            # NEW: make the frozen barrier re-send the old field to this molid
                            self._reset_inflight_for(st.molecule_id)
                        continue

                    if reply == NEEDINIT:
                        # Bind this client to the first UNBOUND molecule id present in requests
                        # Preserve the requests' order (Python dicts keep insertion order).
                        chosen = None
                        for mid in requests.keys():
                            if self.bound.get(mid) is None:
                                chosen = mid
                                break
                        # Fallback: allow a rebind if this client was previously bound and crashed/reconnected
                        if (
                            chosen is None
                            and st.molecule_id >= 0
                            and self.bound.get(st.molecule_id) is None
                        ):
                            chosen = st.molecule_id
                        if chosen is None:
                            # Nothing to serve right now; keep the client idle
                            continue
                        init_payload = requests.get(chosen, {}).get(
                            "init", {"molecule_id": chosen}
                        )
                        self._bind_client_locked(st, int(chosen), init_payload, st_key)
                        continue  # next client

                    if reply == READY:
                        molid = st.molecule_id
                        if (
                            molid in self._inflight["wants"]
                            and not st.pending_send
                            and not self._inflight["sent"][molid]
                        ):
                            evec = self._inflight["efields"][molid]
                            meta = self._inflight["meta"][molid]
                            self._dispatch_field(st, evec, meta)
                        continue

                    if reply == HAVEDATA:
                        if st.molecule_id in ready:
                            ready[st.molecule_id] = True
                        continue

                    # Some clients may erroneously send STATUS; ignore gracefully
                    if reply == STATUS:
                        continue

                # exit condition: filled everything we wanted
                if all(ready.get(mid, False) for mid in wants):
                    break

        # Abort if not everyone is ready (e.g., disconnect); keep the frozen barrier.
        if not all(
            self._inflight["ready"].get(mid, False) for mid in self._inflight["wants"]
        ):
            return {}

        # Phase C: commit all together (send GETSOURCE to everyone now)
        for mid in self._inflight["wants"]:
            st = self.clients.get(mid)
            if not st or not st.alive:
                return {}  # keep barrier for retry
            try:
                amp, extra = self._query_result(st)
                results[mid] = {"amp": amp, "extra": extra}
            except (socket.timeout, _SocketClosed, OSError):
                return {}  # keep barrier for retry

        # SUCCESS — clear the frozen barrier
        self._inflight = None

        # Fallback: any missing results -> use last known amplitude (if any),
        # We turn off this fallback for now to avoid silent errors.
        if False:
            with self._lock:
                for mid in wants - set(results.keys()):
                    st = self.clients.get(mid)
                    if st and st.last_amp is not None:
                        results[mid] = {"amp": st.last_amp.copy(), "extra": b""}
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

        while True:
            if self.all_bound(wanted, require_init=require_init):
                self._resume()
                return True

            # Progress the INIT handshakes without dispatching any field steps
            with self._lock:
                # Only touch clients for molecule-ids that are still unbound
                pending_ids = {mid for mid in wanted if self.bound.get(mid) is None}
                if not pending_ids:
                    # (shouldn't happen because of the all_bound check, but be safe)
                    time.sleep(self.latency)
                    continue

                for st_key, st in list(self.clients.items()):
                    if not st or not st.alive:
                        continue
                    # Only ping clients that are currently unbound (either brand-new or reconnecting)
                    if st.molecule_id >= 0 and st.molecule_id not in pending_ids:
                        continue
                    try:
                        _send_msg(st.sock, STATUS)
                        reply = _recv_msg(st.sock)
                    except (socket.timeout, _SocketClosed, OSError):
                        st.alive = False
                        # free a binding if this was a re-connect case
                        if st.molecule_id >= 0 and self.bound.get(st.molecule_id) is st:
                            self._log(
                                f"DISCONNECTED: mol {st.molecule_id} from {st.address}"
                            )
                            self.bound[st.molecule_id] = None
                            self._pause()
                            # NEW: make the frozen barrier re-send the old field to this molid
                            self._reset_inflight_for(st.molecule_id)
                        continue

                    if reply == NEEDINIT:
                        # choose an unclaimed id from 'wanted'
                        chosen = None
                        for mid in pending_ids:
                            if self.bound.get(mid) is None:
                                chosen = mid
                                break
                        if chosen is not None:
                            self._bind_client_locked(
                                st, int(chosen), init_payloads[int(chosen)], st_key
                            )

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
                    st.alive = False
                    if st.molecule_id >= 0 and self.bound.get(st.molecule_id) is st:
                        self._log(
                            f"DISCONNECTED: mol {st.molecule_id} from {st.address}"
                        )
                        self.bound[st.molecule_id] = None
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
                            st.alive = False
                            if (
                                st.molecule_id >= 0
                                and self.bound.get(st.molecule_id) is st
                            ):
                                self._log(
                                    f"DISCONNECTED: mol {st.molecule_id} from {st.address}"
                                )
                                self.bound[st.molecule_id] = None
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
                    try:
                        st.sock.close()
                    except Exception:
                        pass

        # if unix socket, remove the path
        if self.unixsocket_path and os.path.exists(self.unixsocket_path):
            os.unlink(self.unixsocket_path)
            print(f"[SocketHub] Unlinked unix socket path {self.unixsocket_path}")
