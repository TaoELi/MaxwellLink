# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

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
import socket, struct, json, time, threading, os, selectors
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


# --------- precomputed constants for the fast path ---------

_FIELDDATA_HDR = _pad12(FIELDDATA)
_GETSOURCE_HDR = _pad12(GETSOURCE)
_EYE3_BYTES = bytes(
    memoryview(np.ascontiguousarray(np.eye(3, dtype=DT_FLOAT))).cast("B")
)
_NAT1_BYTES = _INT32.pack(1)


def _build_step_request(efield_au) -> bytes:
    """
    Build a single coalesced request blob that tells a driver to advance one step.

    The blob concatenates a FIELDDATA/POSDATA message carrying the electric
    field vector followed immediately by a GETSOURCE/GETFORCE request. Sending
    both headers in one ``sendall`` avoids a round-trip: the driver consumes
    FIELDDATA, computes, then sees GETSOURCE waiting in its recv buffer and
    replies with SOURCEREADY without any intervening STATUS handshake.

    Parameters
    ----------
    efield_au : array-like, shape (3,)
        Electric field vector in atomic units.

    Returns
    -------
    bytes
        A single buffer suitable for one ``sock.sendall`` call.
    """

    vec = np.ascontiguousarray(np.asarray(efield_au, dtype=DT_FLOAT).reshape(3))
    vec_bytes = bytes(memoryview(vec).cast("B"))
    return b"".join(
        (
            _FIELDDATA_HDR,
            _EYE3_BYTES,  # cell (identity; unused by EM drivers)
            _EYE3_BYTES,  # invcell (identity; unused by EM drivers)
            _NAT1_BYTES,  # nat = 1
            vec_bytes,  # positions payload = [Ex, Ey, Ez]
            _GETSOURCE_HDR,
        )
    )


# --------- fast-path send/recv layout ---------
#
# Send blob (196 bytes; written in place into a reusable bytearray):
#   [0  :12 ] FIELDDATA header
#   [12 :84 ] cell (3x3 float64, identity)
#   [84 :156] invcell (3x3 float64, identity)
#   [156:160] nat (int32 = 1)
#   [160:184] field vector (3 x float64)     <-- only this window changes
#   [184:196] GETSOURCE header
#
# Fixed reply (124 bytes; read into a reusable bytearray via recv_into):
#   [0  :12 ] SOURCEREADY header
#   [12 :20 ] energy (float64)
#   [20 :24 ] nat (int32, expected = 1)
#   [24 :48 ] forces (1 x 3 float64)
#   [48 :120] virial (3x3 float64)
#   [120:124] extra_len (int32)
#   (followed by `extra_len` trailing bytes of JSON/etc., read separately)

_SEND_FIELD_OFFSET = 12 + 72 + 72 + 4  # = 160
_SEND_TOTAL_LEN = _SEND_FIELD_OFFSET + 24 + 12  # = 196
_SEND_TEMPLATE = (
    _FIELDDATA_HDR
    + _EYE3_BYTES
    + _EYE3_BYTES
    + _NAT1_BYTES
    + b"\x00" * 24
    + _GETSOURCE_HDR
)
assert len(_SEND_TEMPLATE) == _SEND_TOTAL_LEN

_REPLY_FIXED_LEN = 12 + 8 + 4 + 24 + 72 + 4  # = 124
_REPLY_NAT_OFFSET = 12 + 8  # = 20
_REPLY_FORCES_OFFSET = 12 + 8 + 4  # = 24
_REPLY_EXTRA_LEN_OFFSET = 12 + 8 + 4 + 24 + 72  # = 120

_STRUCT_3D = struct.Struct("<3d")
_STRUCT_I = struct.Struct("<i")


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

    def _mark_dead(self, st: _ClientState, molid: Optional[int], reason: str) -> None:
        """
        Mark a client dead, unregister it from the selector, and clear binding.

        This centralizes the bookkeeping that used to be duplicated across
        every ``except`` arm of the old STATUS-based sweep. It is safe to call
        from any phase and takes ``self._lock`` only briefly for the ``bound``
        mutation so blocking I/O never runs while the lock is held.

        Parameters
        ----------
        st : _ClientState
            The client whose socket failed.
        molid : int or None
            The molecule id the client was bound to (or ``None`` if unknown).
        reason : str
            Short tag used in the disconnect log line (e.g. ``"send"``, ``"recv"``).
        """

        st.alive = False
        self._unregister_sock(st.sock)
        if molid is None:
            molid = st.molecule_id
        if molid is not None and molid >= 0:
            with self._lock:
                if self.bound.get(molid) is st:
                    self._log(f"DISCONNECTED ({reason}): mol {molid} from {st.address}")
                    self.bound[molid] = None

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
                    st.alive = False
                    self._unregister_sock(st.sock)
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
                            self._unregister_sock(st.sock)
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
