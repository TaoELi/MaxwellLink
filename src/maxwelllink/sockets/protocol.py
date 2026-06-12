# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
Wire protocol for every MaxwellLink socket connection.

This module is the single home of the byte formats exchanged between the
socket hubs and their three client families:

- ``mxl_driver`` Python clients and the LAMMPS ``fix mxl`` client speak the
  i-PI-style header protocol (``STATUS``/``INIT``/``POSDATA``/``GETFORCE``/...),
  including the fixed-layout fast path used by ``SocketHub.step_barrier``.
- ``mxl_bridge`` aggregate nodes speak the AGG frame protocol
  (``AGGHELLO``/``AGGINIT``/``AGGSTEP``/``AGGRESULT``).
- Meep's C-level ``MXLSocketSusceptibility`` client reuses the AGG step/result
  frames after its ``MXLINIT`` handshake (see ``_meep_hub_base.py``).

Because the counterparts live outside this package (LAMMPS C++ driver, Meep's
``src/susceptibility.cpp``), every byte layout in this file is frozen: change
nothing here without bumping the protocol versions and updating those clients.
The layouts are pinned by golden-bytes tests in ``tests/test_sockets``.

This module intentionally imports nothing from the rest of MaxwellLink so any
driver- or hub-side code can depend on it without import cycles.
"""

from __future__ import annotations

import json
import socket
import struct
import time
from typing import Dict, Mapping, Optional

import numpy as np

# ======================================================================
# Protocol constants and wire dtypes
# ======================================================================

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


# ======================================================================
# Low-level wire helpers (headers, ints, arrays, byte strings)
# ======================================================================


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


# ======================================================================
# Compound payload codecs (i-PI compatible)
# ======================================================================


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


# ======================================================================
# EM convenience wrappers (i-PI compatible)
# ======================================================================


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


# ======================================================================
# Fast-path constants and send/recv layout
# ======================================================================

_FIELDDATA_HDR = _pad12(FIELDDATA)
_GETSOURCE_HDR = _pad12(GETSOURCE)
_EYE3_BYTES = bytes(
    memoryview(np.ascontiguousarray(np.eye(3, dtype=DT_FLOAT))).cast("B")
)
_NAT1_BYTES = _INT32.pack(1)


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


# ---------------------------------------------------------------------------
# Aggregate wire protocol
# ---------------------------------------------------------------------------
# All aggregate frames begin with a fixed 12-byte header (one of the banners
# below, right-padded with spaces). HELLO/INIT carry a JSON payload; STEP and
# RESULT use the packed binary layouts described next to their codecs. The byte
# layout is shared by the hub and bridge processes and must stay stable.

AGGHELLO = b"AGGHELLO"
AGGINIT = b"AGGINIT"
AGGREADY = b"AGGREADY"
AGGSTEP = b"AGGSTEP"
AGGRESULT = b"AGGRESULT"
AGGREGATION_INFO_VERSION = 1

_INT32_LEN = _INT32.size  # 4
_FIELD_LEN = _STRUCT_3D.size  # 24: one packed efield/amp vector (3 doubles)

_AGG_HEADER_LEN = 12
_AGGSTEP_HDR = AGGSTEP.ljust(_AGG_HEADER_LEN, b" ")
_AGGRESULT_HDR = AGGRESULT.ljust(_AGG_HEADER_LEN, b" ")

# AGGSTEP head: header + nreq + nuniq; each member record: molecule_id + field_idx.
_AGGSTEP_HEAD_LEN = _AGG_HEADER_LEN + _INT32_LEN + _INT32_LEN
_AGGSTEP_RECORD_LEN = _INT32_LEN + _INT32_LEN
_STEP_FIELDIDX_OFF = _INT32_LEN  # field_idx follows molecule_id within a record

# AGGRESULT head: header + nresp; each record: molecule_id + amp(vec3) + extra_len.
_AGGRESULT_HEAD_LEN = _AGG_HEADER_LEN + _INT32_LEN
_AGGRESULT_RECORD_LEN = _INT32_LEN + _FIELD_LEN + _INT32_LEN
_RESULT_AMP_OFF = _INT32_LEN  # amp follows molecule_id
_RESULT_EXTRALEN_OFF = _INT32_LEN + _FIELD_LEN  # extra_len follows amp


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------


def _json_dumps_bytes(payload: Mapping) -> bytes:
    """
    Encode a mapping into compact, sorted UTF-8 JSON bytes.

    Parameters
    ----------
    payload : Mapping
        JSON-serializable mapping to encode.

    Returns
    -------
    bytes
        Compact UTF-8 JSON encoding with sorted keys and no extra whitespace.

    Notes
    -----
    Keys are sorted so that the same payload always produces identical bytes,
    which keeps the HELLO/INIT framing deterministic across hub and bridge.
    """

    return json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _json_loads_bytes(payload: bytes) -> dict:
    """
    Decode a UTF-8 JSON payload, defaulting empty content to ``{}``.

    Parameters
    ----------
    payload : bytes
        UTF-8 encoded JSON bytes. An empty buffer is treated as an empty
        object.

    Returns
    -------
    dict
        The decoded JSON object.
    """

    return json.loads(payload.decode("utf-8") or "{}")


def _recv_msg_with_timeout(sock: socket.socket, timeout: float) -> bytes:
    """
    Receive one 12-byte MaxwellLink header using a temporary timeout.

    This is used while discovering fresh bridge clients so the hub can poll
    for their HELLO payload without blocking the whole EM-side wait loop.

    Parameters
    ----------
    sock : socket.socket
        Socket to read one header banner from.
    timeout : float
        Temporary receive timeout (seconds) applied for the duration of the
        read; the socket's previous timeout is restored on return.

    Returns
    -------
    bytes
        The right-stripped 12-byte header banner.

    Raises
    ------
    socket.timeout
        If no header arrives within ``timeout`` seconds.
    _SocketClosed
        If the peer closes the connection mid-read.
    """

    old_timeout = sock.gettimeout()
    try:
        sock.settimeout(timeout)
        return _recv_msg(sock)
    finally:
        sock.settimeout(old_timeout)


# Selector (un)register raises these when a socket is unknown or already closed;
# they are always safe to ignore on a best-effort detach.
_SELECTOR_ERRORS = (KeyError, ValueError, OSError)


def _close_socket(sock: Optional[socket.socket]) -> None:
    """
    Close a socket, ignoring the error if it is already gone.

    Parameters
    ----------
    sock : socket.socket or None
        Socket to close. ``None`` is accepted and ignored so callers can close
        optional/best-effort handles without guarding first.
    """

    if sock is None:
        return
    try:
        sock.close()
    except OSError:
        pass


def _recv_exact_into(sock: socket.socket, buf, nbytes: int) -> None:
    """
    Read exactly ``nbytes`` into the start of ``buf``.

    Parameters
    ----------
    sock : socket.socket
        Socket to read from.
    buf : bytearray or writable buffer
        Destination buffer; the first ``nbytes`` bytes are overwritten.
    nbytes : int
        Number of bytes to read. The call loops until exactly this many bytes
        have been received.

    Raises
    ------
    _SocketClosed
        If the peer closes the connection before ``nbytes`` bytes arrive.
    """

    mv = memoryview(buf)
    got = 0
    while got < nbytes:
        nrecv = sock.recv_into(mv[got:nbytes], nbytes - got)
        if nrecv == 0:
            raise _SocketClosed("Peer closed")
        got += nrecv


def _expect_header(buf, expected: bytes) -> None:
    """
    Validate the 12-byte banner at the start of ``buf``.

    Parameters
    ----------
    buf : bytes-like
        Buffer whose first 12 bytes hold a right-padded header banner.
    expected : bytes
        The banner the caller requires (compared after trailing whitespace is
        stripped).

    Raises
    ------
    RuntimeError
        If the decoded banner does not match ``expected``.
    """

    got = bytes(memoryview(buf)[:_AGG_HEADER_LEN]).rstrip()
    if got != expected:
        raise RuntimeError(f"Expected {expected!r}, got {got!r}")


def _connect_tcp_with_retry(
    address: str,
    port: int,
    timeout: float,
    *,
    label: str = "MaxwellLink server",
) -> socket.socket:
    """
    Connect to a TCP server with bounded retries.

    Parameters
    ----------
    address : str
        Host name or IP address of the upstream server.
    port : int
        TCP port of the upstream server.
    timeout : float
        Total budget (seconds) for establishing the connection. The call
        retries with exponential backoff until this deadline; the returned
        socket is left configured with this value as its operation timeout.
    label : str, default: "MaxwellLink server"
        Human-readable endpoint label used only in timeout diagnostics.

    Returns
    -------
    socket.socket
        A connected TCP socket with ``TCP_NODELAY`` and ``SO_KEEPALIVE`` set
        where supported.

    Raises
    ------
    TimeoutError
        If no connection can be established before the deadline; the last
        underlying connection error is chained as the cause.
    """

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
            _close_socket(sock)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(delay, remaining))
            delay = min(delay * 1.5, 1.0)

    raise TimeoutError(
        f"Timed out connecting to {label} at {(address, port)!r} "
        f"after {timeout} seconds"
    ) from last_error


# ---------------------------------------------------------------------------
# HELLO / INIT framing (JSON payloads)
# ---------------------------------------------------------------------------


def _send_aggregate_hello(sock: socket.socket, *, group_id: str) -> None:
    """
    Send the bridge HELLO banner used by the aggregate protocol.

    Parameters
    ----------
    sock : socket.socket
        Upstream connection to the :class:`AggregatedSocketHub`.
    group_id : str
        Aggregate group identifier this bridge serves; sent in the JSON
        payload so the hub can match the connection to a configured group.
    """

    _send_msg(sock, AGGHELLO)
    _send_bytes(sock, _json_dumps_bytes({"group_id": str(group_id), "version": 1}))


def _send_aggregate_init(
    sock: socket.socket,
    *,
    group_id: str,
    init_payloads: Mapping[int, dict],
) -> None:
    """
    Send group membership plus per-molecule INIT payloads to a bridge.

    Parameters
    ----------
    sock : socket.socket
        Upstream connection to the bridge being initialized.
    group_id : str
        Aggregate group identifier the payload applies to.
    init_payloads : Mapping[int, dict]
        Mapping from molecule ID to its INIT payload. Each payload is copied
        and stamped with its own ``"molecule_id"`` before transmission.
    """

    payload = {
        "group_id": str(group_id),
        "molecule_ids": [int(mid) for mid in init_payloads.keys()],
        "init_payloads": {
            str(int(mid)): {**dict(init_payloads[mid]), "molecule_id": int(mid)}
            for mid in init_payloads.keys()
        },
    }
    _send_msg(sock, AGGINIT)
    _send_bytes(sock, _json_dumps_bytes(payload))


# ---------------------------------------------------------------------------
# STEP / RESULT codecs (packed binary frames)
# ---------------------------------------------------------------------------


class _FrameCodec:
    """
    Base class holding reusable scratch buffers for the packed frame codecs.

    A codec instance is used in only one direction (the hub sends and the
    bridge receives, or vice versa), so the named scratch buffers requested via
    :meth:`_scratch` are reused across calls to avoid per-step allocations.
    """

    def __init__(self) -> None:
        self._scratch_buffers: Dict[str, bytearray] = {}

    def _scratch(self, name: str, size: int) -> bytearray:
        """
        Return a reusable named buffer holding at least ``size`` bytes.

        Parameters
        ----------
        name : str
            Logical name of the scratch slot (e.g. ``"send"``, ``"head"``).
            Each name maps to one persistent buffer reused across calls.
        size : int
            Minimum capacity required. The buffer is grown (reallocated) only
            when the existing one is too small.

        Returns
        -------
        bytearray
            A buffer of length at least ``size``; its leading bytes may hold
            stale data and must be overwritten by the caller.
        """

        buf = self._scratch_buffers.get(name)
        if buf is None or len(buf) < size:
            buf = bytearray(size)
            self._scratch_buffers[name] = buf
        return buf


class _StepCodec(_FrameCodec):
    """
    Encoder/decoder for the AGGSTEP fan-out frame.

    The hub encodes (``send``) and the bridge decodes (``recv``).

    Frame layout::

        [ header(12) | nreq(i32) | nuniq(i32) ]
        [ nuniq * field(3 doubles)            ]
        [ nreq  * (molecule_id(i32), field_idx(i32)) ]

    Repeated efields are de-duplicated so molecules sharing a field reference
    the same packed vector by index.
    """

    def send(
        self, sock: socket.socket, requests: Mapping[int, Mapping[str, np.ndarray]]
    ) -> None:
        """
        Pack and send one grouped fan-out step as a single frame.

        Parameters
        ----------
        sock : socket.socket
            Bridge connection to write the frame to.
        requests : Mapping[int, Mapping[str, np.ndarray]]
            Mapping from molecule ID to a request dict carrying an
            ``"efield_au"`` array-like ``(3,)`` field vector in a.u.

        Notes
        -----
        Identical field vectors are de-duplicated: each unique field is packed
        once and molecules sharing it reference it by index, which shrinks the
        frame when many molecules see the same field.
        """

        unique_fields: list[tuple[float, float, float]] = []
        field_to_idx: dict[tuple[float, float, float], int] = {}
        members: list[tuple[int, int]] = []
        for mid, payload in requests.items():
            field = np.asarray(payload["efield_au"], dtype=DT_FLOAT).reshape(3)
            key = (float(field[0]), float(field[1]), float(field[2]))
            field_idx = field_to_idx.get(key)
            if field_idx is None:
                field_idx = len(unique_fields)
                unique_fields.append(key)
                field_to_idx[key] = field_idx
            members.append((int(mid), field_idx))

        frame_len = (
            _AGGSTEP_HEAD_LEN
            + _FIELD_LEN * len(unique_fields)
            + _AGGSTEP_RECORD_LEN * len(members)
        )
        buf = self._scratch("send", frame_len)
        buf[:_AGG_HEADER_LEN] = _AGGSTEP_HDR
        _INT32.pack_into(buf, _AGG_HEADER_LEN, len(members))
        _INT32.pack_into(buf, _AGG_HEADER_LEN + _INT32_LEN, len(unique_fields))

        offset = _AGGSTEP_HEAD_LEN
        for fx, fy, fz in unique_fields:
            _STRUCT_3D.pack_into(buf, offset, fx, fy, fz)
            offset += _FIELD_LEN
        for mid, field_idx in members:
            _INT32.pack_into(buf, offset, mid)
            _INT32.pack_into(buf, offset + _STEP_FIELDIDX_OFF, field_idx)
            offset += _AGGSTEP_RECORD_LEN

        sock.sendall(memoryview(buf)[:frame_len])

    def recv(
        self, sock: socket.socket, *, header_already_read: bool = False
    ) -> Dict[int, np.ndarray]:
        """
        Receive one grouped fan-out step.

        Parameters
        ----------
        sock : socket.socket
            Bridge connection to read the frame from.
        header_already_read : bool, default: False
            Set when the caller already consumed the 12-byte banner (e.g. the
            bridge's main dispatch loop) so only the rest of the header is read
            here.

        Returns
        -------
        dict[int, np.ndarray]
            Mapping from molecule ID to its ``(3,)`` field vector. Molecules
            that shared a field upstream alias the SAME decoded array, so
            callers must copy before mutating (``step_barrier`` copies fields
            into its frozen barrier; tests pin this aliasing contract).

        Raises
        ------
        RuntimeError
            If the decoded banner is not ``AGGSTEP``.
        _SocketClosed
            If the peer closes the connection mid-frame.
        """

        head = self._scratch("head", _AGGSTEP_HEAD_LEN)
        if header_already_read:
            head[:_AGG_HEADER_LEN] = _AGGSTEP_HDR
            _recv_exact_into(
                sock,
                memoryview(head)[_AGG_HEADER_LEN:],
                _AGGSTEP_HEAD_LEN - _AGG_HEADER_LEN,
            )
        else:
            _recv_exact_into(sock, head, _AGGSTEP_HEAD_LEN)
        _expect_header(head, AGGSTEP)

        nreq = _INT32.unpack_from(head, _AGG_HEADER_LEN)[0]
        nuniq = _INT32.unpack_from(head, _AGG_HEADER_LEN + _INT32_LEN)[0]
        body_len = _FIELD_LEN * nuniq + _AGGSTEP_RECORD_LEN * nreq
        body = self._scratch("body", body_len)
        if body_len:
            _recv_exact_into(sock, body, body_len)

        offset = 0
        fields: list[np.ndarray] = []
        for _ in range(nuniq):
            fx, fy, fz = _STRUCT_3D.unpack_from(body, offset)
            fields.append(np.array((fx, fy, fz), dtype=float))
            offset += _FIELD_LEN

        requests: Dict[int, np.ndarray] = {}
        for _ in range(nreq):
            mid = int(_INT32.unpack_from(body, offset)[0])
            field_idx = _INT32.unpack_from(body, offset + _STEP_FIELDIDX_OFF)[0]
            offset += _AGGSTEP_RECORD_LEN
            requests[mid] = fields[field_idx]
        return requests


class _ResultCodec(_FrameCodec):
    """
    Encoder/decoder for the AGGRESULT reply frame.

    The bridge encodes (``send``) and the hub decodes (``recv``).

    Frame layout::

        [ header(12) | nresp(i32)                                   ]
        [ nresp * (molecule_id(i32), amp(3 doubles), extra_len(i32)) ]
        [ concatenated extra payload bytes                          ]
    """

    def send(
        self, sock: socket.socket, responses: Mapping[int, Mapping[str, object]]
    ) -> None:
        """
        Pack and send grouped molecule responses as a single frame.

        Parameters
        ----------
        sock : socket.socket
            Hub connection to write the frame to.
        responses : Mapping[int, Mapping[str, object]]
            Mapping from molecule ID to a response dict with keys:
            - ``"amp"`` : array-like ``(3,)`` source amplitude vector.
            - ``"extra"`` : bytes or str, optional opaque per-molecule payload
              (``str`` is UTF-8 encoded; defaults to empty).

        Notes
        -----
        The fixed-size records (id, amplitude, extra length) are packed first
        and the variable-length ``extra`` blobs are concatenated afterwards, so
        the receiver can size its reads from the record table alone.
        """

        packed: list[tuple[int, tuple[float, float, float], bytes]] = []
        total_extra = 0
        for mid, payload in responses.items():
            amp = np.asarray(payload["amp"], dtype=DT_FLOAT).reshape(3)
            extra = payload.get("extra", b"")
            if isinstance(extra, str):
                extra = extra.encode("utf-8")
            extra = bytes(extra)
            packed.append(
                (int(mid), (float(amp[0]), float(amp[1]), float(amp[2])), extra)
            )
            total_extra += len(extra)

        fixed_len = _AGGRESULT_HEAD_LEN + _AGGRESULT_RECORD_LEN * len(packed)
        frame_len = fixed_len + total_extra
        buf = self._scratch("send", frame_len)
        buf[:_AGG_HEADER_LEN] = _AGGRESULT_HDR
        _INT32.pack_into(buf, _AGG_HEADER_LEN, len(packed))

        offset = _AGGRESULT_HEAD_LEN
        extra_offset = fixed_len
        for mid, amp, extra in packed:
            _INT32.pack_into(buf, offset, mid)
            _STRUCT_3D.pack_into(buf, offset + _RESULT_AMP_OFF, amp[0], amp[1], amp[2])
            _INT32.pack_into(buf, offset + _RESULT_EXTRALEN_OFF, len(extra))
            offset += _AGGRESULT_RECORD_LEN
            if extra:
                buf[extra_offset : extra_offset + len(extra)] = extra
                extra_offset += len(extra)

        sock.sendall(memoryview(buf)[:frame_len])

    def recv(self, sock: socket.socket) -> Dict[int, dict]:
        """
        Receive grouped molecule responses from a bridge.

        Parameters
        ----------
        sock : socket.socket
            Bridge connection to read the frame from.

        Returns
        -------
        dict[int, dict]
            Mapping from molecule ID to ``{"amp": ndarray(3,), "extra": bytes}``.

        Raises
        ------
        RuntimeError
            If the decoded banner is not ``AGGRESULT``.
        _SocketClosed
            If the peer closes the connection mid-frame.
        """

        head = self._scratch("head", _AGGRESULT_HEAD_LEN)
        _recv_exact_into(sock, head, _AGGRESULT_HEAD_LEN)
        _expect_header(head, AGGRESULT)

        nresp = _INT32.unpack_from(head, _AGG_HEADER_LEN)[0]
        fixed_len = _AGGRESULT_RECORD_LEN * nresp
        fixed = self._scratch("fixed", fixed_len)
        if fixed_len:
            _recv_exact_into(sock, fixed, fixed_len)

        offset = 0
        meta: list[tuple[int, tuple[float, float, float], int]] = []
        total_extra = 0
        for _ in range(nresp):
            mid = int(_INT32.unpack_from(fixed, offset)[0])
            amp = _STRUCT_3D.unpack_from(fixed, offset + _RESULT_AMP_OFF)
            extra_len = _INT32.unpack_from(fixed, offset + _RESULT_EXTRALEN_OFF)[0]
            meta.append((mid, amp, extra_len))
            total_extra += extra_len
            offset += _AGGRESULT_RECORD_LEN

        extras = self._scratch("extras", total_extra)
        if total_extra:
            _recv_exact_into(sock, extras, total_extra)

        responses: Dict[int, dict] = {}
        extra_offset = 0
        for mid, amp, extra_len in meta:
            extra = (
                bytes(memoryview(extras)[extra_offset : extra_offset + extra_len])
                if extra_len
                else b""
            )
            responses[mid] = {"amp": np.array(amp, dtype=float), "extra": extra}
            extra_offset += extra_len
        return responses

