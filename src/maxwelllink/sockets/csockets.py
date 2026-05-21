# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
Native-backed socket helpers for MaxwellLink.

This module intentionally mirrors :mod:`maxwelllink.sockets.sockets` so test
scripts can opt in with::

    from maxwelllink.sockets.csockets import SocketHub

The public API names match the Python module. The native extension accelerates
the hot protocol operations while the Python wrapper preserves the existing
SocketHub state machine and reconnect semantics.
"""

from __future__ import annotations

import json
from typing import Tuple

import numpy as np

from . import sockets as _py
from .sockets import (  # noqa: F401
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
    _SocketClosed,
    am_master,
    get_available_host_port,
    mpi_bcast_from_master,
)

try:
    from . import _csockets as _native
except Exception:  # pragma: no cover - exercised only when extension is absent
    _native = None


def native_available() -> bool:
    """Return whether the compiled native socket helper extension is available."""

    return _native is not None


def _pad12(msg: bytes) -> bytes:
    """Pad a message to the fixed 12-byte ASCII header width."""

    return _py._pad12(msg)


def _send_msg(sock, msg: bytes) -> None:
    """Send a 12-byte ASCII header."""

    if _native is None:
        return _py._send_msg(sock, msg)
    return _native.send_msg(sock, msg)


def _recvall(sock, n: int) -> bytes:
    """Read exactly ``n`` bytes from a socket."""

    if _native is None:
        return _py._recvall(sock, n)
    try:
        return _native.recv_exact(sock, int(n))
    except OSError as exc:
        raise _SocketClosed(str(exc)) from exc


def _recv_msg(sock) -> bytes:
    """Receive a 12-byte ASCII header."""

    if _native is None:
        return _py._recv_msg(sock)
    try:
        return _native.recv_msg(sock)
    except OSError as exc:
        raise _SocketClosed(str(exc)) from exc


def _send_array(sock, arr, dtype) -> None:
    """Send a NumPy array over a socket using a contiguous memory view."""

    if _native is None:
        return _py._send_array(sock, arr, dtype)
    a = np.asarray(arr, dtype=dtype, order="C")
    return _native.send_all(sock, memoryview(a).cast("B"))


def _recv_array(sock, shape, dtype):
    """Receive a NumPy array of a given shape and dtype from a socket."""

    # Keep the Python implementation here because it already receives directly
    # into a NumPy-owned buffer. The native extension focuses on fixed-layout
    # hot-path frames where it can avoid Python-level loops.
    return _py._recv_array(sock, shape, dtype)


def _send_int(sock, x: int) -> None:
    """Send a 32-bit little-endian integer."""

    if _native is None:
        return _py._send_int(sock, x)
    return _native.send_int(sock, int(x))


def _recv_int(sock) -> int:
    """Receive a 32-bit little-endian integer."""

    if _native is None:
        return _py._recv_int(sock)
    try:
        return int(_native.recv_int(sock))
    except OSError as exc:
        raise _SocketClosed(str(exc)) from exc


def _send_bytes(sock, b: bytes) -> None:
    """Send a length-prefixed byte string."""

    if _native is None:
        return _py._send_bytes(sock, b)
    return _native.send_bytes(sock, b)


def _recv_bytes(sock) -> bytes:
    """Receive a length-prefixed byte string."""

    if _native is None:
        return _py._recv_bytes(sock)
    try:
        return _native.recv_bytes(sock)
    except OSError as exc:
        raise _SocketClosed(str(exc)) from exc


def _recv_posdata(sock):
    """Read a POSDATA/FIELDDATA block."""

    return _py._recv_posdata(sock)


def _send_force_ready(
    sock,
    energy_ha: float,
    forces_Nx3_ha_per_bohr,
    virial_3x3_ha,
    more: bytes = b"",
):
    """Send a FORCEREADY/SOURCEREADY message."""

    _send_msg(sock, FORCEREADY)
    _send_array(sock, np.array([energy_ha], dtype=DT_FLOAT), DT_FLOAT)
    forces = np.asarray(forces_Nx3_ha_per_bohr, dtype=DT_FLOAT)
    if forces.ndim != 2 or forces.shape[1] != 3:
        raise AssertionError("forces must have shape (N, 3)")
    _send_int(sock, forces.shape[0])
    _send_array(sock, forces, DT_FLOAT)
    _send_array(sock, np.asarray(virial_3x3_ha, dtype=DT_FLOAT).T, DT_FLOAT)
    _send_bytes(sock, more)


def _pack_init(sock, init_dict: dict):
    """Send an INIT handshake containing a JSON payload."""

    _send_msg(sock, INIT)
    molid = int(init_dict.get("molecule_id", 0))
    _send_int(sock, molid)
    _send_bytes(sock, json.dumps(init_dict).encode("utf-8"))


class SocketHub(_py.SocketHub):
    """
    Drop-in, native-backed variant of :class:`sockets.SocketHub`.

    The constructor and public methods are inherited unchanged. Native helpers
    accelerate the fixed FIELDDATA/GETSOURCE send and SOURCEREADY receive path.
    """

    native_available = staticmethod(native_available)

    def _maybe_init_client(self, st, init_payload: dict):
        if _native is None:
            return super()._maybe_init_client(st, init_payload)
        _pack_init(st.sock, init_payload)
        st.initialized = True

    def _dispatch_field(self, st, blob, meta: dict) -> None:
        if _native is None:
            return super()._dispatch_field(st, blob, meta)
        _native.send_all(st.sock, blob)
        st.pending_send = True
        if meta:
            st.extras.update(meta)

    def _read_source_ready(self, st) -> Tuple[np.ndarray, bytes]:
        if _native is None:
            return super()._read_source_ready(st)
        try:
            fx, fy, fz, extra = _native.recv_source_ready(st.sock)
        except OSError as exc:
            raise _SocketClosed(str(exc)) from exc
        amp = np.array((fx, fy, fz), dtype=float)
        st.last_amp = amp
        st.pending_send = False
        return amp, extra


__all__ = [
    "HEADER_LEN",
    "STATUS",
    "READY",
    "HAVEDATA",
    "NEEDINIT",
    "INIT",
    "POSDATA",
    "GETFORCE",
    "FORCEREADY",
    "STOP",
    "BYE",
    "FIELDDATA",
    "GETSOURCE",
    "SOURCEREADY",
    "DT_FLOAT",
    "DT_INT",
    "_SocketClosed",
    "_pad12",
    "_send_msg",
    "_recvall",
    "_recv_msg",
    "_send_array",
    "_recv_array",
    "_send_int",
    "_recv_int",
    "_send_bytes",
    "_recv_bytes",
    "_recv_posdata",
    "_send_force_ready",
    "_pack_init",
    "get_available_host_port",
    "am_master",
    "mpi_bcast_from_master",
    "native_available",
    "SocketHub",
]
