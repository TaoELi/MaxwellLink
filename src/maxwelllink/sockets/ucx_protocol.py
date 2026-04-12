# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
Message-oriented UCX protocol helpers for MaxwellLink.

This module intentionally keeps the UCX wire format separate from the legacy
socket/i-PI framing in :mod:`maxwelllink.sockets.sockets`. The solver-facing
API remains the same, but UCX exchanges use a compact binary header plus
binary payloads tailored to the hot path.
"""

from __future__ import annotations

import importlib
import json
import struct
from dataclasses import dataclass
from typing import Dict, Optional

import numpy as np

_MAGIC = b"MXLU"
_VERSION = 1
_HEADER = struct.Struct("<4sHHI")
_INT32 = struct.Struct("<i")
_FLOAT64X3 = struct.Struct("<3d")

HELLO = 1
INIT = 2
STEP_REQUEST = 3
STEP_RESPONSE = 4
STOP = 5
BYE = 6

_OPCODE_NAMES = {
    HELLO: "HELLO",
    INIT: "INIT",
    STEP_REQUEST: "STEP_REQUEST",
    STEP_RESPONSE: "STEP_RESPONSE",
    STOP: "STOP",
    BYE: "BYE",
}


@dataclass(frozen=True)
class UCXMessage:
    """
    Decoded UCX message container.

    Attributes
    ----------
    opcode : int
        Message opcode.
    payload : bytes
        Raw message payload.
    """

    opcode: int
    payload: bytes


def opcode_name(opcode: int) -> str:
    """
    Return a human-readable name for a UCX opcode.

    Parameters
    ----------
    opcode : int
        Numeric opcode.

    Returns
    -------
    str
        Symbolic opcode name.
    """

    return _OPCODE_NAMES.get(int(opcode), f"UNKNOWN({int(opcode)})")


def _json_dumps(data: Dict) -> bytes:
    """
    Serialize a dictionary to a compact UTF-8 JSON payload.

    Parameters
    ----------
    data : dict
        Dictionary to serialize.

    Returns
    -------
    bytes
        UTF-8 JSON bytes.
    """

    return json.dumps(
        data,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _json_loads(blob: bytes) -> Dict:
    """
    Deserialize a UTF-8 JSON payload, returning an empty dict for empty blobs.

    Parameters
    ----------
    blob : bytes
        UTF-8 JSON bytes.

    Returns
    -------
    dict
        Decoded dictionary.
    """

    if not blob:
        return {}
    return json.loads(blob.decode("utf-8"))


def pack_message(opcode: int, payload: bytes = b"") -> bytes:
    """
    Build one complete UCX message.

    Parameters
    ----------
    opcode : int
        Message opcode.
    payload : bytes, optional
        Raw payload bytes.

    Returns
    -------
    bytes
        Header + payload.
    """

    payload = bytes(payload)
    return _HEADER.pack(_MAGIC, _VERSION, int(opcode), len(payload)) + payload


def unpack_message(blob: bytes) -> UCXMessage:
    """
    Decode and validate one complete UCX message.

    Parameters
    ----------
    blob : bytes
        Serialized message bytes.

    Returns
    -------
    UCXMessage
        Decoded message.

    Raises
    ------
    ValueError
        If the message header or payload length is invalid.
    """

    blob = bytes(blob)
    if len(blob) < _HEADER.size:
        raise ValueError("UCX message too short")
    magic, version, opcode, payload_len = _HEADER.unpack_from(blob, 0)
    if magic != _MAGIC:
        raise ValueError(f"Invalid UCX message magic: {magic!r}")
    if version != _VERSION:
        raise ValueError(f"Unsupported UCX protocol version: {version}")
    if opcode not in _OPCODE_NAMES:
        raise ValueError(f"Unsupported UCX opcode: {opcode}")
    expected = _HEADER.size + int(payload_len)
    if len(blob) != expected:
        raise ValueError(
            f"UCX message length mismatch: expected {expected}, got {len(blob)}"
        )
    return UCXMessage(opcode=opcode, payload=blob[_HEADER.size :])


def pack_hello(payload: Optional[Dict] = None) -> bytes:
    """
    Build a HELLO message.

    Parameters
    ----------
    payload : dict or None, optional
        Informational connection metadata.

    Returns
    -------
    bytes
        Serialized HELLO message.
    """

    return pack_message(HELLO, _json_dumps(payload or {}))


def unpack_hello(payload: bytes) -> Dict:
    """
    Decode a HELLO payload.

    Parameters
    ----------
    payload : bytes
        Raw HELLO payload.

    Returns
    -------
    dict
        Decoded metadata.
    """

    return _json_loads(payload)


def pack_init(init_dict: Dict) -> bytes:
    """
    Build an INIT message.

    Parameters
    ----------
    init_dict : dict
        Initialization payload. ``molecule_id`` is required.

    Returns
    -------
    bytes
        Serialized INIT message.
    """

    init_dict = dict(init_dict)
    molecule_id = int(init_dict.get("molecule_id", 0))
    payload = _INT32.pack(molecule_id) + _json_dumps(init_dict)
    return pack_message(INIT, payload)


def unpack_init(payload: bytes) -> Dict:
    """
    Decode an INIT payload.

    Parameters
    ----------
    payload : bytes
        Raw INIT payload.

    Returns
    -------
    dict
        Initialization payload with ``molecule_id`` populated.
    """

    if len(payload) < _INT32.size:
        raise ValueError("INIT payload too short")
    molecule_id = _INT32.unpack_from(payload, 0)[0]
    init_dict = _json_loads(payload[_INT32.size :])
    init_dict["molecule_id"] = int(molecule_id)
    return init_dict


def pack_step_request(efield_au_vec3) -> bytes:
    """
    Build a STEP_REQUEST message carrying one electric-field vector.

    Parameters
    ----------
    efield_au_vec3 : array-like, shape (3,)
        Electric field vector in atomic units.

    Returns
    -------
    bytes
        Serialized STEP_REQUEST message.
    """

    vec = np.asarray(efield_au_vec3, dtype=float).reshape(3)
    payload = _FLOAT64X3.pack(float(vec[0]), float(vec[1]), float(vec[2]))
    return pack_message(STEP_REQUEST, payload)


def unpack_step_request(payload: bytes) -> np.ndarray:
    """
    Decode a STEP_REQUEST payload.

    Parameters
    ----------
    payload : bytes
        Raw STEP_REQUEST payload.

    Returns
    -------
    numpy.ndarray
        ``(3,)`` electric field vector.
    """

    if len(payload) != _FLOAT64X3.size:
        raise ValueError(
            f"STEP_REQUEST payload must be {_FLOAT64X3.size} bytes, got {len(payload)}"
        )
    return np.asarray(_FLOAT64X3.unpack(payload), dtype=float)


def pack_step_response(amp_vec3, extra: bytes = b"") -> bytes:
    """
    Build a STEP_RESPONSE message.

    Parameters
    ----------
    amp_vec3 : array-like, shape (3,)
        Source amplitude vector.
    extra : bytes, optional
        Raw trailing payload.

    Returns
    -------
    bytes
        Serialized STEP_RESPONSE message.
    """

    amp = np.asarray(amp_vec3, dtype=float).reshape(3)
    extra = bytes(extra)
    payload = (
        _FLOAT64X3.pack(float(amp[0]), float(amp[1]), float(amp[2]))
        + _INT32.pack(len(extra))
        + extra
    )
    return pack_message(STEP_RESPONSE, payload)


def unpack_step_response(payload: bytes) -> tuple[np.ndarray, bytes]:
    """
    Decode a STEP_RESPONSE payload.

    Parameters
    ----------
    payload : bytes
        Raw STEP_RESPONSE payload.

    Returns
    -------
    tuple
        ``(amp_vec3, extra_bytes)``.
    """

    minimum = _FLOAT64X3.size + _INT32.size
    if len(payload) < minimum:
        raise ValueError("STEP_RESPONSE payload too short")
    amp = np.asarray(_FLOAT64X3.unpack_from(payload, 0), dtype=float)
    extra_len = _INT32.unpack_from(payload, _FLOAT64X3.size)[0]
    extra = payload[minimum:]
    if len(extra) != extra_len:
        raise ValueError(
            f"STEP_RESPONSE extra length mismatch: expected {extra_len}, got {len(extra)}"
        )
    return amp, extra


def pack_stop(reason: Optional[str] = None) -> bytes:
    """
    Build a STOP message.

    Parameters
    ----------
    reason : str or None, optional
        Optional shutdown reason.

    Returns
    -------
    bytes
        Serialized STOP message.
    """

    payload = b"" if reason is None else reason.encode("utf-8")
    return pack_message(STOP, payload)


def unpack_stop(payload: bytes) -> str:
    """
    Decode a STOP payload.

    Parameters
    ----------
    payload : bytes
        Raw STOP payload.

    Returns
    -------
    str
        UTF-8 shutdown reason, or an empty string.
    """

    return payload.decode("utf-8") if payload else ""


def pack_bye() -> bytes:
    """
    Build a BYE message.

    Returns
    -------
    bytes
        Serialized BYE message.
    """

    return pack_message(BYE)


def load_ucx_module():
    """
    Import a compatible Python UCX module.

    Returns
    -------
    module
        Imported ``ucxx`` or ``ucp`` module.

    Raises
    ------
    ImportError
        If no supported UCX Python binding is available.
    """

    errors = []
    for module_name in ("ucxx", "ucp"):
        try:
            return importlib.import_module(module_name)
        except ImportError as exc:
            errors.append(f"{module_name}: {exc}")

    message = (
        "SocketHubUCX requires the optional UCX Python bindings. "
        "Install a compatible UCX package first. On conda, install `ucxx` "
        "from `conda-forge`/`rapidsai`. On pip, upstream publishes "
        "`ucxx-cu12` and `ucxx-cu13` wheels on supported Linux platforms. "
        "A plain `ucxx` pip package is not published."
    )
    raise ImportError(message + " Tried: " + "; ".join(errors))


def init_ucx_module(ucx, options: Optional[Dict] = None) -> None:
    """
    Best-effort UCX runtime initialization.

    Parameters
    ----------
    ucx : module
        Imported UCX module.
    options : dict or None, optional
        Optional UCX runtime options.
    """

    init = getattr(ucx, "init", None)
    if not callable(init):
        return

    try:
        if options:
            try:
                init(options=options, env_takes_precedence=True)
            except TypeError:
                try:
                    init(options=options)
                except TypeError:
                    init(options)
        else:
            init()
    except RuntimeError as exc:
        if "initialized" not in str(exc).lower():
            raise


def reset_ucx_module(ucx) -> None:
    """
    Best-effort UCX runtime reset.

    Parameters
    ----------
    ucx : module
        Imported UCX module.
    """

    reset = getattr(ucx, "reset", None)
    if callable(reset):
        reset()
