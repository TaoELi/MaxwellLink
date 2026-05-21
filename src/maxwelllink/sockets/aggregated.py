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
communication topology::

    EM solver -> AggregatedSocketHub ==TCP==> LocalSocketHubBridge
              -> local SocketHub ==TCP/UNIX==> many molecular drivers
"""

from __future__ import annotations

import argparse
from collections.abc import Iterable
import json
import os
import selectors
import socket
import struct
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
    _recv_bytes,
    _recv_msg,
    _send_bytes,
    _send_msg,
)

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

_INT32 = struct.Struct("<i")
_STRUCT_3D = struct.Struct("<3d")
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


# Selector (un)register raises these when a socket is unknown or already closed;
# they are always safe to ignore on a best-effort detach.
_SELECTOR_ERRORS = (KeyError, ValueError, OSError)


def _close_socket(sock: Optional[socket.socket]) -> None:
    """Close a socket, ignoring the error if it is already gone."""

    if sock is None:
        return
    try:
        sock.close()
    except OSError:
        pass


def _recv_exact_into(sock: socket.socket, buf, nbytes: int) -> None:
    """Read exactly ``nbytes`` into the start of ``buf``."""

    mv = memoryview(buf)
    got = 0
    while got < nbytes:
        nrecv = sock.recv_into(mv[got:nbytes], nbytes - got)
        if nrecv == 0:
            raise _SocketClosed("Peer closed")
        got += nrecv


def _expect_header(buf, expected: bytes) -> None:
    """Validate the 12-byte banner at the start of ``buf``."""

    got = bytes(memoryview(buf)[:_AGG_HEADER_LEN]).rstrip()
    if got != expected:
        raise RuntimeError(f"Expected {expected!r}, got {got!r}")


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
            _close_socket(sock)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(delay, remaining))
            delay = min(delay * 1.5, 1.0)

    raise TimeoutError(
        f"Timed out connecting to aggregated hub at {(address, port)!r}"
    ) from last_error


# ---------------------------------------------------------------------------
# HELLO / INIT framing (JSON payloads)
# ---------------------------------------------------------------------------


def _send_aggregate_hello(sock: socket.socket, *, group_id: str) -> None:
    """Send the bridge HELLO banner used by the aggregate protocol."""

    _send_msg(sock, AGGHELLO)
    _send_bytes(sock, _json_dumps_bytes({"group_id": str(group_id), "version": 1}))


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
        """Return a reusable named buffer holding at least ``size`` bytes."""

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
        """Pack and send one grouped fan-out step as a single frame."""

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

        ``header_already_read`` is set when the caller already consumed the
        12-byte banner (e.g. the bridge's main dispatch loop) so only the rest
        of the header is read here.
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
        """Pack and send grouped molecule responses as a single frame."""

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
        """Receive grouped molecule responses from a bridge."""

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


# ---------------------------------------------------------------------------
# Hub-side state and manifest specs
# ---------------------------------------------------------------------------


@dataclass
class _AggregateGroupState:
    """Per-bridge group state tracked by :class:`AggregatedSocketHub`."""

    group_id: str
    molecule_ids: list[int] = field(default_factory=list)
    init_payloads: Dict[int, dict] = field(default_factory=dict)
    bridge: Optional[_ClientState] = None
    step_codec: _StepCodec = field(default_factory=_StepCodec)
    result_codec: _ResultCodec = field(default_factory=_ResultCodec)


@dataclass(frozen=True)
class RemoteBridgeSpec:
    """
    One remote aggregate bridge entry produced by ``init_remote_bridges``.

    Parameters
    ----------
    idx : int
        Zero-based bridge index used by :func:`run_bridge_node`.
    group_id : str
        Aggregate group identifier transmitted upstream.
    unixsocket : str
        Downstream UNIX-socket address local drivers should connect to.
    n_molecules : int
        Number of molecules assigned to this bridge.
    """

    idx: int
    group_id: str
    unixsocket: str
    n_molecules: int

    def to_dict(self) -> dict:
        """Return a JSON-serializable bridge specification mapping."""

        return {
            "idx": int(self.idx),
            "group_id": str(self.group_id),
            "unixsocket": str(self.unixsocket),
            "n_molecules": int(self.n_molecules),
        }

    @classmethod
    def from_dict(cls, payload: Mapping) -> "RemoteBridgeSpec":
        """Build one bridge specification from JSON-decoded manifest data."""

        return cls(
            idx=int(payload["idx"]),
            group_id=str(payload["group_id"]),
            unixsocket=str(payload["unixsocket"]),
            n_molecules=int(payload["n_molecules"]),
        )


def _as_molecule_list(molecules) -> list:
    """Normalize one molecule or an iterable of molecules into a list."""

    if hasattr(molecules, "init_payload"):
        return [molecules]
    if isinstance(molecules, Iterable) and not isinstance(
        molecules, (str, bytes, bytearray)
    ):
        return list(molecules)
    raise TypeError(
        "Expected one molecule or an iterable of molecules with 'init_payload'."
    )


def _assign_molecule_to_group(
    molecule,
    *,
    expected_hub: "AggregatedSocketHub",
    group_id: str,
) -> None:
    """Assign one molecule to the given aggregate group in-place."""

    if not hasattr(molecule, "init_payload"):
        raise TypeError(
            "Expected a molecule-like object carrying an 'init_payload' attribute."
        )
    molecule_hub = getattr(molecule, "hub", expected_hub)
    if molecule_hub is not expected_hub:
        raise ValueError(
            "All molecules assigned to remote aggregate bridges must use the same hub."
        )

    payload = molecule.init_payload
    if payload is None:
        payload = {}
        molecule.init_payload = payload
    elif not isinstance(payload, dict):
        payload = dict(payload)
        molecule.init_payload = payload

    previous = payload.get("aggregate_group")
    if previous is not None and str(previous).strip() not in ("", group_id):
        raise ValueError(
            f"Molecule is already assigned to aggregate_group {previous!r}, "
            f"cannot move it to {group_id!r}."
        )
    payload["aggregate_group"] = group_id


def _load_aggregation_info(info="aggregation.json") -> dict:
    """Load one JSON aggregation manifest from disk."""

    with open(os.fspath(info), "r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise ValueError("Aggregation info file must contain a JSON object.")
    return payload


def _coerce_remote_bridge_specs(payload: Mapping) -> list[RemoteBridgeSpec]:
    """Decode and validate the ``bridges`` section of an aggregation manifest."""

    raw_bridges = payload.get("bridges", [])
    if not isinstance(raw_bridges, list):
        raise ValueError("Aggregation info must contain a 'bridges' list.")
    specs = [RemoteBridgeSpec.from_dict(item) for item in raw_bridges]
    seen = set()
    for spec in specs:
        if spec.idx in seen:
            raise ValueError(
                f"Aggregation info contains duplicate bridge idx {spec.idx}."
            )
        seen.add(spec.idx)
    return specs


# ---------------------------------------------------------------------------
# Bridge-node entry points
# ---------------------------------------------------------------------------


def run_bridge_node(info="aggregation.json", *, idx: int = 0) -> None:
    """
    Run one bridge node from a manifest written by ``init_remote_bridges``.

    Parameters
    ----------
    info : str or path-like, default: ``"aggregation.json"``
        JSON manifest written by :meth:`AggregatedSocketHub.init_remote_bridges`.
    idx : int, default: 0
        Zero-based bridge index identifying which bridge entry in ``info`` this
        node should start.
    """

    payload = _load_aggregation_info(info)
    specs = _coerce_remote_bridge_specs(payload)
    bridge_idx = int(idx)
    try:
        spec = next(spec for spec in specs if spec.idx == bridge_idx)
    except StopIteration as exc:
        available = ", ".join(str(spec.idx) for spec in specs) or "<none>"
        raise IndexError(
            f"Bridge idx {bridge_idx} not found in aggregation info. "
            f"Available bridge indices: {available}."
        ) from exc

    bridge = LocalSocketHubBridge(
        group_id=spec.group_id,
        upstream_host=str(payload["hub_host"]),
        upstream_port=int(payload["hub_port"]),
        timeout=float(payload.get("timeout", 60.0)),
        latency=float(payload.get("latency", 0.01)),
        local_unixsocket=spec.unixsocket,
    )
    thread = bridge.start()

    try:
        while thread.is_alive():
            thread.join(timeout=1.0)
    except KeyboardInterrupt:
        pass
    finally:
        try:
            bridge.stop(wait=max(2.0, 10.0 * bridge.latency))
        except Exception:
            pass


def mxl_bridge_main(argv: list[str] | None = None) -> int:
    """
    CLI entry point for running one aggregate bridge from a manifest.

    Examples
    --------
    ``mxl_bridge --info aggregation.json --idx 0``
    """

    parser = argparse.ArgumentParser(
        description="Run one MaxwellLink aggregate bridge node."
    )
    parser.add_argument(
        "--info",
        type=str,
        default="aggregation.json",
        help="Path to the aggregation manifest written by init_remote_bridges().",
    )
    parser.add_argument(
        "--idx",
        type=int,
        default=0,
        help="Zero-based bridge index within the aggregation manifest.",
    )
    args = parser.parse_args(argv)

    run_bridge_node(info=args.info, idx=args.idx)
    return 0


# ---------------------------------------------------------------------------
# Hub-owned convenience bridge handle
# ---------------------------------------------------------------------------


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

        for molecule in _as_molecule_list(molecules):
            _assign_molecule_to_group(
                molecule,
                expected_hub=self.hub,
                group_id=self.group_id,
            )

    def start(self) -> threading.Thread:
        """Start the underlying local bridge thread."""

        return self._bridge.start()

    def stop(self, wait: float = 2.0) -> None:
        """Stop the underlying local bridge."""

        self._bridge.stop(wait=wait)


# ---------------------------------------------------------------------------
# EM-side aggregated hub
# ---------------------------------------------------------------------------


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
        self.remote_bridges: list[RemoteBridgeSpec] = []
        self.remote_bridge_info: Optional[dict] = None
        self._bridge_selector = selectors.DefaultSelector()

    # -- Bridge setup ------------------------------------------------------

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

    def init_remote_bridges(
        self,
        molecules,
        *,
        molecules_per_bridge: int,
        unix_prefix: str = "bridge_",
        save_file: str = "aggregation.json",
    ) -> list[RemoteBridgeSpec]:
        """
        Partition molecules across remote bridge groups and save a manifest.

        This helper does not start any bridge threads locally. Instead it
        assigns ``molecule.init_payload["aggregate_group"]`` for each molecule
        and writes one JSON manifest that bridge-node scripts can consume via
        :func:`run_bridge_node`.

        Parameters
        ----------
        molecules : molecule or iterable of molecules
            Molecules to distribute across remote bridges.
        molecules_per_bridge : int
            Maximum number of molecules assigned to one bridge.
        unix_prefix : str, default: ``"bridge_"``
            Prefix used to generate downstream UNIX socket names
            ``f"{unix_prefix}{idx}"``.
        save_file : str, default: ``"aggregation.json"``
            Path where the bridge manifest should be written.

        Returns
        -------
        list[RemoteBridgeSpec]
            The generated bridge specifications in order.
        """

        items = _as_molecule_list(molecules)
        if not items:
            raise ValueError("init_remote_bridges(...) requires at least one molecule.")
        molecules_per_group = int(molecules_per_bridge)
        if molecules_per_group <= 0:
            raise ValueError("molecules_per_bridge must be a positive integer.")

        prefix = str(unix_prefix)
        specs: list[RemoteBridgeSpec] = []
        for start in range(0, len(items), molecules_per_group):
            idx = len(specs)
            group_items = items[start : start + molecules_per_group]
            unixsocket = f"{prefix}{idx}"
            group_id = unixsocket
            for molecule in group_items:
                _assign_molecule_to_group(
                    molecule,
                    expected_hub=self,
                    group_id=group_id,
                )
            specs.append(
                RemoteBridgeSpec(
                    idx=idx,
                    group_id=group_id,
                    unixsocket=unixsocket,
                    n_molecules=len(group_items),
                )
            )

        payload = {
            "version": AGGREGATION_INFO_VERSION,
            "hub_host": self._bridge_connect_host,
            "hub_port": self._bridge_connect_port,
            "timeout": float(self.timeout),
            "latency": float(self.latency),
            "unix_prefix": prefix,
            "molecules_per_bridge": molecules_per_group,
            "bridges": [spec.to_dict() for spec in specs],
        }
        with open(os.fspath(save_file), "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=True)

        self.remote_bridges = list(specs)
        self.remote_bridge_info = payload
        self._log(
            f"Prepared {len(specs)} remote aggregate bridge(s); "
            f"manifest saved to {save_file!r}."
        )
        for spec in specs:
            self._log(
                f"REMOTE BRIDGE {spec.idx}: unix={spec.unixsocket!r} "
                f"group={spec.group_id!r} molecules={spec.n_molecules}"
            )
        return specs

    # -- Group bookkeeping -------------------------------------------------

    def _deadline(self, timeout: Optional[float]) -> float:
        """Return an absolute monotonic-wall deadline for ``timeout`` seconds.

        Falls back to the hub-wide ``self.timeout`` when ``timeout`` is ``None``.
        """

        span = float(timeout) if timeout is not None else float(self.timeout)
        return time.time() + span

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

    def _group_and_bridge(
        self, group_id: str
    ) -> tuple[Optional[_AggregateGroupState], Optional[_ClientState]]:
        """Return ``(group, bridge)`` for ``group_id`` under the hub lock."""

        with self._lock:
            group = self._groups.get(group_id)
            st = None if group is None else group.bridge
        return group, st

    # -- Bridge socket registration ---------------------------------------

    def _register_bridge_sock(self, sock: socket.socket, group_id: str) -> None:
        """Register a bridge socket for readable events with its group id."""

        self._unregister_bridge_sock(sock)
        try:
            self._bridge_selector.register(sock, selectors.EVENT_READ, data=group_id)
        except _SELECTOR_ERRORS:
            pass

    def _unregister_bridge_sock(self, sock: socket.socket) -> None:
        """Unregister a bridge socket from the aggregate selector."""

        try:
            self._bridge_selector.unregister(sock)
        except _SELECTOR_ERRORS:
            pass

    def _detach_sock_locked(self, st: _ClientState) -> None:
        """Unregister a client from both selectors and mark it dead.

        Caller must hold ``self._lock`` and is responsible for closing the
        socket afterwards (typically outside the lock).
        """

        self._unregister_bridge_sock(st.sock)
        self._unregister_sock(st.sock)
        st.alive = False
        st.initialized = False

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
        self._register_bridge_sock(st.sock, group_id)
        self._log(f"CONNECTED: aggregate group {group_id!r} <- {st.address}")

    def _drop_client_locked(self, st_key, st: _ClientState, reason: str) -> None:
        """Remove a temporary or duplicate bridge client."""

        self.clients.pop(st_key, None)
        self._detach_sock_locked(st)
        _close_socket(st.sock)
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
            self._detach_sock_locked(st)
            for mid in group.molecule_ids:
                if self.bound.get(mid) is st:
                    self.bound[mid] = None

        self._log(f"DISCONNECTED ({reason}): aggregate group {group_id!r}")
        _close_socket(st.sock)
        self._pause()

    # -- Binding handshake -------------------------------------------------

    def _snapshot_unbound_clients(self, *, identified: bool) -> list:
        """Snapshot still-unbound bridge clients under the hub lock.

        ``identified`` selects clients that have already announced their
        ``aggregate_group`` via HELLO (``True``) versus those still awaiting it
        (``False``). Returns a list of ``(client_key, client_state)`` pairs.
        """

        with self._lock:
            return [
                (st_key, st)
                for st_key, st in list(self.clients.items())
                if st is not None
                and st.alive
                and st.molecule_id < 0
                and ("aggregate_group" in st.extras) == identified
            ]

    def _try_identify_fresh_clients(self) -> None:
        """
        Poll newly accepted sockets for bridge HELLO messages.

        A bridge sends HELLO immediately after connecting. We keep the read
        timeout short here so one slow client cannot stall the entire hub.
        """

        for st_key, st in self._snapshot_unbound_clients(identified=False):
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
            for st_key, st in self._snapshot_unbound_clients(identified=True):
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
        deadline = self._deadline(timeout)
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

    # -- Stepping ----------------------------------------------------------

    def _plan_step_locked(
        self, requests: Dict[int, dict]
    ) -> Optional[Dict[str, Dict[int, dict]]]:
        """
        Validate a step request and group it by aggregate group.

        Returns a ``{group_id: {molecule_id: {"efield_au": ndarray}}}`` mapping,
        or ``None`` if not every requested molecule is bound and initialized.
        Must be called while holding ``self._lock``.
        """

        wants = {int(mid) for mid in requests.keys()}
        needs_prepare = any("init" in requests[mid] for mid in requests.keys()) or any(
            int(mid) not in self._molecule_to_group for mid in requests.keys()
        )
        if needs_prepare:
            payloads = {
                int(mid): dict(requests[mid].get("init") or {"molecule_id": int(mid)})
                for mid in requests.keys()
            }
            self._prepare_groups_locked(payloads)

        if not self.all_bound(wants, require_init=True):
            return None

        grouped_requests: Dict[str, Dict[int, dict]] = {}
        for mid in wants:
            group_id = self._molecule_to_group[mid]
            grouped_requests.setdefault(group_id, {})[mid] = {
                "efield_au": np.asarray(requests[mid]["efield_au"], dtype=float)
            }
        return grouped_requests

    def _send_step_to_group(self, group_id: str, group_request: Dict[int, dict]) -> bool:
        """Send one grouped fan-out step; return ``False`` on a dead bridge."""

        group, st = self._group_and_bridge(group_id)
        if group is None or st is None or not st.alive or not st.initialized:
            self._pause()
            return False
        try:
            group.step_codec.send(st.sock, group_request)
        except (socket.timeout, _SocketClosed, OSError):
            self._mark_group_dead(group_id, reason="send")
            return False
        return True

    def _collect_group_result(
        self, group_id: str, expected_ids: set[int], deadline: float
    ) -> Optional[Dict[int, dict]]:
        """
        Receive and validate one group's reply.

        Returns the per-molecule responses, or ``None`` if the bridge died or
        the deadline passed (failure side effects are handled internally).
        Raises ``RuntimeError`` if the bridge returns the wrong molecule ids.
        """

        group, st = self._group_and_bridge(group_id)
        if group is None or st is None or not st.alive:
            self._pause()
            return None

        remaining = deadline - time.time()
        if remaining <= 0.0:
            return None

        old_timeout = st.sock.gettimeout()
        try:
            st.sock.settimeout(max(0.0, remaining))
            group_responses = group.result_codec.recv(st.sock)
        except (socket.timeout, RuntimeError, _SocketClosed, OSError):
            self._mark_group_dead(group_id, reason="recv")
            return None
        finally:
            st.sock.settimeout(old_timeout)

        actual = set(group_responses.keys())
        if actual != expected_ids:
            self._mark_group_dead(group_id, reason="protocol")
            raise RuntimeError(
                f"Aggregate group {group_id!r} returned molecule ids {sorted(actual)}, "
                f"expected {sorted(expected_ids)}."
            )
        return group_responses

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

        deadline = self._deadline(timeout)

        with self._lock:
            grouped_requests = self._plan_step_locked(requests)
        if not grouped_requests:
            return {}

        for group_id, group_request in grouped_requests.items():
            if not self._send_step_to_group(group_id, group_request):
                return {}

        responses: Dict[int, dict] = {}
        pending_groups = set(grouped_requests.keys())

        # Fast path: a single group needs only a blocking recv, no selector.
        if len(pending_groups) == 1:
            group_id = next(iter(pending_groups))
            expected = set(grouped_requests[group_id].keys())
            group_responses = self._collect_group_result(group_id, expected, deadline)
            if group_responses is None:
                return {}
            responses.update(group_responses)
            return responses

        # Multiple groups: wait on whichever bridge becomes readable next.
        while pending_groups:
            remaining = deadline - time.time()
            if remaining <= 0.0:
                return {}

            try:
                events = self._bridge_selector.select(timeout=min(remaining, 1.0))
            except OSError:
                return {}
            if not events:
                continue

            for key, _mask in events:
                group_id = key.data
                if group_id not in pending_groups:
                    continue
                expected = set(grouped_requests[group_id].keys())
                group_responses = self._collect_group_result(
                    group_id, expected, deadline
                )
                if group_responses is None:
                    return {}
                responses.update(group_responses)
                pending_groups.discard(group_id)

        return responses

    # -- Shutdown ----------------------------------------------------------

    def _snapshot_stop_targets(self):
        """
        Snapshot the bridge groups and any stray clients to tear down.

        Returns ``(group_clients, other_clients)`` where ``group_clients`` is a
        list of ``(group_id, bridge_state, molecule_ids)`` and ``other_clients``
        is a list of ``(client_key, client_state)`` not already covered above.
        """

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
        return group_clients, other_clients

    def _request_bridge_shutdown(self, group_clients) -> None:
        """Send ``STOP`` to every live bridge group."""

        for _group_id, st, _molecule_ids in group_clients:
            if not st.alive:
                continue
            try:
                _send_msg(st.sock, STOP)
            except Exception:
                pass

    def _await_bridge_byes(self, group_clients) -> None:
        """Wait briefly for each bridge to acknowledge ``STOP`` with ``BYE``."""

        deadline = time.time() + max(2.0, 10.0 * self.latency)
        while time.time() < deadline:
            remaining_alive = False
            for _group_id, st, _molecule_ids in group_clients:
                if not st.alive:
                    continue
                remaining_alive = True
                try:
                    st.sock.settimeout(self.latency)
                    if _recv_msg(st.sock) == BYE:
                        st.alive = False
                except (socket.timeout, _SocketClosed, OSError):
                    continue
            if not remaining_alive:
                break
            time.sleep(self.latency)

    def _teardown_stop_targets(self, group_clients, other_clients) -> None:
        """Clear all bridge/molecule state and close every snapshotted socket."""

        sockets_to_close = []
        with self._lock:
            for group_id, st, molecule_ids in group_clients:
                group = self._groups.get(group_id)
                if group is not None and group.bridge is st:
                    group.bridge = None
                self.clients.pop(group_id, None)
                self._detach_sock_locked(st)
                for mid in molecule_ids:
                    if self.bound.get(mid) is st:
                        self.bound[mid] = None
                sockets_to_close.append((f"aggregate group {group_id!r}", st.sock))

            for key, st in other_clients:
                self.clients.pop(key, None)
                self._detach_sock_locked(st)
                sockets_to_close.append((f"client {key!r}", st.sock))

        for label, sock in sockets_to_close:
            self._log(f"DISCONNECTED: {label}")
            _close_socket(sock)

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

        group_clients, other_clients = self._snapshot_stop_targets()
        self._request_bridge_shutdown(group_clients)
        self._await_bridge_byes(group_clients)
        self._teardown_stop_targets(group_clients, other_clients)

        for selector in (self._selector, self._bridge_selector):
            try:
                selector.close()
            except Exception:
                pass

        for handle in owned_bridges:
            try:
                handle.stop(wait=max(2.0, 10.0 * self.latency))
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Node-local bridge
# ---------------------------------------------------------------------------


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
            sanitized = "".join(
                ch if ch.isalnum() or ch in ("-", "_") else "_"
                for ch in str(group_id)
            ).strip("_")
            local_unixsocket = f"agg_{sanitized or 'bridge'}"

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
        self._request_cache: Dict[int, dict] = {}
        self._upstream_sock: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._step_codec = _StepCodec()
        self._result_codec = _ResultCodec()

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
        self._request_cache = {
            int(mid): {"efield_au": np.zeros(3, dtype=float)}
            for mid in init_payloads.keys()
        }

    def _build_local_requests(
        self, efields: Mapping[int, np.ndarray]
    ) -> Dict[int, dict]:
        """
        Map upstream efields onto the reusable downstream request cache.

        When the requested molecule set matches the cache exactly we update the
        cached arrays in place; otherwise we patch/extend the cache as needed.
        """

        cache_hit = (
            len(efields) == len(self._request_cache)
            and self._request_cache
            and all(int(mid) in self._request_cache for mid in efields.keys())
        )
        if cache_hit:
            for mid, field in efields.items():
                np.copyto(
                    self._request_cache[int(mid)]["efield_au"],
                    np.asarray(field, dtype=float).reshape(3),
                )
            return self._request_cache

        requests: Dict[int, dict] = {}
        for mid, field in efields.items():
            molid = int(mid)
            cached = self._request_cache.get(molid)
            if cached is None:
                cached = {"efield_au": np.zeros(3, dtype=float)}
                self._request_cache[molid] = cached
            np.copyto(cached["efield_au"], np.asarray(field, dtype=float).reshape(3))
            requests[molid] = cached
        return requests

    def _run_local_step(self, efields: Mapping[int, np.ndarray]) -> Dict[int, dict]:
        """Fan out one grouped step to the downstream local hub."""

        requests = self._build_local_requests(efields)
        responses = self.local_hub.step_barrier(requests)
        while not responses:
            self._ensure_local_hub_ready(self._init_payloads)
            responses = self.local_hub.step_barrier(requests)
        return responses

    def run(self) -> None:
        """Run the bridge loop until the hub sends ``STOP`` or disconnects."""

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
                    efields = self._step_codec.recv(sock, header_already_read=True)
                    responses = self._run_local_step(efields)
                    self._result_codec.send(sock, responses)

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
            _close_socket(sock)
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
            _close_socket(sock)

        if self._thread is not None:
            self._thread.join(timeout=float(wait))

        self.local_hub.stop()


__all__ = [
    "AggregatedBridge",
    "AggregatedSocketHub",
    "LocalSocketHubBridge",
    "RemoteBridgeSpec",
    "mxl_bridge_main",
    "run_bridge_node",
]
