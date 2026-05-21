# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
Native-backed aggregate socket transport for MaxwellLink.

This module mirrors :mod:`maxwelllink.sockets.aggregated` for opt-in testing::

    from maxwelllink.sockets.caggregated import AggregatedSocketHub

The public classes and helpers keep the Python API shape. The packed aggregate
STEP/RESULT codecs and bridge-local SocketHub hot path use the native helpers
from :mod:`maxwelllink.sockets._csockets` when the extension is available.
"""

from __future__ import annotations

import argparse
import socket
import threading
from typing import Dict, Mapping, Optional

import numpy as np

from . import aggregated as _pyagg
from . import csockets as _csockets
from .aggregated import (  # noqa: F401
    AGGHELLO,
    AGGINIT,
    AGGREADY,
    AGGRESULT,
    AGGSTEP,
    AGGREGATION_INFO_VERSION,
    AggregatedBridge,
    RemoteBridgeSpec,
)

try:
    from . import _csockets as _native
except Exception:  # pragma: no cover - exercised only when extension is absent
    _native = None


def native_available() -> bool:
    """Return whether the compiled native aggregate helper extension is available."""

    return _native is not None


class _CStepCodec:
    """Native-backed encoder/decoder for AGGSTEP frames."""

    def __init__(self) -> None:
        self._fallback = _pyagg._StepCodec()

    def send(
        self, sock: socket.socket, requests: Mapping[int, Mapping[str, np.ndarray]]
    ):
        if _native is None:
            return self._fallback.send(sock, requests)
        frame = _native.encode_step_frame(requests)
        return _native.send_all(sock, frame)

    def recv(
        self, sock: socket.socket, *, header_already_read: bool = False
    ) -> Dict[int, np.ndarray]:
        if _native is None:
            return self._fallback.recv(sock, header_already_read=header_already_read)
        rows = _native.decode_step_frame(sock, header_already_read=header_already_read)
        return {int(mid): np.array(field, dtype=float) for mid, field in rows}


class _CResultCodec:
    """Native-backed encoder/decoder for AGGRESULT frames."""

    def __init__(self) -> None:
        self._fallback = _pyagg._ResultCodec()

    def send(self, sock: socket.socket, responses: Mapping[int, Mapping[str, object]]):
        if _native is None:
            return self._fallback.send(sock, responses)
        frame = _native.encode_result_frame(responses)
        return _native.send_all(sock, frame)

    def recv(self, sock: socket.socket) -> Dict[int, dict]:
        if _native is None:
            return self._fallback.recv(sock)
        rows = _native.decode_result_frame(sock)
        return {
            int(mid): {"amp": np.array(amp, dtype=float), "extra": extra}
            for mid, amp, extra in rows
        }


def _install_native_group_codecs(groups: Mapping[str, object]) -> None:
    """Replace per-group Python codecs with native-backed codecs in place."""

    for group in groups.values():
        if not isinstance(group.step_codec, _CStepCodec):
            group.step_codec = _CStepCodec()
        if not isinstance(group.result_codec, _CResultCodec):
            group.result_codec = _CResultCodec()


class AggregatedSocketHub(_pyagg.AggregatedSocketHub):
    """
    Drop-in, native-backed variant of :class:`aggregated.AggregatedSocketHub`.
    """

    native_available = staticmethod(native_available)

    def _prepare_groups_locked(self, init_payloads: Mapping[int, dict]) -> None:
        super()._prepare_groups_locked(init_payloads)
        _install_native_group_codecs(self._groups)

    def add_bridge(self, local_unixsocket: str) -> AggregatedBridge:
        """
        Create, start, and return one hub-owned native-backed local bridge.
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


# Common typo-compatible alias; the canonical API remains AggregatedSocketHub.
AggregatedSockethub = AggregatedSocketHub


class LocalSocketHubBridge(_pyagg.LocalSocketHubBridge):
    """
    Native-backed bridge process/thread.

    The public constructor and methods match the Python implementation. The
    downstream local hub uses :class:`csockets.SocketHub`, and aggregate frame
    codecs use the native extension when available.
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
                ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in str(group_id)
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

        self.local_hub = _csockets.SocketHub(
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
        self._step_codec = _CStepCodec()
        self._result_codec = _CResultCodec()


def run_bridge_node(info="aggregation.json", *, idx: int = 0) -> None:
    """
    Run one native-backed bridge node from an aggregation manifest.
    """

    payload = _pyagg._load_aggregation_info(info)
    specs = _pyagg._coerce_remote_bridge_specs(payload)
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
    CLI entry point for running one native-backed aggregate bridge.
    """

    parser = argparse.ArgumentParser(
        description="Run one MaxwellLink native-backed aggregate bridge node."
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


__all__ = [
    "AGGHELLO",
    "AGGINIT",
    "AGGREADY",
    "AGGSTEP",
    "AGGRESULT",
    "AGGREGATION_INFO_VERSION",
    "AggregatedBridge",
    "AggregatedSocketHub",
    "AggregatedSockethub",
    "LocalSocketHubBridge",
    "RemoteBridgeSpec",
    "mxl_bridge_main",
    "native_available",
    "run_bridge_node",
]
