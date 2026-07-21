# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
Shared helpers for socket hub tests.

These tests characterize the *current* behavior of the four socket hubs so the
planned sockets/ refactor can be verified to leave the user-visible surface
untouched. Nothing here should be clever: helpers mirror exactly what real
clients (mxl_driver, mxl_bridge, and Meep's C-level MXLSocketSusceptibility)
do on the wire.
"""

from __future__ import annotations

import json
import socket
import sys
import threading
import time
from pathlib import Path

import numpy as np

SRC_ROOT = Path(__file__).resolve().parents[2] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from maxwelllink.mxl_drivers.python.models.dummy_model import DummyModel
from maxwelllink.mxl_drivers.python.mxl_driver import run_driver
from maxwelllink.sockets.aggregated import _ResultCodec, _StepCodec
from maxwelllink.sockets.sockets import _recv_msg, _send_bytes, _send_msg
from maxwelllink.sockets.susceptibility import MXLINIT, MXLREADY


def can_create_sockets() -> bool:
    """Return whether this environment permits opening localhost sockets."""

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except PermissionError:
        return False
    try:
        sock.close()
    except OSError:
        pass
    return True


def pick_free_port() -> int:
    """Ask the OS for a free localhost TCP port."""

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class EchoModel(DummyModel):
    """
    Test driver model returning a molecule-dependent copy of the E field.

    ``amp = (molecule_id + 1) * E`` lets every test assert that per-molecule
    routing survived the transport, whichever hub carried the request.
    """

    def __init__(self):
        super().__init__(verbose=False)
        self.last_e = np.zeros(3, dtype=float)

    def _snapshot(self):
        snapshot = super()._snapshot()
        snapshot["last_e"] = self.last_e.copy()
        return snapshot

    def _restore(self, snapshot):
        super()._restore(snapshot)
        self.last_e = np.asarray(snapshot["last_e"], dtype=float).copy()

    def propagate(self, effective_efield_vec):
        self.last_e = np.asarray(effective_efield_vec, dtype=float).copy()
        self.t += self.dt

    def calc_amp_vector(self):
        return (self.molecule_id + 1.0) * self.last_e

    def append_additional_data(self):
        return {"molecule_id": int(self.molecule_id), "time_au": float(self.t)}


def start_driver_thread(
    *, unix: bool, address: str, port: int | None = None, timeout: float = 20.0
):
    """Start one blocking MaxwellLink driver loop in a daemon thread."""

    kwargs = {
        "unix": unix,
        "address": address,
        "timeout": timeout,
        "driver": EchoModel(),
    }
    if port is not None:
        kwargs["port"] = int(port)

    thread = threading.Thread(target=run_driver, kwargs=kwargs, daemon=True)
    thread.start()
    return thread


def wait_for_path(path: str | Path, timeout: float = 30.0) -> Path:
    """Wait until a filesystem path exists and return it."""

    target = Path(path)
    deadline = time.time() + float(timeout)
    while time.time() < deadline:
        if target.exists():
            return target
        time.sleep(0.02)
    raise TimeoutError(f"Timed out waiting for {target}")


class FakeMeepRank:
    """
    Minimal stand-in for Meep's C-level ``MXLSocketSusceptibility`` client.

    Speaks exactly the wire protocol implemented in meep/src/susceptibility.cpp:
    a 12-byte ``MXLINIT`` banner, a JSON init payload declaring
    ``protocol="mxl_socket_susceptibility_v1"``, a blocking wait for
    ``MXLREADY``, then ``AGGSTEP`` frames answered by ``AGGRESULT`` frames.
    """

    def __init__(
        self,
        host: str,
        port: int,
        *,
        molecule_ids,
        rank: int = 0,
        dt_au: float = 1.0,
        rescaling_factor: float = 2.5,
        time_units_fs: float = 0.1,
        expected_total_molecules: int | None = None,
        timeout: float = 60.0,
    ):
        self.sock = socket.create_connection((host, int(port)), timeout=timeout)
        self.sock.settimeout(timeout)
        self._step_codec = _StepCodec()
        self._result_codec = _ResultCodec()

        payload = {
            "protocol": "mxl_socket_susceptibility_v1",
            "rank": int(rank),
            "molecule_ids": [int(mid) for mid in molecule_ids],
            "dt_au": float(dt_au),
            "rescaling_factor": float(rescaling_factor),
            "time_units_fs": float(time_units_fs),
        }
        if expected_total_molecules is not None:
            payload["expected_total_molecules"] = int(expected_total_molecules)
        _send_msg(self.sock, MXLINIT)
        _send_bytes(self.sock, json.dumps(payload).encode("utf-8"))

    def wait_ready(self) -> None:
        header = _recv_msg(self.sock)
        if header != MXLREADY:
            raise RuntimeError(f"Expected MXLREADY, got {header!r}")

    def step(self, efields: dict) -> dict:
        requests = {
            int(mid): {"efield_au": np.asarray(vec, dtype=float)}
            for mid, vec in efields.items()
        }
        self._step_codec.send(self.sock, requests)
        return self._result_codec.recv(self.sock)

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass
