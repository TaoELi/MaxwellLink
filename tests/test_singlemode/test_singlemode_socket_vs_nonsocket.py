# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

import sys
import time
import shutil
import socket
import subprocess
from pathlib import Path

import numpy as np
import pytest

mxl = pytest.importorskip("maxwelllink", reason="maxwelllink is required for this test")


def _pick_free_port() -> int:
    """Ask the OS for a free TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _resolve_driver_path() -> list[str]:
    """
    Return an argv list to run mxl_driver.py.

    Prefers the script located in the same directory as this test file.
    Falls back to a script on PATH.
    Runs with the current Python to avoid venv mismatches.
    """
    # 1) local file next to this test
    here = Path(__file__).resolve().parent
    local = here / "mxl_driver"
    if local.exists():
        return [sys.executable, "-u", str(local)]

    # 2) installed console script on PATH
    which = shutil.which("mxl_driver")
    if which:
        # Use directly; assume it has correct shebang/venv
        return [which]

    pytest.skip("mxl_driver not found (neither next to the test nor on PATH).")


def _run_single_mode_nonsocket_simulation(
    *,
    frequency_au: float,
    mu12: float,
    dt_au: float,
    damping_au: float,
    coupling_strength: float,
    total_steps: int,
    qc_initial: float,
    orientation: int,
    pe_initial: float,
) -> tuple[np.ndarray, np.ndarray]:
    """Run the single-mode cavity with an embedded (non-socket) TLS driver."""

    molecule = mxl.Molecule(
        driver="tls",
        driver_kwargs={
            "omega": frequency_au,
            "mu12": mu12,
            "orientation": orientation,
            "pe_initial": pe_initial,
        },
    )

    sim = mxl.SingleModeSimulation(
        dt_au=dt_au,
        frequency_au=frequency_au,
        damping_au=damping_au,
        molecules=[molecule],
        drive=0.0,
        coupling_strength=coupling_strength,
        qc_initial=qc_initial,
        pc_initial=0.0,
        record_history=True,
    )

    sim.run(steps=total_steps)
    return np.asarray(sim.time_history), np.asarray(sim.qc_history)


def _run_single_mode_socket_simulation(
    *,
    hub: mxl.SocketHub,
    frequency_au: float,
    dt_au: float,
    damping_au: float,
    coupling_strength: float,
    total_steps: int,
    qc_initial: float,
) -> tuple[np.ndarray, np.ndarray]:
    """Run the single-mode cavity with a socket-connected TLS driver."""

    molecule = mxl.Molecule(hub=hub)
    sim = mxl.SingleModeSimulation(
        dt_au=dt_au,
        frequency_au=frequency_au,
        damping_au=damping_au,
        molecules=[molecule],
        drive=0.0,
        coupling_strength=coupling_strength,
        qc_initial=qc_initial,
        pc_initial=0.0,
        hub=hub,
        record_history=True,
    )

    sim.run(steps=total_steps)
    return np.asarray(sim.time_history), np.asarray(sim.qc_history)


@pytest.mark.core
def test_singlemode_tls_socket_vs_nonsocket(plotting: bool = False):
    """
    End-to-end test for single-mode TLS via sockets and non-socket connections.

    Pass criteria:
        The cavity coordinate history must match between the two coupling modes.
    """

    frequency_au = 0.242
    mu12 = 187.0
    dt_au = 0.5
    damping_au = 0.0
    coupling_strength = 1e-4
    total_steps = 4096
    qc_initial = 1e-5
    orientation = 2
    pe_initial = 0.0

    time_nonsocket, qc_nonsocket = _run_single_mode_nonsocket_simulation(
        frequency_au=frequency_au,
        mu12=mu12,
        dt_au=dt_au,
        damping_au=damping_au,
        coupling_strength=coupling_strength,
        total_steps=total_steps,
        qc_initial=qc_initial,
        orientation=orientation,
        pe_initial=pe_initial,
    )

    # --- choose a free port & set up the hub first (server must be up before the client connects) ---
    port = _pick_free_port()
    host = "127.0.0.1"

    hub = mxl.SocketHub(host=host, port=port, timeout=10.0, latency=1e-5)

    time_socket = None
    qc_socket = None

    proc = None
    try:
        driver_argv = _resolve_driver_path() + [
            "--model",
            "tls",
            "--address",
            host,
            "--port",
            str(port),
            "--param",
            f"omega={frequency_au}, mu12={mu12}, orientation={orientation}, pe_initial={pe_initial}",
        ]

        proc = subprocess.Popen(driver_argv)
        time.sleep(0.5)

        time_socket, qc_socket = _run_single_mode_socket_simulation(
            hub=hub,
            frequency_au=frequency_au,
            dt_au=dt_au,
            damping_au=damping_au,
            coupling_strength=coupling_strength,
            total_steps=total_steps,
            qc_initial=qc_initial,
        )

    finally:
        try:
            hub.stop()
        except Exception:
            pass

        if proc is not None:
            try:
                proc.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                proc.terminate()
                try:
                    proc.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    proc.kill()

    assert time_socket is not None and qc_socket is not None

    np.testing.assert_allclose(
        time_socket,
        time_nonsocket,
        rtol=0.0,
        atol=1e-12,
        err_msg="Socket and non-socket simulations produced different time grids.",
    )
    np.testing.assert_allclose(
        qc_socket,
        qc_nonsocket,
        rtol=1e-9,
        atol=1e-12,
        err_msg="Socket and non-socket cavity trajectories diverge.",
    )

    if plotting:
        import matplotlib.pyplot as plt

        plt.figure(figsize=(7, 4))
        plt.plot(time_nonsocket, qc_nonsocket, label="Non-socket TLS")
        plt.plot(time_socket, qc_socket, "--", label="Socket TLS")
        plt.xlabel("time (a.u.)")
        plt.ylabel("q_c (a.u.)")
        plt.legend()
        plt.tight_layout()
        plt.show()


if __name__ == "__main__":
    test_singlemode_tls_socket_vs_nonsocket(plotting=True)
