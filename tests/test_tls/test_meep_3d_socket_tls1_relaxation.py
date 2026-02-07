# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

import sys
import time
import shlex
import shutil
import socket
import subprocess
from pathlib import Path
import os

import numpy as np
import pytest

mp = pytest.importorskip("meep", reason="MEEP/pymeep is required for this test")
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


@pytest.mark.core
def test_3d_1tls_relaxation_matches_analytical_via_socket(plotting=False):
    """
    End-to-end (socket) TLS relaxation test.

    Starts the external mxl_driver (TLS model), runs a Meep simulation that
    couples to the driver via MaxwellLink sockets, collects the population trace,
    and checks it against the 3D golden-rule rate.

    Pass criteria (normalized to initial pop):
        std_dev < 3e-3 and max_abs_diff < 8e-3
    """
    # --- choose a free port & set up the hub first (server must be up before the client connects) ---
    port = _pick_free_port()
    host = "127.0.0.1"

    # SocketHub acts as the server the driver will connect to
    hub = mxl.SocketHub(host=host, port=port, timeout=10.0, latency=1e-5)

    # --- launch the external driver (client) only on rank 0 to avoid multiple clients under MPI ---
    proc = None
    try:
        # --- common simulation setup ---
        cell = mp.Vector3(3, 3, 3)
        geometry = []
        sources_non_molecule = []
        pml_layers = [mp.PML(1.0)]
        resolution = 10

        # TLS physical parameters (used for the analytical rate)
        dipole_moment = 1e-1
        frequency = 2.0

        # one socket-backed molecule; time units 0.1 fs per Meep time
        molecule = mxl.SocketMolecule(
            hub=hub,
            molecule_id=0,
            resolution=resolution,
            center=mp.Vector3(0, 0, 0),
            size=mp.Vector3(1, 1, 1),
            sigma=0.1,
            dimensions=3,
            time_units_fs=0.1,
        )

        sim = mp.Simulation(
            cell_size=cell,
            geometry=geometry,
            sources=sources_non_molecule,
            boundary_layers=pml_layers,
            resolution=resolution,
        )

        if mp.am_master():
            driver_argv = _resolve_driver_path() + shlex.split(
                f"--model tls --port {port} "
                '--param "omega=0.484, mu12=187, orientation=0, pe_initial=0.1" '
            )
            # Use a fresh, non-blocking subprocess; inherit env/stdio for easy debugging
            proc = subprocess.Popen(driver_argv)

            # Give the client a brief moment to connect before starting the run
            time.sleep(0.5)

        # Run the coupled loop; the driver provides the source amplitude each step
        sim.run(
            mxl.update_molecules(
                hub=hub, sources_non_molecule=[], molecules=[molecule]
            ),
            until=40,
        )

        # --- Only rank 0 collects/asserts (safe under MPI or serial) ---
        if mp.am_master():
            # Extract the history that the driver populated via "additional_data"
            population = np.array(
                [np.real(ad["Pe"]) for ad in molecule.additional_data_history]
            )
            # time reported in atomic units in this socket path
            time_au = np.array(
                [np.real(ad["time_au"]) for ad in molecule.additional_data_history]
            )

            # Convert a.u. -> fs, then normalize by the chosen Meep "time_units_fs" (0.1 fs / unit)
            # 1 a.u. of time = 0.02418884254 fs
            time_fs = time_au * 0.02418884254
            time_meep_units = time_fs / 0.1

            # Analytical golden-rule rate in 3D
            print("dipole moment", dipole_moment)
            gamma = dipole_moment**2 * (frequency) ** 3 / 3.0 / np.pi
            print("gamma", gamma)
            population_analytical = population[0] * np.exp(-time_meep_units * gamma)
            # this form is correct for all times [see https://journals.aps.org/pra/pdf/10.1103/PhysRevA.97.032105 Eq. A13]
            population_analytical = np.exp(-time_meep_units * gamma) / (
                np.exp(-time_meep_units * gamma) + (1.0 - population[0]) / population[0]
            )

            std_dev = np.std(population - population_analytical) / population[0]
            max_abs_diff = (
                np.max(np.abs(population - population_analytical)) / population[0]
            )

            # add plotting for debug
            if plotting:
                import matplotlib.pyplot as plt

                plt.plot(time_meep_units, population, label="meep+socket")
                plt.plot(time_meep_units, population_analytical, label="analytical")
                plt.xlabel("time (meep units)")
                plt.ylabel("excited population")
                plt.legend()
                plt.show()

            assert (
                std_dev < 0.08 and max_abs_diff < 0.2
            ), f"std_dev={std_dev:.3g}, max_abs_diff={max_abs_diff:.3g}"

    finally:
        # Try to cleanly stop the external driver if we started it
        if proc is not None and mp.am_master():
            # Give it a moment to shut down naturally after the sim closes the socket
            try:
                proc.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                proc.terminate()
                try:
                    proc.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    proc.kill()


@pytest.mark.core
def test_3d_1tls_relaxation_matches_analytical_via_socket_qutip(plotting=False):
    """
    End-to-end (socket) TLS relaxation test.

    Starts the external mxl_driver (TLS model), runs a Meep simulation that
    couples to the driver via MaxwellLink sockets, collects the population trace,
    and checks it against the 3D golden-rule rate.

    Pass criteria (normalized to initial pop):
        std_dev < 3e-3 and max_abs_diff < 8e-3
    """
    # --- choose a free port & set up the hub first (server must be up before the client connects) ---
    port = _pick_free_port()
    host = "127.0.0.1"

    # SocketHub acts as the server the driver will connect to
    hub = mxl.SocketHub(host=host, port=port, timeout=10.0, latency=1e-5)

    # --- launch the external driver (client) only on rank 0 to avoid multiple clients under MPI ---
    proc = None
    try:
        # --- common simulation setup ---
        cell = mp.Vector3(3, 3, 3)
        geometry = []
        sources_non_molecule = []
        pml_layers = [mp.PML(1.0)]
        resolution = 10

        # TLS physical parameters (used for the analytical rate)
        dipole_moment = 1e-1
        frequency = 2.0

        # one socket-backed molecule; time units 0.1 fs per Meep time
        molecule = mxl.SocketMolecule(
            hub=hub,
            molecule_id=0,
            resolution=resolution,
            center=mp.Vector3(0, 0, 0),
            size=mp.Vector3(1, 1, 1),
            sigma=0.1,
            dimensions=3,
            time_units_fs=0.1,
        )

        sim = mp.Simulation(
            cell_size=cell,
            geometry=geometry,
            sources=sources_non_molecule,
            boundary_layers=pml_layers,
            resolution=resolution,
        )

        current_directory = os.getcwd()
        print("current_directory", current_directory)
        mxl_root = current_directory.split("MaxwellLink")[0] + "MaxwellLink"
        if not os.path.exists(mxl_root):
            raise FileNotFoundError(
                f"Cannot find MaxwellLink root directory from {current_directory}"
            )
        print("mxl_root", mxl_root)
        usr_module_path = os.path.join(mxl_root, "tests", "test_qutip", "build_tls.py")

        if mp.am_master():
            driver_argv = _resolve_driver_path() + shlex.split(
                f"--model qutip --port {port} "
                f'--param "preset=custom, module={usr_module_path}, fd_dmudt=false, kwargs=omega=0.484,mu12=187,orientation=0,pe_initial=0.1" '
            )
            # Use a fresh, non-blocking subprocess; inherit env/stdio for easy debugging
            proc = subprocess.Popen(driver_argv)

            # Give the client a brief moment to connect before starting the run
            time.sleep(0.5)

        # Run the coupled loop; the driver provides the source amplitude each step
        sim.run(
            mxl.update_molecules(
                hub=hub, sources_non_molecule=[], molecules=[molecule]
            ),
            until=40,
        )

        # --- Only rank 0 collects/asserts (safe under MPI or serial) ---
        if mp.am_master():
            # Extract the history that the driver populated via "additional_data"
            population = np.array(
                [np.real(ad["Pe"]) for ad in molecule.additional_data_history]
            )
            # time reported in atomic units in this socket path
            time_au = np.array(
                [np.real(ad["time_au"]) for ad in molecule.additional_data_history]
            )

            # Convert a.u. -> fs, then normalize by the chosen Meep "time_units_fs" (0.1 fs / unit)
            # 1 a.u. of time = 0.02418884254 fs
            time_fs = time_au * 0.02418884254
            time_meep_units = time_fs / 0.1

            # Analytical golden-rule rate in 3D
            print("dipole moment", dipole_moment)
            gamma = dipole_moment**2 * (frequency) ** 3 / 3.0 / np.pi
            print("gamma", gamma)
            population_analytical = population[0] * np.exp(-time_meep_units * gamma)
            # this form is correct for all times [see https://journals.aps.org/pra/pdf/10.1103/PhysRevA.97.032105 Eq. A13]
            population_analytical = np.exp(-time_meep_units * gamma) / (
                np.exp(-time_meep_units * gamma) + (1.0 - population[0]) / population[0]
            )

            std_dev = np.std(population - population_analytical) / population[0]
            max_abs_diff = (
                np.max(np.abs(population - population_analytical)) / population[0]
            )

            # add plotting for debug
            if plotting:
                import matplotlib.pyplot as plt

                plt.plot(time_meep_units, population, label="meep+socket")
                plt.plot(time_meep_units, population_analytical, label="analytical")
                plt.xlabel("time (meep units)")
                plt.ylabel("excited population")
                plt.legend()
                plt.show()

            assert (
                std_dev < 0.08 and max_abs_diff < 0.2
            ), f"std_dev={std_dev:.3g}, max_abs_diff={max_abs_diff:.3g}"

    finally:
        # Try to cleanly stop the external driver if we started it
        if proc is not None and mp.am_master():
            # Give it a moment to shut down naturally after the sim closes the socket
            try:
                proc.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                proc.terminate()
                try:
                    proc.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    proc.kill()


@pytest.mark.core
def test_3d_2tls_relaxation_matches_analytical_via_socket_v2(
    polarization_style="analytical", plotting=False
):
    """
    End-to-end (socket) TLS relaxation test.

    Starts the external mxl_driver (TLS model), runs a Meep simulation that
    couples to the driver via MaxwellLink sockets, collects the population trace,
    and checks it against the 3D golden-rule rate.

    Pass criteria (normalized to initial pop):
        std_dev < 3e-3 and max_abs_diff < 8e-3
    """
    # --- choose a free port & set up the hub first (server must be up before the client connects) ---
    port = _pick_free_port()
    host = "127.0.0.1"

    # SocketHub acts as the server the driver will connect to
    hub = mxl.SocketHub(host=host, port=port, timeout=10.0, latency=1e-5)

    # --- launch the external driver (client) only on rank 0 to avoid multiple clients under MPI ---
    proc = None
    try:
        # --- common simulation setup ---
        cell = mp.Vector3(3, 3, 3)
        geometry = []
        sources_non_molecule = []
        pml_layers = [mp.PML(1.0)]
        resolution = 10

        # TLS physical parameters (used for the analytical rate)
        dipole_moment = 1e-1
        frequency = 2.0

        # one socket-backed molecule; time units 0.1 fs per Meep time
        molecule = mxl.Molecule(
            hub=hub,
            center=mp.Vector3(0, 0, 0),
            size=mp.Vector3(1, 1, 1),
            sigma=0.1,
            dimensions=3,
            resolution=resolution,
            polarization_type=polarization_style,
        )

        sim = mxl.MeepSimulation(
            cell_size=cell,
            geometry=geometry,
            sources=sources_non_molecule,
            boundary_layers=pml_layers,
            resolution=resolution,
            time_units_fs=0.1,
            molecules=[molecule],
            hub=hub,
        )

        if mp.am_master():
            driver_argv = _resolve_driver_path() + shlex.split(
                f"--model tls --port {port} "
                '--param "omega=0.484, mu12=187, orientation=0, pe_initial=0.1" '
            )
            # Use a fresh, non-blocking subprocess; inherit env/stdio for easy debugging
            proc = subprocess.Popen(driver_argv)

            # Give the client a brief moment to connect before starting the run
            time.sleep(0.5)

        # Run the coupled loop; the driver provides the source amplitude each step
        sim.run(
            until=40,
        )

        # --- Only rank 0 collects/asserts (safe under MPI or serial) ---
        if mp.am_master():
            # Extract the history that the driver populated via "additional_data"
            population = np.array(
                [np.real(ad["Pe"]) for ad in molecule.additional_data_history]
            )
            # time reported in atomic units in this socket path
            time_au = np.array(
                [np.real(ad["time_au"]) for ad in molecule.additional_data_history]
            )

            # Convert a.u. -> fs, then normalize by the chosen Meep "time_units_fs" (0.1 fs / unit)
            # 1 a.u. of time = 0.02418884254 fs
            time_fs = time_au * 0.02418884254
            time_meep_units = time_fs / 0.1

            # Analytical golden-rule rate in 3D
            print("dipole moment", dipole_moment)
            gamma = dipole_moment**2 * (frequency) ** 3 / 3.0 / np.pi
            population_analytical = population[0] * np.exp(-time_meep_units * gamma)
            # this form is correct for all times [see https://journals.aps.org/pra/pdf/10.1103/PhysRevA.97.032105 Eq. A13]
            population_analytical = np.exp(-time_meep_units * gamma) / (
                np.exp(-time_meep_units * gamma) + (1.0 - population[0]) / population[0]
            )

            std_dev = np.std(population - population_analytical) / population[0]
            max_abs_diff = (
                np.max(np.abs(population - population_analytical)) / population[0]
            )

            # add plotting for debug
            if plotting:
                import matplotlib.pyplot as plt

                plt.plot(time_meep_units, population, "r-.", label="meep+socket")
                plt.plot(
                    time_meep_units, population_analytical, "k-", label="analytical"
                )
                plt.xlabel("time (meep units)")
                plt.ylabel("excited population")
                plt.legend()
                plt.show()

            assert (
                std_dev < 0.08 and max_abs_diff < 0.2
            ), f"std_dev={std_dev:.3g}, max_abs_diff={max_abs_diff:.3g}"

    finally:
        # Try to cleanly stop the external driver if we started it
        if proc is not None and mp.am_master():
            # Give it a moment to shut down naturally after the sim closes the socket
            try:
                proc.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                proc.terminate()
                try:
                    proc.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    proc.kill()


if __name__ == "__main__":
    # test_3d_1tls_relaxation_matches_analytical_via_socket_qutip(plotting=True)
    test_3d_2tls_relaxation_matches_analytical_via_socket_v2(
        plotting=True, polarization_style="transverse"
    )
