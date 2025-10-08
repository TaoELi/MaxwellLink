import sys
import time
import shlex
import shutil
import socket
import subprocess
from pathlib import Path

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
    Return an argv list to run mxl_driver.

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
def test_2d_2tls_relaxation_matches_analytical_via_socket(plotting=False):
    """
    End-to-end (socket) TLS relaxation test in the superradiance limit.

    Starts the external mxl_driver (2 TLS models), runs a Meep simulation that
    couples to the driver via MaxwellLink sockets, collects the population trace,
    and checks it against the 2D golden-rule rate.

    Pass criteria (normalized to initial pop):
        std_dev < 3e-3 and max_abs_diff < 8e-3
    """
    # --- choose a free port & set up the hub first (server must be up before the client connects) ---
    port = _pick_free_port()
    host = "127.0.0.1"

    # SocketHub acts as the server the driver will connect to
    hub = mxl.SocketHub(host=host, port=port, timeout=10.0, latency=1e-5)

    # --- launch the external driver (client) only on rank 0 to avoid multiple clients under MPI ---
    proc1 = None
    proc2 = None
    try:
        # --- common simulation setup ---
        cell = mp.Vector3(8, 8, 0)
        geometry = []
        sources_non_molecule = []
        pml_layers = [mp.PML(3.0)]
        resolution = 10

        # TLS physical parameters (used for the analytical rate)
        dipole_moment = 1e-1 / np.sqrt(2)  # rescale for two TLS
        frequency = 1.0

        # one socket-backed molecule; time units 0.1 fs per Meep time
        molecule1 = mxl.SocketMolecule(
            hub=hub,
            molecule_id=0,
            resolution=resolution,
            center=mp.Vector3(0, 0, 0),
            size=mp.Vector3(1, 1, 1),
            sigma=0.1,
            dimensions=2,
            time_units_fs=0.1,
        )

        molecule2 = mxl.SocketMolecule(
            hub=hub,
            molecule_id=1,
            resolution=resolution,
            center=mp.Vector3(0, 0, 0),
            size=mp.Vector3(1, 1, 1),
            sigma=0.1,
            dimensions=2,
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
            mu12 = 187 / np.sqrt(2)  # rescale for two TLS
            driver_argv = _resolve_driver_path() + shlex.split(
                f"--model tls --port {port} "
                f'--param "omega=0.242, mu12={mu12}, orientation=2, pe_initial=1e-4" '
            )
            # Use a fresh, non-blocking subprocess; inherit env/stdio for easy debugging
            proc1 = subprocess.Popen(driver_argv)

            proc2 = subprocess.Popen(driver_argv)

            # Give the client a brief moment to connect before starting the run
            time.sleep(0.5)

        # Run the coupled loop; the driver provides the source amplitude each step
        sim.run(
            mxl.update_molecules(
                hub=hub, sources_non_molecule=[], molecules=[molecule1, molecule2]
            ),
            until=90,
        )

        # --- Only rank 0 collects/asserts (safe under MPI or serial) ---
        if mp.am_master():
            # Extract the history that the driver populated via "additional_data"
            population = np.array(
                [np.real(ad["Pe"]) for ad in molecule1.additional_data_history]
            )
            # time reported in atomic units in this socket path
            time_au = np.array(
                [np.real(ad["time_au"]) for ad in molecule1.additional_data_history]
            )

            # Convert a.u. -> fs, then normalize by the chosen Meep "time_units_fs" (0.1 fs / unit)
            # 1 a.u. of time = 0.02418884254 fs
            time_fs = time_au * 0.02418884254
            time_meep_units = time_fs / 0.1

            # Analytical golden-rule rate in 2D
            print("dipole moment", dipole_moment)
            gamma = dipole_moment**2 * (frequency) ** 2 / 2.0 * 2
            print("gamma", gamma)
            population_analytical = population[0] * np.exp(-time_meep_units * gamma)
            # this form is correct for all times [see https://journals.aps.org/pra/pdf/10.1103/PhysRevA.97.032105 Eq. A13]
            population_analytical = np.exp(-time_meep_units * gamma) / (np.exp(-time_meep_units * gamma) + (1.0 - population[0]) / population[0])

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
                std_dev < 3e-3 and max_abs_diff < 8e-3
            ), f"std_dev={std_dev:.3g}, max_abs_diff={max_abs_diff:.3g}"

    finally:
        # Try to cleanly stop the external driver if we started it
        if proc1 is not None and mp.am_master():
            # Give it a moment to shut down naturally after the sim closes the socket
            try:
                proc1.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                proc1.terminate()
                try:
                    proc1.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    proc1.kill()

        if proc2 is not None and mp.am_master():
            # Give it a moment to shut down naturally after the sim closes the socket
            try:
                proc2.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                proc2.terminate()
                try:
                    proc2.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    proc2.kill()


if __name__ == "__main__":
    test_2d_2tls_relaxation_matches_analytical_via_socket(plotting=True)
