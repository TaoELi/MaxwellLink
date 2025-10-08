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
psi4 = pytest.importorskip("psi4", reason="psi4 is required for this test")


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


@pytest.mark.optional
def test_2d_rttddft_psi4_via_socket(plotting=False):
    """
    End-to-end (socket) Psi4 HCN RT-TDDFT test in vacuum.

    Starts the external mxl_driver (Psi4 RT-TDDFT HCN model), runs a Meep simulation that
    couples to the driver via MaxwellLink sockets, collects the dipole moment,
    and checks it against reference values.

    Pass criteria:
        exact match to reference values
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
        cell = mp.Vector3(8, 8, 0)
        geometry = []
        sources_non_molecule = []
        pml_layers = [mp.PML(3.0)]
        resolution = 10

        # one socket-backed molecule; time units 0.1 fs per Meep time
        molecule = mxl.SocketMolecule(
            hub=hub,
            molecule_id=0,
            resolution=resolution,
            center=mp.Vector3(0, 0, 0),
            size=mp.Vector3(1, 1, 1),
            sigma=0.1,
            dimensions=2,
            time_units_fs=0.1,
            rescaling_factor=1.0,  # no rescaling
        )

        sim = mp.Simulation(
            cell_size=cell,
            geometry=geometry,
            sources=sources_non_molecule,
            boundary_layers=pml_layers,
            resolution=resolution,
        )

        if mp.am_master():
            # use a user-built model quantum Hamiltonian in the ./build_tls.py file, which supplies a build_model(**kwargs) function
            # adding a minor relaxation to test the Linblad term 
            # the issue is that when this pytest is run outside this directory, it cannot find the build_tls.py file
            # so we set the path to the current directory explicitly
            current_directory = os.getcwd()
            print("current_directory", current_directory)
            mxl_root = current_directory.split("MaxwellLink")[0] + "MaxwellLink"
            if not os.path.exists(mxl_root):
                raise FileNotFoundError(f"Cannot find MaxwellLink root directory from {current_directory}")
            print("mxl_root", mxl_root)
            xyz_path = os.path.join(mxl_root, "tests", "data", "hcn.xyz")
            test_path = os.path.join(mxl_root, "tests", "test_psi4_rttddft")
            print("xyz_path", xyz_path)
            gamma_relax = 1e-3  # in atomic units
            driver_argv = _resolve_driver_path() + shlex.split(
                f"--model rttddft --port {port} "
                f'--param "molecule_xyz={xyz_path}, functional=SCF, basis=sto-3g, delta_kick_au=1e-1, dt_rttddft_au=0.04" '
            )
            print("driver_argv", driver_argv)
            # Use a fresh, non-blocking subprocess; inherit env/stdio for easy debugging
            proc = subprocess.Popen(driver_argv)

            # Give the client a brief moment to connect before starting the run
            time.sleep(0.5)

        # Run the coupled loop; the driver provides the source amplitude each step
        sim.run(
            mxl.update_molecules(
                hub=hub, sources_non_molecule=[], molecules=[molecule]
            ),
            until=5,
        )

        # --- Only rank 0 collects/asserts (safe under MPI or serial) ---
        if mp.am_master():
            # Extract the history that the driver populated via "additional_data"
            mu_z_au = np.array(
                [np.real(ad["mu_z_au"]) for ad in molecule.additional_data_history]
            )
            # time reported in atomic units in this socket path
            time_au = np.array(
                [np.real(ad["time_au"]) for ad in molecule.additional_data_history]
            )

            # save to file
            # np.savetxt(f"{test_path}/test_meep_2d_socket_rttddft_mu_z_au_ref.txt", np.c_[time_au, mu_z_au])

            # read from file
            data = np.loadtxt(f"{test_path}/test_meep_2d_socket_rttddft_mu_z_au_ref.txt")
            time_au_ref = data[:, 0]
            mu_z_au_ref = data[:, 1]

            assert np.allclose(time_au, time_au_ref, atol=1e-8)
            assert np.allclose(mu_z_au, mu_z_au_ref, atol=1e-8)

            # add plotting for debug
            if plotting:
                import matplotlib.pyplot as plt

                plt.plot(time_au, mu_z_au, label="meep+socket")
                plt.plot(time_au_ref, mu_z_au_ref, "y--", label="reference calculation")
                plt.xlabel("time (a.u.)")
                plt.ylabel(r"$\mu_z$ (a.u.)")
                plt.legend()
                plt.show()

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
    test_2d_rttddft_psi4_via_socket(plotting=True)
