# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

import os
import numpy as np
import pytest

mp = pytest.importorskip("meep", reason="MEEP/pymeep is required for this test")
mxl = pytest.importorskip("maxwelllink", reason="maxwelllink is required for this test")
psi4 = pytest.importorskip("psi4", reason="psi4 is required for this test")
ase = pytest.importorskip("ase", reason="ASE is required for this test")

current_directory = os.getcwd()
print("current_directory", current_directory)
mxl_root = current_directory.split("MaxwellLink")[0] + "MaxwellLink"
if not os.path.exists(mxl_root):
    raise FileNotFoundError(
        f"Cannot find MaxwellLink root directory from {current_directory}"
    )
print("mxl_root", mxl_root)
xyz_path = os.path.join(mxl_root, "tests", "data", "hcn.xyz")
print("xyz_path", xyz_path)


@pytest.mark.slow
def test_rteh_ase_bomd_comparison(n_run=10, plotting=False):
    """
    This test compares the BOMD results from PSI4 in our RT-Ehrenfest implementation
    and ASE (PSI4) BOMD interface for cross-validation between the two implementations.
    A constant electric field is applied to drive the dynamics (because MaxwellLink
    is designed to work with electrodynamics simulations).

    Pass criteria:
        exact match between the two implementations
    """
    traj_R_rt = []
    traj_R_ase = []

    model_rt = mxl.RTEhrenfestModel(
        engine="psi4",
        molecule_xyz=xyz_path,
        functional="b3lyp",
        basis="sto-3g",
        dt_rttddft_au=10.0,
        delta_kick_au=0.0,
        memory="2GB",
        verbose=True,
        remove_permanent_dipole=False,
        n_fock_per_nuc=1,
        n_elec_per_fock=1,
        homo_to_lumo=False,
        force_type="bo",
        partial_charges=[1.0, -1.0, 0.0],
        electron_propagation="etrs",
    )
    model_rt.initialize(dt_new=10.0, molecule_id=0)
    for i in range(n_run):
        model_rt.propagate(effective_efield_vec=np.array([1e-2, 1e-2, 0.0]))
    traj_R_rt = model_rt.traj_R

    model_ase = mxl.ASEModel(
        atoms=xyz_path,
        calculator="psi4",
        calc_kwargs="method=b3lyp, basis=sto-3g",
        charges="[1.0 -1.0 0.0]",
    )
    model_ase.initialize(dt_new=10.0, molecule_id=0)
    angstrom2bohr = 1.8897259886
    positions = model_ase._snapshot()["positions"]
    traj_R_ase.append(positions * angstrom2bohr)
    for i in range(n_run):
        model_ase.propagate(effective_efield_vec=np.array([1e-2, 1e-2, 0.0]))
        snapshot = model_ase._snapshot()
        positions = snapshot["positions"]
        traj_R_ase.append(positions * angstrom2bohr)

    # plot dynamics
    bond_rt = [np.sum((R[0, :] - R[1, :]) ** 2) ** 0.5 for R in traj_R_rt]
    bond_ase = [np.sum((R[0, :] - R[1, :]) ** 2) ** 0.5 for R in traj_R_ase]
    assert np.allclose(bond_rt, bond_ase, atol=1e-6)

    if plotting:
        import matplotlib.pyplot as plt

        plt.figure(figsize=(10, 5))
        plt.plot(bond_rt, label="RT-Ehrenfest (PSI4)")
        plt.plot(bond_ase, label="ASE (PSI4)")
        plt.xlabel("Time steps")
        plt.ylabel("Nuclear Positions (Bohr)")
        plt.legend()
        plt.title("BOMD Dynamics Comparison")
        plt.show()


@pytest.mark.slow
def test_rteh_rtdynamics_ase_bomd_comparison(n_run=10, plotting=False):
    """
    This test compares the MD results from PSI4 in our RT-Ehrenfest implementation
    (using Ehrenfest gradients) and ASE (PSI4) BOMD interface for cross-validation between
    the two implementations.

    Pass criteria:
        exact match between the two implementations
    """
    traj_R_rt = []
    traj_R_ase = []

    model_rt = mxl.RTEhrenfestModel(
        engine="psi4",
        molecule_xyz=xyz_path,
        functional="b3lyp",
        basis="sto-3g",
        dt_rttddft_au=0.1,
        delta_kick_au=0.0,
        memory="2GB",
        verbose=True,
        remove_permanent_dipole=False,
        n_fock_per_nuc=1,
        n_elec_per_fock=10,
        homo_to_lumo=False,
        force_type="ehrenfest",
        electron_propagation="etrs",
    )
    model_rt.initialize(dt_new=1.0, molecule_id=0)
    for i in range(n_run):
        model_rt.propagate(effective_efield_vec=np.array([0.0, 0.0, 0.0]))
    traj_R_rt = model_rt.traj_R

    model_ase = mxl.ASEModel(
        atoms=xyz_path,
        calculator="psi4",
        calc_kwargs="method=b3lyp, basis=sto-3g",
        charges="[0.0 0.0 0.0]",
    )
    model_ase.initialize(dt_new=1.0, molecule_id=0)
    angstrom2bohr = 1.8897259886
    positions = model_ase._snapshot()["positions"]
    traj_R_ase.append(positions * angstrom2bohr)
    for i in range(n_run):
        model_ase.propagate(effective_efield_vec=np.array([0.0, 0.0, 0.0]))
        snapshot = model_ase._snapshot()
        positions = snapshot["positions"]
        traj_R_ase.append(positions * angstrom2bohr)

    # plot dynamics
    bond_rt = [np.sum((R[0, :] - R[1, :]) ** 2) ** 0.5 for R in traj_R_rt]
    bond_ase = [np.sum((R[0, :] - R[1, :]) ** 2) ** 0.5 for R in traj_R_ase]
    assert np.allclose(bond_rt, bond_ase, atol=1e-6)

    if plotting:
        import matplotlib.pyplot as plt

        plt.figure(figsize=(10, 5))
        plt.plot(bond_rt, label="RT-Ehrenfest (PSI4)")
        plt.plot(bond_ase, label="ASE (PSI4)")
        plt.xlabel("Time steps")
        plt.ylabel("Nuclear Positions (Bohr)")
        plt.legend()
        plt.title("BOMD Dynamics Comparison")
        plt.show()


if __name__ == "__main__":
    test_rteh_rtdynamics_ase_bomd_comparison(n_run=30, plotting=True)
    test_rteh_ase_bomd_comparison(n_run=30, plotting=True)
