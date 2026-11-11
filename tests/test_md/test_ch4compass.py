# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# --------------------------------------------------------------------------------------#

"""
End-to-end tests: ch4compass energies, forces and NVE dynamics vs LAMMPS on the
reference 100-CH4 box.

The references in ``tests/data/ch4compass_reference.npz`` come from LAMMPS (10 Dec
2025, CLASS2 + KSPACE + MOLECULE) on the bundled geometry: the production setup of the
VSC runs (``lj/class2/coul/long 10.0`` Angstrom + ``pppm 1e-4``), the same with a
tightly converged ``ewald 1e-8`` sum and the cutoff clamped to half the box (9.18
Angstrom, as MaxwellLink does), the bonded + Lennard-Jones terms alone, and 200-step
NVE trajectories from rest. The LAMMPS inputs live in
``projects/2026-10-15-CH4CH4ForceFields/lammps_reference``.
"""

import os

import numpy as np
import pytest

ch4_mod = pytest.importorskip(
    "maxwelllink.matter.md.force_fields.ch4compass",
    reason="maxwelllink is required for this test",
)
CH4COMPASSForceField = ch4_mod.CH4COMPASSForceField

_REFERENCE = os.path.join(
    os.path.dirname(__file__), os.pardir, "data", "ch4compass_reference.npz"
)


@pytest.fixture(scope="module")
def reference():
    """The LAMMPS reference data of the bundled 100-CH4 box."""

    return dict(np.load(_REFERENCE))


def _two_molecule_cluster():
    """Two equilibrium CH4 molecules side by side, no periodic box."""

    one = CH4COMPASSForceField._single_molecule()
    positions = np.vstack([one, one + np.array([7.5, 0.5, 0.5])])
    return CH4COMPASSForceField(positions=positions, box=None)


@pytest.mark.core
def test_energy_terms_match_lammps(reference):
    """Bond, angle (with bond-bond coupling), LJ and Coulomb energies vs LAMMPS."""

    ff = CH4COMPASSForceField()  # zero-config default: bundled 100-CH4 periodic box
    assert ff.n_atoms == 500 and ff.n_molecules == 100
    np.testing.assert_allclose(ff.box, 18.36 * 1.889726124565062)

    x5 = ff.positions.reshape(100, 5, 3)
    terms = np.zeros(2)
    ff.compute(ff.positions, np.zeros(3), terms)
    _, v_lj = ff._lennard_jones(x5)
    _, v_coul = ff._coulomb_site_forces(x5)

    # identical formulas to LAMMPS class2 and lj/class2: machine precision
    assert abs(terms[0] - reference["ebond"]) < 1e-10
    assert abs(terms[1] - reference["eangle"]) < 1e-10
    assert abs(v_lj - reference["evdwl"]) < 1e-10
    # Ewald vs tightly converged LAMMPS Ewald; observed 3.5e-6 Ha
    assert abs(v_coul - reference["ecoul_ewald"]) < 2e-5


@pytest.mark.core
def test_forces_match_lammps(reference):
    """Default (periodic) CH4 force field reproduces the LAMMPS reference forces."""

    ff = CH4COMPASSForceField()
    forces, _ = ff.compute(ff.positions, np.zeros(3))  # field-free forces
    assert forces.shape == (500, 3)

    # bonded + 9-6 Lennard-Jones alone: identical formulas, machine precision
    x5 = ff.positions.reshape(100, 5, 3)
    f_intra, _, _ = ff._intramolecular(x5)
    f_lj, _ = ff._lennard_jones(x5)
    np.testing.assert_allclose(
        (f_intra + f_lj).reshape(500, 3), reference["forces_lj_bonded"], atol=1e-12
    )

    # full forces vs LAMMPS with a tightly converged Ewald sum and the clamped
    # cutoff; observed 1.5e-7 Ha/Bohr (the Ewald truncation of the kernels)
    assert np.max(np.abs(forces - reference["forces_ewald"])) < 1e-6

    # full forces vs the production LAMMPS setup (pppm 1e-4): the remaining
    # difference is the PPPM grid error plus the 10 -> 9.18 Angstrom cutoff clamp;
    # observed max 8.6e-6, rms 1.8e-6 Ha/Bohr
    diff = forces - reference["forces_pppm"]
    assert np.max(np.abs(diff)) < 5e-5
    assert np.sqrt(np.mean(diff**2)) < 1e-5


@pytest.mark.core
def test_forces_are_energy_gradients():
    """Finite differences of the potential reproduce the forces (cluster and box)."""

    rng = np.random.default_rng(0)
    for ff in (_two_molecule_cluster(), CH4COMPASSForceField()):
        x = ff.positions + 0.05 * rng.standard_normal(ff.positions.shape)
        forces, _ = ff.compute(x, np.zeros(3))
        h = 1e-5
        for _ in range(6):
            i, c = rng.integers(ff.n_atoms), rng.integers(3)
            xp, xm = x.copy(), x.copy()
            xp[i, c] += h
            xm[i, c] -= h
            numerical = -(ff.compute(xp, np.zeros(3))[1] - ff.compute(xm, np.zeros(3))[1])
            assert abs(numerical / (2.0 * h) - forces[i, c]) < 1e-8
        assert np.max(np.abs(forces.sum(axis=0))) < 1e-12  # no net force


@pytest.mark.core
def test_single_molecule_field_and_dipole():
    """A lone tetrahedral molecule is nearly relaxed; the field pushes the charges."""

    ff = CH4COMPASSForceField(n_molecules=1)
    forces_free, potential = ff.compute(ff.positions, np.zeros(3))
    # the regular tetrahedron is not the exact class2 minimum (107.6 degree angle)
    assert abs(potential) < 1e-3
    assert np.max(np.abs(forces_free)) < 1e-2

    efield = np.array([1e-3, -2e-3, 5e-4])
    forces, _ = ff.compute(ff.positions, efield)
    charges = np.array([ch4_mod._Q_C] + [ch4_mod._Q_H] * 4)
    np.testing.assert_allclose(forces - forces_free, charges[:, None] * efield, atol=1e-14)

    x = ff.positions + 0.1 * np.random.default_rng(3).standard_normal((5, 3))
    np.testing.assert_allclose(ff.dipole(x), charges @ x, atol=1e-14)
    np.testing.assert_allclose(ff.dipole_velocity(x), charges @ x, atol=1e-14)


@pytest.mark.core
def test_compiled_cpu_kernels_match_numpy():
    """The numba CPU kernels reproduce the NumPy reference for every geometry type."""

    rng = np.random.default_rng(1)
    efield = np.array([1e-3, -2e-3, 5e-4])
    for ff in (
        CH4COMPASSForceField(),
        CH4COMPASSForceField(n_molecules=1),
        _two_molecule_cluster(),
    ):
        x = ff.positions + 0.03 * rng.standard_normal(ff.positions.shape)
        terms_np, terms_nb = np.zeros(2), np.zeros(2)
        f_np, v_np = ff.compute(x, efield, terms_np)
        f_nb, v_nb = ff.compute_fast(x, efield, terms_nb)
        np.testing.assert_allclose(f_nb, f_np, atol=1e-12)
        assert abs(v_nb - v_np) < 1e-12
        np.testing.assert_allclose(terms_nb, terms_np, atol=1e-12)


@pytest.mark.core
def test_nve_from_rest_matches_lammps(reference):
    """200 velocity-Verlet steps from rest follow the LAMMPS trajectories."""

    from maxwelllink.matter import MD

    cell = MD(ff="ch4compass", dt_fs=0.5, init_velocities=False, backend="cpu")
    result = cell.run(n_steps=200, snapshot_every=50, thermostat="nve")
    coords = np.concatenate([cell.engine.x0[None], result.snapshots["coords_bohr"]])
    cell.close()
    assert coords.shape == reference["nve_coords_bohr_ewald"].shape
    # same integrator as LAMMPS with a tight Ewald sum; the light hydrogens amplify
    # the 1e-7 Ha/Bohr Ewald-truncation force difference; observed 1.0e-4 Bohr
    assert np.max(np.abs(coords - reference["nve_coords_bohr_ewald"])) < 1e-3
    # the production PPPM run drifts away by its grid noise; observed 8.3e-3 Bohr
    assert np.max(np.abs(coords - reference["nve_coords_bohr"])) < 5e-2


def test_gpu_kernels_match_numpy():
    """The CUDA kernels (float32 non-bonded sums) reproduce the NumPy reference."""

    cp = pytest.importorskip("cupy", reason="cupy not installed")
    pytest.importorskip("numba", reason="numba not installed")
    try:
        if cp.cuda.runtime.getDeviceCount() < 1:
            pytest.skip("No CUDA device available.")
    except Exception:  # pragma: no cover - cupy present but no usable driver
        pytest.skip("No usable CUDA runtime.")

    ff = CH4COMPASSForceField()
    rng = np.random.default_rng(2)
    x = ff.positions + 0.03 * rng.standard_normal(ff.positions.shape)
    efield = np.array([1e-3, -2e-3, 5e-4])
    terms_np = np.zeros(2)
    f_np, v_np = ff.compute(x, efield, terms_np)

    kernels = ff.build_force_kernels(cp)
    num = 2
    x_dev = cp.asarray(np.broadcast_to(x, (num,) + x.shape).copy())
    f_dev = cp.zeros_like(x_dev)
    v_dev = cp.zeros(num)
    e_dev = cp.asarray(np.broadcast_to(efield, (num, 3)).copy())
    t_dev = cp.zeros((num, 2))
    kernels.forces_gpu(x_dev, f_dev, v_dev, e_dev, t_dev)
    f_gpu, v_gpu, t_gpu = (cp.asnumpy(a) for a in (f_dev, v_dev, t_dev))
    for d in range(num):  # observed 4.9e-8 Ha/Bohr (single-precision non-bonded)
        np.testing.assert_allclose(f_gpu[d], f_np, atol=1e-6)
        assert abs(v_gpu[d] - v_np) < 1e-6
        np.testing.assert_allclose(t_gpu[d], terms_np, atol=1e-12)
