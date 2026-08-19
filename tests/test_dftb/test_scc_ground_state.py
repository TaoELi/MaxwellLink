# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
SCC ground-state tests for the direct DFTB model.
"""

import os
import subprocess
import sys

import numpy as np
import pytest

from slko_helpers import sk_path_for

dftb_mod = pytest.importorskip(
    "maxwelllink.mxl_drivers.python.models.rtdftb_model",
    reason="maxwelllink is required for this test",
)
SlaterKosterSet = dftb_mod.SlaterKosterSet
DFTBSystem = dftb_mod.DFTBSystem
build_h0_overlap = dftb_mod.build_h0_overlap
scf = dftb_mod.scf
total_force = dftb_mod.total_force
dipole_moment = dftb_mod.dipole_moment

_REFERENCE = os.path.join(
    os.path.dirname(__file__), os.pardir, "data", "dftb_scc_reference.npz"
)

# DFTB+'s own forces come from a finite difference, so its numbers carry a ~1e-9
# truncation error of their own; the tolerances below sit comfortably above it.
_TOL_ENERGY = 1e-8  # Hartree
_TOL_FORCE = 1e-6  # Hartree / Bohr
_TOL_CHARGE = 1e-7  # electrons
_TOL_DIPOLE = 1e-7  # atomic units

_SYSTEMS = [
    "gs_h2o",
    "gs_ch2o",
    "gs_nh3",
    "gs_znh2o",
    "gb_ch3oh_gen",
    "gb_h2o_squeezed",
    "gb_h2o_stretched",
    "gb_znoh_gen",
    "gb_znscl_gen",
]


def _reference_or_skip():
    """Return the bundled DFTB+ ground states, or skip when unavailable."""
    if not os.path.isfile(_REFERENCE):
        pytest.skip("DFTB+ SCC reference bundle is not available.")
    return np.load(_REFERENCE, allow_pickle=True)


def _sk_path(reference, name):
    """Parameter-set directory of one bundled system, downloaded on first use."""

    return sk_path_for(
        str(reference[f"{name}/sk_set"]),
        [str(s) for s in reference[f"{name}/species"]],
        skip=pytest.skip,
    )


def _solve(reference, name):
    """Converge one bundled system and return its system, result and force."""
    species = [str(s) for s in reference[f"{name}/species"]]
    max_l = dict(zip(species, [str(v) for v in reference[f"{name}/max_l"]]))
    sk_set = SlaterKosterSet(_sk_path(reference, name), species, max_l)
    system = DFTBSystem(
        [str(s) for s in reference[f"{name}/elements"]],
        reference[f"{name}/positions_ang"],
        sk_set,
    )
    h0, overlap = build_h0_overlap(system)
    result = scf(system, h0, overlap, tolerance=1e-14, max_iterations=500)
    return system, result, total_force(system, result)


@pytest.mark.core
@pytest.mark.parametrize("name", _SYSTEMS)
def test_ground_state_matches_dftbplus(name):
    """Energy, forces, Mulliken charges and dipole all reproduce DFTB+."""
    reference = _reference_or_skip()
    system, result, force = _solve(reference, name)
    assert result.converged

    assert (
        abs(result.energy_mermin - float(reference[f"{name}/mermin_energy"]))
        < _TOL_ENERGY
    )

    # DFTB+ stores both arrays Fortran-style, with the fast index first
    assert np.abs(force - reference[f"{name}/forces"].T).max() < _TOL_FORCE

    charge_reference = reference[f"{name}/orbital_charges"]
    charge = np.zeros_like(charge_reference)
    offset = system.atom_offset
    for atom in range(system.n_atom):
        n_orb = offset[atom + 1] - offset[atom]
        charge[:n_orb, atom] = result.q_orb[offset[atom] : offset[atom + 1]]
    assert np.abs(charge - charge_reference).max() < _TOL_CHARGE

    dipole = dipole_moment(system, result.q_orb, result.layout)
    reference_dipole = reference[f"{name}/dipole_moments"].reshape(-1)
    assert np.abs(dipole - reference_dipole).max() < _TOL_DIPOLE


@pytest.mark.core
def test_force_is_the_gradient_of_the_energy():
    """The analytic force is minus the numerical gradient of our own energy.

    This checks the force expression against the energy expression without DFTB+ in the
    loop, so a disagreement points at the Pulay or gamma-derivative term rather than at
    the reference.
    """
    reference = _reference_or_skip()
    name = "gs_znh2o"  # a d-shell metal, so the spd derivative path is exercised
    species = [str(s) for s in reference[f"{name}/species"]]
    max_l = dict(zip(species, [str(v) for v in reference[f"{name}/max_l"]]))
    sk_set = SlaterKosterSet(_sk_path(reference, name), species, max_l)
    elements = [str(s) for s in reference[f"{name}/elements"]]

    def energy_at(shifted_bohr):
        system = DFTBSystem(elements, shifted_bohr, sk_set, units="bohr")
        h0, overlap = build_h0_overlap(system)
        return scf(
            system, h0, overlap, tolerance=1e-14, max_iterations=500
        ).energy_mermin

    system, result, force = _solve(reference, name)
    coords = system.coords.copy()

    step = 1e-4  # Bohr
    for atom in (0, 1):  # the Zn and the O carry the interesting derivatives
        for axis in range(3):
            plus, minus = coords.copy(), coords.copy()
            plus[atom, axis] += step
            minus[atom, axis] -= step
            numerical = -(energy_at(plus) - energy_at(minus)) / (2.0 * step)
            assert abs(numerical - force[atom, axis]) < 1e-6


@pytest.mark.core
def test_warm_electronic_temperature_smears_the_filling():
    """A warm run converges with Fermi-Dirac occupations and a Mermin free energy."""
    reference = _reference_or_skip()
    name = "gs_h2o"
    species = [str(s) for s in reference[f"{name}/species"]]
    max_l = dict(zip(species, [str(v) for v in reference[f"{name}/max_l"]]))
    sk_set = SlaterKosterSet(_sk_path(reference, name), species, max_l)
    system = DFTBSystem(
        [str(s) for s in reference[f"{name}/elements"]],
        reference[f"{name}/positions_ang"],
        sk_set,
    )
    h0, overlap = build_h0_overlap(system)
    warm = scf(system, h0, overlap, electronic_temperature_au=9.5e-3)  # 3000 K
    assert warm.converged
    assert abs(np.sum(warm.filling) - system.n_electrons()) < 1e-10
    assert warm.entropy_ts >= 0.0
    assert warm.energy_mermin <= warm.energy_total + 1e-12


@pytest.mark.core
def test_kernels_survive_being_compiled_in_any_order():
    """Compiling a kernel before its callers must not poison the process.

    numba freezes a kernel's globals into the IR the first time it compiles, and the
    result is cached for the lifetime of the process. A kernel that resolved a callee
    through a name bound lazily by its own wrapper therefore worked only when the
    wrapper happened to compile it first: call the kernel directly and ``scf`` was dead
    for the rest of the process, unrecoverably. This runs the poisoning order in a fresh
    interpreter, because a defect of this kind cannot be reproduced in a process where
    something else already compiled the kernel correctly.
    """

    reference = _reference_or_skip()
    name = "gs_h2o"
    sk_path = _sk_path(reference, name)
    script = f"""
import numpy as np
from maxwelllink.mxl_drivers.python.models.rtdftb_model import (
    DFTBSystem, SlaterKosterSet, build_h0_overlap, scf, scc)

sk_set = SlaterKosterSet({sk_path!r}, ["O", "H"], {{"O": "p", "H": "s"}})
system = DFTBSystem(["O", "H", "H"],
                    [[0.0, 0.0, 0.0], [0.96, 0.0, 0.0], [-0.24, 0.93, 0.0]], sk_set)
# the poisoning order: the bare kernel compiles before any wrapper touches it
scc.repulsive_sum(sk_set.tables, system.coords, system.atom_species,
                  system.n_atom, np.zeros(2))
h0, overlap = build_h0_overlap(system)
result = scf(system, h0, overlap)
assert result.converged
print("%.10f" % result.energy_mermin)
"""
    finished = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=900
    )
    assert finished.returncode == 0, finished.stderr[-2000:]
    assert float(finished.stdout.strip()) < 0.0


if __name__ == "__main__":
    for system in ("gs_h2o", "gs_znh2o", "gb_znscl_gen"):
        test_ground_state_matches_dftbplus(system)
    test_force_is_the_gradient_of_the_energy()
    print("SCC ground state matches DFTB+ on every tested system")


@pytest.mark.core
def test_fermi_smearing_fills_like_dftbplus():
    """Finite-temperature Fermi-Dirac occupations: count, limits and entropy."""

    from maxwelllink.mxl_drivers.python.models.rtdftb_model.scc import (
        MIN_TEMP,
        fermi_entropy,
        fermi_filling,
    )

    # a gapped spectrum: smearing far below the gap reproduces the zero-T filling
    eigenvalues = np.array([-0.5, -0.4, -0.3, 0.3, 0.4])
    cold = np.zeros(5)
    warm = np.zeros(5)
    fermi_filling(eigenvalues, 6.0, MIN_TEMP, cold)
    e_fermi = fermi_filling(eigenvalues, 6.0, 300.0 * 3.166811563e-6, warm)  # 300 K
    assert np.allclose(cold, [2.0, 2.0, 2.0, 0.0, 0.0])
    assert np.allclose(warm, cold, atol=1e-12)
    assert -0.3 < e_fermi < 0.3
    assert fermi_entropy(warm, 300.0 * 3.166811563e-6) < 1e-12

    # a metallic-like cluster of levels at the Fermi energy: fractional occupations,
    # an exact electron count, and a positive entropy
    eigenvalues = np.array([-0.2, -0.01, 0.0, 0.01, 0.2])
    kT = 0.01
    filling = np.zeros(5)
    e_fermi = fermi_filling(eigenvalues, 5.0, kT, filling)
    assert abs(filling.sum() - 5.0) < 1e-10
    assert 0.0 < filling[3] < filling[2] < filling[1] < 2.0  # monotone in energy
    expected = 2.0 / (1.0 + np.exp((eigenvalues - e_fermi) / kT))
    assert np.allclose(filling, expected, atol=1e-12)
    assert fermi_entropy(filling, kT) > 1e-4  # T S of the smeared levels


@pytest.mark.core
def test_scf_accepts_a_finite_electronic_temperature():
    """A smeared SCF converges and stays consistent with the zero-T result."""

    from maxwelllink.mxl_drivers.python.models.rtdftb_model.dftb_params import (
        DFTBSystem,
        load_sk_set,
    )
    from maxwelllink.mxl_drivers.python.models.rtdftb_model.h0_overlap import (
        build_h0_overlap,
    )
    from maxwelllink.mxl_drivers.python.models.rtdftb_model.scc import scf

    sk_set = load_sk_set(
        sk_path_for("3ob", ["O", "H"], skip=pytest.skip), ["O", "H"], None
    )
    system = DFTBSystem(
        ["O", "H", "H"],
        np.array([[0.0, 0.0, 0.1173], [0.0, 0.7572, -0.4692], [0.0, -0.7572, -0.4692]]),
        sk_set,
        units="angstrom",
    )
    h0, overlap = build_h0_overlap(system)
    cold = scf(system, h0, overlap, tolerance=1e-10)
    warm = scf(
        system,
        h0,
        overlap,
        tolerance=1e-10,
        electronic_temperature_au=300.0 * 3.166811563e-6,
    )
    assert warm.converged
    # water's gap dwarfs 300 K: the free energy and charges match the cold result
    assert abs(warm.energy_mermin - cold.energy_total) < 1e-9
    assert np.abs(warm.dq_shell - cold.dq_shell).max() < 1e-8
