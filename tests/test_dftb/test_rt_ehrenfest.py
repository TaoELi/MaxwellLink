# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Real-time TD-DFTB and Ehrenfest tests for the direct DFTB model.
"""

import os

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
run_kick = dftb_mod.run_kick
run_ehrenfest = dftb_mod.run_ehrenfest

_REFERENCE = os.path.join(
    os.path.dirname(__file__), os.pardir, "data", "dftb_rt_reference.npz"
)
_DT = 0.2  # atomic units, as DFTB+ was run
_KICK_Z = 2

_RT_SYSTEMS = [
    "rt_h2_kick",
    "rt_h2o_kick",
    "rt_ch2o_kick",
    "rt_znh2o_kick",
]
_EHR_SYSTEMS = ["ehr_h2o_kick", "ehr_ch2o_kick", "ehr_znh2o_kick"]


def _reference_or_skip():
    """Return the bundled DFTB+ trajectories, or skip when unavailable."""
    if not os.path.isfile(_REFERENCE):
        pytest.skip("DFTB+ RT reference bundle is not available.")
    return np.load(_REFERENCE, allow_pickle=True)


def _ground_state(reference, name):
    """Geometry and converged ground state of one bundled system."""
    species = [str(s) for s in reference[f"{name}/species"]]
    max_l = dict(zip(species, [str(v) for v in reference[f"{name}/max_l"]]))
    sk_set = SlaterKosterSet(
        sk_path_for(str(reference[f"{name}/sk_set"]), species, skip=pytest.skip),
        species,
        max_l,
    )
    system = DFTBSystem(
        [str(s) for s in reference[f"{name}/elements"]],
        reference[f"{name}/positions_ang"],
        sk_set,
    )
    h0, overlap = build_h0_overlap(system)
    return system, scf(system, h0, overlap, tolerance=1e-13, max_iterations=500)


@pytest.mark.core
@pytest.mark.parametrize("name", _RT_SYSTEMS)
def test_kick_dipole_trajectory_matches_dftbplus(name):
    """The dipole after a delta kick follows DFTB+ point for point."""
    reference = _reference_or_skip()
    system, ground = _ground_state(reference, name)

    n_steps = 300  # the trajectory is deterministic, so a prefix is a real test
    out = run_kick(
        system, ground, n_steps, _DT, 0.001 * dftb_mod.V_PER_AA_TO_AU, _KICK_Z
    )
    mine = out["dipole"] * dftb_mod.BOHR_TO_AA  # DFTB+ writes e.Angstrom
    theirs = reference[f"{name}/series_muz"][: n_steps + 1, 1:]

    assert np.abs(mine - theirs).max() < 1e-6


@pytest.mark.slow
@pytest.mark.parametrize("name", _RT_SYSTEMS)
def test_full_kick_trajectory_and_final_values(name):
    """The whole 2000-step trajectory, and the final dipole and energy."""
    reference = _reference_or_skip()
    system, ground = _ground_state(reference, name)

    out = run_kick(system, ground, 2000, _DT, 0.001 * dftb_mod.V_PER_AA_TO_AU, _KICK_Z)
    mine = out["dipole"] * dftb_mod.BOHR_TO_AA
    assert np.abs(mine - reference[f"{name}/series_muz"][:, 1:]).max() < 1e-6

    # DFTB+ reports the final dipole in atomic units, not e.Angstrom
    final = reference[f"{name}/final_dipole_moment"].reshape(-1)[:3]
    assert np.abs(out["final_dipole"] - final).max() < 1e-8
    assert (
        abs(out["final_scc_energy"] - float(reference[f"{name}/final_energy"])) < 1e-8
    )


@pytest.mark.slow
@pytest.mark.parametrize("name", _EHR_SYSTEMS)
def test_ehrenfest_trajectory_matches_dftbplus(name):
    """Forces, geometry and velocities after 1000 Ehrenfest steps."""
    reference = _reference_or_skip()
    system, ground = _ground_state(reference, name)

    out = run_ehrenfest(
        system, ground, 1000, _DT, 0.01 * dftb_mod.V_PER_AA_TO_AU, _KICK_Z
    )
    # DFTB+ stores these Fortran-style, with the fast index first
    assert (
        np.abs(out["final_force"] - reference[f"{name}/final_ehrenfest_forc"].T).max()
        < 1e-6
    )
    assert (
        np.abs(out["final_coords"] - reference[f"{name}/final_ehrenfest_geom"].T).max()
        < 1e-6
    )
    assert (
        np.abs(
            out["final_velocity"] - reference[f"{name}/final_ehrenfest_velo"].T
        ).max()
        < 1e-6
    )


@pytest.mark.core
def test_ground_state_is_stationary_without_a_field():
    """With no kick and no field the ground-state density must not move."""
    reference = _reference_or_skip()
    system, ground = _ground_state(reference, "rt_h2o_kick")

    for propagator in ("leapfrog", "cayley-midpoint"):
        out = run_kick(system, ground, 400, _DT, 0.0, _KICK_Z, propagator=propagator)
        drift = np.abs(out["dipole"] - out["dipole"][0]).max()
        assert drift < 1e-11, f"{propagator} drifted by {drift:.3e}"


@pytest.mark.core
def test_static_field_shifts_the_dipole():
    """A static external field polarises the system.

    The field coupling is dead code in a kick trajectory -- the kick perturbs the density
    once and the system then evolves field-free -- yet it is exactly the path MaxwellLink
    drives, so it needs its own test.
    """
    reference = _reference_or_skip()
    system, ground = _ground_state(reference, "rt_h2o_kick")

    field = np.array([0.0, 0.0, 1e-3])  # atomic units
    driven = run_kick(system, ground, 200, _DT, 0.0, _KICK_Z, field=field)
    quiet = run_kick(system, ground, 200, _DT, 0.0, _KICK_Z)

    response = np.abs(driven["dipole"][-1] - quiet["dipole"][-1]).max()
    assert response > 1e-5, "the external field did nothing"
    # and it must act along the field, not somewhere else
    shift = driven["dipole"][-1] - quiet["dipole"][-1]
    assert abs(shift[2]) > 10.0 * max(abs(shift[0]), abs(shift[1]))


@pytest.mark.core
def test_cayley_midpoint_converges_at_second_order():
    """Halving the step should quarter the error, as Crank-Nicolson must.

    Plain ``cayley`` does not manage this: the SCC charges make H time dependent, so
    evaluating it at ``t`` rather than ``t + dt/2`` degrades the scheme to first order
    and, on some systems, destabilises it. The usual conservation checks cannot see
    that -- the density stays perfectly normalised while the charge-feedback loop runs
    away -- which is why the order of convergence is tested here directly.
    """
    reference = _reference_or_skip()
    system, ground = _ground_state(reference, "rt_h2_kick")
    kick = 0.001 * dftb_mod.V_PER_AA_TO_AU

    def dipole_at_t(dt, n_steps):
        out = run_kick(
            system, ground, n_steps, dt, kick, _KICK_Z, propagator="cayley-midpoint"
        )
        return out["dipole"][-1, 2]

    total_time = 40.0  # atomic units, the same window for every step size
    fine = dipole_at_t(0.0125, int(total_time / 0.0125))
    errors = [
        abs(dipole_at_t(dt, int(total_time / dt)) - fine) for dt in (0.2, 0.1, 0.05)
    ]

    # each halving must cut the error by roughly four
    for coarse, fine_error in zip(errors[:-1], errors[1:]):
        assert (
            coarse / fine_error > 3.0
        ), f"ratio {coarse / fine_error:.2f} is not second order"


if __name__ == "__main__":
    for system in _RT_SYSTEMS:
        test_kick_dipole_trajectory_matches_dftbplus(system)
    test_ground_state_is_stationary_without_a_field()
    test_static_field_shifts_the_dipole()
    print("RT-TDDFTB trajectories match DFTB+ on every tested system")
