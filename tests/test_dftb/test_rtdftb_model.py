# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Driver-level tests for ``RTDFTBModel``.
"""

import numpy as np
import pytest

from slko_helpers import sk_path

models = pytest.importorskip(
    "maxwelllink.mxl_drivers.python.models",
    reason="maxwelllink is required for this test",
)
dftb_mod = pytest.importorskip(
    "maxwelllink.mxl_drivers.python.models.rtdftb_model",
    reason="maxwelllink is required for this test",
)
RTDFTBModel = dftb_mod.RTDFTBModel

_DT = 0.2  # atomic units
_N_STEPS = 40
_FIELD = np.array([0.0, 0.0, 5.0e-3])  # atomic units

_ELEMENTS = ["O", "H", "H"]
_POSITIONS = np.array(
    [[0.0, 0.0, 0.1173], [0.0, 0.7572, -0.4692], [0.0, -0.7572, -0.4692]]
)
_VELOCITIES = np.array(
    [[1.0e-4, 0.0, 2.0e-4], [0.0, -3.0e-4, 1.0e-4], [2.0e-4, 1.0e-4, 0.0]]
)

_REPLY_KEYS = {  # mxlcommon.F90:120-133
    "time_au",
    "energy_au",
    "energy_kin_au",
    "mux_au",
    "muy_au",
    "muz_au",
    "mux_m_au",
    "muy_m_au",
    "muz_m_au",
}


def _slko_or_skip():
    """Skip when the Slater-Koster parameter sets are not installed."""


def _driver(**kwargs):
    """An initialized water driver, with every argument defaulted for these tests."""

    _slko_or_skip()
    kwargs.setdefault("reset_dipole", False)
    model = RTDFTBModel(
        sk_path=sk_path(skip=pytest.skip),
        elements=_ELEMENTS,
        positions=_POSITIONS,
        **kwargs,
    )
    model.initialize(_DT, 0)
    return model


def _run(model, n_steps=_N_STEPS, field=_FIELD):
    """Drive ``n_steps`` EM steps and collect the reply of each one."""

    dipole, energy, amplitude = [], [], []
    for _ in range(n_steps):
        model.propagate(field)
        data = model.append_additional_data()
        assert set(data) == _REPLY_KEYS
        dipole.append([data["mux_au"], data["muy_au"], data["muz_au"]])
        # the same value twice, so the EM solver's 2 * mu_half - mu_force is a no-op
        assert data["mux_au"] == data["mux_m_au"]
        assert data["muy_au"] == data["muy_m_au"]
        assert data["muz_au"] == data["muz_m_au"]
        energy.append(data["energy_au"])
        amplitude.append(model.calc_amp_vector().copy())
    return np.array(dipole), np.array(energy), np.array(amplitude)


def _reference(ehrenfest, velocities=None):
    """The same trajectory through the batch runner the DFTB+ oracles validate.

    The field table starts at zero because ``initializeDynamics`` runs before the socket
    has delivered anything, and the socket path does not lag the field by a step.
    """

    sk_set = dftb_mod.load_sk_set(sk_path(skip=pytest.skip), sorted(set(_ELEMENTS)))
    system = dftb_mod.DFTBSystem(_ELEMENTS, _POSITIONS, sk_set)
    h0, overlap = dftb_mod.build_h0_overlap(system)
    ground = dftb_mod.scf(system, h0, overlap, tolerance=1e-13)

    table = np.tile(_FIELD, (_N_STEPS + 2, 1))
    table[0] = 0.0
    runner = dftb_mod.run_ehrenfest if ehrenfest else dftb_mod.run_kick
    extra = {"velocities": velocities} if ehrenfest else {}
    return runner(
        system,
        ground,
        _N_STEPS + 1,
        _DT,
        0.0,
        2,
        field_table=table,
        field_lag=False,
        **extra,
    )


@pytest.mark.core
def test_frozen_nuclei_driver_matches_the_batch_runner():
    """One EM step is one ``doTdStep``, with the midpoint-averaged reply."""

    dipole, energy, _ = _run(_driver())
    out = _reference(ehrenfest=False)

    # the driver's first step begins one bootstrap step in, as DFTB+'s socket loop does
    reference_dipole = 0.5 * (out["dipole"][1:-1] + out["dipole"][2:])
    reference_energy = 0.5 * (
        out["energies"][1:-1].sum(axis=1) + out["energies"][2:].sum(axis=1)
    )
    assert np.abs(dipole - reference_dipole).max() < 1e-10
    assert np.abs(energy - reference_energy).max() < 1e-12


@pytest.mark.core
def test_ehrenfest_driver_matches_the_batch_runner():
    """With the nuclei moving, and the nuclear kinetic energy reported separately."""

    model = _driver(ehrenfest=True, velocities=_VELOCITIES)
    dipole, energy, _ = _run(model)
    out = _reference(ehrenfest=True, velocities=_VELOCITIES)

    assert (
        np.abs(dipole - 0.5 * (out["dipole"][1:-1] + out["dipole"][2:])).max() < 1e-10
    )

    # DFTB+ evaluates both ends of the step with the kinetic energy of its *start*
    # (getPositionDependentEnergy runs once, at the top of doTdStep), so the reference
    # midpoint has to be built the same way rather than from two consecutive totals.
    terms, kinetic = out["energies"][:, :4].sum(axis=1), out["energies"][:, 4]
    reference = 0.5 * (terms[1:-1] + terms[2:]) + kinetic[1:-1]
    assert np.abs(energy - reference).max() < 1e-12
    assert model.energy_kin > 0.0


@pytest.mark.core
def test_amplitude_is_the_finite_difference_of_the_dipole():
    """``dmu/dt`` is a difference across the step, not an analytic derivative."""

    _, _, amplitude = _run(_driver())
    out = _reference(ehrenfest=False)

    reference = (out["dipole"][2:] - out["dipole"][1:-1]) / _DT
    assert np.abs(amplitude - reference).max() < 1e-9
    # and it is a genuine signal, not a wash of round-off
    assert np.abs(amplitude).max() > 1e-6


@pytest.mark.core
def test_reset_dipole_shifts_the_dipole_but_not_the_amplitude():
    """The baseline is captured once and subtracted from every reported dipole."""

    raw_dipole, raw_energy, raw_amplitude = _run(_driver(reset_dipole=False))
    dipole, energy, amplitude = _run(_driver(reset_dipole=True))

    baseline = raw_dipole[0] - dipole[0]
    assert np.abs(baseline).max() > 1e-3, "water has a permanent dipole to subtract"
    assert np.abs((raw_dipole - dipole) - baseline).max() < 1e-14
    assert np.abs(raw_amplitude - amplitude).max() < 1e-14
    assert np.abs(raw_energy - energy).max() < 1e-14


@pytest.mark.core
def test_sub_stepping_converges_to_the_single_step_trajectory():
    """A finer electronic step only refines the trajectory; it does not change it.

    The convergence is tested as a monotone decrease rather than as a rate. The
    propagator is second order, but ``dt = 0.2`` on water is not yet in its asymptotic
    regime -- halving it there cuts the error by about two, not four -- so an order
    assertion here would be measuring the wrong thing.
    """

    model = _driver(dt_rtdftb_au=_DT / 4)
    assert model.n_substeps == 4

    resolved, _, _ = _run(_driver(dt_rtdftb_au=_DT / 32))
    errors = [
        np.abs(
            _run(_driver(dt_rtdftb_au=None if n == 1 else _DT / n))[0] - resolved
        ).max()
        for n in (1, 2, 4, 8)
    ]
    assert errors[0] < 5e-3, "sub-stepping must refine the trajectory, not change it"
    for coarse, fine in zip(errors[:-1], errors[1:]):
        assert fine < coarse, f"error grew from {coarse:.3e} to {fine:.3e}"


@pytest.mark.core
def test_driver_is_registered_for_maxwelllink():
    """The driver is reachable both as a class and through the driver registry."""

    assert models.RTDFTBModel is RTDFTBModel
    assert "rtdftb" in models.__drivers__

    _slko_or_skip()
    model = models.__drivers__["rtdftb"](
        sk_path=sk_path(skip=pytest.skip), elements=_ELEMENTS, positions=_POSITIONS
    )
    assert isinstance(model, RTDFTBModel)


@pytest.mark.core
def test_batch_xyz_frames_are_picked_by_molecule_id(tmp_path):
    """Molecule ``m`` reads frame ``m``; too few frames or other atoms are refused."""
    from maxwelllink.mxl_drivers.python.models.rtdftb_model.rtdftb_model import (
        read_xyz_frames,
    )

    path = tmp_path / "frames.xyz"
    frames = [_POSITIONS * (1.0 + 0.05 * k) for k in range(3)]
    with open(path, "w") as handle:
        for k, positions in enumerate(frames):
            handle.write(f"3\nframe {k}\n")
            for symbol, (x, y, z) in zip(_ELEMENTS, positions):
                handle.write(f"{symbol} {x:.8f} {y:.8f} {z:.8f}\n")
    elements, read = read_xyz_frames(str(path))
    assert elements == _ELEMENTS and read.shape == (3, 3, 3)
    np.testing.assert_allclose(read, np.array(frames))

    for molecule_id in (0, 2):
        model = RTDFTBModel(sk_path=sk_path(skip=pytest.skip), batch_xyz=str(path))
        model.initialize(_DT, molecule_id)
        np.testing.assert_allclose(
            model.system.coords, frames[molecule_id] / dftb_mod.BOHR_TO_AA, atol=1e-12
        )
    model = RTDFTBModel(sk_path=sk_path(skip=pytest.skip), batch_xyz=str(path))
    with pytest.raises(ValueError):
        model.initialize(_DT, 3)  # only three frames
    with pytest.raises(ValueError):
        RTDFTBModel(
            sk_path=sk_path(skip=pytest.skip),
            batch_xyz=str(path),
            elements=["O", "H", "D"],
            positions=_POSITIONS,
        )


@pytest.mark.core
def test_pre_nvt_moves_the_geometry_deterministically_per_molecule():
    """The Born-Oppenheimer pre-equilibration follows ``seed + molecule_id``: same ID,
    same thermalized state; another ID, another one; the SCC stays converged."""
    settings = dict(ehrenfest=True, pre_nvt=True, pre_nvt_duration_ps=0.005)
    first = _driver(**settings)
    first.initialize(_DT, 0)
    again = _driver(**settings)
    again.initialize(_DT, 0)
    other = _driver(**settings)
    other.initialize(_DT, 1)
    still = _driver(ehrenfest=True)
    still.initialize(_DT, 0)
    assert np.array_equal(first.system.coords, again.system.coords)
    assert np.array_equal(first.dynamics.velocity, again.dynamics.velocity)
    assert np.abs(first.system.coords - other.system.coords).max() > 1e-4
    assert np.abs(first.system.coords - still.system.coords).max() > 1e-4
    assert first.ground.converged and np.isfinite(first.ground.energy_total)
    with pytest.raises(ValueError):
        _driver(pre_nvt=True, pre_nvt_duration_ps=0.0)


@pytest.mark.core
def test_bad_arguments_are_rejected():
    """The constructor checks what it can before any expensive work happens."""

    with pytest.raises(ValueError, match="elements and positions"):
        RTDFTBModel(sk_path=sk_path(skip=pytest.skip))
    with pytest.raises(ValueError, match="propagator"):
        RTDFTBModel(
            sk_path=sk_path(skip=pytest.skip),
            elements=_ELEMENTS,
            positions=_POSITIONS,
            propagator="magnus",
        )
    with pytest.raises(ValueError, match="kick_direction"):
        RTDFTBModel(
            sk_path=sk_path(skip=pytest.skip),
            elements=_ELEMENTS,
            positions=_POSITIONS,
            kick_direction="w",
        )


if __name__ == "__main__":
    test_frozen_nuclei_driver_matches_the_batch_runner()
    test_ehrenfest_driver_matches_the_batch_runner()
    test_reset_dipole_shifts_the_dipole_but_not_the_amplitude()
    print("RTDFTBModel reproduces the DFTB+ socket contract")
