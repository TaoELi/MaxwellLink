# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

import numpy as np
import pytest

mxl = pytest.importorskip("maxwelllink", reason="maxwelllink is required for this test")

from maxwelllink.tools import (  # noqa: E402
    gaussian_pulse,
    k_parallel_pulse,
    k_parallel_pulse_with_seed,
)


def _make_cavity(**overrides):
    params = dict(
        frequency_au=0.1,
        n_grid_x=12,
        n_grid_y=12,
        delta_omega_x_au=0.004,
        delta_omega_y_au=0.006,
        n_mode_x=5,
        n_mode_y=4,
    )
    params.update(overrides)
    return mxl.FabryPerotCavity(**params)


@pytest.mark.core
def test_molecule_target_matches_windowed_cosine():
    """
    A molecule-targeted source must evaluate to
    ``amplitude * window * cos(omega * t - pi * k / delta_omega * (y - y0) + phase)``
    on the selected grid points.
    """
    cavity = _make_cavity()
    k = 2.0 * cavity.delta_omega_y_au
    source = k_parallel_pulse(
        cavity=cavity,
        envelope=1.0,
        omega_au=0.11,
        k_parallel_au=k,
        direction="y",
        center=(0.5, 0.5),
        size=(0.5, 0.5),
        amplitude_au=1.7,
        phase_rad=0.3,
    )

    assert source.target == "molecule"
    assert len(source.excited_grid_list) > 0
    expected_phase = (
        np.pi * (k / cavity.delta_omega_y_au) * (source.grid_xy[:, 1] - 0.5)
    )
    np.testing.assert_allclose(source.spatial_phase, expected_phase, atol=1e-12)

    for t in (0.0, 13.7, 244.9):
        value = source(t)
        assert value.shape == (len(source.excited_grid_list),)
        expected = (
            1.7 * source.spatial_window * np.cos(0.11 * t - source.spatial_phase + 0.3)
        )
        np.testing.assert_allclose(value, expected, atol=1e-12)


@pytest.mark.core
def test_scalar_direction_equals_xy_pair():
    """``direction="x"``/``"-y"`` must be shorthands for ``"xy"`` wave vectors."""
    cavity = _make_cavity()
    kx = 3.0 * cavity.delta_omega_x_au
    ky = 1.5 * cavity.delta_omega_y_au
    common = dict(
        cavity=cavity,
        envelope=1.0,
        omega_au=0.11,
        center=(0.45, 0.55),
        size=(0.5, 0.4),
    )
    pairs = [
        (
            dict(k_parallel_au=kx, direction="x"),
            dict(k_parallel_au=[kx, 0.0], direction="xy"),
        ),
        (
            dict(k_parallel_au=ky, direction="-y"),
            dict(k_parallel_au=[0.0, -ky], direction="xy"),
        ),
    ]
    for scalar_kwargs, pair_kwargs in pairs:
        source_scalar = k_parallel_pulse(**common, **scalar_kwargs)
        source_pair = k_parallel_pulse(**common, **pair_kwargs)
        assert source_scalar.excited_grid_list == source_pair.excited_grid_list
        np.testing.assert_allclose(
            source_scalar.k_order, source_pair.k_order, atol=1e-12
        )
        for t in (0.0, 57.3):
            np.testing.assert_allclose(source_scalar(t), source_pair(t), atol=1e-12)


@pytest.mark.core
def test_photon_target_projects_onto_modes():
    """
    A photon-targeted source must return one value per selected mode,
    normalized so that the largest mode amplitude is one, and follow
    ``amplitude * envelope(t) * Re[exp(i (omega t + phase)) * amplitude_k]``.
    """
    cavity = _make_cavity()
    source = k_parallel_pulse(
        cavity=cavity,
        envelope=1.0,
        omega_au=0.11,
        k_parallel_au=2.0 * cavity.delta_omega_y_au,
        direction="y",
        center=(0.5, 0.5),
        size=(0.5, 0.5),
        amplitude_au=0.8,
        phase_rad=0.1,
        target="photon",
    )

    assert source.target == "photon"
    assert len(source.excited_mode_list) > 0
    assert source.mode_complex_amplitude.shape == (len(source.excited_mode_list),)
    assert np.isclose(np.max(np.abs(source.mode_complex_amplitude)), 1.0)

    for t in (0.0, 31.4):
        expected = 0.8 * np.real(
            np.exp(1j * (0.11 * t + 0.1)) * source.mode_complex_amplitude
        )
        np.testing.assert_allclose(source(t), expected, atol=1e-12)


@pytest.mark.core
def test_envelope_modulates_drive():
    """A callable envelope must rescale the constant-envelope drive pointwise."""
    cavity = _make_cavity()
    envelope = gaussian_pulse(amplitude_au=1.3, t0_au=40.0, sigma_au=15.0)
    common = dict(
        cavity=cavity,
        omega_au=0.11,
        k_parallel_au=2.0 * cavity.delta_omega_y_au,
        direction="y",
        center=(0.5, 0.5),
        size=(0.5, 0.5),
    )
    source_unit = k_parallel_pulse(envelope=1.0, **common)
    source_env = k_parallel_pulse(envelope=envelope, **common)
    for t in (0.0, 40.0, 66.0):
        np.testing.assert_allclose(
            source_env(t), envelope(t) * source_unit(t), atol=1e-12
        )


@pytest.mark.core
def test_invalid_inputs_raise():
    cavity = _make_cavity()
    base = dict(
        cavity=cavity,
        envelope=1.0,
        omega_au=0.11,
        k_parallel_au=0.008,
        direction="y",
    )

    for overrides in [
        dict(direction="z"),
        dict(
            direction="x", k_parallel_au=1.1 * cavity.n_mode_x * cavity.delta_omega_x_au
        ),
        dict(direction="xy"),  # scalar k for "xy"
        dict(direction="x", k_parallel_au=[0.008, 0.0]),
        dict(target="laser"),
        dict(center=(3.0, 3.0), size=(0.1, 0.1)),  # empty grid selection
        dict(target="photon", projection_axis="z"),
    ]:
        with pytest.raises(ValueError):
            k_parallel_pulse(**{**base, **overrides})

    # nonzero k along an axis without mode dispersion
    cavity_flat = _make_cavity(delta_omega_x_au=0.0)
    with pytest.raises(ValueError):
        k_parallel_pulse(**{**base, "cavity": cavity_flat, "direction": "x"})


# a lopsided cavity, so a mixed-up grid or mode ordering cannot go unnoticed
_LOPSIDED = dict(n_grid_x=7, n_grid_y=5, n_repeat_x=2, n_mode_x=6, n_mode_y=3)


@pytest.mark.core
def test_photon_target_needs_no_stored_mode_functions():
    """
    The photon-target projection is evaluated from the cavity's factorized mode
    functions, so a cavity built with ``save_mode_functions=False`` must give the
    modes and amplitudes of the definition ``ftilde_k[:, grid, axis] @ source``.
    """
    stored = _make_cavity(save_mode_functions=True, **_LOPSIDED)  # the reference table
    lean = _make_cavity(save_mode_functions=False, **_LOPSIDED)
    assert not hasattr(lean, "ftilde_k")

    for projection_axis in ("x", "y"):
        source = k_parallel_pulse(
            cavity=lean,
            envelope=1.0,
            omega_au=0.11,
            k_parallel_au=2.0 * lean.delta_omega_y_au,
            direction="y",
            center=(0.5, 0.5),
            size=(0.5, 0.5),
            target="photon",
            projection_axis=projection_axis,
        )
        axis = 0 if projection_axis == "x" else 1
        grid = np.asarray(source.excited_grid_list)
        raw = stored.ftilde_k[:, grid, axis] @ (
            source.spatial_window * np.exp(-1j * source.spatial_phase)
        )
        expected = raw / np.max(np.abs(raw))
        assert (
            source.excited_mode_list
            == np.flatnonzero(np.abs(expected) > 1e-12).tolist()
        )
        np.testing.assert_allclose(
            source.mode_complex_amplitude,
            expected[source.excited_mode_list],
            atol=1e-13,
        )


@pytest.mark.core
def test_vortex_seed_projects_like_the_stored_mode_functions():
    """
    A vortex seed selects a non-rectangular patch of the grid; its photon-mode
    projection must still follow the definition and not depend on whether the
    cavity stores its mode functions.
    """
    stored = _make_cavity(save_mode_functions=True, **_LOPSIDED)  # the reference table
    lean = _make_cavity(save_mode_functions=False, **_LOPSIDED)

    def build(cavity):
        pulse = k_parallel_pulse_with_seed(
            cavity=cavity,
            envelope=1.0,
            omega_au=0.11,
            k_parallel_au=2.0 * cavity.delta_omega_y_au,
            direction="y",
            center=(0.5, 0.5),
            size=(0.5, 0.5),
            target="photon",
            projection_axis="x",
        )
        return pulse.add_vortex_seed(
            charge=2,
            omega_au=0.1,
            amplitude_au=0.5,
            t0_au=100.0,
            sigma_au=20.0,
            center=(0.45, 0.55),
            waist=0.2,
        )

    with_stored, without = build(stored), build(lean)
    assert with_stored.excited_mode_list == without.excited_mode_list
    for t in (0.0, 100.0, 250.0):
        np.testing.assert_allclose(without(t), with_stored(t), atol=1e-13)

    seed = without._pulses[-1]
    grid = np.asarray(seed.excited_grid_list)
    raw = stored.ftilde_k[:, grid, 0] @ (
        seed.spatial_window * np.exp(-1j * seed.spatial_phase)
    )
    expected = raw / np.max(np.abs(raw))
    assert seed.excited_mode_list == np.flatnonzero(np.abs(expected) > 1e-12).tolist()
    np.testing.assert_allclose(
        seed.mode_complex_amplitude, expected[seed.excited_mode_list], atol=1e-13
    )
