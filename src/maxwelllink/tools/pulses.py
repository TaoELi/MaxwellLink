# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
Predefined laser electric-field profiles for MaxwellLink simulations.

These helpers return callables ``f(t_au)`` that evaluate the electric field
in atomic units at time ``t_au`` and can be passed directly to
``LaserDrivenSimulation``'s ``drive`` parameter.
"""

from __future__ import annotations

import math
from typing import Callable, Sequence, Union

import numpy as np

__all__ = [
    "gaussian_pulse",
    "gaussian_enveloped_cosine",
    "cosine_drive",
    "k_parallel_pulse",
]


def gaussian_pulse(
    amplitude_au: float = 1.0,
    t0_au: float = 0.0,
    sigma_au: float = 10.0,
    t_start_au: float = 0.0,
    t_end_au: float = 1e10,
) -> Callable[[float], float]:
    r"""
    Return a Gaussian pulse drive.

    .. math::

        E(t) = A \exp\left(-\frac{(t - t_0)^2}{2 \sigma^2}\right)

    Parameters
    ----------
    amplitude_au : float, default: 1.0
        Peak field amplitude in atomic units.
    t0_au : float, default: 0.0
        Temporal center of the pulse in atomic units.
    sigma_au : float, default: 10.0
        Temporal sigma in atomic units.
    t_start_au : float, default: 0.0
        Time before which the pulse is zero (atomic units).
    t_end_au : float, default: 1e10
        Time after which the pulse is zero (atomic units).

    Returns
    -------
    callable
        A function ``f(t_au)`` that evaluates the Gaussian pulse at ``t_au``.
    """
    amplitude = float(amplitude_au)
    sigma = float(sigma_au)
    t0 = float(t0_au)
    t_start = float(t_start_au)
    t_end = float(t_end_au)

    def _drive(t_au: float) -> float:
        if t_au < t_start or t_au > t_end:
            return 0.0
        x = (float(t_au) - t0) / sigma
        return amplitude * math.exp(-0.5 * x * x)

    return _drive


def gaussian_enveloped_cosine(
    amplitude_au: float = 1.0,
    t0_au: float = 0.0,
    sigma_au: float = 10.0,
    omega_au: float = 0.1,
    phase_rad: float = 0.0,
    t_start_au: float = 0.0,
    t_end_au: float = 1e10,
) -> Callable[[float], float]:
    r"""
    Return a Gaussian-enveloped cosine drive.

    .. math::

        E(t) = A \exp\left(-\frac{(t - t_0)^2}{2 \sigma^2}\right)
        \cos\bigl(\omega (t - t_0) + \phi\bigr)

    Parameters
    ----------
    amplitude_au : float, default: 1.0
        Peak field amplitude in atomic units.
    t0_au : float, default: 0.0
        Temporal center of the pulse in atomic units.
    sigma_au : float, default: 10.0
        Temporal sigma in atomic units.
    omega_au : float, default: 0.1
        Angular frequency of the cosine wave in atomic units.
    phase_rad : float, default: 0.0
        Phase of the cosine wave (radians).
    t_start_au : float, default: 0.0
        Time before which the pulse is zero (atomic units).
    t_end_au : float, default: 1e10
        Time after which the pulse is zero (atomic units).

    Returns
    -------
    callable
        A function ``f(t_au)`` for use as a time-dependent electric field.
    """

    amplitude = float(amplitude_au)
    sigma = float(sigma_au)
    t0 = float(t0_au)
    omega = float(omega_au)
    phase = float(phase_rad)
    t_start = float(t_start_au)
    t_end = float(t_end_au)

    def _drive(t_au: float) -> float:
        if t_au < t_start or t_au > t_end:
            return 0.0
        t = float(t_au) - t0
        envelope = math.exp(-0.5 * (t / sigma) ** 2)
        return amplitude * envelope * math.cos(omega * t + phase)

    return _drive


def cosine_drive(
    amplitude_au: float = 1.0,
    omega_au: float = 0.1,
    phase_rad: float = 0.0,
    t_start_au: float = 0.0,
    t_end_au: float = 1e10,
) -> Callable[[float], float]:
    r"""
    Return a continuous cosine drive.

    .. math::

        E(t) = A \cos(\omega t + \phi)

    Parameters
    ----------
    amplitude_au : float, default: 1.0
        Oscillation amplitude in atomic units.
    omega_au : float, default: 0.1
        Angular frequency in atomic units.
    phase_rad : float, default: 0.0
        Phase offset in radians.
    t_start_au : float, default: 0.0
        Time before which the drive is zero (atomic units).
    t_end_au : float, default: 1e10
        Time after which the drive is zero (atomic units).
    Returns
    -------
    callable
        A cosine drive suitable for steady-state excitation.
    """

    amplitude = float(amplitude_au)
    omega = float(omega_au)
    phase = float(phase_rad)
    t_start = float(t_start_au)
    t_end = float(t_end_au)

    def _drive(t_au: float) -> float:
        if t_au < t_start or t_au > t_end:
            return 0.0
        return amplitude * math.cos(omega * float(t_au) + phase)

    return _drive


def _parse_k_parallel_direction(direction: str) -> tuple[str, float]:
    direction = str(direction).strip().lower()
    if not direction:
        raise ValueError("direction must be 'x', 'y', '+x', '-x', '+y', or '-y'.")

    sign = 1.0
    if direction[0] == "+":
        direction = direction[1:]
    elif direction[0] == "-":
        sign = -1.0
        direction = direction[1:]

    if direction not in {"x", "y"}:
        raise ValueError("direction must be 'x', 'y', '+x', '-x', '+y', or '-y'.")
    return direction, sign


def _parse_axis(name: str, value: str) -> str:
    axis = str(value).strip().lower()
    if axis not in {"x", "y"}:
        raise ValueError(f"{name} must be 'x' or 'y'.")
    return axis


def _as_pair(name: str, value: Sequence[float]) -> tuple[float, float]:
    try:
        pair = tuple(float(item) for item in value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a length-2 sequence.") from exc
    if len(pair) != 2:
        raise ValueError(f"{name} must be a length-2 sequence.")
    if not np.all(np.isfinite(pair)):
        raise ValueError(f"{name} values must be finite.")
    return pair[0], pair[1]


def _hann_window_unit(u: np.ndarray) -> np.ndarray:
    window = np.zeros_like(u, dtype=float)
    inside = np.abs(u) <= 1.0
    window[inside] = 0.5 * (1.0 + np.cos(np.pi * u[inside]))
    return window


def k_parallel_pulse(
    cavity,
    envelope: Union[Callable[[float], float], float],
    omega_au: float,
    k_parallel_au: float,
    direction: str = "y",
    center: Sequence[float] = (0.5, 0.5),
    size: Sequence[float] = (0.1, 0.1),
    amplitude_au: float = 1.0,
    phase_rad: float = 0.0,
    target: str = "molecule",
    projection_axis: Union[str, None] = None,
) -> Callable[[float], np.ndarray]:
    r"""
    Build a multimode pulse with a selected in-plane wave vector.

    The returned object is a callable ``source(t_au)`` with shape
    ``(len(source.excited_grid_list),)`` for ``target="molecule"`` or
    ``(len(source.excited_mode_list),)`` for ``target="photon"``. It can be
    passed directly to :class:`maxwelllink.MultiModeSimulation` as either
    ``molecule_pulse_drive`` or ``photon_pulse_drive``.

    The physical in-plane wave-vector scale is the one used by
    :class:`maxwelllink.FabryPerotCavity`'s planar dispersion:

    .. math::

        \omega_k = \sqrt{\omega_c^2 + k_{\parallel,x}^2 + k_{\parallel,y}^2}.

    For ``direction="y"``, ``k_parallel_au`` is mapped to the normalized
    fractional-coordinate phase by
    ``ky_norm = pi * k_parallel_au / cavity.delta_omega_y_au``.

    Parameters
    ----------
    cavity
        A ``FabryPerotCavity`` instance. It must expose ``grid_xy`` and the
        relevant ``delta_omega_*_au`` value.
    envelope
        Time-domain envelope callable ``envelope(t_au)`` or constant scalar
        multiplier. Use helpers such as :func:`gaussian_pulse`; the carrier
        ``cos(omega_au * t - k*r)`` is supplied by this function. Passing
        ``1.0`` gives a continuous cosine source with grid-dependent phases.
    omega_au
        Carrier angular frequency in atomic units.
    k_parallel_au
        Physical in-plane wave-vector contribution in atomic units, in the
        same units as ``delta_omega_x_au`` / ``delta_omega_y_au``.
    direction
        In-plane propagation direction: ``"x"``, ``"y"``, ``"+x"``, ``"-x"``,
        ``"+y"``, or ``"-y"``.
    center
        Source center ``(x, y)`` in fractional cavity coordinates.
    size
        Full source window size ``(size_x, size_y)`` in fractional cavity
        coordinates. A smooth Hann window is applied inside this rectangle.
    amplitude_au
        Additional peak amplitude multiplier.
    phase_rad
        Global carrier phase in radians.
    target
        Source target, either ``"molecule"`` or ``"photon"``. Molecule-targeted
        sources return one value per selected molecular grid point. Photon-
        targeted sources project the same spatial source onto cavity modes and
        return one value per selected mode.
    projection_axis
        Mode-function component used for photon-target projection. Defaults to
        ``"y"`` for ``target="photon"`` and is ignored for
        ``target="molecule"``.

    Returns
    -------
    callable
        Callable source object with attributes including ``target``,
        ``excited_grid_list``, ``excited_mode_list``, ``spatial_window``,
        ``spatial_phase``, and ``k_order``.
    """

    target_clean = str(target).strip().lower()
    if target_clean not in {"molecule", "photon"}:
        raise ValueError("target must be either 'molecule' or 'photon'.")

    if isinstance(envelope, (int, float)):
        envelope_const = float(envelope)
        envelope = lambda _t, c=envelope_const: c
    elif not callable(envelope):
        raise ValueError("envelope must be callable or a scalar.")

    grid_xy = np.asarray(getattr(cavity, "grid_xy", None), dtype=float)
    if grid_xy.ndim != 2 or grid_xy.shape[1] != 2:
        raise ValueError("cavity must expose grid_xy with shape (n_grid, 2).")

    axis, direction_sign = _parse_k_parallel_direction(direction)
    axis_index = 0 if axis == "x" else 1
    center_pair = _as_pair("center", center)
    size_pair = _as_pair("size", size)
    if size_pair[0] <= 0.0 or size_pair[1] <= 0.0:
        raise ValueError("size values must be positive.")

    delta_name = f"delta_omega_{axis}_au"
    delta_omega_axis = float(getattr(cavity, delta_name))
    k_parallel = direction_sign * float(k_parallel_au)
    if not math.isfinite(k_parallel):
        raise ValueError("k_parallel_au must be finite.")
    if delta_omega_axis == 0.0 and k_parallel != 0.0:
        raise ValueError(f"{delta_name} is zero, so nonzero k_parallel_au is undefined.")

    k_order = 0.0 if delta_omega_axis == 0.0 else k_parallel / delta_omega_axis
    k_norm = math.pi * k_order

    half_size = np.array(size_pair, dtype=float) * 0.5
    center_arr = np.array(center_pair, dtype=float)
    rel_xy = grid_xy - center_arr[None, :]
    mask = (np.abs(rel_xy[:, 0]) <= half_size[0]) & (
        np.abs(rel_xy[:, 1]) <= half_size[1]
    )
    selected = np.flatnonzero(mask)
    if selected.size == 0:
        raise ValueError(
            "No molecular grid points selected by center/size. "
            "Increase size or move center inside the cavity grid."
        )

    selected_xy = grid_xy[selected, :]
    selected_rel = rel_xy[selected, :]
    unit_rel = selected_rel / half_size[None, :]
    spatial_window = _hann_window_unit(unit_rel[:, 0]) * _hann_window_unit(
        unit_rel[:, 1]
    )
    max_window = float(np.max(spatial_window))
    if max_window > 0.0:
        spatial_window = spatial_window / max_window
    else:
        raise ValueError(
            "The selected source grid points all lie on the smooth-window "
            "boundary. Increase size or move center."
        )

    spatial_phase = k_norm * selected_rel[:, axis_index]

    excited_grid_list = selected.astype(int).tolist()
    excited_mode_list: list[int] = []
    mode_complex_amplitude = np.zeros(0, dtype=complex)
    projection_norm = 1.0
    projection_axis_clean = None

    if target_clean == "photon":
        projection_axis_clean = _parse_axis(
            "projection_axis", "y" if projection_axis is None else projection_axis
        )
        projection_axis_index = 0 if projection_axis_clean == "x" else 1
        ftilde_k = np.asarray(getattr(cavity, "ftilde_k", None), dtype=float)
        if ftilde_k.ndim != 3 or ftilde_k.shape[2] != 3:
            raise ValueError("cavity must expose ftilde_k with shape (n_mode, n_grid, 3).")
        source_complex = spatial_window * np.exp(-1j * spatial_phase)
        raw_projection = ftilde_k[:, selected, projection_axis_index] @ source_complex
        projection_norm = float(np.max(np.abs(raw_projection)))
        if projection_norm <= 0.0:
            raise ValueError(
                "The photon-target source has zero overlap with all cavity modes. "
                "Try a different projection_axis, center, or size."
            )
        mode_mask = np.abs(raw_projection) > projection_norm * 1e-12
        excited_mode_list = np.flatnonzero(mode_mask).astype(int).tolist()
        mode_complex_amplitude = raw_projection[mode_mask] / projection_norm

    omega = float(omega_au)
    amplitude = float(amplitude_au)
    phase = float(phase_rad)

    if target_clean == "molecule":

        def _drive(t_au: float) -> np.ndarray:
            t = float(t_au)
            temporal = float(envelope(t))
            carrier = np.cos(omega * t - spatial_phase + phase)
            return amplitude * temporal * spatial_window * carrier

    else:

        def _drive(t_au: float) -> np.ndarray:
            t = float(t_au)
            temporal = float(envelope(t))
            phase_factor = np.exp(1j * (omega * t + phase))
            return amplitude * temporal * np.real(
                phase_factor * mode_complex_amplitude
            )

    _drive.target = target_clean
    _drive.excited_grid_list = excited_grid_list
    _drive.excited_mode_list = excited_mode_list
    _drive.grid_xy = selected_xy
    _drive.spatial_window = spatial_window
    _drive.spatial_phase = spatial_phase
    _drive.mode_complex_amplitude = mode_complex_amplitude
    _drive.projection_norm = projection_norm
    _drive.k_parallel_au = k_parallel
    _drive.k_order = k_order
    _drive.direction = ("-" if k_order < 0.0 else "+") + axis
    _drive.projection_axis = projection_axis_clean
    _drive.center = center_pair
    _drive.size = size_pair
    _drive.phase_rad = phase

    return _drive
