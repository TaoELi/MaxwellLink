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
``LaserDrivenSimulation``'s ``drive`` parameter. The exception is
:func:`k_parallel_pulse`, which builds an array-valued source for
:class:`maxwelllink.MultiModeSimulation`.
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


def _get_k_order(
    cavity, k_parallel_au: Union[float, Sequence[float]], direction: str
) -> np.ndarray:
    r"""
    Convert a physical in-plane wave vector into cavity mode-index units:

    .. math::

        k_{\mathrm{order},i} = k_{\parallel,i} / \Delta\omega_i,
        \qquad i \in \{x, y\}.

    Parameters
    ----------
    cavity
        A ``FabryPerotCavity`` instance exposing ``delta_omega_x_au``,
        ``delta_omega_y_au``, ``n_mode_x``, and ``n_mode_y``.
    k_parallel_au
        Physical in-plane wave vector in atomic units; a scalar for
        ``direction="x"``/``"y"``, a length-2 sequence for ``"xy"``.
    direction
        One of ``"x"``, ``"y"``, or ``"xy"``, optionally prefixed with
        ``"+"`` or ``"-"``.

    Returns
    -------
    numpy.ndarray
        Length-2 array ``[kx_order, ky_order]``; the entry along an axis
        with zero mode spacing is zero.
    """
    direction = str(direction).strip().lower()
    sign = 1.0
    if direction.startswith(("+", "-")):
        sign = -1.0 if direction[0] == "-" else 1.0
        direction = direction[1:]
    if direction not in {"x", "y", "xy"}:
        raise ValueError(
            "direction must be 'x', 'y', 'xy', '+x', '-x', '+y', '-y', '+xy', or '-xy'."
        )

    k_parallel = np.zeros(2)
    try:
        if direction == "xy":
            k_parallel[:] = np.asarray(k_parallel_au, dtype=float).reshape(2)
        else:
            k_parallel[0 if direction == "x" else 1] = float(k_parallel_au)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "k_parallel_au must be a scalar for direction 'x'/'y' and a "
            "length-2 sequence [kx_au, ky_au] for 'xy'."
        ) from exc
    k_parallel *= sign
    if not np.all(np.isfinite(k_parallel)):
        raise ValueError("k_parallel_au must be finite.")

    delta_omega = np.array([cavity.delta_omega_x_au, cavity.delta_omega_y_au])
    n_mode_max = np.array([cavity.n_mode_x, cavity.n_mode_y])
    if np.any((delta_omega == 0.0) & (k_parallel != 0.0)):
        raise ValueError(
            "k_parallel_au must be zero along any axis whose delta_omega_x_au "
            "or delta_omega_y_au is zero."
        )
    k_order = np.divide(
        k_parallel, delta_omega, out=np.zeros(2), where=delta_omega != 0.0
    )
    if np.any(np.abs(k_order) > n_mode_max):
        raise ValueError(
            "Absolute k_parallel_au is too large for the cavity mode spacing. "
            f"Maximum allowed is ({delta_omega[0] * n_mode_max[0]:.3e}, "
            f"{delta_omega[1] * n_mode_max[1]:.3e})."
        )
    return k_order


def k_parallel_pulse(
    cavity,
    envelope: Union[Callable[[float], float], float],
    omega_au: float,
    k_parallel_au: Union[float, Sequence[float]],
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

    ``direction="x"`` and ``"y"`` are shorthands for the in-plane wave
    vectors ``[k_parallel_au, 0]`` and ``[0, k_parallel_au]``, while
    ``direction="xy"`` takes a length-2 sequence ``[kx_au, ky_au]``. In all
    cases the spatial phase over the fractional coordinates is

    .. math::

        \phi(x, y) =
        \pi k_{x,\mathrm{au}} (x - x_0) / \Delta\omega_x
        + \pi k_{y,\mathrm{au}} (y - y_0) / \Delta\omega_y.

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
        same units as ``delta_omega_x_au`` / ``delta_omega_y_au``. For
        ``direction="x"`` or ``"y"``, this must be a scalar. For
        ``direction="xy"``, this must be a length-2 sequence
        ``[kx_au, ky_au]``.
    direction
        In-plane propagation direction: ``"x"``, ``"y"``, ``"xy"``, ``"+x"``,
        ``"-x"``, ``"+y"``, ``"-y"``, ``"+xy"``, or ``"-xy"``. For ``"xy"``,
        the optional sign is applied to both sequence components.
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
        Callable source object with attributes ``target``,
        ``excited_grid_list``, ``excited_mode_list``, ``grid_xy``,
        ``spatial_window``, ``spatial_phase``, ``mode_complex_amplitude``,
        and ``k_order``. ``k_order`` is always a length-2 array holding the
        in-plane wave vector in units of the mode spacing along each axis.
    """

    target_clean = str(target).strip().lower()
    if target_clean not in {"molecule", "photon"}:
        raise ValueError("target must be either 'molecule' or 'photon'.")

    if not callable(envelope):
        try:
            envelope_value = float(envelope)
        except (TypeError, ValueError) as exc:
            raise ValueError("envelope must be callable or a scalar.") from exc
        envelope = lambda _t: envelope_value

    grid_xy = np.asarray(cavity.grid_xy, dtype=float)
    if grid_xy.ndim != 2 or grid_xy.shape[1] != 2:
        raise ValueError("cavity must expose grid_xy with shape (n_grid, 2).")

    k_order = _get_k_order(cavity, k_parallel_au, direction)
    k_norm = math.pi * k_order

    try:
        center_xy = np.asarray(center, dtype=float).reshape(2)
        half_size = 0.5 * np.asarray(size, dtype=float).reshape(2)
    except (TypeError, ValueError) as exc:
        raise ValueError("center and size must be length-2 sequences.") from exc
    if np.any(half_size <= 0.0):
        raise ValueError("size values must be positive.")

    rel_xy = grid_xy - center_xy[None, :]
    mask = np.all(np.abs(rel_xy) <= half_size[None, :], axis=1)
    selected = np.flatnonzero(mask)
    if selected.size == 0:
        raise ValueError(
            "No molecular grid points selected by center/size. "
            "Increase size or move center inside the cavity grid."
        )

    # the rectangular mask guarantees |unit_rel| <= 1, where the Hann window
    # 0.5 * (1 + cos(pi * u)) is nonnegative and needs no clipping
    selected_rel = rel_xy[selected, :]
    unit_rel = selected_rel / half_size[None, :]
    hann_x = 0.5 * (1.0 + np.cos(np.pi * unit_rel[:, 0]))
    hann_y = 0.5 * (1.0 + np.cos(np.pi * unit_rel[:, 1]))
    spatial_window = hann_x * hann_y
    max_window = float(np.max(spatial_window))
    if max_window <= 0.0:
        raise ValueError(
            "The selected source grid points all lie on the smooth-window "
            "boundary. Increase size or move center."
        )
    spatial_window = spatial_window / max_window
    spatial_phase = selected_rel @ k_norm

    source_complex = spatial_window * np.exp(-1j * spatial_phase)
    excited_grid_list = selected.astype(int).tolist()
    excited_mode_list: list[int] = []
    mode_complex_amplitude = np.zeros(0, dtype=complex)

    if target_clean == "molecule":
        channel_amplitude = source_complex
    else:
        if projection_axis is None:
            projection_axis = "y"
        projection_axis_clean = str(projection_axis).strip().lower()
        if projection_axis_clean not in {"x", "y"}:
            raise ValueError("projection_axis must be 'x' or 'y'.")
        ftilde_k = getattr(cavity, "ftilde_k", None)
        if ftilde_k is None:
            raise ValueError(
                "target='photon' requires the cavity mode functions; construct "
                "the cavity with save_mode_functions=True."
            )
        ftilde_k = np.asarray(ftilde_k, dtype=float)
        if ftilde_k.ndim != 3 or ftilde_k.shape[2] != 3:
            raise ValueError(
                "cavity must expose ftilde_k with shape (n_mode, n_grid, 3)."
            )
        projection_axis_index = 0 if projection_axis_clean == "x" else 1
        raw_projection = ftilde_k[:, selected, projection_axis_index] @ source_complex
        projection_norm = float(np.max(np.abs(raw_projection)))
        if projection_norm <= 0.0:
            raise ValueError(
                "The photon-target source has zero overlap with all cavity "
                "modes. Try a different projection_axis, center, or size."
            )
        mode_mask = np.abs(raw_projection) > projection_norm * 1e-12
        excited_mode_list = np.flatnonzero(mode_mask).astype(int).tolist()
        mode_complex_amplitude = raw_projection[mode_mask] / projection_norm
        channel_amplitude = mode_complex_amplitude

    omega = float(omega_au)
    amplitude = float(amplitude_au)
    phase = float(phase_rad)

    # Re[exp(i(omega t + phase)) * channel_amplitude] gives the Hann-windowed
    # cos(omega t - k.r + phase) per grid point (molecule target) or the same
    # source projected onto the cavity modes (photon target)
    def _drive(t_au: float) -> np.ndarray:
        t = float(t_au)
        carrier = np.exp(1j * (omega * t + phase))
        return amplitude * float(envelope(t)) * np.real(carrier * channel_amplitude)

    _drive.target = target_clean
    _drive.excited_grid_list = excited_grid_list
    _drive.excited_mode_list = excited_mode_list
    _drive.grid_xy = grid_xy[selected, :]
    _drive.spatial_window = spatial_window
    _drive.spatial_phase = spatial_phase
    _drive.mode_complex_amplitude = mode_complex_amplitude
    _drive.k_order = k_order

    return _drive
