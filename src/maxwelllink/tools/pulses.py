# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Predefined laser electric-field profiles for MaxwellLink simulations.

These helpers return callables ``f(t_au)`` that evaluate the electric field
in atomic units at time ``t_au``.
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
    "k_parallel_pulse_with_seed",
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


def _project_onto_modes(
    cavity, selected: np.ndarray, source_complex: np.ndarray, axis_index: int
) -> np.ndarray:
    """
    Return ``ftilde_k[:, selected, axis_index] @ source_complex`` for a cavity.

    ``FabryPerotCavity`` keeps its mode functions factorized along x and y
    (``Cx``, ``Sy``, ``Sx``, ``Cy`` times ``mode_prefactor``), so the overlap
    with every mode is one small matrix product over the selected grid points
    and the full ``(n_mode, n_grid, 3)`` array is never needed. Cavities that
    expose only ``ftilde_k`` fall back to it.
    """
    factors = ("Cx", "Sy", "Sx", "Cy", "mode_prefactor", "n_grid_x")
    if all(hasattr(cavity, name) for name in factors):
        grid_x = selected % cavity.n_grid_x
        grid_y = selected // cavity.n_grid_x
        if axis_index == 0:
            along_x, along_y = cavity.Cx[:, grid_x], cavity.Sy[:, grid_y]
        else:
            along_x, along_y = cavity.Sx[:, grid_x], cavity.Cy[:, grid_y]
        # (n_mode_y, n_mode_x), which flattens in the cavity's mode order
        overlap = (along_y * source_complex[None, :]) @ along_x.T
        return cavity.mode_prefactor * overlap.reshape(-1)

    ftilde_k = getattr(cavity, "ftilde_k", None)
    if ftilde_k is None:
        raise ValueError(
            "target='photon' requires the cavity mode functions; construct "
            "the cavity with save_mode_functions=True."
        )
    ftilde_k = np.asarray(ftilde_k, dtype=float)
    if ftilde_k.ndim != 3 or ftilde_k.shape[2] != 3:
        raise ValueError("cavity must expose ftilde_k with shape (n_mode, n_grid, 3).")
    return ftilde_k[:, selected, axis_index] @ source_complex


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
        relevant ``delta_omega_*_au`` value; ``target="photon"`` also uses its
        factorized mode functions, so ``save_mode_functions=False`` is fine.
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
        projection_axis_index = 0 if projection_axis_clean == "x" else 1
        raw_projection = _project_onto_modes(
            cavity, selected, source_complex, projection_axis_index
        )
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


class KParallelPulseWithSeed:
    """Callable k-parallel pulse to which short vortex seeds can be added."""

    def __init__(self, base_pulse, cavity, projection_axis: str):
        self.base_pulse = base_pulse
        self.cavity = cavity
        self.projection_axis = projection_axis
        self.target = base_pulse.target

        self._pulses = [base_pulse]
        self._index_maps: list[np.ndarray] = []

        # MultiModeSimulation reads one of these lists when it is constructed.
        self.excited_mode_list: list[int] = []
        self.excited_grid_list: list[int] = []
        self._rebuild_indices()

    def add_vortex_seed(
        self,
        charge: int,
        omega_au: float,
        amplitude_au: float,
        t0_au: float,
        sigma_au: float,
        center: Sequence[float] = (0.5, 0.5),
        waist: float = 0.15,
        phase_rad: float = 0.0,
    ) -> "KParallelPulseWithSeed":
        r"""Add a short vortex seed and return this pulse for optional chaining.

        The real-space seed has an amplitude proportional to
        :math:`r^{|l|}e^{-r^2/2}` and a phase winding :math:`l\theta`, where
        ``charge`` is the integer :math:`l`. Its time envelope is a Gaussian
        centered at ``t0_au`` with standard deviation ``sigma_au``.

        Call this method before starting ``MultiModeSimulation.run``.
        """

        try:
            charge_value = float(charge)
        except (TypeError, ValueError) as exc:
            raise ValueError("charge must be a nonzero integer.") from exc
        if not charge_value.is_integer() or charge_value == 0.0:
            raise ValueError("charge must be a nonzero integer.")
        charge = int(charge_value)

        center = np.asarray(center, dtype=float)
        if center.shape != (2,) or not np.all(np.isfinite(center)):
            raise ValueError("center must contain two finite values.")
        if np.any(center < 0.0) or np.any(center > 1.0):
            raise ValueError(
                "center must lie inside the fractional cavity grid [0, 1]."
            )

        waist = float(waist)
        sigma_au = float(sigma_au)
        if not np.isfinite(waist) or waist <= 0.0:
            raise ValueError("waist must be positive and finite.")
        if not np.isfinite(sigma_au) or sigma_au <= 0.0:
            raise ValueError("sigma_au must be positive and finite.")

        omega_au = float(omega_au)
        amplitude_au = float(amplitude_au)
        t0_au = float(t0_au)
        phase_rad = float(phase_rad)
        if not np.all(np.isfinite([omega_au, amplitude_au, t0_au, phase_rad])):
            raise ValueError(
                "omega_au, amplitude_au, t0_au, and phase_rad must be finite."
            )

        grid_xy = np.asarray(self.cavity.grid_xy, dtype=float)
        relative_xy = (grid_xy - center[None, :]) / waist
        radius = np.hypot(relative_xy[:, 0], relative_xy[:, 1])
        angle = np.arctan2(relative_xy[:, 1], relative_xy[:, 0])

        # The amplitude is zero at the vortex core, while the phase winds by
        # 2*pi*charge around it.
        spatial_window = radius ** abs(charge) * np.exp(-0.5 * radius**2)
        window_norm = float(np.max(spatial_window))
        if window_norm <= 0.0:
            raise ValueError("The vortex seed has zero amplitude on the cavity grid.")
        spatial_window /= window_norm
        spatial_phase = charge * angle

        selected = np.flatnonzero(spatial_window > 1e-12)
        selected_window = spatial_window[selected]
        selected_phase = spatial_phase[selected]
        source_complex = selected_window * np.exp(-1j * selected_phase)

        excited_grid_list = selected.astype(int).tolist()
        excited_mode_list: list[int] = []
        mode_complex_amplitude = np.zeros(0, dtype=complex)

        if self.target == "molecule":
            channel_amplitude = source_complex
        else:
            # Project the real-space vortex pattern directly onto the cavity
            # modes, using the same calculation as k_parallel_pulse.
            axis_index = 0 if self.projection_axis == "x" else 1
            raw_projection = _project_onto_modes(
                self.cavity, selected, source_complex, axis_index
            )
            projection_norm = float(np.max(np.abs(raw_projection)))
            if not np.isfinite(projection_norm) or projection_norm <= 0.0:
                raise ValueError(
                    "The vortex seed has zero overlap with every photon mode."
                )
            mode_mask = np.abs(raw_projection) > projection_norm * 1e-12
            excited_mode_list = np.flatnonzero(mode_mask).astype(int).tolist()
            mode_complex_amplitude = raw_projection[mode_mask] / projection_norm
            channel_amplitude = mode_complex_amplitude

        def seed(time_au: float) -> np.ndarray:
            time = float(time_au)
            gaussian = math.exp(-0.5 * ((time - t0_au) / sigma_au) ** 2)
            carrier = np.exp(1j * (omega_au * time + phase_rad))
            return amplitude_au * gaussian * np.real(carrier * channel_amplitude)

        seed.target = self.target
        seed.excited_grid_list = excited_grid_list
        seed.excited_mode_list = excited_mode_list
        seed.grid_xy = grid_xy[selected, :]
        seed.spatial_window = selected_window
        seed.spatial_phase = selected_phase
        seed.mode_complex_amplitude = mode_complex_amplitude
        seed.charge = charge
        seed.center = center
        seed.waist = waist
        self._pulses.append(seed)
        self._rebuild_indices()
        return self

    def _rebuild_indices(self) -> None:
        """Build one ordered output index list shared by every stored pulse."""

        # Keep both diagnostic lists consistent with k_parallel_pulse. Only
        # one of them determines the returned array, depending on the target.
        for public_name in ("excited_mode_list", "excited_grid_list"):
            combined = sorted(
                {
                    index
                    for pulse in self._pulses
                    for index in getattr(pulse, public_name)
                }
            )
            getattr(self, public_name)[:] = combined

        if self.target == "photon":
            index_name = "excited_mode_list"
        else:
            index_name = "excited_grid_list"

        combined_indices = getattr(self, index_name)

        output_position = {
            index: position for position, index in enumerate(combined_indices)
        }
        self._index_maps = [
            np.array(
                [output_position[index] for index in getattr(pulse, index_name)],
                dtype=int,
            )
            for pulse in self._pulses
        ]

    def __call__(self, time_au: float) -> np.ndarray:
        """Return the real sum of the original pulse and all vortex seeds."""

        if self.target == "photon":
            output_size = len(self.excited_mode_list)
        else:
            output_size = len(self.excited_grid_list)

        total = np.zeros(output_size, dtype=float)
        for pulse, output_indices in zip(self._pulses, self._index_maps):
            total[output_indices] += np.asarray(pulse(time_au), dtype=float)
        return total

    def __getattr__(self, name):
        """Expose diagnostic attributes such as ``k_order`` from the base pulse."""

        return getattr(self.base_pulse, name)


def k_parallel_pulse_with_seed(
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
) -> KParallelPulseWithSeed:
    """Build a k-parallel pulse that accepts additional vortex seeds.

    This function takes the same arguments as :func:`k_parallel_pulse`. After
    construction, call :meth:`add_vortex_seed` one or
    more times. The returned object can then be passed directly to
    ``MultiModeSimulation`` as its photon or molecule pulse drive.
    """

    base_pulse = k_parallel_pulse(
        cavity=cavity,
        envelope=envelope,
        omega_au=omega_au,
        k_parallel_au=k_parallel_au,
        direction=direction,
        center=center,
        size=size,
        amplitude_au=amplitude_au,
        phase_rad=phase_rad,
        target=target,
        projection_axis=projection_axis,
    )
    projection_axis_clean = (
        "y" if projection_axis is None else str(projection_axis).strip().lower()
    )
    return KParallelPulseWithSeed(
        base_pulse=base_pulse,
        cavity=cavity,
        projection_axis=projection_axis_clean,
    )
