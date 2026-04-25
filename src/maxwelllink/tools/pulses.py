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
from units import FS_TO_AU, AU_TO_CM_INV
import math
from typing import Callable

__all__ = [
    "gaussian_pulse",
    "gaussian_enveloped_cosine",
    "cosine_drive",
]


def gaussian_pulse(
    time_unit: str = "fs",
    amplitude_au: float = 1.0,
    t0: float = 0.0,
    sigma: float = 10.0,
    t_start: float = 0.0,
    t_end: float = 1e10,
) -> Callable[[float], float]:
    r"""
    Return a Gaussian pulse drive.

    .. math::

        E(t) = A \exp\left(-\frac{(t - t_0)^2}{2 \sigma^2}\right)

    Parameters
    ----------
    time_unit : str, default: "fs"
        Time unit for the parameters. Can be "fs" (femtoseconds) or "au" (atomic units).
    amplitude_au : float, default: 1.0
        Peak field amplitude in atomic units.
    t0 : float, default: 0.0
        Temporal center of the pulse in atomic units.
    sigma : float, default: 10.0
        Temporal sigma in atomic units.
    t_start : float, default: 0.0
        Time before which the pulse is zero (atomic units).
    t_end : float, default: 1e10
        Time after which the pulse is zero (atomic units).

    Returns
    -------
    callable
        A function ``f(t_au)`` that evaluates the Gaussian pulse at ``t_au``.
    """
    amplitude = float(amplitude_au)

    if time_unit not in ("fs", "au"):
        raise ValueError(f"Invalid time_unit: {time_unit}. Must be 'fs' or 'au'.")

    sigma_func = float(sigma) if time_unit == "au" else float(sigma) * FS_TO_AU
    t0_func = float(t0) if time_unit == "au" else float(t0) * FS_TO_AU
    t_start_func = float(t_start) if time_unit == "au" else float(t_start) * FS_TO_AU
    t_end_func = float(t_end) if time_unit == "au" else float(t_end) * FS_TO_AU

    def _drive(t_au: float) -> float:
        if t_au < t_start_func or t_au > t_end_func:
            return 0.0
        x = (float(t_au) - t0_func) / sigma_func
        return amplitude * math.exp(-0.5 * x * x)

    return _drive


def gaussian_enveloped_cosine(
    time_unit: str = "fs",
    frequency_unit: str = "cm^-1",
    amplitude_au: float = 1.0,
    t0: float = 0.0,
    sigma: float = 10.0,
    omega: float = 0.1,
    phase_rad: float = 0.0,
    t_start: float = 0.0,
    t_end: float = 1e10,
) -> Callable[[float], float]:
    r"""
    Return a Gaussian-enveloped cosine drive.

    .. math::

        E(t) = A \exp\left(-\frac{(t - t_0)^2}{2 \sigma^2}\right)
        \cos\bigl(\omega (t - t_0) + \phi\bigr)

    Parameters
    ----------
    time_unit : str, default: "fs"
        Time unit for the parameters. Can be "fs" (femtoseconds) or "au" (atomic units).
    frequency_unit : str, default: "cm^-1"
        Frequency unit for the ``omega`` parameter. Can be "cm^-1" or "au" (atomic units).
    amplitude_au : float, default: 1.0
        Peak field amplitude in atomic units.
    t0 : float, default: 0.0
        Temporal center of the pulse in atomic units.
    sigma : float, default: 10.0
        Temporal sigma in atomic units.
    omega : float, default: 0.1
        Angular frequency of the cosine wave in atomic units.
    phase_rad : float, default: 0.0
        Phase of the cosine wave (radians).
    t_start : float, default: 0.0
        Time before which the pulse is zero (atomic units).
    t_end : float, default: 1e10
        Time after which the pulse is zero (atomic units).

    Returns
    -------
    callable
        A function ``f(t_au)`` for use as a time-dependent electric field.
    """

    amplitude = float(amplitude_au)

    if time_unit not in ("fs", "au"):
        raise ValueError(f"Invalid time_unit: {time_unit}. Must be 'fs' or 'au'.")

    sigma_func = float(sigma) if time_unit == "au" else float(sigma) * FS_TO_AU
    t0_func = float(t0) if time_unit == "au" else float(t0) * FS_TO_AU
    t_start_func = float(t_start) if time_unit == "au" else float(t_start) * FS_TO_AU
    t_end_func = float(t_end) if time_unit == "au" else float(t_end) * FS_TO_AU

    if frequency_unit not in ("cm^-1", "au"):
        raise ValueError(f"Invalid frequency_unit: {frequency_unit}. Must be 'cm^-1' or 'au'.")

    omega_func = float(omega) if frequency_unit == "au" else float(omega) / AU_TO_CM_INV
    phase = float(phase_rad)

    def _drive(t_au: float) -> float:
        if t_au < t_start_func or t_au > t_end_func:
            return 0.0
        t = float(t_au) - t0_func
        envelope = math.exp(-0.5 * (t / sigma_func) ** 2)
        return amplitude * envelope * math.cos(omega_func * t + phase)

    return _drive


def cosine_drive(
    time_unit: str = "fs",
    frequency_unit: str = "cm^-1",
    amplitude_au: float = 1.0,
    omega: float = 0.1,
    phase_rad: float = 0.0,
    t_start: float = 0.0,
    t_end: float = 1e10,
) -> Callable[[float], float]:
    r"""
    Return a continuous cosine drive.

    .. math::

        E(t) = A \cos(\omega t + \phi)

    Parameters
    ----------
    time_unit : str, default: "fs"
        Time unit for the parameters. Can be "fs" (femtoseconds) or "au" (atomic units).
    frequency_unit : str, default: "cm^-1"
        Frequency unit for the ``omega`` parameter. Can be "cm^-1" or "au" (atomic units).
    amplitude_au : float, default: 1.0
        Oscillation amplitude in atomic units.
    omega_au : float, default: 0.1
        Angular frequency in atomic units.
    phase_rad : float, default: 0.0
        Phase offset in radians.
    t_start : float, default: 0.0
        Time before which the drive is zero (atomic units).
    t_end : float, default: 1e10
        Time after which the drive is zero (atomic units).
    Returns
    -------
    callable
        A cosine drive suitable for steady-state excitation.
    """

    amplitude = float(amplitude_au)
    
    if frequency_unit not in ("cm^-1", "au"):
        raise ValueError(f"Invalid frequency_unit: {frequency_unit}. Must be 'cm^-1' or 'au'.")

    omega_func = float(omega) if frequency_unit == "au" else float(omega) / AU_TO_CM_INV
    phase = float(phase_rad)

    if time_unit not in ("fs", "au"):
        raise ValueError(f"Invalid time_unit: {time_unit}. Must be 'fs' or 'au'.")

    t_start_func = float(t_start) if time_unit == "au" else float(t_start) * FS_TO_AU
    t_end_func = float(t_end) if time_unit == "au" else float(t_end) * FS_TO_AU

    def _drive(t_au: float) -> float:
        if t_au < t_start_func or t_au > t_end_func:
            return 0.0
        return amplitude * math.cos(omega_func * float(t_au) + phase)

    return _drive
