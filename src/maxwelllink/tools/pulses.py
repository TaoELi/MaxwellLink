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
from typing import Callable

__all__ = [
    "gaussian_pulse",
    "gaussian_enveloped_cosine",
    "cosine_drive",
]


def gaussian_pulse(
    amplitude_au: float = 1.0,
    t0_au: float = 0.0,
    sigma_au: float = 10.0,
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

    Returns
    -------
    callable
        A function ``f(t_au)`` that evaluates the Gaussian pulse at ``t_au``.
    """
    amplitude = float(amplitude_au)
    sigma = float(sigma_au)
    t0 = float(t0_au)

    def _drive(t_au: float) -> float:
        x = (float(t_au) - t0) / sigma
        return amplitude * math.exp(-0.5 * x * x)

    return _drive


def gaussian_enveloped_cosine(
    amplitude_au: float = 1.0,
    t0_au: float = 0.0,
    sigma_au: float = 10.0,
    omega_au: float = 0.1,
    phase_rad: float = 0.0,
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

    def _drive(t_au: float) -> float:
        t = float(t_au) - t0
        envelope = math.exp(-0.5 * (t / sigma) ** 2)
        return amplitude * envelope * math.cos(omega * t + phase)

    return _drive


def cosine_drive(
    amplitude_au: float = 1.0,
    omega_au: float = 0.1,
    phase_rad: float = 0.0,
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

    Returns
    -------
    callable
        A cosine drive suitable for steady-state excitation.
    """

    amplitude = float(amplitude_au)
    omega = float(omega_au)
    phase = float(phase_rad)

    def _drive(t_au: float) -> float:
        return amplitude * math.cos(omega * float(t_au) + phase)

    return _drive
