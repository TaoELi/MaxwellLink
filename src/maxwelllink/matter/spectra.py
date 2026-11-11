# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# --------------------------------------------------------------------------------------#

"""
Absorption spectra from delta-kicked dipole trajectories.
"""

from dataclasses import dataclass, field

import numpy as np

from maxwelllink.units import AU_TO_EV, EV_TO_AU

_DIRECTIONS = {"x": 0, "y": 1, "z": 2}


@dataclass
class SpectrumResult:
    """
    An absorption spectrum extracted from a dipole trajectory.

    Attributes
    ----------
    energy_ev : numpy.ndarray, shape (n_omega,)
        Photon energy grid in eV.
    strength_au : numpy.ndarray, shape (n_omega,)
        Dipole strength function ``S(omega)`` along the kicked direction, in
        atomic units (1/Hartree).
    alpha_au : numpy.ndarray of complex, shape (n_omega, 3)
        The kicked column of the polarizability tensor, in atomic units.
    method : str
        ``'fft'`` or ``'pade'``.
    damping_ev : float
        Lorentzian broadening (HWHM) applied to the signal, in eV.
    """

    energy_ev: np.ndarray
    strength_au: np.ndarray
    alpha_au: np.ndarray
    method: str
    damping_ev: float
    details: dict = field(default_factory=dict)

    @property
    def omega_au(self):
        """The energy grid in atomic units (Hartree)."""

        return self.energy_ev * EV_TO_AU


def dipole_spectrum(
    time_au,
    dipole_au,
    kick_au,
    direction="z",
    method="pade",
    damping_ev=0.1,
    energy_range_ev=(0.0, 10.0),
    n_energies=2000,
    subtract_initial=True,
    pade_max_points=5000,
):
    """
    Absorption spectrum of a delta-kicked dipole trajectory.

    Parameters
    ----------
    time_au : array-like of float, shape (n,)
        Uniformly spaced sample times in atomic units (an offset first sample,
        e.g. the drivers' midpoint times ``(k + 1/2) dt``, is handled).
    dipole_au : array-like of float, shape (n, 3) or (n,)
        Dipole samples in atomic units. A 1D array is taken as the component
        along ``direction``.
    kick_au : float
        The delta-kick strength the trajectory was started with.
    direction : {'x', 'y', 'z'}, default: 'z'
        The kicked direction (selects the diagonal element for the strength
        function).
    method : {'pade', 'fft'}, default: 'pade'
        The discrete transform: the Pade-accelerated one of Bruner et al., or
        a plain damped Fourier sum.
    damping_ev : float, default: 0.1
        Lorentzian broadening (HWHM): the signal is multiplied by
        ``exp(-gamma t)`` with ``gamma = damping_ev`` (converted to a.u.).
    energy_range_ev : tuple of float, default: (0.0, 10.0)
        Photon-energy window of the output grid.
    n_energies : int, default: 2000
        Number of grid points.
    subtract_initial : bool, default: True
        Subtract the first dipole sample before transforming.
    pade_max_points : int, default: 5000
        The Pade solve is O(n^2) in the number of samples, so trajectories
        longer than this are truncated first (the method's premise is that a
        short signal is enough). ``None`` uses every sample.

    Returns
    -------
    SpectrumResult
    """

    time_au = np.asarray(time_au, dtype=float).reshape(-1)
    dipole_au = np.asarray(dipole_au, dtype=float)
    if dipole_au.ndim == 1:
        column = np.zeros((dipole_au.shape[0], 3))
        column[:, _DIRECTIONS[str(direction).lower()]] = dipole_au
        dipole_au = column
    if dipole_au.shape != (time_au.shape[0], 3):
        raise ValueError("dipole_au must have shape (n,) or (n, 3) matching time_au.")
    if time_au.shape[0] < 8:
        raise ValueError("need at least 8 samples for a spectrum.")
    if float(kick_au) == 0.0:
        raise ValueError("kick_au must be nonzero.")
    steps = np.diff(time_au)
    dt = float(steps[0])
    if not np.allclose(steps, dt, rtol=1e-8, atol=1e-12):
        raise ValueError("time_au must be uniformly spaced.")

    signal = dipole_au - dipole_au[0] if subtract_initial else dipole_au.copy()
    gamma_au = float(damping_ev) * EV_TO_AU
    elapsed = time_au - time_au[0]
    signal = signal * np.exp(-gamma_au * elapsed)[:, None]

    omega_au = (
        np.linspace(
            float(energy_range_ev[0]), float(energy_range_ev[1]), int(n_energies)
        )
        * EV_TO_AU
    )

    method = str(method).lower()
    if method == "fft":
        mu_omega = _fourier_sum(elapsed, signal, omega_au) * dt
    elif method == "pade":
        mu_omega = _pade_sum(signal, dt, omega_au, pade_max_points) * dt
    else:
        raise ValueError("method must be 'pade' or 'fft'.")
    # the first sample sits at time_au[0], not zero
    mu_omega = mu_omega * np.exp(1j * omega_au * time_au[0])[:, None]

    alpha = mu_omega / float(kick_au)
    kicked = _DIRECTIONS[str(direction).lower()]
    strength = (2.0 / np.pi) * omega_au * alpha[:, kicked].imag

    return SpectrumResult(
        energy_ev=omega_au * AU_TO_EV,
        strength_au=strength,
        alpha_au=alpha,
        method=method,
        damping_ev=float(damping_ev),
        details=dict(kick_au=float(kick_au), direction=str(direction).lower()),
    )


def _fourier_sum(elapsed, signal, omega_au, chunk=256):
    """``sum_k signal_k exp(i w t_k)`` on the requested grid, in omega chunks."""

    out = np.zeros((omega_au.shape[0], signal.shape[1]), dtype=complex)
    for start in range(0, omega_au.shape[0], chunk):
        block = omega_au[start : start + chunk]
        phases = np.exp(1j * block[:, None] * elapsed[None, :])
        out[start : start + chunk] = phases @ signal
    return out


def _pade_sum(signal, dt, omega_au, max_points):
    """
    The Pade approximant to ``sum_k signal_k z^k`` at ``z = exp(i w dt)``,
    column by column (Bruner, LaMaster, Lopata 2016).
    """

    n = signal.shape[0]
    if max_points is not None:
        n = min(n, int(max_points))
    order = (n - 1) // 2  # the [order/order] approximant needs 2*order+1 samples
    if order < 4:
        raise ValueError("too few samples for a Pade approximant.")

    z = np.exp(1j * omega_au * dt)
    out = np.zeros((omega_au.shape[0], signal.shape[1]), dtype=complex)
    for c in range(signal.shape[1]):
        d = signal[:n, c]
        if np.max(np.abs(d)) == 0.0:
            continue
        b = _pade_denominator(d, order)
        # a_n = sum_{m=0..n} b_m d_{n-m}: a polynomial product, truncated
        a = np.convolve(b, d[: order + 1])[: order + 1]
        out[:, c] = _polyval_ascending(z, a) / _polyval_ascending(z, b)
    return out


def _pade_denominator(d, order):
    """Solve the Toeplitz system ``G b = -d`` for the denominator coefficients."""

    col = d[order : 2 * order]  # G[k, 0] = d[order + k]
    row = d[order:0:-1]  # G[0, m] = d[order - m]
    rhs = -d[order + 1 : 2 * order + 1]
    try:
        from scipy.linalg import solve_toeplitz

        tail = solve_toeplitz((col, row), rhs)
        if not np.all(np.isfinite(tail)):
            raise np.linalg.LinAlgError
    except Exception:
        # Levinson recursion can be unstable; fall back to least squares
        from scipy.linalg import toeplitz

        tail = np.linalg.lstsq(toeplitz(col, row), rhs, rcond=None)[0]
    return np.concatenate(([1.0], tail))


def _polyval_ascending(z, coefficients):
    """Evaluate ``sum_k c_k z^k`` (ascending order) with Horner's scheme."""

    result = np.full_like(z, complex(coefficients[-1]))
    for c in coefficients[-2::-1]:
        result = result * z + c
    return result
