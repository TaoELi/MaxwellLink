"""Tools for spectral post-processing of RT- and LR-TDDFT simulations."""

import numpy as np
from maxwelllink.units import AU_TO_FS, AU_TO_EV

linewidth = 1e-2


def _pade(
    time,
    signal,
    sigma=1e5,
    max_len=None,
    w_min=0.05,
    w_max=2,
    w_step=1e-5,
    read_freq=None,
):
    """Compute the Pade-approximant Fourier transform of a time signal.

    Parameters
    ----------
    time : array_like
        Sampling times of the input signal.
    signal : array_like
        Time-domain signal values.
    sigma : float, default: 1e5
        Exponential damping factor controlling the peak full width at half
        maximum (``linewidth ~ 2 / sigma``).
    max_len : int or None, optional
        Maximum number of time samples to include. ``None`` uses the full
        signal.
    w_min : float, default: 0.05
        Lower bound of the returned frequency window in atomic units.
    w_max : float, default: 2
        Upper bound of the returned frequency window in atomic units.
    w_step : float, default: 1e-5
        Frequency grid spacing in atomic units.
    read_freq : numpy.ndarray or None, optional
        Predefined frequency grid. If provided, ``w_min``, ``w_max``, and
        ``w_step`` are ignored.

    Returns
    -------
    numpy.ndarray
        Complex-valued spectrum evaluated on the frequency grid.
    numpy.ndarray
        Frequency grid in atomic units.

    References
    ----------
    .. [1] A. Bruner, D. LaMaster, K. Lopata,
       *Accelerated broadband spectra using transition signal decomposition
       and Pade approximants*, J. Chem. Theory Comput. **12**, 3741 (2016).
    """

    # center signal about zero
    signal = np.asarray(signal) - signal[0]

    stepsize = time[1] - time[0]

    # Damp the signal with an exponential decay.
    damp = np.exp(-(stepsize * np.arange(len(signal))) / float(sigma))
    signal *= damp

    M = len(signal)
    N = int(np.floor(M / 2))

    # Check signal length, and truncate if too long
    if max_len:
        if M > max_len:
            N = int(np.floor(max_len / 2))

    # G and d are (N-1) x (N-1)
    # d[k] = -signal[N+k] for k in range(1,N)
    d = -signal[N + 1 : 2 * N]

    try:
        from scipy.linalg import toeplitz, solve_toeplitz

        # Instead, form G = (c,r) as toeplitz
        # c = signal[N:2*N-1]
        # r = np.hstack((signal[1],signal[N-1:1:-1]))
        b = solve_toeplitz(
            (signal[N : 2 * N - 1], np.hstack((signal[1], signal[N - 1 : 1 : -1]))),
            d,
            check_finite=False,
        )
    except (ImportError, np.linalg.linalg.LinAlgError) as e:
        # OLD CODE: sometimes more stable
        # G[k,m] = signal[N - m + k] for m,k in range(1,N)
        G = signal[N + np.arange(1, N)[:, None] - np.arange(1, N)]
        b = np.linalg.solve(G, d)
        print("Warning: using slower Pade code", e)

    # Now make b Nx1 where b0 = 1
    b = np.hstack((1, b))

    # b[m]*signal[k-m] for k in range(0,N), for m in range(k)
    a = np.dot(np.tril(toeplitz(signal[0:N])), b)
    p = np.poly1d(np.flip(a))
    q = np.poly1d(np.flip(b))

    if read_freq is None:
        # choose frequencies to evaluate over
        frequency = np.arange(w_min, w_max, w_step)
    else:
        frequency = read_freq

    W = np.exp(-1j * frequency * stepsize)

    fsignal = p(W) / q(W)

    return fsignal, frequency


def _lorentz(x, w0, gamma, I):
    """Evaluate a Lorentzian line shape.

    Parameters
    ----------
    x : numpy.ndarray
        Frequency grid (in the same units as ``w0``).
    w0 : float
        Line-center frequency.
    gamma : float
        Full width at half maximum.
    I : float
        Line intensity prefactor.

    Returns
    -------
    numpy.ndarray
        Lorentzian line profile.
    """
    return I / np.pi * (0.5 * gamma) / ((x - w0) ** 2 + (0.5 * gamma) ** 2)


def rt_tddft_spectrum(
    mu,
    dt_au,
    sp_form="absorption",
    e_start_ev=0.5,
    e_cutoff_ev=30.0,
    sigma=1e5,
    w_step=1e-5,
):
    """Compute an RT-TDDFT spectrum via Pade-approximant Fourier transform.

    Parameters
    ----------
    mu : numpy.ndarray
        Time-dependent dipole moment in atomic units.
    dt_au : float
        Time step in atomic units.
    sp_form : {'absorption', 'absolute'}, default: 'absorption'
        Spectrum representation. ``'absorption'`` returns ``-omega * Im(mu_tilde(omega))``;
        ``'absolute'`` returns ``abs(mu_tilde(omega))``.
    e_start_ev : float, default: 0.5
        Lower energy cutoff in electron volts.
    e_cutoff_ev : float, default: 30.0
        Upper energy cutoff in electron volts.
    sigma : float, default: 1e5
        Damping factor passed to :func:`_pade`.
    w_step : float, default: 1e-5
        Frequency grid spacing in atomic units used by :func:`_pade`.

    Returns
    -------
    numpy.ndarray
        Frequency grid in electron volts.
    numpy.ndarray
        Spectrum on the selected grid with units determined by ``sp_form``.
    numpy.ndarray
        Time grid in femtoseconds.
    numpy.ndarray
        Dipole moment trajectory (identical to the input ``mu``).
    """

    t = np.arange(0, np.size(mu) * dt_au, dt_au)
    t_fs = t * AU_TO_FS

    e_cutoff_au = e_cutoff_ev / AU_TO_EV
    e_start_au = e_start_ev / AU_TO_EV

    sp, freq = _pade(
        t, mu, w_min=e_start_au, w_max=e_cutoff_au, w_step=w_step, sigma=sigma
    )

    freq_ev = freq * AU_TO_EV
    if sp_form == "absolute":
        sp_tot = np.abs(sp)
    elif sp_form == "absorption":
        sp_tot = -freq * np.imag(sp)
    df = freq_ev[1] - freq_ev[0]
    # output only selectively number of data
    nmax = int(e_cutoff_ev // df)
    freq_ev = freq_ev[0:nmax]
    sp_tot = sp_tot[0:nmax]
    return freq_ev, sp_tot, t_fs, mu


def lr_tddft_spectrum(energy_au, e_osc, e_cutoff_ev=30.0, linewidth=1e-2, w_step=1e-5):
    """Construct an LR-TDDFT spectrum using Lorentzian broadening.

    Parameters
    ----------
    energy_au : numpy.ndarray
        Excitation energies in atomic units.
    e_osc : numpy.ndarray
        Oscillator strengths corresponding to ``energy_au``.
    e_cutoff_ev : float, default: 30.0
        Upper bound of the returned frequency grid in electron volts.
    linewidth : float, default: 1e-2
        Lorentzian full width at half maximum in electron volts.
    w_step : float, default: 1e-5
        Energy grid spacing in electron volts.

    Returns
    -------
    numpy.ndarray
        Frequency grid in electron volts.
    numpy.ndarray
        Lorentzian-broadened spectrum.
    """

    energy_ev = energy_au * AU_TO_EV

    freq_ev = np.linspace(0, e_cutoff_ev, int(e_cutoff_ev / w_step) + 1)
    n = np.size(freq_ev)
    e_sp = np.zeros(n)
    for idx in range(len(energy_ev)):
        w0 = energy_ev[idx]
        I_e = e_osc[idx]
        e_sp += _lorentz(freq_ev, w0, linewidth, I_e)
    return freq_ev, e_sp
