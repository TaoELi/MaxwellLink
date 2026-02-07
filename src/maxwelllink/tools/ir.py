# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
Utilities for spectral post-processing of MD simulations.
"""

import numpy as np
from scipy import signal, fftpack


def smooth(x, window_len=11, window="hamming"):
    """Smooth 1D data using a windowed moving-average convolution.

    Parameters
    ----------
    x : numpy.ndarray
        1D input signal to smooth.
    window_len : int, default: 11
        Size of the smoothing window. Must be an odd integer greater than or
        equal to 3.
    window : {'flat', 'hanning', 'hamming', 'bartlett', 'blackman'}, default: 'hamming'
        Window function used for smoothing. ``'flat'`` corresponds to a moving
        average.

    Returns
    -------
    numpy.ndarray
        Smoothed signal.
    """
    if window_len < 3:
        return x

    s = np.r_[x[window_len - 1 : 0 : -1], x, x[-2 : -window_len - 1 : -1]]
    if window == "flat":  # moving average
        w = np.ones(window_len, "d")
    else:
        w = eval("np." + window + "(window_len)")

    y = np.convolve(w / w.sum(), s, mode="valid")
    return y[window_len // 2 - 1 : -window_len // 2]


def auto_correlation_function(x):
    """Compute the autocorrelation of a 1D array via FFT-based convolution.

    Parameters
    ----------
    x : numpy.ndarray
        1D input array for which to compute the autocorrelation function.

    Returns
    -------
    numpy.ndarray
        Autocorrelation of the input signal, truncated to half-length.
    """
    n = x.size
    if n % 2 == 0:
        x_shifted = np.zeros(n * 2)
    else:
        x_shifted = np.zeros(n * 2 - 1)
    x_shifted[n // 2 : n // 2 + n] = x
    # Convolute the shifted array with the flipped array, which is equivalent to performing a correlation
    autocorr_full = signal.fftconvolve(x_shifted, x[::-1], mode="same")[
        -n:
    ] / np.arange(n, 0, -1)
    # Truncate the autocorrelation array
    autocorr = autocorr_full[0 : n // 2]
    return autocorr


def fft(x, dtfs, N=None, field_description="square"):
    """Compute a DCT-based spectrum for a 1D signal.

    Parameters
    ----------
    x : numpy.ndarray
        1D time-domain signal.
    dtfs : float
        Time step in femtoseconds.
    N : int or None, optional
        Number of points for the discrete cosine transform. ``None`` (default)
        uses ``x.size``. Values greater than ``x.size`` result in zero-padding.
    field_description : {'square', 'none'}, default: 'square'
        Field prefactor applied to the DCT output. Use ``'square'`` for dipole
        autocorrelation functions and ``'none'`` for velocity autocorrelations.

    Returns
    -------
    numpy.ndarray
        Frequencies in :math:`\\text{cm}^{-1}`.
    numpy.ndarray
        Spectral intensities.

    Raises
    ------
    ValueError
        If ``field_description`` is not ``'square'`` or ``'none'``.
    """
    # Adding zeros to the end of x
    if N is not None:
        n = N
    else:
        n = np.size(x)
    lineshape = fftpack.dct(x, type=1, n=n)
    freq_au = np.linspace(0, 0.5 / dtfs * 1e15, n)
    # Because dt has the unit of fs, I need to transform fs^{-1} to cm^{-1}
    freq_cminverse = freq_au / (100.0 * 299792458.0)
    # Calculate spectra
    if field_description == "square":
        field_description = freq_au**2
    elif field_description == "none":
        field_description = 1.0
    else:
        raise ValueError("field_description must be 'square' or 'none'")
    spectra = field_description * lineshape
    return freq_cminverse, spectra


def ir_spectrum(x, dtfs, N=None, field_description="square", smooth_window_len=11):
    """Compute an infrared spectrum from a dipole trajectory.

    Parameters
    ----------
    x : numpy.ndarray
        Dipole moment trajectory.
    dtfs : float
        Time step in femtoseconds.
    N : int or None, optional
        Number of DCT points. ``None`` (default) uses ``x.size``. Values greater
        than ``x.size`` result in zero-padding.
    field_description : {'square', 'none'}, default: 'square'
        Field prefactor passed to :func:`fft`. Use ``'square'`` for dipole
        autocorrelation functions and ``'none'`` for velocity autocorrelations.
    smooth_window_len : int or None, optional
        Window length applied to smooth the spectrum. ``None`` disables
        smoothing. Default is 11.

    Returns
    -------
    numpy.ndarray
        Frequencies in :math:`\\text{cm}^{-1}`.
    numpy.ndarray
        Smoothed IR spectral intensities.

    Raises
    ------
    ValueError
        If ``field_description`` is not ``'square'`` or ``'none'``.
    """
    # 1. compute autocorrelation function
    x = auto_correlation_function(x)
    # 2. compute FFT of the autocorrelation function
    freq_cminverse, ir_spectra = fft(x, dtfs, N=N, field_description=field_description)
    # 3. smooth the IR spectra if required
    if smooth_window_len is not None:
        ir_spectra = smooth(ir_spectra, window_len=smooth_window_len, window="hamming")
    return freq_cminverse, ir_spectra
