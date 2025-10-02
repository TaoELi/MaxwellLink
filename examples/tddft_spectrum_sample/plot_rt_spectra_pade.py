"""
This script contains the following two figures

(a) Plot dipole dynamics of a given molecule
(b) Plot electronic excited-state transitions using real-time TDDFT vs linear-response TDDFT
calculated from calculate_lr_rt_tddft.py, where the real-time dipole info is stored in
rt_tddft_energy_dipoles_0.txt, and the linear-response TDDFT states are stored in
psi4_lrtddft_output_id_0.txt.
"""

import numpy as np
import columnplots as clp

au2fs = 0.02418884254
au2eV = 27.211399
eV2cminv = 8065.540106923572

sigma = 1e5
w_step = 1e-5
linewidth = 4.0 / sigma * au2eV
linewidth = 1e-2
print("linewidth is", linewidth)
e_cutoff_ev = 30.0
e_cutoff_au = e_cutoff_ev / au2eV
e_start_ev = 0.5
e_start_au = e_start_ev / au2eV

fsinv2eV = 4.135668  # 1fs-1 to 4.13 eV

nskip = 10


def pade(
    time,
    signal,
    sigma=sigma,
    max_len=None,
    w_min=e_start_au,
    w_max=e_cutoff_au,
    w_step=w_step,
    read_freq=None,
):
    """Routine to take the Fourier transform of a time signal using the method
      of Pade approximants.
    Inputs:
      time:      (list or Numpy NDArray) signal sampling times
      signal:    (list or Numpy NDArray)
    Optional Inputs:
      sigma:     (float) signal damp factor, yields peaks with
                   FWHM of 2/sigma
      max_len:   (int) maximum number of points to use in Fourier transform
      w_min:     (float) lower returned frequency bound
      w_max:     (float) upper returned frequency bound
      w_step:    (float) returned frequency bin width
    Returns:
      fsignal:   (complex NDArray) transformed signal
      frequency: (NDArray) transformed signal frequencies
    From: Bruner, Adam, Daniel LaMaster, and Kenneth Lopata. "Accelerated
      broadband spectra using transition signal decomposition and Pade
      approximants." Journal of chemical theory and computation 12.8
      (2016): 3741-3750.
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


def lorentz(x, w0, gamma, I):
    return I / np.pi * (0.5 * gamma) / ((x - w0) ** 2 + (0.5 * gamma) ** 2)


def gaussian(x, w0, gamma, I):
    return (
        I
        / (2.0 * np.pi * gamma**2) ** (0.5)
        * np.exp(-((x - w0) ** 2) / 2.0 / gamma**2)
    )


def get_LR_spectrum(filename):
    data = np.loadtxt(filename)
    energy, e_osc = data[:, 0], data[:, -1]
    freq_ev = np.linspace(0, e_cutoff_ev, int(e_cutoff_ev / w_step) + 1)
    n = np.size(freq_ev)
    e_sp = np.zeros(n)
    for idx in range(len(energy)):
        w0 = energy[idx]
        I_e = e_osc[idx]
        e_sp += lorentz(freq_ev, w0, linewidth, I_e)
    return freq_ev, e_sp


def get_dipole(filename):
    data = np.loadtxt(filename)
    nst = int(np.shape(data)[0] * 0.0)
    nend = int(np.shape(data)[0] * 1.0)
    t, mux, muy, muz = (
        data[nst:nend:nskip, 0],
        data[nst:nend:nskip, 2],
        data[nst:nend:nskip, 3],
        data[nst:nend:nskip, 4],
    )
    mux -= mux[0]
    muy -= muy[0]
    muz -= muz[0]
    t_fs = t * au2fs

    spx, freq = pade(t, mux)
    spy, freq = pade(t, muy)
    spz, freq = pade(t, muz)

    freq_ev = freq * au2eV
    # sp_tot = np.abs(spx) + np.abs(spy) + np.abs(spz)
    sp_tot = -freq * (
        np.imag(spz) + np.imag(spy) + np.imag(spx)
    )  # dipole velocity form
    df = freq_ev[1] - freq_ev[0]
    # output only selectively number of data
    nmax = int(e_cutoff_ev // df)
    freq_ev = freq_ev[0:nmax]
    sp_tot = sp_tot[0:nmax]
    return t_fs, muz, freq_ev, sp_tot


# get linear response data
freq_lr, e_sp_lr = get_LR_spectrum(filename="./psi4_lrtddft_output_id_0.txt")


def plot_dynamics():
    # get real-time data
    path = "./"
    t, mu_z, freq, sp = get_dipole(filename=path + "rt_tddft_energy_dipoles_0.txt")

    fig, axes = clp.initialize(
        1,
        2,
        width=8,
        height=4,
        return_fig_args=True,
        fontsize=12,
        LaTeX=False,
        labelthem=True,
        labelthemPosition=[0.1, 0.963],
        labelsize=13,
    )

    labels = ["RT-TDDFT", "LR-TDDFT"]
    colors = [clp.navy_blue, "k--"]

    x1s, y1s = [t], [mu_z]
    clp.plotone(
        x1s,
        y1s,
        axes[0],
        colors=colors,
        labels=labels,
        lw=0.5,
        ylabel=r"$\mu_z^{\rm e}(t)$ [a.u.]",
        xlabel=r"Time [fs]",
        showlegend=False,
        yscientific=True,
    )

    x2s, y2s = [freq, freq_lr], [
        sp / np.max(sp),
        e_sp_lr / np.max(e_sp_lr),
    ]
    clp.plotone(
        x2s,
        y2s,
        axes[1],
        colors=colors,
        labels=labels,
        lw=1,
        ylabel=r"$P_{\rm e}(\omega)$ [arb. units]",
        xlabel=r"Frequency [eV]",
    )

    clp.adjust(tight_layout=True, savefile="rt_vs_lr_tddft_spectrum.png")


if __name__ == "__main__":
    plot_dynamics()
