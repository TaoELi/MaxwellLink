# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
Evaluating the transverse components of a Gaussian polarization distribution in 3D, 2D, and 1D systems.
"""

import numpy as np

transverse_component_dir = {}


def calc_transverse_components_3d(
    size=(20, 20, 20), dx=1.0, sigma=1.0, mu12=0.10, local_size=100, component="z"
):
    """
    Calculate the transverse components of a 3D Gaussian polarization distribution.

    Parameters
    ----------
    size : tuple of float of length 3
        The size of the simulation box in each dimension.
    dx : float
        The spatial resolution (grid spacing) = 1 / resolution.
    sigma : float
        The width of the Gaussian distribution.
    mu12 : float
        The transition dipole moment scaling factor.
    local_size : float
        The size of the local box for FFT calculations, which should be larger than `size` for convergence.
    component : str
        The component of the polarization to calculate ('x', 'y', or 'z').  Default is 'z'.

    """
    # Check if the result is already computed
    key = (size, dx, sigma, mu12, local_size, component)
    if key in transverse_component_dir:
        return transverse_component_dir[key]

    # Doing FFT, I need a huge box to make sure my result is converged
    sigma0 = sigma
    x = np.arange(-local_size / 2.0, local_size / 2.0, dx)
    y = np.arange(-local_size / 2.0, local_size / 2.0, dx)
    z = np.arange(-local_size / 2.0, local_size / 2.0, dx)
    [X, Y, Z] = np.meshgrid(x, y, z)

    R2 = X * X + Y * Y + Z * Z

    Px = 0.0 * X
    Py = 0.0 * Y
    Pz = 0.0 * Z
    if component == "z":
        Pz = (
            (mu12 / (2.0 * np.pi) ** (1.5) / sigma0**5)
            * Z**2
            * np.exp(-R2 / 2.0 / sigma0**2)
        )
    elif component == "x":
        Px = (
            (mu12 / (2.0 * np.pi) ** (1.5) / sigma0**5)
            * X**2
            * np.exp(-R2 / 2.0 / sigma0**2)
        )
    elif component == "y":
        Py = (
            (mu12 / (2.0 * np.pi) ** (1.5) / sigma0**5)
            * Y**2
            * np.exp(-R2 / 2.0 / sigma0**2)
        )

    Pz0 = (
        (mu12 / (2.0 * np.pi) ** (1.5) / sigma0**5)
        * Z**2
        * np.exp(-R2 / 2.0 / sigma0**2)
    )

    # Perform FFT of Px, Py, and Pz
    kx = 2.0 * np.pi * np.fft.fftfreq(np.size(x), d=dx) + 1e-20
    ky = 2.0 * np.pi * np.fft.fftfreq(np.size(y), d=dx) + 1e-20
    kz = 2.0 * np.pi * np.fft.fftfreq(np.size(z), d=dx) + 1e-20
    [Kx, Ky, Kz] = np.meshgrid(kx, ky, kz)

    K2 = Kx**2 + Ky**2 + Kz**2

    FPx = np.fft.fftn(Px)
    FPy = np.fft.fftn(Py)
    FPz = np.fft.fftn(Pz)

    # renormalization factor
    RF = 1.0  # KM2 / (K2 + KM2)

    FPx_t = (
        (1.0 * RF - Kx * Kx / (K2 / RF)) * FPx
        + (0.0 * RF - Kx * Ky / (K2 / RF)) * FPy
        + (0.0 * RF - Kx * Kz / (K2 / RF)) * FPz
    )
    FPy_t = (
        (0.0 * RF - Ky * Kx / (K2 / RF)) * FPx
        + (1.0 * RF - Ky * Ky / (K2 / RF)) * FPy
        + (0.0 * RF - Ky * Kz / (K2 / RF)) * FPz
    )
    FPz_t = (
        (0.0 * RF - Kz * Kx / (K2 / RF)) * FPx
        + (0.0 * RF - Kz * Ky / (K2 / RF)) * FPy
        + (1.0 * RF - Kz * Kz / (K2 / RF)) * FPz
    )

    # Perform the reverse FFT to get the transverse components
    Px_t = np.real(np.fft.ifftn(FPx_t))
    Py_t = np.real(np.fft.ifftn(FPy_t))
    Pz_t = np.real(np.fft.ifftn(FPz_t))

    # Up to now, I calculate the local variables, they are huge to make sure that our FFT is convergent
    # Here, I need to make sure to output only the needed results
    start_idx_x, end_idx_x = int((local_size - size[0]) / 2 / dx), int(
        (local_size + size[0]) / 2 / dx
    )
    start_idx_y, end_idx_y = int((local_size - size[1]) / 2 / dx), int(
        (local_size + size[1]) / 2 / dx
    )
    start_idx_z, end_idx_z = int((local_size - size[2]) / 2 / dx), int(
        (local_size + size[2]) / 2 / dx
    )

    # print(K2[start_idx_x:end_idx_x, start_idx_y:end_idx_y, start_idx_z:end_idx_z])
    Pz = Pz[start_idx_x:end_idx_x, start_idx_y:end_idx_y, start_idx_z:end_idx_z]
    Pz0 = Pz0[start_idx_x:end_idx_x, start_idx_y:end_idx_y, start_idx_z:end_idx_z]
    Px_t = Px_t[start_idx_x:end_idx_x, start_idx_y:end_idx_y, start_idx_z:end_idx_z]
    Py_t = Py_t[start_idx_x:end_idx_x, start_idx_y:end_idx_y, start_idx_z:end_idx_z]
    Pz_t = Pz_t[start_idx_x:end_idx_x, start_idx_y:end_idx_y, start_idx_z:end_idx_z]

    prefactor = dx**3 * (np.sum(Px_t) + np.sum(Py_t) + np.sum(Pz_t)) / mu12

    # let's include only the diagonal component for normalization
    if component == "z":
        prefactor = dx**3 * np.sum(Pz_t) / mu12
    elif component == "x":
        prefactor = dx**3 * np.sum(Px_t) / mu12
    elif component == "y":
        prefactor = dx**3 * np.sum(Py_t) / mu12

    Px_t = np.copy(Px_t.astype(np.complex128), order="C") / prefactor
    Py_t = np.copy(Py_t.astype(np.complex128), order="C") / prefactor
    Pz_t = np.copy(Pz_t.astype(np.complex128), order="C") / prefactor
    Pz = np.copy(Pz0.astype(np.complex128), order="C")

    # save to the global dictionary
    transverse_component_dir[key] = (Pz, Px_t, Py_t, Pz_t)

    return Pz, Px_t, Py_t, Pz_t
