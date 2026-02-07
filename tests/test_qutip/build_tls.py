# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

import numpy as np
import qutip as qt


def build_model(
    omega=1.0,
    mu12=0.1,
    orientation=2,
    pe_initial=0.0,
    gamma_relax=0.0,
    gamma_dephase=0.0,
):
    r"""
    Simple 2-level model preset like TLSModel, but using QuTiP objects.
    H0 = $\vert e \rangle \langle e \vert$ * omega
    mu = mu12 * ($\vert g \rangle \langle e \vert$ + $\vert e \rangle \langle g \vert$) along chosen axis
    Lindblad: relaxation ($\sigma_{-}$) at rate gamma_relax, pure dephasing at gamma_dephase.

    This function provides a reference implementation for the `build_model(**kwargs)` function.

    + **`omega`** (float): Transition frequency in atomic units (a.u.).
    + **`mu12`** (float): Dipole moment in atomic units (a.u.).
    + **`orientation`** (int): Orientation of the dipole moment, can be 0 (x), 1 (y), or 2 (z). Default is 2 (z).
    + **`pe_initial`** (float): Initial population in the excited state. Default is 0.0.
    + **`gamma_relax`** (float): Relaxation rate (a.u.). Default is 0.0.
    + **`gamma_dephase`** (float): Pure dephasing rate (a.u.). Default is 0.0.
    """
    # basis
    g = qt.basis(2, 0)
    e = qt.basis(2, 1)
    H0 = omega * e * e.dag()

    # dipole operator along chosen axis; others set to None
    sigmax = g * e.dag() + e * g.dag()
    mux = muy = muz = None
    dip = mu12 * sigmax
    if orientation == 0:
        mux = dip
    elif orientation == 1:
        muy = dip
    else:
        muz = dip
    mu_ops = {"x": mux, "y": muy, "z": muz}

    # collapse operators
    c_ops = []
    if gamma_relax > 0.0:
        c_ops.append(np.sqrt(gamma_relax) * (g * e.dag()))
    if gamma_dephase > 0.0:
        c_ops.append(np.sqrt(gamma_dephase) * (e * e.dag() - g * g.dag()))

    # initial state (density matrix form of a coherent state)
    pe = pe_initial
    rho0 = (
        (1.0 - pe) * (g * g.dag())
        + pe * (e * e.dag())
        + np.sqrt(pe * (1.0 - pe)) * (g * e.dag() + e * g.dag())
    )

    return dict(H0=H0, mu_ops=mu_ops, c_ops=c_ops, rho0=rho0)
