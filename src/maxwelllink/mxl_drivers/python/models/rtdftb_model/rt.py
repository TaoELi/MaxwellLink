# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# --------------------------------------------------------------------------------------#

"""
Real-time TD-DFTB: the density matrix in time, and the propagators that move it.

The density matrix ``rho`` lives in the non-orthogonal atomic-orbital basis, so the
electron count is ``Tr(rho S)``. It is moved by ``d rho/dt = -i (S^-1 H rho - rho H
S^-1)`` plus a non-adiabatic term that only exists while the nuclei move.

Three propagators are provided. ``leapfrog`` is the default because it is what DFTB+
runs, so it reproduces the external DFTB+ driver step for step. ``cayley-midpoint`` is a
non-orthogonal Crank-Nicolson step with one predictor-corrector pass; it is more accurate
per step but does not match DFTB+ exactly. Plain ``cayley`` is kept for comparison only:
with the SCC charge feedback it is first order and can be unstable.

Every kernel here is a scalar loop over pre-allocated scratch, so the same body compiles
for the CPU and for the GPU.
"""

import math

import numpy as np

try:  # inside the package
    from .h0_overlap import build_h0_overlap
    from .kernels_dftb import kernel
    from .scc import build_gamma, repulsive_total, scc_hamiltonian, scc_potential
except (ImportError, ValueError):  # allow running as a stand-alone script
    from h0_overlap import build_h0_overlap
    from kernels_dftb import kernel
    from scc import build_gamma, repulsive_total, scc_hamiltonian, scc_potential


# ---------------------------------------------------------------------------- #
# dense linear algebra, written out so it transcribes into a device function    #
# ---------------------------------------------------------------------------- #
@kernel
def lu_factor(a, n, pivot):
    """
    In-place LU factorisation with partial pivoting, ``getrf`` in loop form.

    ``a`` comes back holding ``L`` strictly below the diagonal (unit diagonal implied)
    and ``U`` on and above it; ``pivot[k]`` is the row that was swapped into row ``k``.
    Returns the index of the first zero pivot, or ``-1`` when the factorisation is
    regular. Works unchanged on a real or a complex matrix.
    """

    singular = -1
    for k in range(n):
        biggest = abs(a[k, k])
        row = k
        for i in range(k + 1, n):
            candidate = abs(a[i, k])
            if candidate > biggest:
                biggest = candidate
                row = i
        pivot[k] = row
        if row != k:
            for j in range(n):
                swap = a[k, j]
                a[k, j] = a[row, j]
                a[row, j] = swap
        if biggest == 0.0:
            if singular < 0:
                singular = k
            continue
        diagonal = a[k, k]
        for i in range(k + 1, n):
            factor = a[i, k] / diagonal
            a[i, k] = factor
            for j in range(k + 1, n):
                a[i, j] -= factor * a[k, j]
    return singular


@kernel
def lu_solve(a, pivot, b, n, n_rhs):
    """Solve ``A X = B`` in place from the output of :func:`lu_factor`, ``getrs``."""

    for k in range(n):
        row = pivot[k]
        if row != k:
            for j in range(n_rhs):
                swap = b[k, j]
                b[k, j] = b[row, j]
                b[row, j] = swap
    for k in range(n):  # forward substitution, unit lower triangle
        for i in range(k + 1, n):
            factor = a[i, k]
            for j in range(n_rhs):
                b[i, j] -= factor * b[k, j]
    for k in range(n - 1, -1, -1):  # back substitution
        diagonal = a[k, k]
        for j in range(n_rhs):
            b[k, j] /= diagonal
        for i in range(k):
            factor = a[i, k]
            for j in range(n_rhs):
                b[i, j] -= factor * b[k, j]


@kernel
def lu_invert(a, n, work, pivot, inverse):
    """Full inverse of ``a`` through the same LU; this is how DFTB+ builds ``S^-1``."""

    for i in range(n):
        for j in range(n):
            work[i, j] = a[i, j]
            inverse[i, j] = 0.0
        inverse[i, i] = 1.0
    lu_factor(work, n, pivot)
    lu_solve(work, pivot, inverse, n, n)


@kernel
def matmul(a, b, out, n):
    """``out = a @ b`` for square matrices of either precision."""

    for i in range(n):
        for j in range(n):
            total = a[i, 0] * b[0, j]
            for k in range(1, n):
                total += a[i, k] * b[k, j]
            out[i, j] = total


@kernel
def matmul_adj(a, b, out, n):
    """``out = a @ b^dagger``, the second half of the Cayley similarity transform."""

    for i in range(n):
        for j in range(n):
            total = a[i, 0] * b[j, 0].conjugate()
            for k in range(1, n):
                total += a[i, k] * b[j, k].conjugate()
            out[i, j] = total


# ---------------------------------------------------------------------------- #
# initial state and the delta kick                                             #
# ---------------------------------------------------------------------------- #
@kernel
def initial_density(vectors, filling, rho, n):
    """
    Ground-state density matrix as complex128, ``rho_mu,nu = sum_i f_i C_mu,i C_nu,i``.

    ``C`` is S-orthonormal so ``Tr(rho S) = sum_i f_i = N_elec``; for a spin-restricted
    run the occupations run 0 to 2, not 0 to 1 (timeprop.F90:2657-2666).
    """

    for mu in range(n):
        for nu in range(n):
            total = 0.0
            for i in range(n):
                total += filling[i] * vectors[mu, i] * vectors[nu, i]
            rho[mu, nu] = total


@kernel
def kick_density(
    rho, overlap, s_inv, coords, orb_atom, kappa, pol_dir, phase, work_a, work_b, n
):
    """
    Apply the delta kick to the density matrix, ``kickDM``, timeprop.F90:1861-1876.

    Each atom's orbital block is multiplied by ``exp(-i kappa R_A[pol_dir])`` from the
    left and ``exp(+i ...)`` from the right through the overlap, and the result is
    re-hermitised: with ``W = E- rho S E+ S^-1`` the new density is ``(W + W^H) / 2``.
    This is the atom-centred (Mulliken) approximation of ``exp(-i kappa x)``, consistent
    with the point-charge dipole used everywhere else in the DFTB path.
    """

    for mu in range(n):
        phase[mu] = kappa * coords[orb_atom[mu], pol_dir]
    for i in range(n):  # left factor E- = diag(exp(-i phase))
        left = complex(math.cos(phase[i]), -math.sin(phase[i]))
        for j in range(n):
            work_a[i, j] = left * rho[i, j]
    matmul(work_a, overlap, work_b, n)
    for j in range(n):  # right factor E+ = diag(exp(+i phase))
        right = complex(math.cos(phase[j]), math.sin(phase[j]))
        for i in range(n):
            work_a[i, j] = work_b[i, j] * right
    matmul(work_a, s_inv, work_b, n)
    for i in range(n):
        for j in range(n):
            rho[i, j] = 0.5 * (work_b[i, j] + work_b[j, i].conjugate())


# ---------------------------------------------------------------------------- #
# observables                                                                  #
# ---------------------------------------------------------------------------- #
@kernel
def rt_orbital_charges(rho, overlap, q_orb, n):
    """Mulliken gross populations of a complex density, ``q_mu = Re[(S rho)_mu,mu]``."""

    for mu in range(n):
        total = 0.0
        for nu in range(n):
            total += rho[nu, mu].real * overlap[nu, mu]
        q_orb[mu] = total


@kernel
def atom_charges(q_orb, q0_orb, orb_atom, dq_atom, n, n_atom):
    """Per-atom electron excess ``dq_A = sum_{mu in A} (q_mu - q0_mu)``."""

    for a in range(n_atom):
        dq_atom[a] = 0.0
    for mu in range(n):
        dq_atom[orb_atom[mu]] += q_orb[mu] - q0_orb[mu]


@kernel
def shell_charges_from_orbital(q_orb, q0_orb, orb_shell, dq_shell, n, n_shell):
    """Per-shell electron excess; the SCC potential is shell resolved, not atom resolved."""

    for i in range(n_shell):
        dq_shell[i] = 0.0
    for mu in range(n):
        dq_shell[orb_shell[mu]] += q_orb[mu] - q0_orb[mu]


@kernel
def rt_dipole(dq_atom, coords, dipole, n_atom):
    """Dipole in atomic units, ``mu = -sum_A R_A dq_A`` (timeprop.F90:2148)."""

    for k in range(3):
        dipole[k] = 0.0
    for a in range(n_atom):
        for k in range(3):
            dipole[k] -= coords[a, k] * dq_atom[a]


@kernel
def trace_overlap(rho, overlap, n):
    """``Tr(rho S)``, the electron count the propagator must conserve."""

    total = 0.0
    for mu in range(n):
        for nu in range(n):
            total += rho[mu, nu].real * overlap[nu, mu]
    return total


@kernel
def idempotency_error(rho, overlap, work_a, work_b, n):
    """Largest element of ``rho S rho - rho``; zero for an exact one-determinant state."""

    matmul(rho, overlap, work_a, n)
    matmul(work_a, rho, work_b, n)
    worst = 0.0
    for i in range(n):
        for j in range(n):
            deviation = abs(work_b[i, j] - 2.0 * rho[i, j])
            if deviation > worst:
                worst = deviation
    return worst


# ---------------------------------------------------------------------------- #
# the instantaneous Hamiltonian                                                #
# ---------------------------------------------------------------------------- #
@kernel
def external_potential(coords, field, shell_atom, v_shell, n_shell):
    """
    Add the external-field potential ``V_A = +R_A . E(t)`` to the shell potential.

    A per-atom scalar, shell independent, exactly as ``updateH`` builds it
    (timeprop.F90:1675); it then folds into H through the same
    ``0.5 S (V_mu + V_nu)`` as the SCC shift.
    """

    for i in range(n_shell):
        atom = shell_atom[i]
        v_shell[i] += (
            coords[atom, 0] * field[0]
            + coords[atom, 1] * field[1]
            + coords[atom, 2] * field[2]
        )


@kernel
def band_energy(rho, h0, n):
    """``Tr(rho H0)``, the non-SCC part of the electronic energy."""

    total = 0.0
    for mu in range(n):
        for nu in range(n):
            total += rho[mu, nu].real * h0[mu, nu]
    return total


@kernel
def scc_energy(v_scc_shell, dq_shell, n_shell):
    """SCC double-counting energy ``0.5 sum_i V_i dq_i`` (scc.F90:664-693)."""

    total = 0.0
    for i in range(n_shell):
        total += v_scc_shell[i] * dq_shell[i]
    return 0.5 * total


@kernel
def external_energy(dq_atom, coords, field, n_atom):
    """Energy of the excess charges in the external field, ``sum_A dq_A R_A . E``."""

    total = 0.0
    for a in range(n_atom):
        total += dq_atom[a] * (
            coords[a, 0] * field[0] + coords[a, 1] * field[1] + coords[a, 2] * field[2]
        )
    return total


# ---------------------------------------------------------------------------- #
# propagators                                                                  #
# ---------------------------------------------------------------------------- #
@kernel
def cayley_operator(overlap, h, d, dt, left, propagator, pivot, n):
    """
    Build the non-orthogonal Cayley operator ``X`` of one step.

    ``left = S + (dt/2)(iH + D)`` is factorised in place and ``propagator``, which enters
    holding ``S - (dt/2)(iH + D)``, leaves holding ``X = left^-1 right``. Since
    ``(iH + D)`` is the generator ``S K`` of ``d rho/dt = -(K rho + rho K^dagger)``, the
    operator satisfies ``X^dagger S X = S`` exactly when ``D = 0``, which is what makes
    the step norm- and idempotency-preserving at any ``dt``.
    """

    half = 0.5 * dt
    for i in range(n):
        for j in range(n):
            generator = half * complex(d[i, j], h[i, j])
            left[i, j] = overlap[i, j] + generator
            propagator[i, j] = overlap[i, j] - generator
    lu_factor(left, n, pivot)
    lu_solve(left, pivot, propagator, n, n)


@kernel
def cayley_step(propagator, rho, work, rho_new, n):
    """One Cayley step, ``rho(t+dt) = X rho(t) X^dagger``."""

    matmul(propagator, rho, work, n)
    matmul_adj(work, propagator, rho_new, n)


@kernel
def leapfrog_step(rho_old, rho, h, s_inv, d, step, h1, t1, work, n):
    """
    One DFTB+ leapfrog step, ``propagateRho``, timeprop.F90:2790-2819.

    ``rho_old`` enters holding ``rho(t - dt)`` and leaves holding ``rho(t + dt)``; the
    caller passes ``step = 2 dt`` for the leapfrog proper and ``step = dt`` for the Euler
    bootstrap and for the periodic Euler restart. ``h1 = D + i H`` is the combined
    generator DFTB+ assembles in place at timeprop.F90:5211.
    """

    for i in range(n):
        for j in range(n):
            h1[i, j] = complex(d[i, j], h[i, j])
    matmul(s_inv, h1, t1, n)
    matmul(t1, rho, work, n)
    for i in range(n):
        for j in range(n):
            rho_old[i, j] -= step * work[i, j]
    matmul_adj(rho, t1, work, n)
    for i in range(n):
        for j in range(n):
            rho_old[i, j] -= step * work[i, j]


# ---------------------------------------------------------------------------- #
# one system's real-time state                                               #
# ---------------------------------------------------------------------------- #
class RTState:
    """
    Everything one system needs to be propagated, plus its scratch space.

    Parameters
    ----------
    system : dftb_params.DFTBSystem
        Geometry and basis layout. ``system.coords`` is updated in place by the
        Ehrenfest driver, so this object owns the current geometry.
    ground : scc.SCCResult
        Converged ground state at the initial geometry; supplies the initial density
        matrix, the gamma matrix and the shell layout.

    Attributes
    ----------
    rho : numpy.ndarray of complex, shape (n_orb, n_orb)
        The density matrix at the current time.
    h : numpy.ndarray of float, shape (n_orb, n_orb)
        The Hamiltonian built from the current charges, geometry and field.
    dipole : numpy.ndarray of float, shape (3,)
        Dipole moment in atomic units.
    """

    def __init__(self, system, ground):
        self.system = system
        self.layout = ground.layout
        n = system.n_orb
        self.n_orb = n
        self.n_atom = system.n_atom
        self.n_shell = self.layout.n_shell

        self.h0 = np.zeros((n, n))
        self.overlap = np.zeros((n, n))
        self.s_inv = np.zeros((n, n))
        self.gamma = np.zeros((self.n_shell, self.n_shell))
        self.h = np.zeros((n, n))
        self.coupling = np.zeros((n, n))  # D, non-zero only with moving nuclei

        self.rho = np.zeros((n, n), dtype=np.complex128)
        self.rho_old = np.zeros((n, n), dtype=np.complex128)

        self.q_orb = np.zeros(n)
        self.dq_atom = np.zeros(self.n_atom)
        self.dq_shell = np.zeros(self.n_shell)
        self.v_shell = np.zeros(self.n_shell)
        self.v_scc_shell = np.zeros(self.n_shell)
        self.v_orb = np.zeros(n)
        self.dipole = np.zeros(3)
        self.field = np.zeros(3)

        self._phase = np.zeros(n)
        self._pivot = np.zeros(n, dtype=np.int64)
        self._work_r = np.zeros((n, n))
        self._work_a = np.zeros((n, n), dtype=np.complex128)
        self._work_b = np.zeros((n, n), dtype=np.complex128)
        self._work_c = np.zeros((n, n), dtype=np.complex128)
        self._left = np.zeros((n, n), dtype=np.complex128)
        self._propagator = np.zeros((n, n), dtype=np.complex128)
        self._h_start = np.zeros((n, n))

        self.refresh_geometry()
        initial_density(ground.vectors, ground.filling, self.rho, n)

    # -- geometry-dependent matrices ------------------------------------------
    def refresh_geometry(self):
        """Rebuild H0, S, S^-1 and gamma at the current coordinates (``updateH0S``)."""

        h0, overlap = build_h0_overlap(self.system)
        self.h0 = h0
        self.overlap = overlap
        lu_invert(overlap, self.n_orb, self._work_r, self._pivot, self.s_inv)
        build_gamma(
            self.system.coords,
            self.layout.shell_atom,
            self.layout.shell_u,
            self.gamma,
        )

    # -- charges, dipole, Hamiltonian -----------------------------------------
    def update_charges(self):
        """Mulliken charges and dipole of the current density (``getChargeDipole``)."""

        rt_orbital_charges(self.rho, self.overlap, self.q_orb, self.n_orb)
        atom_charges(
            self.q_orb,
            self.layout.q0_orb,
            self.layout.orb_atom,
            self.dq_atom,
            self.n_orb,
            self.n_atom,
        )
        shell_charges_from_orbital(
            self.q_orb,
            self.layout.q0_orb,
            self.layout.orb_shell,
            self.dq_shell,
            self.n_orb,
            self.n_shell,
        )
        rt_dipole(self.dq_atom, self.system.coords, self.dipole, self.n_atom)

    def update_hamiltonian(self, field=None):
        """Rebuild H from the instantaneous charges and field (``updateH``)."""

        if field is None:
            self.field[:] = 0.0
        else:
            self.field[:] = field
        scc_potential(
            self.gamma,
            self.dq_shell,
            self.layout.orb_shell,
            self.v_scc_shell,
            self.v_orb,
        )
        self.v_shell[:] = self.v_scc_shell
        external_potential(
            self.system.coords,
            self.field,
            self.layout.shell_atom,
            self.v_shell,
            self.n_shell,
        )
        for mu in range(self.n_orb):
            self.v_orb[mu] = self.v_shell[self.layout.orb_shell[mu]]
        scc_hamiltonian(self.h0, self.overlap, self.v_orb, self.h)

    # -- energies --------------------------------------------------------------
    def energies(self):
        """Energy components in Hartree, matching ``calcEnergies`` for our scope."""

        e_band = band_energy(self.rho, self.h0, self.n_orb)
        e_scc = scc_energy(self.v_scc_shell, self.dq_shell, self.n_shell)
        e_ext = external_energy(
            self.dq_atom, self.system.coords, self.field, self.n_atom
        )
        e_rep = repulsive_total(self.system)
        return e_band, e_scc, e_ext, e_rep

    # -- perturbation ----------------------------------------------------------
    def kick(self, kappa, pol_dir):
        """Apply the delta kick once, before the first propagation step."""

        kick_density(
            self.rho,
            self.overlap,
            self.s_inv,
            self.system.coords,
            self.layout.orb_atom,
            kappa,
            pol_dir,
            self._phase,
            self._work_a,
            self._work_b,
            self.n_orb,
        )

    # -- propagation -----------------------------------------------------------
    def propagate_cayley(
        self, dt, midpoint=False, field=None, overlap=None, coupling=None
    ):
        """One Cayley step; ``self.rho_old`` keeps the previous density.

        With ``midpoint`` the step runs twice, a predictor with ``H(t)`` and then the step
        proper with ``0.5 (H(t) + H(t+dt))``: Crank-Nicolson is second order only with the
        Hamiltonian at the middle of the interval, and the SCC charges make ``H`` time
        dependent. ``overlap`` and ``coupling`` override the state's own; the Ehrenfest driver
        passes the midpoint values, which keeps ``Tr(S rho)`` from drifting while the nuclei
        move.
        """

        if overlap is None:
            overlap = self.overlap
        if coupling is None:
            coupling = self.coupling
        self.rho_old[:] = self.rho
        if midpoint:
            self._h_start[:] = self.h
            cayley_operator(
                overlap,
                self._h_start,
                coupling,
                dt,
                self._left,
                self._propagator,
                self._pivot,
                self.n_orb,
            )
            cayley_step(
                self._propagator, self.rho_old, self._work_a, self.rho, self.n_orb
            )
            self.update_charges()
            self.update_hamiltonian(field)
            for i in range(self.n_orb):
                for j in range(self.n_orb):
                    self.h[i, j] = 0.5 * (self._h_start[i, j] + self.h[i, j])
        cayley_operator(
            overlap,
            self.h,
            coupling,
            dt,
            self._left,
            self._propagator,
            self._pivot,
            self.n_orb,
        )
        cayley_step(self._propagator, self.rho_old, self._work_a, self.rho, self.n_orb)

    def propagate_leapfrog(self, step):
        """
        One DFTB+ leapfrog step with the two-buffer ping-pong.

        ``self.rho_old`` enters as ``rho(t - dt)`` and the two buffers are exchanged, so
        on exit ``self.rho`` is ``rho(t + dt)`` and ``self.rho_old`` is ``rho(t)``.
        """

        leapfrog_step(
            self.rho_old,
            self.rho,
            self.h,
            self.s_inv,
            self.coupling,
            step,
            self._work_a,
            self._work_b,
            self._work_c,
            self.n_orb,
        )
        self.rho, self.rho_old = self.rho_old, self.rho

    # -- diagnostics -----------------------------------------------------------
    def electron_count(self):
        """``Tr(rho S)``."""

        return trace_overlap(self.rho, self.overlap, self.n_orb)

    def idempotency(self):
        """``max |rho S rho - rho|``; the factor two is the closed-shell occupation."""

        return idempotency_error(
            self.rho, self.overlap, self._work_a, self._work_b, self.n_orb
        )


# ---------------------------------------------------------------------------- #
# the field table DFTB+ precomputes for a laser                                #
# ---------------------------------------------------------------------------- #
def laser_field(n_steps, dt, amplitude, omega, direction, phase=0.0):
    """
    The ``tdFunction`` table DFTB+ precomputes for a laser, ``getTDFunction``.

    ``E(k dt) = E0 * envelope * Im[ exp(i (omega t + phase)) * e ]`` with a constant
    envelope, i.e. ``E0 sin(omega t + phase) e`` for a linear polarisation
    (timeprop.F90:1938-1950). ``e`` is normalised by the norm of its *real* part.
    """

    direction = np.asarray(direction, dtype=float)
    direction = direction / math.sqrt(direction @ direction)
    table = np.zeros((n_steps + 1, 3))
    for step in range(n_steps + 1):
        table[step] = amplitude * math.sin(step * dt * omega + phase) * direction
    return table
