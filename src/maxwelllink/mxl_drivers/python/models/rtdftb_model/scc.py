# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Self-consistent-charge (SCC) ground state on top of the non-SCC Hamiltonian.

The SCC correction adds one number per shell to the Hamiltonian, the electrostatic
potential of every other shell's Mulliken charge fluctuation. :func:`scc_loop` iterates
charges -> potential -> Hamiltonian -> eigenvectors -> charges, with DIIS mixing, until
the charges stop moving; :func:`scf` is the host-side convenience around it.

The loop is written once against an array module ``xp`` (``numpy`` or ``cupy``): the
dense linear algebra -- a Cholesky factorisation, an inversion, a symmetric eigensolve --
runs wherever the matrices live, while the small serial pieces (the Fermi filling, the
shell charges, the DIIS mixer) run on the host. The scalar driver calls it with NumPy
matrices; the GPU batch driver calls it once per system with CuPy matrices.

The compiled kernels here are the charge, potential, Hamiltonian and energy pieces a
real-time step needs as well; they take a real or a complex density alike.

Two conventions are worth stating once. ``dq = q - q0`` counts *excess electrons*, so
an atom that gained electrons has a positive ``dq``; and the electronic temperature is
the DFTB+ floor of 1e-8 Hartree, so the filling is the exact zero-temperature limit and
the reported energy is the free energy ``E - TS``.
"""

import math
import os
from collections import namedtuple
from dataclasses import dataclass

import numpy as np

from .dftb_params import ShellLayout
from .jit import kernel
from .skfiles import repulsive_pair

# accuracy.F90:69, 110, 106, 64 -- the tolerances the branch structure keys on.
TOL_SAME_DIST = 1.0e-5
MIN_HUB_DIFF = 0.3125e-5
MIN_HUB_TOL = 1.0e-6  # DFTB+ aborts below this; every distributed U is far above it
MIN_TEMP = 1.0e-8

# shortgammafuncs.F90:141 writes 1/48 as this literal; keep it for bit-comparability.
C_1_48 = 0.02083333333333333333

# Levels further than this many kT from the Fermi energy are fully occupied or empty.
FERMI_CUT = 40.0

# The guard DFTB+ puts on the entropy logarithms, epsilon(1.0_dp) at etemp.F90:479.
EPSILON = 2.220446049250313e-16

#: Two-phase SCF (``hybrid_precision``): iterate in FP32 until the charge error drops
#: below this floor, then finish in FP64 to the requested tolerance. Single-precision
#: eigenvectors floor the Mulliken error at ~1e-6, so the switch sits safely above it.
SCF_FP32_SWITCH = 1.0e-5
#: FP32 iterations without a factor-of-two error reduction before the FP64 phase is
#: forced -- degenerate frontier orbitals (metals) can stall the FP32 phase.
SCF_FP32_STALL = 8


# ---------------------------------------------------------------------------- #
# short-range gamma                                                            #
# ---------------------------------------------------------------------------- #
@kernel
def _gamma_sub(r, tau1, tau2):
    """One ordering of the non-degenerate short-range gamma, shortgammafuncs.F90:292."""

    d = tau1 * tau1 - tau2 * tau2
    t2_4 = tau2 * tau2 * tau2 * tau2
    t2_6 = t2_4 * tau2 * tau2
    return math.exp(-tau1 * r) * (
        0.5 * t2_4 * tau1 / (d * d)
        - (t2_6 - 3.0 * t2_4 * tau1 * tau1) / (r * d * d * d)
    )


@kernel
def _gamma_sub_prime(r, tau1, tau2):
    """d/dr of :func:`_gamma_sub`, shortgammafuncs.F90:326."""

    d = tau1 * tau1 - tau2 * tau2
    t2_4 = tau2 * tau2 * tau2 * tau2
    t2_6 = t2_4 * tau2 * tau2
    return -tau1 * math.exp(-tau1 * r) * (
        0.5 * t2_4 * tau1 / (d * d)
        - (t2_6 - 3.0 * t2_4 * tau1 * tau1) / (r * d * d * d)
    ) + math.exp(-tau1 * r) * (t2_6 - 3.0 * t2_4 * tau1 * tau1) / (r * r * d * d * d)


@kernel
def exp_gamma(r, u_a, u_b):
    """
    Short-range part of the charge-charge interaction, shortgammafuncs.F90:97-148.

    Returns the DFTB+ quantity, which is the *negative* of the physical short-range
    gamma: it equals ``-U`` at ``r = 0`` and decays to zero, and the full interaction
    is ``1 / r - exp_gamma(r, U_a, U_b)``.
    """

    tau_a = 3.2 * u_a  # 16/5 * U, shortgammafuncs.F90:125
    tau_b = 3.2 * u_b
    if r < TOL_SAME_DIST:
        if abs(u_a - u_b) < MIN_HUB_DIFF:
            return -0.5 * (u_a + u_b)
        prod = tau_a * tau_b
        total = tau_a + tau_b
        return -0.5 * (prod / total + prod * prod / (total * total * total))
    if abs(u_a - u_b) < MIN_HUB_DIFF:
        tau = 0.5 * (tau_a + tau_b)
        return math.exp(-tau * r) * (
            1.0 / r
            + 0.6875 * tau
            + 0.1875 * r * tau * tau
            + C_1_48 * r * r * tau * tau * tau
        )
    return _gamma_sub(r, tau_a, tau_b) + _gamma_sub(r, tau_b, tau_a)


@kernel
def exp_gamma_prime(r, u_a, u_b):
    """d/dr of :func:`exp_gamma`, shortgammafuncs.F90:153-201; zero at ``r = 0``."""

    if r < TOL_SAME_DIST:
        return 0.0
    if abs(u_a - u_b) < MIN_HUB_DIFF:
        tau = 3.2 * 0.5 * (u_a + u_b)
        value = (
            1.0 / r
            + 0.6875 * tau
            + 0.1875 * r * tau * tau
            + C_1_48 * r * r * tau * tau * tau
        )
        slope = -1.0 / (r * r) + 0.1875 * tau * tau + 2.0 * C_1_48 * r * tau * tau * tau
        return math.exp(-tau * r) * (-tau * value + slope)
    tau_a = 3.2 * u_a
    tau_b = 3.2 * u_b
    return _gamma_sub_prime(r, tau_a, tau_b) + _gamma_sub_prime(r, tau_b, tau_a)


@kernel
def gamma_element(coords, shell_atom, shell_u, i, j):
    """
    One entry of the shell-pair charge-charge interaction matrix.

    ``1 / R - exp_gamma(R, U_i, U_j)`` with the 1/R term dropped for shells sitting on
    the same atom (coulomb.F90:707 never writes the diagonal), so a same-atom entry is
    exactly ``+U`` when the two shells share their U.
    """

    atom_i = shell_atom[i]
    atom_j = shell_atom[j]
    if atom_i == atom_j:
        return -exp_gamma(0.0, shell_u[i], shell_u[j])
    dx = coords[atom_i, 0] - coords[atom_j, 0]
    dy = coords[atom_i, 1] - coords[atom_j, 1]
    dz = coords[atom_i, 2] - coords[atom_j, 2]
    dist = math.sqrt(dx * dx + dy * dy + dz * dz)
    return 1.0 / dist - exp_gamma(dist, shell_u[i], shell_u[j])


@kernel
def build_gamma(coords, shell_atom, shell_u, gamma):
    """Fill the shell-pair charge-charge interaction matrix of one geometry."""

    n_shell = shell_atom.shape[0]
    for i in range(n_shell):
        for j in range(n_shell):
            gamma[i, j] = gamma_element(coords, shell_atom, shell_u, i, j)


# ---------------------------------------------------------------------------- #
# charges and the dipole                                                       #
# ---------------------------------------------------------------------------- #
@kernel
def orbital_charge(rho, overlap, mu, n):
    """Mulliken gross population of one orbital, ``Re[(S rho)_mu,mu]``.

    ``rho`` may be the real ground-state density or the complex real-time one; the
    imaginary part never enters a population.
    """

    total = 0.0
    for nu in range(n):
        total += rho[nu, mu].real * overlap[nu, mu]
    return total


@kernel
def orbital_charges(rho, overlap, q_orb, n):
    """Mulliken gross populations of every orbital, ``q_mu = Re[(S rho)_mu,mu]``."""

    for mu in range(n):
        q_orb[mu] = orbital_charge(rho, overlap, mu, n)


@kernel
def shell_charges(q_orb, q0_orb, orb_shell, dq_shell):
    """Per-shell electron excess ``dq_i = sum_{mu in i} (q_mu - q0_mu)``."""

    for i in range(dq_shell.shape[0]):
        dq_shell[i] = 0.0
    for mu in range(q_orb.shape[0]):
        dq_shell[orb_shell[mu]] += q_orb[mu] - q0_orb[mu]


@kernel
def atom_charges(q_orb, q0_orb, orb_atom, dq_atom):
    """Per-atom electron excess ``dq_A = sum_{mu in A} (q_mu - q0_mu)``."""

    for a in range(dq_atom.shape[0]):
        dq_atom[a] = 0.0
    for mu in range(q_orb.shape[0]):
        dq_atom[orb_atom[mu]] += q_orb[mu] - q0_orb[mu]


@kernel
def dipole_from_charges(dq_atom, coords, dipole):
    """Dipole in atomic units, ``mu = -sum_A R_A dq_A`` (timeprop.F90:2148)."""

    for k in range(3):
        dipole[k] = 0.0
    for a in range(dq_atom.shape[0]):
        for k in range(3):
            dipole[k] -= coords[a, k] * dq_atom[a]


# ---------------------------------------------------------------------------- #
# potential and Hamiltonian                                                    #
# ---------------------------------------------------------------------------- #
@kernel
def scc_potential_row(gamma, dq_shell, v_shell, i):
    """Row ``i`` of the shell potential ``V = gamma . dq``."""

    total = 0.0
    for j in range(dq_shell.shape[0]):
        total += gamma[i, j] * dq_shell[j]
    v_shell[i] = total


@kernel
def scc_potential(gamma, dq_shell, v_shell):
    """Electrostatic potential per shell, ``V = gamma . dq``."""

    for i in range(dq_shell.shape[0]):
        scc_potential_row(gamma, dq_shell, v_shell, i)


@kernel
def orbital_potential(v_scc_shell, coords, field, orb_shell, orb_atom, mu):
    """
    Potential of one orbital: the SCC shell potential plus the external field's.

    The external-field potential is ``V_A = +R_A . E(t)`` per atom, shell independent,
    exactly as ``updateH`` builds it (timeprop.F90:1675); it folds into ``H`` through
    the same ``0.5 S (V_mu + V_nu)`` as the SCC shift, so both are collected here.
    """

    atom = orb_atom[mu]
    return v_scc_shell[orb_shell[mu]] + (
        coords[atom, 0] * field[0]
        + coords[atom, 1] * field[1]
        + coords[atom, 2] * field[2]
    )


@kernel
def orbital_potentials(v_scc_shell, coords, field, orb_shell, orb_atom, v_orb):
    """Potential of every orbital, :func:`orbital_potential`; each is independent."""

    for mu in range(v_orb.shape[0]):
        v_orb[mu] = orbital_potential(
            v_scc_shell, coords, field, orb_shell, orb_atom, mu
        )


@kernel
def scc_hamiltonian_row(h0, overlap, v_orb, h, mu):
    """Row ``mu`` of :func:`scc_hamiltonian`."""

    for nu in range(v_orb.shape[0]):
        h[mu, nu] = h0[mu, nu] + 0.5 * overlap[mu, nu] * (v_orb[mu] + v_orb[nu])


@kernel
def scc_hamiltonian(h0, overlap, v_orb, h):
    """``H = H0 + 0.5 * S * (V_mu + V_nu)``, shift.F90:212 plus hamiltonian.F90:386."""

    for mu in range(v_orb.shape[0]):
        scc_hamiltonian_row(h0, overlap, v_orb, h, mu)


# ---------------------------------------------------------------------------- #
# energy terms                                                                 #
# ---------------------------------------------------------------------------- #
@kernel
def band_energy_row(rho, h0, mu, n):
    """Row ``mu`` of ``Tr(rho H0)``."""

    total = 0.0
    for nu in range(n):
        total += rho[mu, nu].real * h0[mu, nu]
    return total


@kernel
def band_energy(rho, h0, n):
    """``Tr(rho H0)``, the non-SCC part of the electronic energy."""

    total = 0.0
    for mu in range(n):
        total += band_energy_row(rho, h0, mu, n)
    return total


@kernel
def scc_energy(v_scc_shell, dq_shell):
    """SCC double-counting energy ``0.5 sum_i V_i dq_i`` (scc.F90:664-693)."""

    total = 0.0
    for i in range(dq_shell.shape[0]):
        total += v_scc_shell[i] * dq_shell[i]
    return 0.5 * total


@kernel
def external_energy(dq_atom, coords, field):
    """Energy of the excess charges in the external field, ``sum_A dq_A R_A . E``."""

    total = 0.0
    for a in range(dq_atom.shape[0]):
        total += dq_atom[a] * (
            coords[a, 0] * field[0] + coords[a, 1] * field[1] + coords[a, 2] * field[2]
        )
    return total


@kernel
def repulsive_sum(sk, coords, atom_species, n_atom, pair):
    """
    Sum the repulsive pair potential over unordered atom pairs, in Hartree.

    Every pair goes through :func:`skfiles.repulsive_pair`, the routine the *gradient*
    uses too, so the energy and its derivative cannot come from two implementations
    that drift apart. ``pair`` is its two-element ``(V, dV/dr)`` buffer.
    """

    total = 0.0
    for a in range(n_atom):
        for b in range(a + 1, n_atom):
            e_pair, gx, gy, gz = repulsive_pair(sk, coords, atom_species, a, b, pair)
            total += e_pair
    return total


def repulsive_total(system):
    """Sum of the pair repulsive over unordered atom pairs of one system, in Hartree."""

    return repulsive_sum(
        system.tables, system.coords, system.atom_species, system.n_atom, np.zeros(2)
    )


# ---------------------------------------------------------------------------- #
# occupations                                                                  #
# ---------------------------------------------------------------------------- #
@kernel
def fermi_electron_count(eigenvalues, e_fermi, temperature):
    """Electrons held by the Fermi-Dirac occupations at ``e_fermi`` (etemp.F90:220)."""

    total = 0.0
    for i in range(eigenvalues.shape[0]):
        x = (eigenvalues[i] - e_fermi) / temperature
        if x < FERMI_CUT:
            total += 2.0 / (1.0 + math.exp(x))
    return total


@kernel
def fermi_smeared_filling(eigenvalues, n_electron, temperature, filling):
    """Fermi-Dirac occupations at finite electronic temperature, ``etemp.F90:59``.

    The Fermi energy is located as DFTB+ does: the middle of the gap is tried first
    (an exact root whenever the temperature is small against the gap), then bisection
    between brackets grown around the spectrum, polished by Newton-Raphson steps on
    the analytic derivative. Occupations are ``2 / (1 + exp((eps - E_F) / kT))``.
    """

    n_level = eigenvalues.shape[0]
    # bracket the Fermi energy, growing the interval if the temperature is huge
    lower = eigenvalues[0] - 0.01
    upper = eigenvalues[n_level - 1] + 0.01
    while fermi_electron_count(eigenvalues, lower, temperature) > n_electron:
        lower = 2.0 * (lower - upper) + lower
    while fermi_electron_count(eigenvalues, upper, temperature) < n_electron:
        upper = 2.0 * (upper - lower) + lower
    e_fermi = 0.5 * (lower + upper)
    count = fermi_electron_count(eigenvalues, e_fermi, temperature)
    while (
        abs(n_electron - count) > 1.0e-12
        and (upper - lower) > max(abs(e_fermi), 1.0) * EPSILON
    ):
        if count < n_electron:
            lower = e_fermi
        else:
            upper = e_fermi
        e_fermi = 0.5 * (lower + upper)
        count = fermi_electron_count(eigenvalues, e_fermi, temperature)
    # Newton polish on dN/dE_F = sum 2 f (1 - f) / kT (etemp.F90:344)
    for _ in range(3):
        residual = fermi_electron_count(eigenvalues, e_fermi, temperature) - n_electron
        slope = 0.0
        for i in range(n_level):
            x = (eigenvalues[i] - e_fermi) / temperature
            if abs(x) < FERMI_CUT:
                occupied = 1.0 / (1.0 + math.exp(x))
                slope += 2.0 * occupied * (1.0 - occupied) / temperature
        if slope < EPSILON:
            break
        e_fermi -= residual / slope
    for i in range(n_level):
        x = (eigenvalues[i] - e_fermi) / temperature
        if x < FERMI_CUT:
            filling[i] = 2.0 / (1.0 + math.exp(x))
        else:
            filling[i] = 0.0
    return e_fermi


@kernel
def fermi_filling(eigenvalues, n_electron, temperature, filling):
    """Level occupations of a spin-restricted system, up to 2 electrons per level.

    DFTB+ floors its electronic temperature at 1e-8 Hartree, so a "0 K" run is a step
    function except for levels sitting at the Fermi energy, which share what is left. That
    limit is evaluated directly rather than through the Fermi function, whose exponent
    divided by 1e-8 would turn a one-ulp error in the Fermi energy into a 1e-9 error in
    the occupation and stall the SCF. Returns the Fermi energy: the partially filled level
    itself, or the middle of the gap, as DFTB+ chooses (``etemp.F90:564``). Above the
    floor (``temperature > 10 * MIN_TEMP``) the occupations are the finite-temperature
    Fermi-Dirac ones of :func:`fermi_smeared_filling` instead.
    """

    if temperature > 10.0 * MIN_TEMP:
        return fermi_smeared_filling(eigenvalues, n_electron, temperature, filling)
    n_level = eigenvalues.shape[0]
    tolerance = FERMI_CUT * temperature
    remaining = n_electron
    e_fermi = eigenvalues[n_level - 1]
    i = 0
    while i < n_level:
        j = i  # last level of the degenerate group starting at i
        while j + 1 < n_level and eigenvalues[j + 1] - eigenvalues[i] <= tolerance:
            j += 1
        capacity = 2.0 * (j - i + 1)
        if remaining >= capacity:
            share = capacity
        elif remaining > 0.0:
            share = remaining
        else:
            share = 0.0
        for k in range(i, j + 1):
            filling[k] = share / (j - i + 1)
        remaining -= share
        if share > 0.0:
            if share < capacity:
                e_fermi = eigenvalues[i]  # partly filled: the Fermi energy sits on it
            elif j + 1 < n_level:
                e_fermi = 0.5 * (eigenvalues[j] + eigenvalues[j + 1])  # middle of gap
        i = j + 1
    return e_fermi


@kernel
def fermi_entropy(filling, temperature):
    """Electronic entropy term ``T S`` in Hartree, etemp.F90:464-492.

    The occupation entering the entropy is ``filling / 2``, as DFTB+ fills one electron per
    level and doubles afterwards. A gapped system has no entropy; a level at the Fermi
    energy contributes ``2 kT ln 2`` = 1.4e-8 Hartree, which is above the tolerance this
    driver is validated to.
    """

    total = 0.0
    for i in range(filling.shape[0]):
        occupation = 0.5 * filling[i]
        if EPSILON < occupation < 1.0 - EPSILON:
            total -= occupation * math.log(occupation)
            total -= (1.0 - occupation) * math.log(1.0 - occupation)
    return 2.0 * temperature * total


# ---------------------------------------------------------------------------- #
# the SCF loop                                                                 #
# ---------------------------------------------------------------------------- #
def solve_generalised(h, overlap, xp=np, dtype=None):
    """
    Solve ``H c = eps S c`` through the Cholesky factor of the overlap.

    Returns the eigenvalues ascending and the eigenvectors as columns, normalised so
    that ``C^T S C`` is the identity. Both matrices are cast to ``dtype`` first when one
    is given, which is how the FP32 phase of the hybrid SCF runs.
    """

    if dtype is not None and h.dtype != dtype:
        h = h.astype(dtype)
        overlap = overlap.astype(dtype)
    chol = xp.linalg.cholesky(overlap)
    inv_chol = xp.linalg.inv(chol)
    transformed = inv_chol @ h @ inv_chol.T
    transformed = 0.5 * (transformed + transformed.T)
    eigenvalues, vectors = xp.linalg.eigh(transformed)
    return eigenvalues, inv_chol.T @ vectors


class DIISMixer:
    """
    Pulay (DIIS) mixing of the charge vector, the accelerator of the SCF loop.

    Each call to :meth:`mix` stores the charge vector fed into the last iteration and
    the change it produced, then returns the combination of the stored vectors whose
    residuals cancel best -- the least-squares problem ``min ||sum_i c_i r_i||`` under
    ``sum_i c_i = 1``. With one stored vector it is plain linear mixing, so no special
    first iteration is needed. The history rolls only once the buffer is full: rolling
    earlier would shift in slots never written, and a zero residual row makes the
    normal equations singular.

    Parameters
    ----------
    n_shell : int
        Length of the charge vector.
    history : int
        Number of previous iterations kept.
    mixing : float
        Linear mixing parameter applied to every stored residual.
    """

    def __init__(self, n_shell, history=8, mixing=0.2):
        self.history = int(history)
        self.mixing = float(mixing)
        self._in = np.zeros((self.history, n_shell))
        self._residual = np.zeros((self.history, n_shell))
        self.n_filled = 0

    def reset(self):
        """Forget the stored history (the hybrid SCF does this at its precision switch)."""

        self.n_filled = 0

    def mix(self, dq_in, dq_out):
        """
        Store ``(dq_in, dq_out - dq_in)`` and overwrite ``dq_in`` with the mixed charges.

        Parameters
        ----------
        dq_in : numpy.ndarray of float, shape (n_shell,)
            Charges fed into the last iteration; receives the mixed charges in place.
        dq_out : numpy.ndarray of float, shape (n_shell,)
            Charges the iteration produced.
        """

        if self.n_filled < self.history:
            slot = self.n_filled
            self.n_filled += 1
        else:
            self._in[:-1] = self._in[1:]
            self._residual[:-1] = self._residual[1:]
            slot = self.history - 1
        self._in[slot] = dq_in
        self._residual[slot] = dq_out - dq_in
        n = self.n_filled
        if n == 1:
            dq_in[:] = self._in[0] + self.mixing * self._residual[0]
            return

        # Bordered normal equations: the last row and column carry the sum-to-one
        # constraint, the last unknown is its Lagrange multiplier.
        matrix = np.zeros((n + 1, n + 1))
        matrix[:n, :n] = self._residual[:n] @ self._residual[:n].T
        matrix[:n, n] = 1.0
        matrix[n, :n] = 1.0
        rhs = np.zeros(n + 1)
        rhs[n] = 1.0
        try:
            weights = np.linalg.solve(matrix, rhs)[:n]
        except np.linalg.LinAlgError:  # nearly linearly dependent history
            dq_in[:] = self._in[n - 1] + self.mixing * self._residual[n - 1]
            return
        dq_in[:] = weights @ (self._in[:n] + self.mixing * self._residual[:n])


@dataclass
class SCCResult:
    """
    Everything the SCF loop produced, in Hartree atomic units.

    The dense matrices (``rho``, ``edm``, ``h``, ``vectors``) live on the array module
    the loop ran on; every vector and scalar is a host value.

    Attributes
    ----------
    rho, edm : array, shape (n_orb, n_orb)
        Density matrix and energy-weighted density matrix.
    h : array, shape (n_orb, n_orb)
        The converged SCC Hamiltonian.
    vectors : array, shape (n_orb, n_orb)
        Eigenvectors as columns, S-orthonormal.
    eigenvalues, filling : numpy.ndarray of float, shape (n_orb,)
        Level energies and occupations (0 to 2).
    e_fermi : float
        Fermi energy.
    q_orb, dq_shell, v_shell, v_orb : numpy.ndarray of float
        Gross orbital populations, shell charge excess, shell and orbital potentials.
    gamma : array, shape (n_shell, n_shell)
        The shell-pair interaction matrix the loop used.
    layout : ShellLayout
        The shell layout the charges refer to.
    energy_h0, energy_scc, energy_repulsive, entropy_ts : float
        Band, SCC double-counting and repulsive energies, and ``T S``.
    energy_total, energy_mermin : float
        Their sum, and the free energy ``E - TS`` DFTB+ reports; the two differ only
        when a level sits at the Fermi energy.
    n_iteration, n_iteration_fp32 : int
        Iterations taken, and how many of them ran in single precision.
    converged : bool
        Whether the charge change fell below the tolerance.
    """

    rho: object
    edm: object
    h: object
    vectors: object
    eigenvalues: np.ndarray
    filling: np.ndarray
    e_fermi: float
    q_orb: np.ndarray
    dq_shell: np.ndarray
    v_shell: np.ndarray
    v_orb: np.ndarray
    gamma: object
    layout: ShellLayout
    energy_h0: float
    energy_scc: float
    energy_repulsive: float
    entropy_ts: float
    energy_total: float
    energy_mermin: float
    n_iteration: int
    n_iteration_fp32: int
    converged: bool


#: What one pass of the SCC loop produces from a set of shell charges.
_SCCPass = namedtuple(
    "_SCCPass", "v_shell v_orb h eigenvalues vectors e_fermi rho q_orb"
)


def _to_host(xp, array):
    """A host NumPy copy of ``array``, whichever array module holds it."""

    asnumpy = getattr(xp, "asnumpy", None)
    return np.asarray(asnumpy(array) if asnumpy is not None else array)


def scc_loop(
    layout,
    n_electron,
    h0,
    overlap,
    gamma,
    xp=np,
    tolerance=1.0e-13,
    max_iterations=500,
    mixing=0.2,
    history=8,
    electronic_temperature_au=MIN_TEMP,
    dq_shell_start=None,
    hybrid_precision=False,
    energy_repulsive=0.0,
    verbose=False,
):
    """
    Converge the SCC charges of one system whose matrices live on ``xp``.

    Parameters
    ----------
    layout : ShellLayout
        The shell layout the charges refer to.
    n_electron : float
        Number of electrons to fill.
    h0, overlap, gamma : array
        Non-SCC Hamiltonian, overlap and shell-pair interaction matrix, as ``xp``
        arrays of ``float64``.
    xp : module, default: numpy
        Array module the dense matrices live on (``numpy`` or ``cupy``).
    tolerance : float
        Convergence threshold on the largest shell-charge change between iterations.
    max_iterations : int
        Give up after this many iterations.
    mixing, history : float, int
        DIIS mixing parameter and history length.
    electronic_temperature_au : float
        Electronic temperature of the Fermi-Dirac filling in Hartree. At the DFTB+
        floor (1e-8, the default) the filling is the exact zero-temperature limit.
    dq_shell_start : numpy.ndarray of float, shape (n_shell,), optional
        Shell charges to start the loop from instead of the neutral atoms, e.g. the
        converged charges of a nearby geometry, which cuts the iteration count of a
        Born-Oppenheimer trajectory several-fold.
    hybrid_precision : bool, default: False
        Run the bulk of the iterations in FP32 and only the final stretch in FP64.
        The two phases converge the same fixed point to the same tolerance; the
        single-precision phase stops at :data:`SCF_FP32_SWITCH`, when it stalls, or
        when the FP32 factorisation fails, and the DIIS history starts clean at the
        switch (single-precision noise correlated across the FP32-era residuals
        misleads the least squares).
    energy_repulsive : float, default: 0.0
        Repulsive energy of the geometry, added to the reported totals.
    verbose : bool
        Print the charge change of every iteration.

    Returns
    -------
    SCCResult
        The converged state, rebuilt once in FP64 from the converged charges so that
        every reported quantity belongs to one and the same set of charges.
    """

    n_orb, n_shell = layout.orb_shell.shape[0], layout.n_shell
    orb_shell = xp.asarray(layout.orb_shell)
    dq_shell = np.zeros(n_shell)
    if dq_shell_start is not None:
        dq_shell[:] = dq_shell_start
    dq_new = np.zeros(n_shell)
    filling = np.zeros(n_orb)
    mixer = DIISMixer(n_shell, history, mixing)

    def iterate(dtype):
        """Charges -> potential -> H -> eigenvectors -> density -> populations."""

        v_shell = gamma @ xp.asarray(dq_shell)
        v_orb = v_shell[orb_shell]
        h = h0 + 0.5 * overlap * (v_orb[:, None] + v_orb[None, :])
        eigenvalues, vectors = solve_generalised(h, overlap, xp, dtype)
        eig_host = _to_host(xp, eigenvalues).astype(np.float64)
        e_fermi = fermi_filling(
            eig_host, n_electron, electronic_temperature_au, filling
        )
        rho = (vectors * xp.asarray(filling).astype(vectors.dtype)) @ vectors.T
        q_orb = _to_host(xp, (rho * overlap.astype(rho.dtype, copy=False)).sum(axis=1))
        return _SCCPass(v_shell, v_orb, h, eigenvalues, vectors, e_fermi, rho, q_orb)

    # the two-phase schedule: with hybrid_precision the bulk of the iterations runs in
    # FP32 and a short FP64 tail finishes to the requested tolerance, converging the
    # same fixed point; without it, FP64 throughout (the default)
    fp32_active = bool(hybrid_precision)
    n_fp32 = n_iteration = 0
    best_error, stalled = np.inf, 0
    converged = False
    for n_iteration in range(1, max_iterations + 1):
        try:
            passed = iterate(xp.float32 if fp32_active else xp.float64)
        except np.linalg.LinAlgError:
            if not fp32_active:
                raise
            # ill-conditioned in single precision: hand over to FP64 for good
            fp32_active = False
            mixer.reset()
            passed = iterate(xp.float64)
        shell_charges(
            passed.q_orb.astype(np.float64), layout.q0_orb, layout.orb_shell, dq_new
        )
        error = float(np.max(np.abs(dq_new - dq_shell)))
        if verbose:
            print(f"    iteration {n_iteration:3d}  charge change {error:.3e}")
        if fp32_active:
            n_fp32 += 1
            if error < 0.5 * best_error:
                best_error, stalled = error, 0
            else:
                stalled += 1
            if error < SCF_FP32_SWITCH or stalled >= SCF_FP32_STALL:
                fp32_active = False
                mixer.reset()
        elif error < tolerance:
            dq_shell[:] = dq_new
            converged = True
            break
        mixer.mix(dq_shell, dq_new)

    # Rebuild the potential, the Hamiltonian and the density from the converged charges
    # in FP64 so that every reported quantity belongs to one and the same set of charges.
    final = iterate(xp.float64)
    rho, vectors = final.rho, final.vectors
    eig_host = _to_host(xp, final.eigenvalues)
    edm = (vectors * xp.asarray(filling * eig_host)) @ vectors.T
    # one device-to-host copy for the potentials and the band energy together
    host = _to_host(
        xp, xp.concatenate((final.v_shell, final.v_orb, xp.sum(rho * h0).reshape(1)))
    )
    v_shell_host, v_orb_host = host[:n_shell], host[n_shell : n_shell + n_orb]
    energy_h0 = float(host[-1])
    energy_scc = 0.5 * float(v_shell_host @ dq_shell)
    entropy_ts = fermi_entropy(filling, electronic_temperature_au)
    energy_total = energy_h0 + energy_scc + energy_repulsive
    return SCCResult(
        rho=rho,
        edm=edm,
        h=final.h,
        vectors=vectors,
        eigenvalues=eig_host,
        filling=filling,
        e_fermi=float(final.e_fermi),
        q_orb=final.q_orb.astype(np.float64),
        dq_shell=dq_shell,
        v_shell=v_shell_host,
        v_orb=v_orb_host,
        gamma=gamma,
        layout=layout,
        energy_h0=energy_h0,
        energy_scc=energy_scc,
        energy_repulsive=float(energy_repulsive),
        entropy_ts=float(entropy_ts),
        energy_total=energy_total,
        energy_mermin=energy_total - entropy_ts,
        n_iteration=n_iteration,
        n_iteration_fp32=n_fp32,
        converged=converged,
    )


def scf(
    system,
    h0,
    overlap,
    tolerance=1.0e-13,
    max_iterations=500,
    mixing=0.2,
    history=8,
    electronic_temperature_au=MIN_TEMP,
    charge=0.0,
    shell_resolved=False,
    verbose=False,
    dq_shell_start=None,
):
    """
    Converge the SCC charges of one system on the host and report energies and matrices.

    Parameters
    ----------
    system : dftb_params.DFTBSystem
        Geometry and basis layout.
    h0, overlap : numpy.ndarray of float, shape (n_orb, n_orb)
        The non-SCC Hamiltonian and overlap of :func:`h0_overlap.build_h0_overlap`.
    tolerance, max_iterations, mixing, history, electronic_temperature_au, verbose,
    dq_shell_start
        As in :func:`scc_loop`.
    charge : float
        Net charge of the system in units of ``+e``; the electron count is the
        neutral one minus this.
    shell_resolved : bool
        Whether every shell keeps its own Hubbard U; DFTB+ defaults to ``False``.

    Returns
    -------
    SCCResult
        The converged ground state, including the repulsive energy of the geometry.
    """

    layout = (
        ShellLayout(system, shell_resolved=True) if shell_resolved else system.layout
    )
    gamma = np.zeros((layout.n_shell, layout.n_shell))
    build_gamma(system.coords, layout.shell_atom, layout.shell_u, gamma)
    return scc_loop(
        layout,
        system.n_electrons() - charge,
        h0,
        overlap,
        gamma,
        tolerance=tolerance,
        max_iterations=max_iterations,
        mixing=mixing,
        history=history,
        electronic_temperature_au=electronic_temperature_au,
        dq_shell_start=dq_shell_start,
        energy_repulsive=repulsive_total(system),
        verbose=verbose,
    )


def dipole_moment(system, q_orb, layout):
    """Dipole moment in atomic units, ``-sum_A dq_A R_A`` (Bohr times e)."""

    dq_atom = np.zeros(system.n_atom)
    atom_charges(q_orb, layout.q0_orb, layout.orb_atom, dq_atom)
    dipole = np.zeros(3)
    dipole_from_charges(dq_atom, system.coords, dipole)
    return dipole


# ---------------------------------------------------------------------------- #
# host utilities                                                               #
# ---------------------------------------------------------------------------- #
#: Thread-count setters of the BLAS builds numpy ships or links against.
_BLAS_SET_THREADS = (
    "openblas_set_num_threads",
    "openblas_set_num_threads64_",
    "scipy_openblas_set_num_threads",
    "scipy_openblas_set_num_threads64_",
    "MKL_Set_Num_Threads",
    "bli_thread_set_num_threads",
)


def limit_blas_threads(n_threads=1):
    """
    Cap the thread count of every BLAS library loaded in this process.

    The DFTB matrices are small (a few hundred orbitals at most), and a threaded BLAS
    only spins on them: with numpy's default OpenBLAS on a 48-core host one SCC
    iteration at 120 orbitals took 200 ms instead of 3 ms. This is what
    ``OMP_NUM_THREADS=1`` does before start-up, applied after the fact.

    Parameters
    ----------
    n_threads : int, default: 1
        Thread count to set.

    Returns
    -------
    list of str
        The libraries that were capped; empty on platforms without ``/proc``.
    """

    import ctypes

    capped = []
    try:
        with open("/proc/self/maps") as handle:
            paths = {line.split()[-1] for line in handle if "/" in line}
    except OSError:
        return capped
    for path in sorted(paths):
        name = os.path.basename(path).lower()
        if not any(tag in name for tag in ("openblas", "mkl_rt", "libblis")):
            continue
        try:
            library = ctypes.CDLL(path)
        except OSError:
            continue
        for symbol in _BLAS_SET_THREADS:
            setter = getattr(library, symbol, None)
            if setter is not None:
                setter(int(n_threads))
                capped.append(os.path.basename(path))
                break
    return capped
