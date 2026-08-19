# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# --------------------------------------------------------------------------------------#

"""
Self-consistent-charge (SCC) ground state on top of the non-SCC Hamiltonian.

The SCC correction adds one number per shell to the Hamiltonian, the electrostatic
potential of every other shell's Mulliken charge fluctuation. :func:`scf` iterates
charges -> potential -> Hamiltonian -> eigenvectors -> charges, with DIIS mixing, until
the charges stop moving.

Two conventions are worth stating once. ``dq = q - q0`` counts *excess electrons*, so
an atom that gained electrons has a positive ``dq``; and the electronic temperature is
the DFTB+ floor of 1e-8 Hartree, so the filling is the exact zero-temperature limit and
the reported energy is the free energy ``E - TS``.
"""

import math
from collections import namedtuple

import os

import numpy as np

try:  # inside the package
    from .kernels_dftb import kernel
    from .skfiles import repulsive_pair
except (ImportError, ValueError):  # allow running as a stand-alone script
    from kernels_dftb import kernel
    from skfiles import repulsive_pair

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


# ---------------------------------------------------------------------------- #
# shell bookkeeping                                                            #
# ---------------------------------------------------------------------------- #
#: The plain arrays of a :class:`ShellLayout`, i.e. the shell-resolved half of a
#: kernel's arguments, the way :data:`dftb_params.SKTables` is the parameter-set half.
#: ``n_shell`` is not a field: a kernel reads it off ``shell_atom.shape[0]``.
ShellArrays = namedtuple("ShellArrays", "shell_atom shell_u orb_shell orb_atom q0_orb")


class ShellLayout:
    """
    Flat shell list of one system: which atom each shell belongs to, its U, its
    reference occupation, and the map from dense orbital index to shell index.

    Parameters
    ----------
    system : dftb_params.DFTBSystem
        Geometry and basis layout.
    shell_resolved : bool
        Whether every shell keeps its own Hubbard U. DFTB+ defaults to ``False``
        (``ShellResolvedSCC = No``, parser.F90:1375), in which case every shell of a
        species inherits the U of its s shell (parser.F90:3699). The difference is
        invisible in 3ob, whose files list one U three times, but Ag and H in mio-1-1
        really do carry three different values.

    Attributes
    ----------
    n_shell : int
        Total number of shells in the system.
    shell_atom : numpy.ndarray of int, shape (n_shell,)
        Atom index owning each shell.
    shell_u : numpy.ndarray of float, shape (n_shell,)
        Hubbard U of each shell in Hartree.
    orb_shell : numpy.ndarray of int, shape (n_orb,)
        Shell index owning each dense orbital.
    orb_atom : numpy.ndarray of int, shape (n_orb,)
        Atom index owning each dense orbital.
    q0_orb : numpy.ndarray of float, shape (n_orb,)
        Free-atom reference population of each orbital, the shell occupation spread
        evenly over its ``2 * l + 1`` members (sccinit.F90:132).
    """

    def __init__(self, system, shell_resolved=False):
        sk_set = system.sk_set
        shell_atom = []
        shell_u = []
        orb_shell = []
        orb_atom = []
        q0_orb = []
        for atom in range(system.n_atom):
            sp = system.atom_species[atom]
            for i_shell in range(sk_set.n_shell[sp]):
                ang = sk_set.ang_shell[sp, i_shell]
                shell_atom.append(atom)
                if shell_resolved:
                    shell_u.append(sk_set.hubbard_u_shell[sp, i_shell])
                else:
                    shell_u.append(sk_set.hubbard_u[sp])
                for _ in range(2 * ang + 1):
                    orb_shell.append(len(shell_atom) - 1)
                    orb_atom.append(atom)
                    q0_orb.append(sk_set.occupation[sp, i_shell] / (2 * ang + 1))
        self.n_shell = len(shell_atom)
        self.shell_atom = np.array(shell_atom, dtype=np.int64)
        self.shell_u = np.array(shell_u)
        self.orb_shell = np.array(orb_shell, dtype=np.int64)
        self.orb_atom = np.array(orb_atom, dtype=np.int64)
        self.q0_orb = np.array(q0_orb)

    def arrays(self):
        """Return this layout as a :data:`ShellArrays` pack of plain arrays."""

        return ShellArrays(
            self.shell_atom,
            self.shell_u,
            self.orb_shell,
            self.orb_atom,
            self.q0_orb,
        )


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
# charges, potential, Hamiltonian                                              #
# ---------------------------------------------------------------------------- #
@kernel
def mulliken_charges(rho, overlap, q):
    """Gross orbital populations, ``q[mu] = sum_nu rho[mu, nu] * S[mu, nu]``."""

    n_orb = q.shape[0]
    for mu in range(n_orb):
        total = 0.0
        for nu in range(n_orb):
            total += rho[mu, nu] * overlap[mu, nu]
        q[mu] = total


@kernel
def shell_charges(q, q0_orb, orb_shell, dq_shell):
    """Sum the per-orbital excess electron count ``q - q0`` onto shells."""

    for i in range(dq_shell.shape[0]):
        dq_shell[i] = 0.0
    for mu in range(q.shape[0]):
        dq_shell[orb_shell[mu]] += q[mu] - q0_orb[mu]


@kernel
def scc_potential(gamma, dq_shell, orb_shell, v_shell, v_orb):
    """Electrostatic potential per shell and per orbital, ``V = gamma . dq``."""

    n_shell = dq_shell.shape[0]
    for i in range(n_shell):
        total = 0.0
        for j in range(n_shell):
            total += gamma[i, j] * dq_shell[j]
        v_shell[i] = total
    for mu in range(orb_shell.shape[0]):
        v_orb[mu] = v_shell[orb_shell[mu]]


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
# eigenproblem and occupations                                                 #
# ---------------------------------------------------------------------------- #
# :func:`solve_generalised`, :func:`diis_mix` and the :func:`scf` loop around them are
# deliberately *not* kernels. They run once per geometry, never on the real-time step
# path, and all their work is ``np.linalg`` -- a Cholesky factorisation, an inversion,
# a symmetric eigensolve, a small linear solve -- which goes into multi-threaded LAPACK
# and beats anything a hand-written scalar loop could do.
def solve_generalised(h, overlap):
    """
    Solve ``H c = eps S c`` through the Cholesky factor of the overlap.

    Returns the eigenvalues ascending and the eigenvectors as columns, normalised so
    that ``C^T S C`` is the identity.
    """

    chol = np.linalg.cholesky(overlap)
    inv_chol = np.linalg.inv(chol)
    transformed = inv_chol @ h @ inv_chol.T
    transformed = 0.5 * (transformed + transformed.T)
    eigenvalues, vectors = np.linalg.eigh(transformed)
    return eigenvalues, inv_chol.T @ vectors


@kernel
def fermi_filling(eigenvalues, n_electron, temperature, filling):
    """Level occupations of a spin-restricted system, up to 2 electrons per level.

    DFTB+ floors its electronic temperature at 1e-8 Hartree, so a "0 K" run is a step
    function except for levels sitting at the Fermi energy, which share what is left. That
    limit is evaluated directly rather than through the Fermi function, whose exponent
    divided by 1e-8 would turn a one-ulp error in the Fermi energy into a 1e-9 error in
    the occupation and stall the SCF. Returns the Fermi energy: the partially filled level
    itself, or the middle of the gap, as DFTB+ chooses (``etemp.F90:564``).
    """

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


@kernel
def density_matrices(vectors, eigenvalues, filling):
    """Density matrix ``sum_i f_i c_i c_i^T`` and its energy-weighted counterpart."""

    rho = (vectors * filling) @ vectors.T
    edm = (vectors * (filling * eigenvalues)) @ vectors.T
    return rho, edm


# ---------------------------------------------------------------------------- #
# repulsive energy                                                             #
# ---------------------------------------------------------------------------- #
#: Scratch of :func:`repulsive_sum`; ``pair`` receives the ``(V, dV/dr)`` of one atom
#: pair, of which only the value is used here.
RepulsiveScratch = namedtuple("RepulsiveScratch", "pair")


@kernel
def repulsive_sum(sk, coords, atom_species, n_atom, scratch):
    """
    Sum the repulsive pair potential over unordered atom pairs, in Hartree.

    Every pair goes through :func:`skfiles.repulsive_pair`, the routine the *gradient*
    uses too, so the energy and its derivative cannot come from two implementations
    that drift apart.
    """

    total = 0.0
    for a in range(n_atom):
        for b in range(a + 1, n_atom):
            e_pair, gx, gy, gz = repulsive_pair(
                sk, coords, atom_species, a, b, scratch.pair
            )
            total += e_pair
    return total


def repulsive_total(system):
    """Sum of the pair repulsive over unordered atom pairs, in Hartree."""

    # RTState.energies() calls this every real-time step, so the parameter pack and the
    # two-element scratch are built once and kept on the system rather than per call.
    cache = getattr(system, "_scc_repulsive_cache", None)
    if cache is None:
        cache = (system.sk_set.tables(), RepulsiveScratch(np.zeros(2)))
        system._scc_repulsive_cache = cache
    tables, scratch = cache
    return repulsive_sum(
        tables, system.coords, system.atom_species, system.n_atom, scratch
    )


# ---------------------------------------------------------------------------- #
# charge mixing                                                                #
# ---------------------------------------------------------------------------- #
def diis_mix(history_in, history_residual, n_history, mixing, mixed):
    """
    Pulay (DIIS) mixing of the charge vector, the accelerator of the SCF loop.

    ``history_in[i]`` is the charge vector fed into iteration ``i`` and
    ``history_residual[i]`` the change the iteration produced. The mix is the
    combination of the stored vectors whose residuals cancel best, which is the
    least-squares problem ``min ||sum_i c_i r_i||`` under ``sum_i c_i = 1``. With one
    stored vector it is plain linear mixing, so no special first iteration is needed.
    """

    if n_history == 1:
        for j in range(mixed.shape[0]):
            mixed[j] = history_in[0, j] + mixing * history_residual[0, j]
        return

    # Bordered normal equations: the last row and column carry the sum-to-one
    # constraint, the last unknown is its Lagrange multiplier.
    matrix = np.zeros((n_history + 1, n_history + 1))
    rhs = np.zeros(n_history + 1)
    for i in range(n_history):
        for j in range(n_history):
            matrix[i, j] = history_residual[i] @ history_residual[j]
        matrix[i, n_history] = 1.0
        matrix[n_history, i] = 1.0
    rhs[n_history] = 1.0
    try:
        weights = np.linalg.solve(matrix, rhs)
    except np.linalg.LinAlgError:  # nearly linearly dependent history
        for j in range(mixed.shape[0]):
            mixed[j] = (
                history_in[n_history - 1, j]
                + mixing * history_residual[n_history - 1, j]
            )
        return
    for j in range(mixed.shape[0]):
        total = 0.0
        for i in range(n_history):
            total += weights[i] * (history_in[i, j] + mixing * history_residual[i, j])
        mixed[j] = total


# ---------------------------------------------------------------------------- #
# the SCF loop                                                                 #
# ---------------------------------------------------------------------------- #
class SCCResult:
    """Everything the SCF loop produced, in Hartree atomic units."""

    def __init__(self, **fields):
        self.__dict__.update(fields)


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


def scf(
    system,
    h0,
    overlap,
    tolerance=1.0e-13,
    max_iterations=500,
    mixing=0.2,
    history=8,
    temperature=MIN_TEMP,
    charge=0.0,
    shell_resolved=False,
    verbose=False,
    dq_shell_start=None,
):
    """
    Converge the SCC charges of one system and report energies and matrices.

    Parameters
    ----------
    system : dftb_params.DFTBSystem
        Geometry and basis layout.
    h0, overlap : numpy.ndarray of float, shape (n_orb, n_orb)
        The Gate A non-SCC Hamiltonian and overlap.
    tolerance : float
        Convergence threshold on the largest shell-charge change between iterations.
    max_iterations : int
        Give up after this many iterations.
    mixing : float
        Mixing parameter on the shell charges.
    history : int
        Number of previous iterations the DIIS mixer keeps.
    temperature : float
        Electronic temperature in Hartree. Only the DFTB+ floor (1e-8) is
        supported: the filling is the exact zero-temperature limit, so a warmer
        electronic distribution is rejected rather than silently approximated.
    charge : float
        Net charge of the system in units of ``+e``; the electron count is the
        neutral one minus this.
    shell_resolved : bool
        Whether every shell keeps its own Hubbard U; DFTB+ defaults to ``False``.
    dq_shell_start : numpy.ndarray of float, shape (n_shell,), optional
        Shell charges to start the loop from instead of the neutral atoms, e.g. the
        converged charges of a nearby geometry, which cuts the iteration count of a
        Born-Oppenheimer trajectory several-fold.

    Returns
    -------
    SCCResult
        Fields ``rho``, ``edm``, ``h``, ``eigenvalues``, ``filling``, ``e_fermi``,
        ``q_orb``, ``dq_shell``, ``v_shell``, ``v_orb``, ``gamma``, ``layout``,
        ``energy_h0``, ``energy_scc``, ``energy_repulsive``, ``entropy_ts``,
        ``energy_total``, ``energy_mermin``, ``n_iteration`` and ``converged``.
        ``energy_mermin`` is the free energy DFTB+ reports; it differs from
        ``energy_total`` only when a level sits at the Fermi energy.
    """

    layout = ShellLayout(system, shell_resolved=shell_resolved)
    n_orb = system.n_orb
    n_shell = layout.n_shell
    n_electron = system.n_electrons() - charge

    gamma = np.zeros((n_shell, n_shell))
    build_gamma(system.coords, layout.shell_atom, layout.shell_u, gamma)

    dq_shell = np.zeros(n_shell)
    if dq_shell_start is not None:
        dq_shell[:] = dq_shell_start
    dq_new = np.zeros(n_shell)
    history_in = np.zeros((history, n_shell))
    history_residual = np.zeros((history, n_shell))
    v_shell = np.zeros(n_shell)
    v_orb = np.zeros(n_orb)
    q_orb = np.zeros(n_orb)
    h = np.zeros((n_orb, n_orb))
    filling = np.zeros(n_orb)

    converged = False
    n_iteration = 0
    n_filled = 0  # DIIS history slots written so far
    rho = np.zeros((n_orb, n_orb))
    edm = np.zeros((n_orb, n_orb))
    eigenvalues = np.zeros(n_orb)
    e_fermi = 0.0

    if temperature > 10.0 * MIN_TEMP:
        raise ValueError(
            "Only the zero-temperature filling is implemented; "
            f"temperature={temperature:g} Ha is above the DFTB+ floor {MIN_TEMP:g}."
        )

    for iteration in range(max_iterations):
        n_iteration = iteration + 1
        scc_potential(gamma, dq_shell, layout.orb_shell, v_shell, v_orb)
        scc_hamiltonian(h0, overlap, v_orb, h)
        eigenvalues, vectors = solve_generalised(h, overlap)
        e_fermi = fermi_filling(eigenvalues, n_electron, temperature, filling)
        rho, edm = density_matrices(vectors, eigenvalues, filling)
        mulliken_charges(rho, overlap, q_orb)
        shell_charges(q_orb, layout.q0_orb, layout.orb_shell, dq_new)

        error = 0.0
        for i in range(n_shell):
            diff = abs(dq_new[i] - dq_shell[i])
            if diff > error:
                error = diff
        if verbose:
            print(f"    iteration {n_iteration:3d}  charge change {error:.3e}")
        if error < tolerance:
            dq_shell[:] = dq_new
            converged = True
            break

        # Append this iteration to the DIIS history, rolling only once the buffer
        # is full. Rolling earlier would shift in slots never written, and a zero
        # residual row makes the DIIS normal equations singular.
        if n_filled < history:
            slot = n_filled
            n_filled += 1
        else:
            for i in range(history - 1):
                history_in[i] = history_in[i + 1]
                history_residual[i] = history_residual[i + 1]
            slot = history - 1
        history_in[slot] = dq_shell
        for i in range(n_shell):
            history_residual[slot, i] = dq_new[i] - dq_shell[i]
        diis_mix(history_in, history_residual, n_filled, mixing, dq_shell)

    # Rebuild the potential and the Hamiltonian from the converged charges so that
    # every reported quantity belongs to one and the same set of charges.
    scc_potential(gamma, dq_shell, layout.orb_shell, v_shell, v_orb)
    scc_hamiltonian(h0, overlap, v_orb, h)
    eigenvalues, vectors = solve_generalised(h, overlap)
    e_fermi = fermi_filling(eigenvalues, n_electron, temperature, filling)
    rho, edm = density_matrices(vectors, eigenvalues, filling)
    mulliken_charges(rho, overlap, q_orb)

    energy_h0 = float(np.sum(rho * h0))
    energy_scc = 0.5 * float(v_shell @ dq_shell)
    energy_repulsive = repulsive_total(system)
    entropy_ts = fermi_entropy(filling, temperature)

    return SCCResult(
        rho=rho,
        edm=edm,
        h=h,
        eigenvalues=eigenvalues,
        vectors=vectors,
        filling=filling,
        e_fermi=e_fermi,
        q_orb=q_orb,
        dq_shell=dq_shell,
        v_shell=v_shell,
        v_orb=v_orb,
        gamma=gamma,
        layout=layout,
        energy_h0=energy_h0,
        energy_scc=energy_scc,
        energy_repulsive=energy_repulsive,
        entropy_ts=entropy_ts,
        energy_total=energy_h0 + energy_scc + energy_repulsive,
        energy_mermin=energy_h0 + energy_scc + energy_repulsive - entropy_ts,
        n_iteration=n_iteration,
        converged=converged,
    )


@kernel
def dipole_from_charges(coords, orb_atom, q0_orb, q_orb, dipole):
    """Dipole of one set of gross orbital populations, summed orbital by orbital."""

    for k in range(3):
        dipole[k] = 0.0
    for mu in range(q_orb.shape[0]):
        atom = orb_atom[mu]
        gross = q0_orb[mu] - q_orb[mu]
        for k in range(3):
            dipole[k] += gross * coords[atom, k]


def dipole_moment(system, q_orb, layout):
    """Dipole moment in atomic units, ``sum_A (q0_A - q_A) * R_A`` (Bohr times e)."""

    dipole = np.zeros(3)
    dipole_from_charges(system.coords, layout.orb_atom, layout.q0_orb, q_orb, dipole)
    return dipole
