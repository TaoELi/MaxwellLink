# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Forces of the SCC DFTB energy, for the ground state and for a real-time density.

Every routine here adds to a gradient ``dE/dR`` and only the callers flip the sign, as
DFTB+ does. The four contributions are the band term ``rho . dH0/dR``, the Pulay term
``-W . dS/dR`` with ``W`` the energy-weighted density, the derivative of the SCC shift,
and the double-counting term ``dq_A dq_B dGamma/dR`` plus the repulsive pair potential.
:func:`energy_gradient` sums them for any real density and weight matrix.

A real-time (Ehrenfest) state differs from the ground state in three ways, all handled
by the callers: only the real part of the complex density enters, the energy-weighted
density is built from ``H`` and ``S^-1`` (:func:`energy_weighted_density`) because there
are no orbital energies during a real-time run, and the external field adds an explicit
``dq_A E`` term (:func:`field_gradient`). The non-adiabatic coupling ``D`` that couples
the moving basis into the propagator is assembled here as well; it is one-sided, not
``dS/dt``, and must not be symmetrised: the identity ``dS/dt = D + D^T`` is what keeps
the electron count fixed while ``S`` itself is changing.

Every pair loop is a ``@kernel`` taking the parameter set as an
:data:`~dftb_params.SKTables` pack, the geometry as plain arrays and a
:data:`~h0_overlap.PairScratch`, so the same body compiles for the CPU and for the GPU.
"""

import math

import numpy as np

from .dftb_params import DIST_FUDGE
from .h0_overlap import pair_scratch
from .jit import kernel
from .scc import exp_gamma_prime
from .sk_deriv import block_derivatives, sk_interpolate_deriv
from .skfiles import repulsive_pair


# ---------------------------------------------------------------------------- #
# the matrices that multiply dH0/dR and dS/dR                                  #
# ---------------------------------------------------------------------------- #
@kernel
def real_part_row(rho, out, i, n):
    """Row ``i`` of ``Re[rho]``."""

    for j in range(n):
        out[i, j] = rho[i, j].real


@kernel
def real_part(rho, out, n):
    """Copy ``Re[rho]`` into a real matrix; the imaginary part never enters a force."""

    for i in range(n):
        real_part_row(rho, out, i, n)


@kernel
def energy_weighted_density(density, h, s_inv, product, weight, n):
    """
    ``W = 0.5 (S^-1 H P + P H S^-1)`` with ``P = Re[rho]``, timeprop.F90:4041-4042.

    ``product`` is scratch holding ``P H``. ``W`` replaces the ground state's
    ``sum_i f_i eps_i c_i c_i^T`` and reduces to it exactly when ``rho`` is a stationary
    state of ``H``.
    """

    for i in range(n):
        for j in range(n):
            total = density[i, 0] * h[0, j]
            for k in range(1, n):
                total += density[i, k] * h[k, j]
            product[i, j] = total
    for i in range(n):
        for j in range(n):
            total = 0.0
            for k in range(n):
                total += s_inv[i, k] * product[j, k] + product[i, k] * s_inv[j, k]
            weight[i, j] = 0.5 * total


@kernel
def overlap_weight_row(rho, edm, v_orb, weight, mu):
    """Row ``mu`` of :func:`overlap_weight`."""

    for nu in range(v_orb.shape[0]):
        weight[mu, nu] = -edm[mu, nu] + 0.5 * rho[mu, nu] * (v_orb[mu] + v_orb[nu])


@kernel
def overlap_weight(rho, edm, v_orb, weight):
    """
    Matrix multiplying ``dS/dR``: the Pulay term plus the derivative of the SCC shift.

    The shift block of the Hamiltonian is ``0.5 * S * (V_mu + V_nu)``, so its overlap
    derivative is weighted by ``0.5 * rho * (V_mu + V_nu)``; the Pulay term contributes
    ``-edm``. Both are collected once here so the pair loop touches one matrix.
    """

    for mu in range(v_orb.shape[0]):
        overlap_weight_row(rho, edm, v_orb, weight, mu)


# ---------------------------------------------------------------------------- #
# the gradient contributions                                                   #
# ---------------------------------------------------------------------------- #
@kernel
def band_gradient_pair(sk, coords, atom_species, atom_offset, a, b, rho, weight, s):
    """
    Band, Pulay and shift gradient of one ordered atom pair ``a != b`` on atom ``a``.

    Returns the three components; the factor two stands for the ``(b, a)`` block,
    which the ordered enumeration covers by visiting the pair again with the atoms
    exchanged. Pairs beyond the table's reach return zeros. ``s`` is a
    :data:`~h0_overlap.PairScratch`.
    """

    sp_a = atom_species[a]
    n_orb_a = sk.n_orb_species[sp_a]
    sp_b = atom_species[b]
    n_orb_b = sk.n_orb_species[sp_b]

    dx = coords[b, 0] - coords[a, 0]
    dy = coords[b, 1] - coords[a, 1]
    dz = coords[b, 2] - coords[a, 2]
    dist = math.sqrt(dx * dx + dy * dy + dz * dz)
    u_x = dx / dist
    u_y = dy / dist
    u_z = dz / dist

    pair = sk.pair_index[sp_a, sp_b]
    n_grid = sk.tab_n_grid[pair]
    grid_dist = sk.tab_grid_dist[pair]
    n_integral = sk.tab_n_integral[pair]
    if dist >= n_grid * grid_dist + DIST_FUDGE:
        return 0.0, 0.0, 0.0
    sk_interpolate_deriv(
        sk.tab_h[pair], n_grid, grid_dist, n_integral, dist, s.sk_h, s.dsk_h, s
    )
    sk_interpolate_deriv(
        sk.tab_s[pair], n_grid, grid_dist, n_integral, dist, s.sk_s, s.dsk_s, s
    )
    block_derivatives(
        s.sk_h,
        s.dsk_h,
        u_x,
        u_y,
        u_z,
        dist,
        sk.ang_shell[sp_a],
        sk.n_shell[sp_a],
        sk.ang_shell[sp_b],
        sk.n_shell[sp_b],
        s.d_h0,
        s,
    )
    block_derivatives(
        s.sk_s,
        s.dsk_s,
        u_x,
        u_y,
        u_z,
        dist,
        sk.ang_shell[sp_a],
        sk.n_shell[sp_a],
        sk.ang_shell[sp_b],
        sk.n_shell[sp_b],
        s.d_overlap,
        s,
    )

    row = atom_offset[b]
    col = atom_offset[a]
    g_x = 0.0
    g_y = 0.0
    g_z = 0.0
    for p in range(n_orb_b):
        for q in range(n_orb_a):
            r = rho[row + p, col + q]
            w = weight[row + p, col + q]
            g_x += r * s.d_h0[0, p, q] + w * s.d_overlap[0, p, q]
            g_y += r * s.d_h0[1, p, q] + w * s.d_overlap[1, p, q]
            g_z += r * s.d_h0[2, p, q] + w * s.d_overlap[2, p, q]
    return 2.0 * g_x, 2.0 * g_y, 2.0 * g_z


@kernel
def band_gradient_kernel(
    sk, coords, atom_species, atom_offset, n_atom, rho, weight, gradient, s
):
    """
    Band, Pulay and shift gradients of every ordered atom pair, forces.F90:625-662.

    ``rho`` multiplies ``dH0/dR`` and ``weight`` (from :func:`overlap_weight`)
    multiplies ``dS/dR``; both must be symmetric, which is what lets the ordered loop
    charge the whole pair to its column atom with a factor two.
    """

    for a in range(n_atom):
        for b in range(n_atom):
            if b == a:
                continue  # on-site blocks do not move, forces.F90:631
            g_x, g_y, g_z = band_gradient_pair(
                sk, coords, atom_species, atom_offset, a, b, rho, weight, s
            )
            gradient[a, 0] += g_x
            gradient[a, 1] += g_y
            gradient[a, 2] += g_z


@kernel
def gamma_gradient_pair(coords, shell_atom, shell_u, dq_shell, i, j):
    """
    SCC double-counting gradient of one ordered shell pair, on the atom of shell ``i``.

    Returns the three components; the atom of shell ``j`` takes the negative. Shells on
    the same atom return zeros, since the on-site gamma does not depend on any position.
    """

    atom_i = shell_atom[i]
    atom_j = shell_atom[j]
    if atom_i == atom_j:
        return 0.0, 0.0, 0.0
    dx = coords[atom_i, 0] - coords[atom_j, 0]
    dy = coords[atom_i, 1] - coords[atom_j, 1]
    dz = coords[atom_i, 2] - coords[atom_j, 2]
    dist = math.sqrt(dx * dx + dy * dy + dz * dz)
    d_gamma = -1.0 / (dist * dist) - exp_gamma_prime(dist, shell_u[i], shell_u[j])
    # Ordered shell pairs, so the 1/2 of the energy expression survives here.
    factor = 0.5 * dq_shell[i] * dq_shell[j] * d_gamma / dist
    return factor * dx, factor * dy, factor * dz


@kernel
def gamma_gradient_kernel(coords, shell_atom, shell_u, dq_shell, gradient):
    """
    SCC double-counting gradient, shortgamma.F90:396 plus coulomb.F90:1164.

    One ordered shell pair per iteration; shells on the same atom are skipped, since
    the on-site gamma does not depend on any position.
    """

    n_shell = shell_atom.shape[0]
    for i in range(n_shell):
        atom_i = shell_atom[i]
        for j in range(n_shell):
            atom_j = shell_atom[j]
            if atom_i == atom_j:
                continue  # the on-site gamma does not depend on any position
            f_x, f_y, f_z = gamma_gradient_pair(
                coords, shell_atom, shell_u, dq_shell, i, j
            )
            gradient[atom_i, 0] += f_x
            gradient[atom_i, 1] += f_y
            gradient[atom_i, 2] += f_z
            gradient[atom_j, 0] -= f_x
            gradient[atom_j, 1] -= f_y
            gradient[atom_j, 2] -= f_z


@kernel
def repulsive_gradient_kernel(sk, coords, atom_species, n_atom, gradient, pair):
    """
    Repulsive pair energy of the whole system; adds its gradient to ``gradient``.

    One unordered pair per iteration, so the derivative is written to both atoms with
    opposite signs. ``pair`` is the two-element ``(V, dV/dr)`` buffer of
    :func:`skfiles.spline_repulsive`.
    """

    energy = 0.0
    for a in range(n_atom):
        for b in range(a + 1, n_atom):
            e_pair, gx, gy, gz = repulsive_pair(sk, coords, atom_species, a, b, pair)
            energy += e_pair
            gradient[a, 0] += gx
            gradient[a, 1] += gy
            gradient[a, 2] += gz
            gradient[b, 0] -= gx
            gradient[b, 1] -= gy
            gradient[b, 2] -= gz
    return energy


@kernel
def field_gradient(dq_atom, field, gradient):
    """External-field force term, ``dE/dR_A += dq_A E`` (timeprop.F90:4110-4113)."""

    for a in range(dq_atom.shape[0]):
        for k in range(3):
            gradient[a, k] += dq_atom[a] * field[k]


# ---------------------------------------------------------------------------- #
# non-adiabatic coupling D                                                     #
# ---------------------------------------------------------------------------- #
@kernel
def coupling_pair(sk, coords, atom_species, atom_offset, a, b, velocities, coupling, s):
    """
    ``D`` block of one ordered atom pair ``a != b``: ``v_a . dS_mu,nu / dR_a`` for the
    rows of ``b`` and the columns of ``a``, added to a zeroed ``coupling``.

    Ordered pairs write disjoint blocks, so they may run in any order or in parallel.
    Pairs beyond the table's reach contribute nothing.
    """

    sp_a = atom_species[a]
    n_orb_a = sk.n_orb_species[sp_a]
    sp_b = atom_species[b]
    n_orb_b = sk.n_orb_species[sp_b]

    dx = coords[b, 0] - coords[a, 0]
    dy = coords[b, 1] - coords[a, 1]
    dz = coords[b, 2] - coords[a, 2]
    dist = math.sqrt(dx * dx + dy * dy + dz * dz)
    pair = sk.pair_index[sp_a, sp_b]
    n_grid = sk.tab_n_grid[pair]
    grid_dist = sk.tab_grid_dist[pair]
    if dist >= n_grid * grid_dist + DIST_FUDGE:
        return
    sk_interpolate_deriv(
        sk.tab_s[pair],
        n_grid,
        grid_dist,
        sk.tab_n_integral[pair],
        dist,
        s.sk_s,
        s.dsk_s,
        s,
    )
    block_derivatives(
        s.sk_s,
        s.dsk_s,
        dx / dist,
        dy / dist,
        dz / dist,
        dist,
        sk.ang_shell[sp_a],
        sk.n_shell[sp_a],
        sk.ang_shell[sp_b],
        sk.n_shell[sp_b],
        s.d_overlap,
        s,
    )

    row = atom_offset[b]
    col = atom_offset[a]
    for p in range(n_orb_b):
        for q in range(n_orb_a):
            total = 0.0
            for k in range(3):
                total += s.d_overlap[k, p, q] * velocities[a, k]
            coupling[row + p, col + q] += total


@kernel
def coupling_kernel(
    sk, coords, atom_species, atom_offset, n_atom, velocities, coupling, s
):
    """
    ``D_mu,nu = v_{A(nu)} . dS_mu,nu / dR_{A(nu)}`` over ordered atom pairs,
    ``getRdotSprime``.

    The loop differentiates each block with respect to its column atom and contracts
    with that atom's velocity; visiting the pair the other way round fills the
    transposed block with the other velocity. ``coupling`` is zeroed here rather than
    by the caller, so the kernel owns the whole matrix; the on-site blocks are simply
    never written, since an atom's own overlap does not change when it moves.
    """

    for i in range(coupling.shape[0]):
        for j in range(coupling.shape[1]):
            coupling[i, j] = 0.0

    for a in range(n_atom):
        for b in range(n_atom):
            if b != a:
                coupling_pair(
                    sk, coords, atom_species, atom_offset, a, b, velocities, coupling, s
                )


@kernel
def overlap_time_derivative_kernel(
    sk, coords, atom_species, atom_offset, n_atom, velocities, sdot, s
):
    """
    ``dS/dt = sum_A v_A . dS/dR_A`` over ordered atom pairs, the identity ``D + D^T``
    must reproduce.

    Only used as a check: if it fails, either the sign convention of the Slater-Koster
    derivative or the row/column order of the block has been transcribed wrongly, and
    the Ehrenfest run will silently leak electrons.
    """

    for i in range(sdot.shape[0]):
        for j in range(sdot.shape[1]):
            sdot[i, j] = 0.0

    for a in range(n_atom):
        sp_a = atom_species[a]
        n_orb_a = sk.n_orb_species[sp_a]
        for b in range(n_atom):
            if b == a:
                continue
            sp_b = atom_species[b]
            n_orb_b = sk.n_orb_species[sp_b]
            dx = coords[b, 0] - coords[a, 0]
            dy = coords[b, 1] - coords[a, 1]
            dz = coords[b, 2] - coords[a, 2]
            dist = math.sqrt(dx * dx + dy * dy + dz * dz)
            pair = sk.pair_index[sp_a, sp_b]
            n_grid = sk.tab_n_grid[pair]
            grid_dist = sk.tab_grid_dist[pair]
            if dist >= n_grid * grid_dist + DIST_FUDGE:
                continue
            sk_interpolate_deriv(
                sk.tab_s[pair],
                n_grid,
                grid_dist,
                sk.tab_n_integral[pair],
                dist,
                s.sk_s,
                s.dsk_s,
                s,
            )
            block_derivatives(
                s.sk_s,
                s.dsk_s,
                dx / dist,
                dy / dist,
                dz / dist,
                dist,
                sk.ang_shell[sp_a],
                sk.n_shell[sp_a],
                sk.ang_shell[sp_b],
                sk.n_shell[sp_b],
                s.d_overlap,
                s,
            )
            row = atom_offset[b]
            col = atom_offset[a]
            for p in range(n_orb_b):
                for q in range(n_orb_a):
                    # d/dR_a of the (b, a) block, contracted with v_a; the a-row block
                    # picks up minus the same thing from v_b through translation.
                    for k in range(3):
                        sdot[row + p, col + q] += s.d_overlap[k, p, q] * (
                            velocities[a, k] - velocities[b, k]
                        )


def overlap_time_derivative(system, velocities, sdot):
    """Fill ``sdot`` with ``dS/dt = sum_A v_A . dS/dR_A`` (a diagnostic)."""

    overlap_time_derivative_kernel(
        system.tables,
        system.coords,
        system.atom_species,
        system.atom_offset,
        system.n_atom,
        velocities,
        sdot,
        pair_scratch(),
    )


# ---------------------------------------------------------------------------- #
# assembly                                                                     #
# ---------------------------------------------------------------------------- #
def energy_gradient(
    system,
    layout,
    density,
    weight,
    dq_shell,
    gradient,
    scratch,
    dq_atom=None,
    field=None,
):
    """
    Energy gradient ``dE/dR`` in Hartree/Bohr of one real density and weight matrix.

    Parameters
    ----------
    system : dftb_params.DFTBSystem
        Geometry and basis layout.
    layout : dftb_params.ShellLayout
        The shell layout the charges refer to.
    density : numpy.ndarray of float, shape (n_orb, n_orb)
        The (real) density matrix multiplying ``dH0/dR``.
    weight : numpy.ndarray of float, shape (n_orb, n_orb)
        The matrix multiplying ``dS/dR``, from :func:`overlap_weight`.
    dq_shell : numpy.ndarray of float, shape (n_shell,)
        Shell charge excess of the SCC double-counting term.
    gradient : numpy.ndarray of float, shape (n_atom, 3)
        Receives the gradient; zeroed here.
    scratch : h0_overlap.PairScratch
        Working arrays of the pair loops.
    dq_atom, field : numpy.ndarray of float, optional
        Atomic charge excess and external field; when both are given the field term
        ``dq_A E`` of a real-time state is added.

    Returns
    -------
    numpy.ndarray of float, shape (n_atom, 3)
        ``gradient``; the force is minus this.
    """

    gradient[:, :] = 0.0
    band_gradient_kernel(
        system.tables,
        system.coords,
        system.atom_species,
        system.atom_offset,
        system.n_atom,
        density,
        weight,
        gradient,
        scratch,
    )
    gamma_gradient_kernel(
        system.coords, layout.shell_atom, layout.shell_u, dq_shell, gradient
    )
    repulsive_gradient_kernel(
        system.tables,
        system.coords,
        system.atom_species,
        system.n_atom,
        gradient,
        scratch.pair,
    )
    if field is not None:
        field_gradient(dq_atom, field, gradient)
    return gradient


def total_gradient(system, result):
    """
    Energy gradient ``dE/dR`` of the converged SCC ground state, in Hartree/Bohr.

    Parameters
    ----------
    system : dftb_params.DFTBSystem
        Geometry and basis layout.
    result : scc.SCCResult
        Output of :func:`scc.scf` for this geometry.

    Returns
    -------
    numpy.ndarray of float, shape (n_atom, 3)
        The gradient; the force is minus this.
    """

    weight = np.zeros((system.n_orb, system.n_orb))
    overlap_weight(result.rho, result.edm, result.v_orb, weight)
    return energy_gradient(
        system,
        result.layout,
        result.rho,
        weight,
        result.dq_shell,
        np.zeros((system.n_atom, 3)),
        pair_scratch(),
    )


def total_force(system, result):
    """Force on every atom in Hartree/Bohr, i.e. minus the gradient."""

    return -total_gradient(system, result)
