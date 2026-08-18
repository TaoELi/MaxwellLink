# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# --------------------------------------------------------------------------------------#

"""
Ehrenfest dynamics: forces from the time-dependent density, and the nuclear step.

Relative to the ground-state force of :mod:`forces`, three things change: only the real
part of the (complex) density enters, the energy-weighted density matrix is built from
``H`` and ``S^-1`` because there are no orbital energies during a real-time run, and the
external field adds an explicit ``dq_A E`` term. The nuclei move with velocity Verlet at
the same step as the electrons.

The non-adiabatic coupling ``D`` couples the moving basis into the propagator. It is
one-sided, not ``dS/dt``, and must not be symmetrised: the identity ``dS/dt = D + D^T``
is what keeps the electron count fixed while ``S`` itself is changing.
"""

import math
from collections import namedtuple

import numpy as np

try:  # inside the package
    from .dftb_params import DIST_FUDGE, MAX_INTEGRAL, MAX_ORB, N_INTERPOLATION
    from .forces import (
        band_gradient,
        gamma_gradient,
        overlap_weight,
        repulsive_energy_gradient,
    )
    from .kernels_dftb import kernel
    from .sk_deriv import block_derivatives, sk_interpolate_deriv
except (ImportError, ValueError):  # allow running as a stand-alone script
    from dftb_params import DIST_FUDGE, MAX_INTEGRAL, MAX_ORB, N_INTERPOLATION
    from forces import (
        band_gradient,
        gamma_gradient,
        overlap_weight,
        repulsive_energy_gradient,
    )
    from kernels_dftb import kernel
    from sk_deriv import block_derivatives, sk_interpolate_deriv

# Masses arrive from the .skf files already converted to electron masses
# (skfiles.py multiplies by amu__au = 1 / 0.00054857990945, constants.F90:49-52), which
# is the unit DFTB+'s `movedMass` carries too.


# ---------------------------------------------------------------------------- #
# energy-weighted density matrix of a real-time state                          #
# ---------------------------------------------------------------------------- #
@kernel
def real_part(rho, out, n):
    """Copy ``Re[rho]`` into a real matrix; the imaginary part never enters a force."""

    for i in range(n):
        for j in range(n):
            out[i, j] = rho[i, j].real


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
def field_gradient(dq_atom, field, gradient, n_atom):
    """External-field force term, ``dE/dR_A += dq_A E`` (timeprop.F90:4110-4113)."""

    for a in range(n_atom):
        for k in range(3):
            gradient[a, k] += dq_atom[a] * field[k]


# ---------------------------------------------------------------------------- #
# non-adiabatic coupling D                                                     #
# ---------------------------------------------------------------------------- #
#: Work arrays of :func:`coupling_kernel` and :func:`overlap_time_derivative_kernel`,
#: allocated once and passed in, because a kernel may not allocate. Fields, in order:
#:
#: ``sk_s``                 -- interpolated overlap integrals of one pair,
#:                             ``MAX_INTEGRAL``
#: ``dsk_s``                -- their radial derivatives, ``MAX_INTEGRAL``
#: ``d_overlap``            -- the three Cartesian derivative blocks,
#:                            ``(3, MAX_ORB, MAX_ORB)``
#: ``weight``, ``first``, ``second``
#:                          -- Lagrange weights and their two derivatives,
#:                             ``N_INTERPOLATION``
#: ``radial``, ``angular``, ``core``, ``dcore``
#:                          -- the ``block_derivatives`` scratch, shaped
#:                             ``(MAX_ORB, MAX_ORB)``, ``(3, MAX_ORB, MAX_ORB)``,
#:                             ``(5, 5)`` and ``(3, 5, 5)``
CouplingScratch = namedtuple(
    "CouplingScratch",
    "sk_s dsk_s d_overlap weight first second radial angular core dcore",
)

#: Lazily allocated, shape-independent scratch of the kernels below, so a trajectory
#: allocates nothing per step. The sizes are compile-time constants, hence one entry.
_SCRATCH = {}


def coupling_scratch():
    """Return the shared :data:`CouplingScratch`, allocating it on first use."""

    if "coupling" not in _SCRATCH:
        _SCRATCH["coupling"] = CouplingScratch(
            np.zeros(MAX_INTEGRAL),
            np.zeros(MAX_INTEGRAL),
            np.zeros((3, MAX_ORB, MAX_ORB)),
            np.zeros(N_INTERPOLATION),
            np.zeros(N_INTERPOLATION),
            np.zeros(N_INTERPOLATION),
            np.zeros((MAX_ORB, MAX_ORB)),
            np.zeros((3, MAX_ORB, MAX_ORB)),
            np.zeros((5, 5)),
            np.zeros((3, 5, 5)),
        )
    return _SCRATCH["coupling"]


@kernel
def coupling_kernel(
    sk, coords, atom_species, atom_offset, n_atom, velocities, coupling, s
):
    """
    ``D_mu,nu = v_{A(nu)} . dS_mu,nu / dR_{A(nu)}`` over ordered atom pairs.

    ``coupling`` is zeroed here rather than by the caller, so the kernel owns the whole
    matrix; the on-site blocks are simply never written. ``s`` is a
    :data:`CouplingScratch`.
    """

    for i in range(coupling.shape[0]):
        for j in range(coupling.shape[1]):
            coupling[i, j] = 0.0

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
                (s.weight, s.first, s.second),
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
                (s.radial, s.angular, s.core, s.dcore),
            )

            row = atom_offset[b]
            col = atom_offset[a]
            for p in range(n_orb_b):
                for q in range(n_orb_a):
                    total = 0.0
                    for k in range(3):
                        total += s.d_overlap[k, p, q] * velocities[a, k]
                    coupling[row + p, col + q] += total


def build_coupling(system, velocities, coupling):
    """Fill ``D_mu,nu = v_{A(nu)} . dS_mu,nu / dR_{A(nu)}``, ``getRdotSprime``.

    The loop runs over ordered atom pairs, differentiating each block with respect to its
    column atom and contracting with that atom's velocity; visiting the pair the other way
    round fills the transposed block with the other velocity. The on-site blocks stay
    zero: an atom's own overlap does not change when it moves.
    """

    coupling_kernel(
        system.sk_set.tables(),
        system.coords,
        system.atom_species,
        system.atom_offset,
        system.n_atom,
        velocities,
        coupling,
        coupling_scratch(),
    )


@kernel
def overlap_time_derivative_kernel(
    sk, coords, atom_species, atom_offset, n_atom, velocities, sdot, s
):
    """
    ``dS/dt = sum_A v_A . dS/dR_A`` over ordered atom pairs; ``s`` is a
    :data:`CouplingScratch`.
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
                (s.weight, s.first, s.second),
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
                (s.radial, s.angular, s.core, s.dcore),
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
    """
    ``dS/dt = sum_A v_A . dS/dR_A``, the identity ``D + D^T`` must reproduce.

    Only used as a check: if it fails, either the sign convention of the Slater-Koster
    derivative or the row/column order of the block has been transcribed wrongly, and
    the Ehrenfest run will silently leak electrons.
    """

    overlap_time_derivative_kernel(
        system.sk_set.tables(),
        system.coords,
        system.atom_species,
        system.atom_offset,
        system.n_atom,
        velocities,
        sdot,
        coupling_scratch(),
    )


# ---------------------------------------------------------------------------- #
# forces of a real-time state                                                  #
# ---------------------------------------------------------------------------- #
def ehrenfest_gradient(state, scratch=None):
    """
    Energy gradient ``dE/dR`` of one real-time state, in Hartree/Bohr.

    Parameters
    ----------
    state : rt.RTState
        Current density matrix, Hamiltonian, overlap, charges and field. ``state.h``
        and ``state.v_orb`` must already belong to the current geometry and charges,
        i.e. ``update_charges`` and ``update_hamiltonian`` have been called.
    scratch : dict, optional
        Re-usable work arrays, so a long trajectory allocates nothing per step.

    Returns
    -------
    numpy.ndarray of float, shape (n_atom, 3)
        The gradient; the force is minus this.
    """

    n = state.n_orb
    if scratch is None:
        scratch = {}
    density = scratch.setdefault("density", np.zeros((n, n)))
    product = scratch.setdefault("product", np.zeros((n, n)))
    weight_e = scratch.setdefault("weight_e", np.zeros((n, n)))
    weight = scratch.setdefault("weight", np.zeros((n, n)))
    gradient = scratch.setdefault("gradient", np.zeros((state.n_atom, 3)))

    real_part(state.rho, density, n)
    energy_weighted_density(density, state.h, state.s_inv, product, weight_e, n)
    overlap_weight(density, weight_e, state.v_orb, weight)

    gradient[:, :] = 0.0
    band_gradient(state.system, density, weight, gradient)
    gamma_gradient(state.system, state.layout, state.dq_shell, gradient)
    repulsive_energy_gradient(state.system, gradient)
    field_gradient(state.dq_atom, state.field, gradient, state.n_atom)
    return gradient


def ehrenfest_force(state, scratch=None):
    """Force on every atom in Hartree/Bohr, minus :func:`ehrenfest_gradient`."""

    return -ehrenfest_gradient(state, scratch=scratch)


# ---------------------------------------------------------------------------- #
# the nuclear step                                                             #
# ---------------------------------------------------------------------------- #
@kernel
def nuclear_step(positions, half_velocities, accel, dt, n_atom, velocity_now):
    """
    One integrator call in plain arrays: ``a(t)`` in, ``r(t+dt)`` and ``v(t)`` out.

    ``half_velocities`` enters as ``v(t - dt/2)`` and is updated in place to
    ``v(t + dt/2)``; ``positions`` enters as ``r(t)`` and is updated to ``r(t+dt)``;
    ``velocity_now`` receives ``v(t)``. The caller supplies that buffer rather than the
    kernel allocating it, because a device function cannot allocate.
    """

    for a in range(n_atom):
        for k in range(3):
            velocity_now[a, k] = half_velocities[a, k] + 0.5 * accel[a, k] * dt
            half_velocities[a, k] = velocity_now[a, k] + 0.5 * accel[a, k] * dt
            positions[a, k] += half_velocities[a, k] * dt


@kernel
def kinetic_sum(mass, velocities, n_atom):
    """Nuclear kinetic energy in Hartree, ``0.5 sum_A m_A v_A^2``."""

    total = 0.0
    for a in range(n_atom):
        for k in range(3):
            total += mass[a] * velocities[a, k] * velocities[a, k]
    return 0.5 * total


def velocity_verlet_next(positions, half_velocities, accel, dt):
    """One integrator call, allocating the ``v(t)`` the scalar driver returns."""

    velocity_now = np.zeros((positions.shape[0], 3))
    nuclear_step(
        positions, half_velocities, accel, dt, positions.shape[0], velocity_now
    )
    return velocity_now


def kinetic_energy(mass, velocities):
    """Nuclear kinetic energy in Hartree, ``0.5 sum_A m_A v_A^2``."""

    return kinetic_sum(mass, velocities, velocities.shape[0])
