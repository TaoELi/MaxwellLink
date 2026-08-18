# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# --------------------------------------------------------------------------------------#

"""
Ground-state forces of the SCC DFTB energy.

Every routine here adds to a gradient ``dE/dR`` and only :func:`total_force` flips the
sign, as DFTB+ does. The four contributions are the band term ``rho . dH0/dR``, the
Pulay term ``-W . dS/dR`` with ``W`` the energy-weighted density, the derivative of the
SCC shift, and the double-counting term ``dq_A dq_B dGamma/dR`` plus the repulsive pair
potential.

Every pair loop is a ``@kernel`` taking the parameter set as an
:data:`~dftb_params.SKTables` pack and the geometry as plain arrays, so the same body
compiles for the CPU and for the GPU; the public functions are thin wrappers that unpack
the objects.
"""

import math
from collections import namedtuple

import numpy as np

try:  # inside the package
    from .kernels_dftb import kernel
    from .skfiles import spline_repulsive
    from .dftb_params import DIST_FUDGE, MAX_INTEGRAL, MAX_ORB, N_INTERPOLATION
    from .scc import exp_gamma_prime
    from .sk_deriv import block_derivatives, sk_interpolate_deriv
except (ImportError, ValueError):  # allow running as a stand-alone script
    from kernels_dftb import kernel
    from skfiles import spline_repulsive
    from dftb_params import DIST_FUDGE, MAX_INTEGRAL, MAX_ORB, N_INTERPOLATION
    from scc import exp_gamma_prime
    from sk_deriv import block_derivatives, sk_interpolate_deriv


#: Work arrays of :func:`band_gradient_kernel`, allocated once and passed in, because a
#: kernel may not allocate. Fields, in order:
#:
#: ``sk_h``, ``sk_s``       -- interpolated H and S integrals of one pair,
#:                             ``MAX_INTEGRAL``
#: ``dsk_h``, ``dsk_s``     -- their radial derivatives, ``MAX_INTEGRAL``
#: ``d_h0``, ``d_overlap``  -- the three Cartesian derivative blocks,
#:                             ``(3, MAX_ORB, MAX_ORB)``
#: ``weight``, ``first``, ``second``
#:                          -- Lagrange weights and their two derivatives,
#:                             ``N_INTERPOLATION``
#: ``radial``, ``angular``, ``core``, ``dcore``
#:                          -- the ``block_derivatives`` scratch, shaped
#:                             ``(MAX_ORB, MAX_ORB)``, ``(3, MAX_ORB, MAX_ORB)``,
#:                             ``(5, 5)`` and ``(3, 5, 5)``
BandScratch = namedtuple(
    "BandScratch",
    "sk_h sk_s dsk_h dsk_s d_h0 d_overlap weight first second radial angular "
    "core dcore",
)

#: Lazily allocated, shape-independent scratch of the kernels below, so a trajectory
#: allocates nothing per step. The sizes are compile-time constants, hence one entry.
_SCRATCH = {}


def band_scratch():
    """Return the shared :data:`BandScratch`, allocating it on first use."""

    if "band" not in _SCRATCH:
        _SCRATCH["band"] = BandScratch(
            np.zeros(MAX_INTEGRAL),
            np.zeros(MAX_INTEGRAL),
            np.zeros(MAX_INTEGRAL),
            np.zeros(MAX_INTEGRAL),
            np.zeros((3, MAX_ORB, MAX_ORB)),
            np.zeros((3, MAX_ORB, MAX_ORB)),
            np.zeros(N_INTERPOLATION),
            np.zeros(N_INTERPOLATION),
            np.zeros(N_INTERPOLATION),
            np.zeros((MAX_ORB, MAX_ORB)),
            np.zeros((3, MAX_ORB, MAX_ORB)),
            np.zeros((5, 5)),
            np.zeros((3, 5, 5)),
        )
    return _SCRATCH["band"]


def repulsive_scratch():
    """Return the shared two-element ``(V, dV/dr)`` buffer of the repulsive kernel."""

    if "repulsive" not in _SCRATCH:
        _SCRATCH["repulsive"] = np.zeros(2)
    return _SCRATCH["repulsive"]


# ---------------------------------------------------------------------------- #
# repulsive pair potential                                                     #
# ---------------------------------------------------------------------------- #
@kernel
def repulsive_gradient_kernel(sk, coords, atom_species, n_atom, gradient, pair):
    """
    Repulsive pair energy of the whole system; adds its gradient to ``gradient``.

    One unordered pair per iteration, so the derivative is written to both atoms with
    opposite signs. ``pair`` is the two-element ``(V, dV/dr)`` buffer of
    :func:`spline_repulsive`.
    """

    energy = 0.0
    for a in range(n_atom):
        sp_a = atom_species[a]
        for b in range(a + 1, n_atom):
            sp_b = atom_species[b]
            dx = coords[a, 0] - coords[b, 0]
            dy = coords[a, 1] - coords[b, 1]
            dz = coords[a, 2] - coords[b, 2]
            dist = math.sqrt(dx * dx + dy * dy + dz * dz)
            # twobodyrep.F90:261 indexes [neighbour, owner], which is the A-B file for
            # the half-list entry whose owner is the lower-numbered atom.
            p = sk.pair_index[sp_a, sp_b]
            spline_repulsive(
                sk.rep_xstart[p],
                sk.rep_coeffs[p],
                sk.rep_last[p],
                sk.rep_exp[p],
                sk.rep_cutoff[p],
                sk.rep_n_interval[p],
                dist,
                pair,
            )
            energy += pair[0]
            slope = pair[1] / dist
            gradient[a, 0] += slope * dx
            gradient[a, 1] += slope * dy
            gradient[a, 2] += slope * dz
            gradient[b, 0] -= slope * dx
            gradient[b, 1] -= slope * dy
            gradient[b, 2] -= slope * dz
    return energy


def repulsive_energy_gradient(system, gradient):
    """Add the repulsive pair gradient to ``gradient`` and return its energy."""

    return repulsive_gradient_kernel(
        system.sk_set.tables(),
        system.coords,
        system.atom_species,
        system.n_atom,
        gradient,
        repulsive_scratch(),
    )


# ---------------------------------------------------------------------------- #
# electronic gradient                                                          #
# ---------------------------------------------------------------------------- #
@kernel
def overlap_weight(rho, edm, v_orb, weight):
    """
    Matrix multiplying ``dS/dR``: the Pulay term plus the derivative of the SCC shift.

    The shift block of the Hamiltonian is ``0.5 * S * (V_mu + V_nu)``, so its overlap
    derivative is weighted by ``0.5 * rho * (V_mu + V_nu)``; the Pulay term contributes
    ``-edm``. Both are collected once here so the pair loop touches one matrix.
    """

    n_orb = v_orb.shape[0]
    for mu in range(n_orb):
        for nu in range(n_orb):
            weight[mu, nu] = -edm[mu, nu] + 0.5 * rho[mu, nu] * (v_orb[mu] + v_orb[nu])


@kernel
def band_gradient_kernel(
    sk, coords, atom_species, atom_offset, n_atom, rho, weight, gradient, s
):
    """
    Band, Pulay and shift gradients of every ordered atom pair, forces.F90:625-662.

    ``rho`` multiplies ``dH0/dR`` and ``weight`` (from :func:`overlap_weight`)
    multiplies ``dS/dR``; both must be symmetric, which is what lets the ordered loop
    charge the whole pair to its column atom with a factor two. ``s`` is a
    :data:`BandScratch`.
    """

    for a in range(n_atom):
        sp_a = atom_species[a]
        n_orb_a = sk.n_orb_species[sp_a]
        for b in range(n_atom):
            if b == a:
                continue  # on-site blocks do not move, forces.F90:631
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
                continue
            sk_interpolate_deriv(
                sk.tab_h[pair],
                n_grid,
                grid_dist,
                n_integral,
                dist,
                s.sk_h,
                s.dsk_h,
                (s.weight, s.first, s.second),
            )
            sk_interpolate_deriv(
                sk.tab_s[pair],
                n_grid,
                grid_dist,
                n_integral,
                dist,
                s.sk_s,
                s.dsk_s,
                (s.weight, s.first, s.second),
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
                (s.radial, s.angular, s.core, s.dcore),
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
                (s.radial, s.angular, s.core, s.dcore),
            )

            row = atom_offset[b]
            col = atom_offset[a]
            for k in range(3):
                total = 0.0
                for p in range(n_orb_b):
                    for q in range(n_orb_a):
                        total += (
                            rho[row + p, col + q] * s.d_h0[k, p, q]
                            + weight[row + p, col + q] * s.d_overlap[k, p, q]
                        )
                # The factor two stands for the (b, a) block, which the ordered loop
                # covers by visiting the pair again with the atoms exchanged.
                gradient[a, k] += 2.0 * total


def band_gradient(system, rho, weight, gradient):
    """Add the band, Pulay and shift gradients, forces.F90:625-662."""

    band_gradient_kernel(
        system.sk_set.tables(),
        system.coords,
        system.atom_species,
        system.atom_offset,
        system.n_atom,
        rho,
        weight,
        gradient,
        band_scratch(),
    )


@kernel
def gamma_gradient_kernel(coords, shell_atom, shell_u, dq_shell, n_shell, gradient):
    """
    SCC double-counting gradient, shortgamma.F90:396 plus coulomb.F90:1164.

    One ordered shell pair per iteration; shells on the same atom are skipped, since
    the on-site gamma does not depend on any position.
    """

    for i in range(n_shell):
        atom_i = shell_atom[i]
        for j in range(n_shell):
            atom_j = shell_atom[j]
            if atom_i == atom_j:
                continue  # the on-site gamma does not depend on any position
            dx = coords[atom_i, 0] - coords[atom_j, 0]
            dy = coords[atom_i, 1] - coords[atom_j, 1]
            dz = coords[atom_i, 2] - coords[atom_j, 2]
            dist = math.sqrt(dx * dx + dy * dy + dz * dz)
            d_gamma = -1.0 / (dist * dist) - exp_gamma_prime(
                dist, shell_u[i], shell_u[j]
            )
            # Ordered shell pairs, so the 1/2 of the energy expression survives here.
            factor = 0.5 * dq_shell[i] * dq_shell[j] * d_gamma / dist
            gradient[atom_i, 0] += factor * dx
            gradient[atom_i, 1] += factor * dy
            gradient[atom_i, 2] += factor * dz
            gradient[atom_j, 0] -= factor * dx
            gradient[atom_j, 1] -= factor * dy
            gradient[atom_j, 2] -= factor * dz


def gamma_gradient(system, layout, dq_shell, gradient):
    """Add the SCC double-counting gradient, shortgamma.F90:396 plus coulomb.F90:1164."""

    gamma_gradient_kernel(
        system.coords,
        layout.shell_atom,
        layout.shell_u,
        dq_shell,
        layout.n_shell,
        gradient,
    )


# ---------------------------------------------------------------------------- #
# assembly                                                                     #
# ---------------------------------------------------------------------------- #
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

    n_orb = system.n_orb
    gradient = np.zeros((system.n_atom, 3))
    weight = np.zeros((n_orb, n_orb))
    overlap_weight(result.rho, result.edm, result.v_orb, weight)
    band_gradient(system, result.rho, weight, gradient)
    gamma_gradient(system, result.layout, result.dq_shell, gradient)
    repulsive_energy_gradient(system, gradient)
    return gradient


def total_force(system, result):
    """Force on every atom in Hartree/Bohr, i.e. minus the gradient."""

    return -total_gradient(system, result)
