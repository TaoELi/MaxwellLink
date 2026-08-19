# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# --------------------------------------------------------------------------------------#

"""
Dense non-SCC Hamiltonian H0 and overlap S, with the full spd Slater-Koster rotation.

The build has three layers: :func:`sk_interpolate` reads one atom pair's radial table
at one distance, :func:`rotate_block` turns the sigma/pi/delta integrals into a diatomic
block through the direction cosines, and :func:`build_h0_overlap_kernel` places the
blocks into the dense matrices. All three are scalar loops over fixed-size scratch, so
the same body compiles for the CPU and for the GPU.

The orbital order inside a shell is ``m = -l ... +l``, so ``p`` is ``(y, z, x)`` and
``d`` is ``(xy, yz, z2, xz, x2-y2)``, and a pair block has rows on the second atom and
columns on the first. Both conventions come from DFTB+ and are easy to get wrong.
"""

import math
from collections import namedtuple

import numpy as np

try:  # inside the package
    from .kernels_dftb import kernel
    from .dftb_params import (
        DELTA_R,
        DIST_FUDGE,
        MAX_INTEGRAL,
        MAX_ORB,
        N_INTERPOLATION,
        N_RIGHT,
    )
except (ImportError, ValueError):  # allow running as a stand-alone script
    from kernels_dftb import kernel
    from dftb_params import (
        DELTA_R,
        DIST_FUDGE,
        MAX_INTEGRAL,
        MAX_ORB,
        N_INTERPOLATION,
        N_RIGHT,
    )

SQRT3 = math.sqrt(3.0)


# ---------------------------------------------------------------------------- #
# radial interpolation                                                         #
# ---------------------------------------------------------------------------- #
@kernel
def _poly_inter_uniform(node, table, row0, n_integral, x, out, cc, dd, delta):
    """Neville interpolation through eight uniform nodes, one column at a time."""

    n = N_INTERPOLATION
    for j in range(n - 1):
        delta[j] = 1.0 / (node[j + 1] - node[0])
    i_start = int(math.ceil((x - node[0]) * delta[0]))
    # Which node the tableau starts from only affects rounding, never the value, so the
    # clamp is free. It matters at r <= grid_dist, where DFTB+'s own index reaches 0
    # and reads out of bounds; that region is unreachable for any physical geometry, but
    # an unclamped Python index would wrap silently instead of failing.
    if i_start < 1:
        i_start = 1
    elif i_start > n:
        i_start = n
    for c in range(n_integral):
        for i in range(n):
            cc[i] = table[row0 + i, c]
            dd[i] = table[row0 + i, c]
        i_close = i_start  # 1-based index of the node closest to x, as in polyInterU2
        value = cc[i_close - 1]
        i_close -= 1
        for m in range(1, n):
            for i in range(n - m):
                slope = (dd[i] - cc[i + 1]) * delta[m - 1]
                cc[i] = (node[i] - x) * slope
                dd[i] = (node[i + m] - x) * slope
            if 2 * i_close < n - m:
                correction = cc[i_close]
            else:
                correction = dd[i_close - 1]
                i_close -= 1
            value += correction
        out[c] = value


@kernel
def poly5_to_zero(y0, y0p, y0pp, x, dx, inv_dx):
    """Quintic matching (y, y', y'') at ``dx`` and vanishing to 2nd order at zero."""

    dx1 = y0p * dx
    dx2 = y0pp * dx * dx
    d = 10.0 * y0 - 4.0 * dx1 + 0.5 * dx2
    e = -15.0 * y0 + 7.0 * dx1 - 1.0 * dx2
    f = 6.0 * y0 - 3.0 * dx1 + 0.5 * dx2
    xr = x * inv_dx
    return ((f * xr + e) * xr + d) * xr * xr * xr


@kernel
def sk_interpolate(table, n_grid, grid_dist, n_integral, r, out, scratch):
    """
    Interpolate one pair's radial Slater-Koster table at distance ``r``.

    Parameters
    ----------
    table : numpy.ndarray of float, shape (>= n_grid, >= n_integral)
        Radial table of the ordered species pair. Row ``k`` sits at
        ``r = (k + 1) * grid_dist``; there is no ``r = 0`` row.
    n_grid : int
        Number of valid rows.
    grid_dist : float
        Grid spacing in Bohr.
    n_integral : int
        Number of valid columns.
    r : float
        Interatomic distance in Bohr.
    out : numpy.ndarray of float, shape (>= n_integral,)
        Receives the integrals; zeroed first, so padded columns stay clean.
    scratch : tuple of numpy.ndarray
        ``(node, cc, dd, delta, y_low, y_high)`` working arrays of sizes 8, 8, 8, 7,
        ``MAX_INTEGRAL`` and ``MAX_INTEGRAL``.
    """

    node, cc, dd, delta, y_low, y_high = scratch
    for c in range(n_integral):
        out[c] = 0.0

    r_max = n_grid * grid_dist + DIST_FUDGE
    if r >= r_max:  # beyond the table plus its tail there is no interaction at all
        return

    ind = int(math.floor(r / grid_dist))
    if ind < n_grid:
        # Inside the tabulated range: eight-point polynomial, four points to the right
        # where possible, with the window frozen at either end of the table.
        # ternaries, not min()/max(): the two-argument builtins compile under njit
        # but not under cuda.jit, which would break only the GPU build
        i_last = ind + N_RIGHT
        if i_last > n_grid:
            i_last = n_grid
        if i_last < N_INTERPOLATION:
            i_last = N_INTERPOLATION
        row0 = i_last - N_INTERPOLATION
        for k in range(N_INTERPOLATION):
            node[k] = (row0 + k + 1) * grid_dist
        _poly_inter_uniform(node, table, row0, n_integral, r, out, cc, dd, delta)
    else:
        # Last Bohr: a fifth-order polynomial that carries the table smoothly to zero.
        # The anchor value is the raw last table row, its derivatives are central
        # differences of the same eight-point polynomial (slakoeqgrid.F90:262-273).
        dr = r - r_max
        row0 = n_grid - N_INTERPOLATION
        for k in range(N_INTERPOLATION):
            node[k] = (row0 + k + 1) * grid_dist
        last = node[N_INTERPOLATION - 1]
        _poly_inter_uniform(
            node, table, row0, n_integral, last - DELTA_R, y_low, cc, dd, delta
        )
        _poly_inter_uniform(
            node, table, row0, n_integral, last + DELTA_R, y_high, cc, dd, delta
        )
        for c in range(n_integral):
            y1 = table[n_grid - 1, c]
            y1p = (y_high[c] - y_low[c]) / (2.0 * DELTA_R)
            y1pp = (y_high[c] + y_low[c] - 2.0 * y1) / (DELTA_R * DELTA_R)
            out[c] = poly5_to_zero(y1, y1p, y1pp, dr, -DIST_FUDGE, -1.0 / DIST_FUDGE)


# ---------------------------------------------------------------------------- #
# elementary Slater-Koster blocks (rows = higher l, columns = lower l)          #
# ---------------------------------------------------------------------------- #
@kernel
def _core_ss(core, ll, mm, nn, sk, off):
    """s-s block, 1 x 1."""

    core[0, 0] = sk[off]


@kernel
def _core_sp(core, ll, mm, nn, sk, off):
    """s-p block, 3 x 1; p rows are ordered (y, z, x)."""

    s0 = sk[off]
    core[0, 0] = mm * s0
    core[1, 0] = nn * s0
    core[2, 0] = ll * s0


@kernel
def _core_sd(core, ll, mm, nn, sk, off):
    """s-d block, 5 x 1; d rows are ordered (xy, yz, z2, xz, x2-y2)."""

    s0 = sk[off]
    core[0, 0] = ll * mm * SQRT3 * s0
    core[1, 0] = mm * SQRT3 * nn * s0
    core[2, 0] = (1.5 * nn * nn - 0.5) * s0
    core[3, 0] = ll * SQRT3 * nn * s0
    core[4, 0] = (2.0 * ll * ll - 1.0 + nn * nn) * SQRT3 * s0 / 2.0


@kernel
def _core_pp(core, ll, mm, nn, sk, off):
    """p-p block, 3 x 3, symmetric."""

    s0 = sk[off]
    s1 = sk[off + 1]
    core[0, 0] = (1.0 - nn * nn - ll * ll) * s0 + (nn * nn + ll * ll) * s1
    core[1, 0] = nn * mm * s0 - nn * mm * s1
    core[2, 0] = ll * mm * s0 - ll * mm * s1
    core[0, 1] = core[1, 0]
    core[1, 1] = nn * nn * s0 + (1.0 - nn * nn) * s1
    core[2, 1] = nn * ll * s0 - nn * ll * s1
    core[0, 2] = core[2, 0]
    core[1, 2] = core[2, 1]
    core[2, 2] = ll * ll * s0 + (1.0 - ll * ll) * s1


@kernel
def _core_pd(core, ll, mm, nn, sk, off):
    """p-d block, 5 rows (d) x 3 columns (p)."""

    s0 = sk[off]
    s1 = sk[off + 1]
    l2 = ll * ll
    n2 = nn * nn
    core[0, 0] = -(-1.0 + n2 + l2) * ll * SQRT3 * s0 + (
        (2.0 * n2 + 2.0 * l2 - 1.0) * ll * s1
    )
    core[1, 0] = -(-1.0 + n2 + l2) * SQRT3 * nn * s0 + (
        (2.0 * n2 + 2.0 * l2 - 1.0) * nn * s1
    )
    core[2, 0] = mm * (3.0 * n2 - 1.0) * s0 / 2.0 - SQRT3 * n2 * mm * s1
    core[3, 0] = mm * ll * SQRT3 * nn * s0 - 2.0 * ll * mm * nn * s1
    core[4, 0] = (
        mm * (2.0 * l2 - 1.0 + n2) * SQRT3 * s0 / 2.0 - (n2 + 2.0 * l2) * mm * s1
    )
    core[0, 1] = ll * mm * nn * SQRT3 * s0 - 2.0 * nn * ll * mm * s1
    core[1, 1] = mm * n2 * SQRT3 * s0 - (2.0 * n2 - 1.0) * mm * s1
    core[2, 1] = (nn * (3.0 * n2 - 1.0) * s0) / 2.0 - nn * SQRT3 * (-1.0 + n2) * s1
    core[3, 1] = ll * n2 * SQRT3 * s0 - (2.0 * n2 - 1.0) * ll * s1
    core[4, 1] = (2.0 * l2 - 1.0 + n2) * nn * SQRT3 * s0 / 2.0 - (
        nn * (2.0 * l2 - 1.0 + n2) * s1
    )
    core[0, 2] = l2 * mm * SQRT3 * s0 - (2.0 * l2 - 1.0) * mm * s1
    core[1, 2] = ll * mm * SQRT3 * nn * s0 - 2.0 * mm * ll * nn * s1
    core[2, 2] = (ll * (3.0 * n2 - 1.0) * s0) / 2.0 - SQRT3 * n2 * ll * s1
    core[3, 2] = l2 * SQRT3 * nn * s0 - (2.0 * l2 - 1.0) * nn * s1
    core[4, 2] = ll * (2.0 * l2 - 1.0 + n2) * SQRT3 * s0 / 2.0 - (
        (n2 - 2.0 + 2.0 * l2) * ll * s1
    )


@kernel
def _core_dd(core, ll, mm, nn, sk, off):
    """d-d block, 5 x 5, symmetric."""

    s0 = sk[off]
    s1 = sk[off + 1]
    s2 = sk[off + 2]
    l2 = ll * ll
    n2 = nn * nn
    l4 = l2 * l2
    n4 = n2 * n2
    core[0, 0] = (
        -3.0 * l2 * (-1.0 + n2 + l2) * s0
        + (4.0 * l2 * n2 - n2 + 4.0 * l4 - 4.0 * l2 + 1.0) * s1
        + (-l2 * n2 + n2 + l2 - l4) * s2
    )
    core[1, 0] = (
        -3.0 * ll * (-1.0 + n2 + l2) * nn * s0
        + (4.0 * n2 + 4.0 * l2 - 3.0) * nn * ll * s1
        - ll * (n2 + l2) * nn * s2
    )
    core[2, 0] = (
        ll * mm * SQRT3 * (3.0 * n2 - 1.0) * s0 / 2.0
        - 2.0 * SQRT3 * mm * ll * n2 * s1
        + ll * mm * (n2 + 1.0) * SQRT3 * s2 / 2.0
    )
    core[3, 0] = (
        3.0 * l2 * mm * nn * s0
        - (4.0 * l2 - 1.0) * nn * mm * s1
        + mm * (-1.0 + l2) * nn * s2
    )
    core[4, 0] = (
        1.5 * mm * ll * (2.0 * l2 - 1.0 + n2) * s0
        - 2.0 * mm * ll * (2.0 * l2 - 1.0 + n2) * s1
        + mm * ll * (2.0 * l2 - 1.0 + n2) * s2 / 2.0
    )
    core[0, 1] = core[1, 0]
    core[1, 1] = (
        -3.0 * (-1.0 + n2 + l2) * n2 * s0
        + (4.0 * n4 - 4.0 * n2 + 4.0 * l2 * n2 + 1.0 - l2) * s1
        - (-1.0 + nn) * (n2 * nn + n2 + l2 * nn + l2) * s2
    )
    core[2, 1] = (
        mm * SQRT3 * nn * (3.0 * n2 - 1.0) * s0 / 2.0
        - nn * SQRT3 * (2.0 * n2 - 1.0) * mm * s1
        + (-1.0 + n2) * SQRT3 * nn * mm * s2 / 2.0
    )
    core[3, 1] = (
        3.0 * mm * ll * n2 * s0
        - (4.0 * n2 - 1.0) * mm * ll * s1
        + ll * mm * (-1.0 + n2) * s2
    )
    core[4, 1] = (
        1.5 * mm * (2.0 * l2 - 1.0 + n2) * nn * s0
        - (2.0 * n2 - 1.0 + 4.0 * l2) * nn * mm * s1
        + mm * (n2 + 2.0 * l2 + 1.0) * nn * s2 / 2.0
    )
    core[0, 2] = core[2, 0]
    core[1, 2] = core[2, 1]
    core[2, 2] = (
        ((3.0 * n2 - 1.0) * (3.0 * n2 - 1.0) * s0) / 4.0
        - (3.0 * (-1.0 + n2) * n2 * s1)
        + 0.75 * ((-1.0 + n2) * (-1.0 + n2)) * s2
    )
    core[3, 2] = (
        ll * (3.0 * n2 - 1.0) * SQRT3 * nn * s0 / 2.0
        - (2.0 * n2 - 1.0) * ll * nn * SQRT3 * s1
        + nn * ll * SQRT3 * (-1.0 + n2) * s2 / 2.0
    )
    core[4, 2] = (
        (2.0 * l2 - 1.0 + n2) * (3.0 * n2 - 1.0) * SQRT3 * s0 / 4.0
        - (2.0 * l2 - 1.0 + n2) * n2 * SQRT3 * s1
        + SQRT3 * (2.0 * l2 - 1.0 + n2) * (n2 + 1.0) * s2 / 4.0
    )
    core[0, 3] = core[3, 0]
    core[1, 3] = core[3, 1]
    core[2, 3] = core[3, 2]
    core[3, 3] = (
        3.0 * l2 * n2 * s0
        + (-4.0 * l2 * n2 + n2 + l2) * s1
        + (-1.0 + nn) * (-nn + l2 * nn - 1.0 + l2) * s2
    )
    core[4, 3] = (
        1.5 * ll * (2.0 * l2 - 1.0 + n2) * nn * s0
        - ((2.0 * n2 - 3.0 + 4.0 * l2) * nn * ll * s1)
        + (ll * (n2 - 3.0 + 2.0 * l2) * nn * s2) / 2.0
    )
    core[0, 4] = core[4, 0]
    core[1, 4] = core[4, 1]
    core[2, 4] = core[4, 2]
    core[3, 4] = core[4, 3]
    core[4, 4] = (
        0.75 * ((2.0 * l2 - 1.0 + n2) * (2.0 * l2 - 1.0 + n2)) * s0
        + ((-n4 + n2 - 4.0 * l2 * n2 - 4.0 * l4 + 4.0 * l2) * s1)
        + (n4 / 4.0 + (l2 * n2) + n2 / 2.0 + 0.25 - l2 + l4) * s2
    )


@kernel
def _fill_core(core, l_min, l_max, ll, mm, nn, sk, off):
    """Dispatch to the elementary block of one shell pair."""

    if l_max == 0:
        _core_ss(core, ll, mm, nn, sk, off)
    elif l_max == 1:
        if l_min == 0:
            _core_sp(core, ll, mm, nn, sk, off)
        else:
            _core_pp(core, ll, mm, nn, sk, off)
    else:
        if l_min == 0:
            _core_sd(core, ll, mm, nn, sk, off)
        elif l_min == 1:
            _core_pd(core, ll, mm, nn, sk, off)
        else:
            _core_dd(core, ll, mm, nn, sk, off)


@kernel
def rotate_block(sk, ll, mm, nn, ang_a, n_shell_a, ang_b, n_shell_b, block, core):
    """
    Rotate the radial integrals of one atom pair into a dense diatomic block.

    Parameters
    ----------
    sk : numpy.ndarray of float
        Interpolated integrals of the ordered pair, packed shell pair by shell pair
        (shells of ``a`` outer, shells of ``b`` inner, sigma/pi/delta innermost).
    ll, mm, nn : float
        The x, y and z direction cosines of the unit vector from atom ``a`` to atom
        ``b``, named as in ``dftb/sk.F90``.
    ang_a, ang_b : numpy.ndarray of int
        Angular momenta of the shells of the two species, ascending.
    n_shell_a, n_shell_b : int
        Number of shells of the two species.
    block : numpy.ndarray of float, shape (MAX_ORB, MAX_ORB)
        Receives the block; rows belong to atom ``b``, columns to atom ``a``.
    core : numpy.ndarray of float, shape (5, 5)
        Scratch for the elementary blocks.
    """

    ind = 0
    i_col = 0
    for i1 in range(n_shell_a):
        a1 = ang_a[i1]
        n_orb1 = 2 * a1 + 1
        i_row = 0
        for i2 in range(n_shell_b):
            a2 = ang_b[i2]
            n_orb2 = 2 * a2 + 1
            l_min = a1 if a1 < a2 else a2
            l_max = a2 if a1 < a2 else a1
            _fill_core(core, l_min, l_max, ll, mm, nn, sk, ind)
            if a1 <= a2:
                for p in range(n_orb2):
                    for q in range(n_orb1):
                        block[i_row + p, i_col + q] = core[p, q]
            else:
                # The column atom carries the higher l: transpose the elementary block
                # and apply (-1)^(l1+l2), which is the direction reversal.
                sign = -1.0 if (a1 + a2) % 2 else 1.0
                for p in range(n_orb2):
                    for q in range(n_orb1):
                        block[i_row + p, i_col + q] = sign * core[q, p]
            ind += l_min + 1
            i_row += n_orb2
        i_col += n_orb1


# ---------------------------------------------------------------------------- #
# dense assembly                                                               #
# ---------------------------------------------------------------------------- #
#: Every working array :func:`build_h0_overlap_kernel` needs, so that the kernel itself
#: allocates nothing -- neither numba target allows an allocation inside a device
#: function, and on the CPU this also keeps the per-step cost free of malloc traffic.
#:
#: ``sk_h``, ``sk_s``
#:     Interpolated radial integrals of one pair, ``MAX_INTEGRAL`` long.
#: ``block_h``, ``block_s``
#:     The two rotated ``(MAX_ORB, MAX_ORB)`` diatomic blocks.
#: ``core``
#:     ``(5, 5)`` scratch of the elementary shell-pair blocks, shared by both rotations.
#: ``node``, ``cc``, ``dd``, ``delta``, ``y_low``, ``y_high``
#:     The six interpolation work arrays that :func:`sk_interpolate` takes as its
#:     ``scratch`` tuple, of sizes 8, 8, 8, 7, ``MAX_INTEGRAL`` and ``MAX_INTEGRAL``.
H0Scratch = namedtuple(
    "H0Scratch", "sk_h sk_s block_h block_s core node cc dd delta y_low y_high"
)


@kernel
def h0_overlap_onsite(sk, atom_species, atom_offset, atom, h0, overlap):
    """
    The on-site block of one atom: free-atom shell energies on the diagonal of ``H0``,
    the identity in ``S``. DFTB+ never rotates an atom against itself.
    """

    sp = atom_species[atom]
    offset = atom_offset[atom]
    for i in range(sk.n_orb_species[sp]):
        shell = sk.shell_of_orbital[sp, i]
        h0[offset + i, offset + i] = sk.on_site_energy[sp, shell]
        overlap[offset + i, offset + i] = 1.0


@kernel
def h0_overlap_pair(sk, coords, atom_species, atom_offset, a, b, h0, overlap, s):
    """
    The two-centre blocks of one unordered atom pair ``a < b``, written to both
    triangles of ``H0`` and ``S``.

    Atom ``a`` supplies the columns and atom ``b`` the rows, matching the DFTB+
    neighbour list, which stores ``iAt2 >= iAt1``. Pairs touch disjoint blocks, so
    they may be assembled in any order or in parallel. ``s`` is an :data:`H0Scratch`.
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

    # No cutoff test here: past the table plus its tail sk_interpolate returns
    # all zeros, and the rotation of zeros is the zero block the pair deserves.
    pair = sk.pair_index[sp_a, sp_b]
    n_integral = sk.tab_n_integral[pair]
    sk_interpolate(
        sk.tab_h[pair],
        sk.tab_n_grid[pair],
        sk.tab_grid_dist[pair],
        n_integral,
        dist,
        s.sk_h,
        (s.node, s.cc, s.dd, s.delta, s.y_low, s.y_high),
    )
    sk_interpolate(
        sk.tab_s[pair],
        sk.tab_n_grid[pair],
        sk.tab_grid_dist[pair],
        n_integral,
        dist,
        s.sk_s,
        (s.node, s.cc, s.dd, s.delta, s.y_low, s.y_high),
    )

    rotate_block(
        s.sk_h,
        u_x,
        u_y,
        u_z,
        sk.ang_shell[sp_a],
        sk.n_shell[sp_a],
        sk.ang_shell[sp_b],
        sk.n_shell[sp_b],
        s.block_h,
        s.core,
    )
    rotate_block(
        s.sk_s,
        u_x,
        u_y,
        u_z,
        sk.ang_shell[sp_a],
        sk.n_shell[sp_a],
        sk.ang_shell[sp_b],
        sk.n_shell[sp_b],
        s.block_s,
        s.core,
    )

    row = atom_offset[b]
    col = atom_offset[a]
    for p in range(n_orb_b):
        for q in range(n_orb_a):
            h0[row + p, col + q] = s.block_h[p, q]
            h0[col + q, row + p] = s.block_h[p, q]
            overlap[row + p, col + q] = s.block_s[p, q]
            overlap[col + q, row + p] = s.block_s[p, q]


@kernel
def build_h0_overlap_kernel(
    sk, coords, atom_species, atom_offset, n_atom, h0, overlap, s
):
    """
    Assemble the dense non-SCC Hamiltonian and overlap from flat arrays.

    Parameters
    ----------
    sk : dftb_params.SKTables
        The parameter set as plain arrays, from ``SlaterKosterSet.tables()``.
    coords : numpy.ndarray of float, shape (n_atom, 3)
        Atomic positions in Bohr.
    atom_species : numpy.ndarray of int, shape (n_atom,)
        Species index of every atom.
    atom_offset : numpy.ndarray of int, shape (n_atom + 1,)
        First dense row/column of every atom.
    n_atom : int
        Number of atoms.
    h0, overlap : numpy.ndarray of float, shape (n_orb, n_orb)
        Receive the Hamiltonian in Hartree and the overlap; both are zeroed here, so
        they may come in uninitialised.
    s : H0Scratch
        Fixed-size working arrays; see :data:`H0Scratch`.
    """

    # The pair loop writes only the blocks it touches and the on-site loop only the
    # diagonal, so the two matrices must start clean: the within-atom off-diagonals are
    # never assigned, and neither is anything beyond the tables' reach.
    for i in range(h0.shape[0]):
        for j in range(h0.shape[1]):
            h0[i, j] = 0.0
            overlap[i, j] = 0.0

    for atom in range(n_atom):
        h0_overlap_onsite(sk, atom_species, atom_offset, atom, h0, overlap)

    for a in range(n_atom):
        for b in range(a + 1, n_atom):
            h0_overlap_pair(sk, coords, atom_species, atom_offset, a, b, h0, overlap, s)


#: The one :data:`H0Scratch` of this process, keyed by the sizes that fix its shapes.
#: Ehrenfest dynamics rebuilds H0 and S at every new geometry, so the wrapper below must
#: not allocate this scratch per call.
_SCRATCH_CACHE = {}


def _h0_scratch():
    """Return the shared assembly scratch, allocating it on first use."""

    key = (MAX_INTEGRAL, MAX_ORB, N_INTERPOLATION)
    if key not in _SCRATCH_CACHE:
        _SCRATCH_CACHE[key] = H0Scratch(
            np.zeros(MAX_INTEGRAL),
            np.zeros(MAX_INTEGRAL),
            np.zeros((MAX_ORB, MAX_ORB)),
            np.zeros((MAX_ORB, MAX_ORB)),
            np.zeros((5, 5)),
            np.zeros(N_INTERPOLATION),
            np.zeros(N_INTERPOLATION),
            np.zeros(N_INTERPOLATION),
            np.zeros(N_INTERPOLATION - 1),
            np.zeros(MAX_INTEGRAL),
            np.zeros(MAX_INTEGRAL),
        )
    return _SCRATCH_CACHE[key]


def build_h0_overlap(system):
    """
    Build the dense non-SCC Hamiltonian and overlap of one system.

    Parameters
    ----------
    system : dftb_params.DFTBSystem
        Geometry and basis layout, carrying the parameter set it was built on.

    Returns
    -------
    h0 : numpy.ndarray of float, shape (n_orb, n_orb)
        Non-SCC Hamiltonian in Hartree, real symmetric.
    overlap : numpy.ndarray of float, shape (n_orb, n_orb)
        Overlap matrix, real symmetric with an exact unit diagonal.
    """

    n_orb = system.n_orb
    # the kernel zeroes both matrices itself, so they need not come from np.zeros
    h0 = np.empty((n_orb, n_orb))
    overlap = np.empty((n_orb, n_orb))
    build_h0_overlap_kernel(
        system.sk_set.tables(),
        system.coords,
        system.atom_species,
        system.atom_offset,
        system.n_atom,
        h0,
        overlap,
        _h0_scratch(),
    )
    return h0, overlap
