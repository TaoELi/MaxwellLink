# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# --------------------------------------------------------------------------------------#

"""
Slater-Koster integrals with their derivatives, for the forces.

A force needs ``dH0/dR`` and ``dS/dR`` as well as the matrices themselves. DFTB+ gets
those from a separate interpolation routine whose tail differs slightly from the
value-only one, so the derivative path is written out here in full rather than obtained
by differencing :mod:`h0_overlap`.

:func:`sk_interpolate_deriv` returns value and ``d/dr`` of one pair's radial table, and
:func:`rotate_block_du` the derivatives of the rotated block with respect to the
direction cosines; :func:`block_derivatives` combines them into the Cartesian gradient
of one diatomic block.
"""

import math

import numpy as np

try:  # inside the package
    from .kernels_dftb import kernel
    from .dftb_params import DIST_FUDGE, N_INTERPOLATION, N_RIGHT
    from .h0_overlap import SQRT3, poly5_to_zero, rotate_block
except (ImportError, ValueError):  # allow running as a stand-alone script
    from kernels_dftb import kernel
    from dftb_params import DIST_FUDGE, N_INTERPOLATION, N_RIGHT
    from h0_overlap import SQRT3, poly5_to_zero, rotate_block

# 1 / prod_{j != k} (k - j) for the eight integer nodes 0 ... 7.
INV_DENOM = np.array(
    [
        -1.0 / 5040.0,
        1.0 / 720.0,
        -1.0 / 240.0,
        1.0 / 144.0,
        -1.0 / 144.0,
        1.0 / 240.0,
        -1.0 / 720.0,
        1.0 / 5040.0,
    ]
)


# ---------------------------------------------------------------------------- #
# radial interpolation with derivatives                                        #
# ---------------------------------------------------------------------------- #
@kernel
def _lagrange_weights(t, weight, first, second):
    """
    Degree-7 Lagrange weights and their first two derivatives at ``t``.

    The nodes are the integers 0 ... 7, so ``t`` is the query point measured in grid
    spacings from the first stencil node. Weight ``k`` is the leave-one-out product
    ``prod_{j != k} (t - j)`` divided by the same product evaluated at ``t = k``; the
    derivatives are those of the numerator, accumulated alongside it.
    """

    for k in range(N_INTERPOLATION):
        product = 1.0
        d_product = 0.0
        dd_product = 0.0
        for j in range(N_INTERPOLATION):
            if j == k:
                continue
            factor = t - j
            dd_product = dd_product * factor + 2.0 * d_product
            d_product = d_product * factor + product
            product = product * factor
        weight[k] = product * INV_DENOM[k]
        first[k] = d_product * INV_DENOM[k]
        second[k] = dd_product * INV_DENOM[k]


@kernel
def _poly5_to_zero_deriv(y0, y0p, y0pp, xx, dx, invdx):
    """d/dxx of :func:`h0_overlap.poly5_to_zero`, slakoeqgrid.F90:573-606."""

    dx1 = y0p * dx
    dx2 = y0pp * dx * dx
    d = 10.0 * y0 - 4.0 * dx1 + 0.5 * dx2
    e = -15.0 * y0 + 7.0 * dx1 - 1.0 * dx2
    f = 6.0 * y0 - 3.0 * dx1 + 0.5 * dx2
    xr = xx * invdx
    return (
        5.0 * f * xr * xr * xr * xr + 4.0 * e * xr * xr * xr + 3.0 * d * xr * xr
    ) * (invdx)


@kernel
def sk_interpolate_deriv(table, n_grid, grid_dist, n_integral, r, out, dout, scratch):
    """
    Interpolate one pair's radial table at ``r`` and return ``d/dr`` as well.

    Parameters
    ----------
    table : numpy.ndarray of float, shape (>= n_grid, >= n_integral)
        Radial table of the ordered species pair; row ``k`` sits at
        ``r = (k + 1) * grid_dist``.
    n_grid : int
        Number of valid rows.
    grid_dist : float
        Grid spacing in Bohr.
    n_integral : int
        Number of valid columns.
    r : float
        Interatomic distance in Bohr.
    out, dout : numpy.ndarray of float, shape (>= n_integral,)
        Receive the integrals and their radial derivatives; zeroed first.
    scratch : tuple of numpy.ndarray
        ``(weight, first, second)`` working arrays of length 8.
    """

    weight, first, second = scratch
    for c in range(n_integral):
        out[c] = 0.0
        dout[c] = 0.0

    r_max = n_grid * grid_dist + DIST_FUDGE
    if r >= r_max:  # beyond the table plus its tail there is no interaction at all
        return

    ind = int(math.floor(r / grid_dist))
    if ind < n_grid:
        # Inside the tabulated range: value and slope of the same degree-7 polynomial.
        # ternaries, not min()/max(): the two-argument builtins compile under njit
        # but not under cuda.jit, which would break only the GPU build
        i_last = ind + N_RIGHT
        if i_last > n_grid:
            i_last = n_grid
        if i_last < N_INTERPOLATION:
            i_last = N_INTERPOLATION
        row0 = i_last - N_INTERPOLATION
        t = r / grid_dist - (row0 + 1)
        _lagrange_weights(t, weight, first, second)
        for c in range(n_integral):
            value = 0.0
            slope = 0.0
            for k in range(N_INTERPOLATION):
                entry = table[row0 + k, c]
                value += weight[k] * entry
                slope += first[k] * entry
            out[c] = value
            dout[c] = slope / grid_dist
    else:
        # Last Bohr: a quintic carrying the table smoothly to zero, anchored on the
        # analytic value, slope and curvature of the polynomial at the last grid point.
        dr = r - r_max
        row0 = n_grid - N_INTERPOLATION
        _lagrange_weights(float(N_INTERPOLATION - 1), weight, first, second)
        for c in range(n_integral):
            value = 0.0
            slope = 0.0
            curvature = 0.0
            for k in range(N_INTERPOLATION):
                entry = table[row0 + k, c]
                value += weight[k] * entry
                slope += first[k] * entry
                curvature += second[k] * entry
            slope /= grid_dist
            curvature /= grid_dist * grid_dist
            out[c] = poly5_to_zero(
                value, slope, curvature, dr, -DIST_FUDGE, -1.0 / DIST_FUDGE
            )
            dout[c] = _poly5_to_zero_deriv(
                value, slope, curvature, dr, -DIST_FUDGE, -1.0 / DIST_FUDGE
            )


# ---------------------------------------------------------------------------- #
# direction-cosine derivatives of the elementary blocks                        #
# ---------------------------------------------------------------------------- #
# The bodies below are the output of gen_rotation_derivs.py, which differentiates the
# Gate A expressions in h0_overlap.py symbolically; dcore[k] is d(core)/d(ll, mm, nn).
@kernel
def _core_ss_deriv(dcore, ll, mm, nn, sk, off):
    """Direction-cosine derivatives of the ss block, all zero."""

    for k in range(3):
        dcore[k, 0, 0] = 0.0


@kernel
def _core_sp_deriv(dcore, ll, mm, nn, sk, off):
    """Direction-cosine derivatives of the sp block."""

    s0 = sk[off]
    for k in range(3):
        for p in range(3):
            for q in range(1):
                dcore[k, p, q] = 0.0
    dcore[0, 2, 0] = s0
    dcore[1, 0, 0] = s0
    dcore[2, 1, 0] = s0


@kernel
def _core_sd_deriv(dcore, ll, mm, nn, sk, off):
    """Direction-cosine derivatives of the sd block."""

    s0 = sk[off]
    for k in range(3):
        for p in range(5):
            for q in range(1):
                dcore[k, p, q] = 0.0
    x0 = SQRT3 * s0
    x1 = mm * x0
    x2 = nn * x0
    x3 = ll * x0
    dcore[0, 0, 0] = x1
    dcore[0, 3, 0] = x2
    dcore[0, 4, 0] = 2.0 * x3
    dcore[1, 0, 0] = x3
    dcore[1, 1, 0] = x2
    dcore[2, 1, 0] = x1
    dcore[2, 2, 0] = 3.0 * nn * s0
    dcore[2, 3, 0] = x3
    dcore[2, 4, 0] = 1.0 * x2


@kernel
def _core_pp_deriv(dcore, ll, mm, nn, sk, off):
    """Direction-cosine derivatives of the pp block."""

    s0 = sk[off]
    s1 = sk[off + 1]
    for k in range(3):
        for p in range(3):
            for q in range(3):
                dcore[k, p, q] = 0.0
    x0 = s0 - s1
    x1 = ll * x0
    x2 = 2 * x1
    x3 = mm * x0
    x4 = nn * x0
    dcore[0, 0, 0] = -x2
    dcore[0, 0, 2] = x3
    dcore[0, 1, 2] = x4
    dcore[0, 2, 0] = x3
    dcore[0, 2, 1] = x4
    dcore[0, 2, 2] = x2
    dcore[1, 0, 1] = x4
    dcore[1, 0, 2] = x1
    dcore[1, 1, 0] = x4
    dcore[1, 2, 0] = x1
    dcore[2, 0, 0] = -2 * nn * x0
    dcore[2, 0, 1] = x3
    dcore[2, 1, 0] = x3
    dcore[2, 1, 1] = 2 * x4
    dcore[2, 1, 2] = x1
    dcore[2, 2, 1] = x1


@kernel
def _core_pd_deriv(dcore, ll, mm, nn, sk, off):
    """Direction-cosine derivatives of the pd block."""

    s0 = sk[off]
    s1 = sk[off + 1]
    for k in range(3):
        for p in range(5):
            for q in range(3):
                dcore[k, p, q] = 0.0
    x0 = nn**2
    x1 = x0 - 1
    x2 = SQRT3 * s0
    x3 = 1.0 * x2
    x4 = 2.0 * x0
    x5 = ll**2
    x6 = x5 * (-6.0 * s1 + 3.0 * x2)
    x7 = 2.0 * s1
    x8 = -x7
    x9 = mm * nn
    x10 = x9 * (x2 + x8)
    x11 = -4.0 * s1
    x12 = ll * (x11 + 2.0 * x2)
    x13 = mm * x12
    x14 = nn * x12
    x15 = -x14
    x16 = 0.5 * s0
    x17 = SQRT3 * s1 * x0
    x18 = 1.5 * s0 * x0 - x16 - x17
    x19 = 1.0 * s1
    x20 = -s1 * x4 + x0 * x2 + x19
    x21 = 0.5 * x1 * x2
    x22 = x3 + x8
    x23 = ll * x22
    x24 = nn * x23
    x25 = x22 * x5
    x26 = x19 + x25
    x27 = mm * x23
    x28 = 3.0 * x0 - 1.0
    x29 = -3.0 * s0
    x30 = 2 * s1
    dcore[0, 0, 0] = s1 * (x4 - 1.0) - x1 * x3 - x6
    dcore[0, 0, 1] = x10
    dcore[0, 0, 2] = x13
    dcore[0, 1, 0] = x15
    dcore[0, 1, 2] = x10
    dcore[0, 2, 2] = x18
    dcore[0, 3, 0] = x10
    dcore[0, 3, 1] = x20
    dcore[0, 3, 2] = x14
    dcore[0, 4, 0] = x13
    dcore[0, 4, 1] = x14
    dcore[0, 4, 2] = -s1 * (1.0 * x0 - 2.0) + x21 + x6
    dcore[1, 0, 1] = x24
    dcore[1, 0, 2] = x26
    dcore[1, 1, 1] = x20
    dcore[1, 1, 2] = x24
    dcore[1, 2, 0] = x18
    dcore[1, 3, 0] = x24
    dcore[1, 4, 0] = -x0 * x19 + x21 + x25
    dcore[2, 0, 0] = x15
    dcore[2, 0, 1] = x27
    dcore[2, 1, 0] = s1 * (6.0 * x0 - 1.0) - x2 * x28 - x25
    dcore[2, 1, 1] = x9 * (x11 + 2 * x2)
    dcore[2, 1, 2] = x27
    dcore[2, 2, 0] = x9 * (-SQRT3 * x30 - x29)
    dcore[2, 2, 1] = 1.0 * SQRT3 * s1 + 4.5 * s0 * x0 - x16 - 3 * x17
    dcore[2, 2, 2] = -ll * nn * (SQRT3 * x7 + x29)
    dcore[2, 3, 0] = x27
    dcore[2, 3, 1] = x14
    dcore[2, 3, 2] = x26
    dcore[2, 4, 0] = x9 * (x3 - x30)
    dcore[2, 4, 1] = -s1 * x28 + x2 * (1.5 * x0 - 0.5) + x25
    dcore[2, 4, 2] = x24


@kernel
def _core_dd_deriv(dcore, ll, mm, nn, sk, off):
    """Direction-cosine derivatives of the dd block."""

    s0 = sk[off]
    s1 = sk[off + 1]
    s2 = sk[off + 2]
    for k in range(3):
        for p in range(5):
            for q in range(5):
                dcore[k, p, q] = 0.0
    x0 = nn**2
    x1 = x0 - 1
    x2 = 6.0 * s0
    x3 = 8.0 * s1
    x4 = 2.0 * s2
    x5 = x1 * x4
    x6 = ll**2
    x7 = ll * (x1 * x2 - x1 * x3 + x5 + x6 * (12.0 * s0 - 16.0 * s1 + 4.0 * s2))
    x8 = 1.0 * s2
    x9 = x0 * x8
    x10 = 3.0 * s0
    x11 = x1 * x10
    x12 = 4.0 * x0
    x13 = 9.0 * s0
    x14 = 12.0 * s1
    x15 = 3.0 * s2
    x16 = x6 * (x13 - x14 + x15)
    x17 = nn * (s1 * (x12 - 3.0) - x11 - x16 - x9)
    x18 = 0.5 * s2
    x19 = 2.0 * x0
    x20 = -s1 * x19
    x21 = -0.5 * s0
    x22 = 1.5 * x0
    x23 = s0 * x22 + x0 * x18 + x20 + x21
    x24 = SQRT3 * mm
    x25 = x24 * (x18 + x23)
    x26 = x2 - x3 + x4
    x27 = ll * nn
    x28 = mm * x27
    x29 = x26 * x28
    x30 = 1.5 * x1
    x31 = s0 * x30
    x32 = x16 + x31
    x33 = 2.0 * s1
    x34 = x1 * x18 - x1 * x33
    x35 = mm * (x32 + x34)
    x36 = ll * (-s1 * (8.0 * x0 - 2.0) + x0 * x2 + x5)
    x37 = -s1 * x12
    x38 = 3.0 * x0
    x39 = s0 * x38
    x40 = s2 * x0
    x41 = 1.0 * s1
    x42 = x41 - x8
    x43 = mm * (x37 + x39 + x40 + x42)
    x44 = -x18 + x41
    x45 = SQRT3 * nn
    x46 = x45 * (x23 + x44)
    x47 = x0 + 1
    x48 = x47 * x8
    x49 = SQRT3 * ll
    x50 = x49 * (s0 * (x38 - 1.0) + x37 + x48)
    x51 = nn * (-s1 * (x19 - 3.0) + s2 * (0.5 * x0 - 1.5) + x32)
    x52 = x18 * x47
    x53 = x22 - 0.5
    x54 = x49 * (s0 * x53 + x20 + x52)
    x55 = -4.0 * s1 + x10 + x8
    x56 = x55 * x6
    x57 = x42 + x56
    x58 = nn * x57
    x59 = x31 + x56
    x60 = ll * (x34 + x59)
    x61 = ll * (-s1 * (x12 - 1.0) + x1 * x8 + x39)
    x62 = nn * (-s1 * (x19 - 1.0) + x52 + x59)
    x63 = x26 * x6
    x64 = -2.0 * s2 + x33 + x63
    x65 = x0 * x15
    x66 = 12.0 * x0
    x67 = -ll * (s0 * (9.0 * x0 - 3.0) - s1 * (x66 - 3.0) + x56 + x65)
    x68 = x24 * x27 * x55
    x69 = mm * x57
    x70 = x28 * x55
    x71 = x12 - 2.0
    x72 = 4.5 * x0
    x73 = 6.0 * x0
    x74 = x24 * (s0 * x72 - s1 * x73 + x21 + 1.5 * x40 + x44)
    x75 = -s1 * (x73 - 1.0)
    x76 = s0 * (x72 - 1.5) + x56
    x77 = mm * (s2 * (x22 + 0.5) + x75 + x76)
    x78 = x49 * (s0 * (x72 - 0.5) + s2 * x53 + x75)
    x79 = -s1 * x71
    x80 = x45 * (s0 * (x38 - 2.0) + x56 + x79 + x9)
    x81 = ll * (-s1 * (x73 - 3.0) + s2 * x30 + x76)
    dcore[0, 0, 0] = -x7
    dcore[0, 0, 1] = x17
    dcore[0, 0, 2] = x25
    dcore[0, 0, 3] = x29
    dcore[0, 0, 4] = x35
    dcore[0, 1, 0] = x17
    dcore[0, 1, 1] = -x36
    dcore[0, 1, 3] = x43
    dcore[0, 1, 4] = x29
    dcore[0, 2, 0] = x25
    dcore[0, 2, 3] = x46
    dcore[0, 2, 4] = x50
    dcore[0, 3, 0] = x29
    dcore[0, 3, 1] = x43
    dcore[0, 3, 2] = x46
    dcore[0, 3, 3] = x36
    dcore[0, 3, 4] = x51
    dcore[0, 4, 0] = x35
    dcore[0, 4, 1] = x29
    dcore[0, 4, 2] = x50
    dcore[0, 4, 3] = x51
    dcore[0, 4, 4] = x7
    dcore[1, 0, 2] = x54
    dcore[1, 0, 3] = x58
    dcore[1, 0, 4] = x60
    dcore[1, 1, 2] = x46
    dcore[1, 1, 3] = x61
    dcore[1, 1, 4] = x62
    dcore[1, 2, 0] = x54
    dcore[1, 2, 1] = x46
    dcore[1, 3, 0] = x58
    dcore[1, 3, 1] = x61
    dcore[1, 4, 0] = x60
    dcore[1, 4, 1] = x62
    dcore[2, 0, 0] = -nn * x64
    dcore[2, 0, 1] = x67
    dcore[2, 0, 2] = x68
    dcore[2, 0, 3] = x69
    dcore[2, 0, 4] = x70
    dcore[2, 1, 0] = x67
    dcore[2, 1, 1] = nn * (-s0 * (x66 - 6.0) + s1 * (16.0 * x0 - 8.0) - s2 * x71 - x63)
    dcore[2, 1, 2] = x74
    dcore[2, 1, 3] = x29
    dcore[2, 1, 4] = x77
    dcore[2, 2, 0] = x68
    dcore[2, 2, 1] = x74
    dcore[2, 2, 2] = nn * (6.0 * s1 + x0 * x13 - x0 * x14 - x10 - x15 + x65)
    dcore[2, 2, 3] = x78
    dcore[2, 2, 4] = x80
    dcore[2, 3, 0] = x69
    dcore[2, 3, 1] = x29
    dcore[2, 3, 2] = x78
    dcore[2, 3, 3] = nn * x64
    dcore[2, 3, 4] = x81
    dcore[2, 4, 0] = x70
    dcore[2, 4, 1] = x77
    dcore[2, 4, 2] = x80
    dcore[2, 4, 3] = x81
    dcore[2, 4, 4] = nn * (x11 + x48 + x63 + x79)


@kernel
def _fill_core_deriv(dcore, l_min, l_max, ll, mm, nn, sk, off):
    """Dispatch to the direction-cosine derivatives of one shell pair."""

    if l_max == 0:
        _core_ss_deriv(dcore, ll, mm, nn, sk, off)
    elif l_max == 1:
        if l_min == 0:
            _core_sp_deriv(dcore, ll, mm, nn, sk, off)
        else:
            _core_pp_deriv(dcore, ll, mm, nn, sk, off)
    else:
        if l_min == 0:
            _core_sd_deriv(dcore, ll, mm, nn, sk, off)
        elif l_min == 1:
            _core_pd_deriv(dcore, ll, mm, nn, sk, off)
        else:
            _core_dd_deriv(dcore, ll, mm, nn, sk, off)


@kernel
def rotate_block_du(sk, ll, mm, nn, ang_a, n_shell_a, ang_b, n_shell_b, blocks, dcore):
    """
    Derivatives of one diatomic block with respect to the three direction cosines.

    Mirrors :func:`h0_overlap.rotate_block` exactly, including the transpose and the
    ``(-1)^(l1 + l2)`` sign that handle a column atom of higher angular momentum.

    Parameters
    ----------
    sk : numpy.ndarray of float
        Interpolated integrals of the ordered pair.
    ll, mm, nn : float
        Direction cosines of the unit vector from the column atom to the row atom.
    ang_a, ang_b : numpy.ndarray of int
        Angular momenta of the shells of the column and row species.
    n_shell_a, n_shell_b : int
        Number of shells of the two species.
    blocks : numpy.ndarray of float, shape (3, MAX_ORB, MAX_ORB)
        Receives ``d block / d(ll, mm, nn)``.
    dcore : numpy.ndarray of float, shape (3, 5, 5)
        Scratch for the elementary derivative blocks.
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
            _fill_core_deriv(dcore, l_min, l_max, ll, mm, nn, sk, ind)
            if a1 <= a2:
                for k in range(3):
                    for p in range(n_orb2):
                        for q in range(n_orb1):
                            blocks[k, i_row + p, i_col + q] = dcore[k, p, q]
            else:
                sign = -1.0 if (a1 + a2) % 2 else 1.0
                for k in range(3):
                    for p in range(n_orb2):
                        for q in range(n_orb1):
                            blocks[k, i_row + p, i_col + q] = sign * dcore[k, q, p]
            ind += l_min + 1
            i_row += n_orb2
        i_col += n_orb1


@kernel
def block_derivatives(
    sk, dskdr, ll, mm, nn, dist, ang_a, n_shell_a, ang_b, n_shell_b, dblock, scratch
):
    """
    Cartesian derivative of one diatomic block with respect to the *column* atom.

    ``dblock[k]`` is ``d block / d R_column,k`` with the block laid out rows on the row
    atom and columns on the column atom, which is the convention DFTB+'s force routine
    uses (``nonscc.F90:492-506``).

    Parameters
    ----------
    sk, dskdr : numpy.ndarray of float
        Interpolated integrals of the ordered pair and their radial derivatives.
    ll, mm, nn : float
        Direction cosines of the unit vector from the column atom to the row atom.
    dist : float
        Interatomic distance in Bohr.
    ang_a, ang_b, n_shell_a, n_shell_b
        Shell layout of the column and row species, as in :func:`rotate_block_du`.
    dblock : numpy.ndarray of float, shape (3, MAX_ORB, MAX_ORB)
        Receives the three Cartesian derivative blocks.
    scratch : tuple of numpy.ndarray
        ``(radial, angular, core, dcore)`` working arrays shaped
        ``(MAX_ORB, MAX_ORB)``, ``(3, MAX_ORB, MAX_ORB)``, ``(5, 5)`` and ``(3, 5, 5)``.
    """

    radial, angular, core, dcore = scratch
    n_orb_a = 0
    for i in range(n_shell_a):
        n_orb_a += 2 * ang_a[i] + 1
    n_orb_b = 0
    for i in range(n_shell_b):
        n_orb_b += 2 * ang_b[i] + 1

    # The radial part: the same rotation applied to d(integral)/dr.
    rotate_block(dskdr, ll, mm, nn, ang_a, n_shell_a, ang_b, n_shell_b, radial, core)
    # The angular part: d(block)/d(direction cosine).
    rotate_block_du(sk, ll, mm, nn, ang_a, n_shell_a, ang_b, n_shell_b, angular, dcore)

    u = (ll, mm, nn)
    for k in range(3):
        for p in range(n_orb_b):
            for q in range(n_orb_a):
                total = radial[p, q] * u[k]
                for j in range(3):
                    projection = -u[j] * u[k] / dist
                    if j == k:
                        projection += 1.0 / dist
                    total += angular[j, p, q] * projection
                # v = R_row - R_col, so moving the column atom reverses the sign.
                dblock[k, p, q] = -total
