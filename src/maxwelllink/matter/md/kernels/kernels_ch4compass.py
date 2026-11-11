# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Compiled ch4compass forces for the CPU and the GPU.

Note: the CPU runs in float64 throughout, while the GPU runs the bonded term in float64
and the non-bonded sums in float32.
"""

import math

import numpy as np

from ...helpers import THREADS_PER_BLOCK
from ..force_fields import ch4compass as ff_module

# atoms per molecule, in the geometry order [C, H, H, H, H]; the carbon is the
# central atom
NPM = 5

# --------------------------------------------------------------------------- #
# Scalar parameter slots. Packing them into one array keeps the kernel         #
# signatures short and lets the GPU hold them at the working precision.        #
# --------------------------------------------------------------------------- #
P_BOXX = 0  # periodic box lengths (Bohr)
P_BOXY = 1
P_BOXZ = 2
P_PERIODIC = 3  # 1.0 for a periodic box, 0.0 for a finite cluster
P_INTER = 4  # 1.0 when the system has more than one molecule
P_LJCUT2 = 5  # squared Lennard-Jones cutoff (periodic only)
P_EWA = 6  # Ewald screening parameter alpha
P_WRCUT2 = 7  # squared Ewald real-space cutoff
P_TWOAPI = 8  # 2 alpha / sqrt(pi)
P_ESELF = 9  # Ewald self-energy (a geometry-independent constant)
P_R0 = 10  # equilibrium bond length
P_K2B = 11  # class2 bond constants K2, K3, K4
P_K3B = 12
P_K4B = 13
P_THETA0 = 14  # equilibrium angle
P_K2A = 15  # class2 angle constants K2, K3, K4
P_K3A = 16
P_K4A = 17
P_MBB = 18  # bond-bond coupling constant M and its reference length
P_RBB = 19
P_NBA = 20  # bond-angle coupling constant N and its reference length
P_RBA = 21
P_SINFLOOR = 22  # floor on sin(theta) in the angle gradient
N_PARAMS = 23

# Abramowitz-Stegun 7.1.26 erfc coefficients. The reference force field uses this
# polynomial rather than the true erfc, so the kernels must use it too.
_ERFC_P = 3.0525860
_ERFC_A1 = 0.254829592
_ERFC_A2 = -0.284496736
_ERFC_A3 = 1.421413741
_ERFC_A4 = -1.453152027
_ERFC_A5 = 1.061405429


def _build_terms(ft, jit, sqrt_, exp_, acos_, rint_, sincos_):
    """
    Build the ch4compass force terms for one target and one precision.

    Parameters
    ----------
    ft : numba scalar type
        ``numba.float32`` or ``numba.float64``; types every literal.
    jit : callable
        ``numba.njit`` or ``numba.cuda.jit(device=True)``.
    sqrt_, exp_, acos_, rint_, sincos_ : callable
        Math functions for this target and precision (``math.*`` on the CPU,
        ``numba.cuda.libdevice.*`` on the GPU).

    Returns
    -------
    dict
        The compiled functions, keyed by name.
    """

    @jit
    def erfc_body(x, gauss):
        """
        Compute erfc(x) by Abramowitz-Stegun 7.1.26, given ``gauss = exp(-x*x)``.
        """

        t = ft(_ERFC_P) / (ft(_ERFC_P) + x)
        poly = ft(_ERFC_A1) + t * (
            ft(_ERFC_A2) + t * (ft(_ERFC_A3) + t * (ft(_ERFC_A4) + t * ft(_ERFC_A5)))
        )
        return gauss * t * poly

    @jit
    def min_image(d, box, periodic):
        """Wrap one displacement component into the primary cell."""

        if periodic > ft(0.0):
            return d - box * rint_(d / box)
        return d

    @jit
    def pair_forces(i, x, qa, lj_a, lj_b, lj_c, par):
        """9-6 Lennard-Jones and real-space Coulomb force on atom ``i``.

        The charge sites are the atoms, so both sums use the same displacement and
        both forces act on the same atom. Bonded pairs are dropped from Lennard-Jones
        and carry ``erfc(alpha r) - 1`` in the Ewald real space, which cancels the
        bare Coulomb that the reciprocal sum includes for them.

        Returns
        -------
        tuple
            ``(fx, fy, fz, e_lj, e_coul)``. The energies are double-counted row sums;
            the caller halves them.
        """

        na = x.shape[0]
        mi = i // NPM
        li = i - NPM * mi
        periodic = par[P_PERIODIC]
        boxx = par[P_BOXX]
        boxy = par[P_BOXY]
        boxz = par[P_BOXZ]
        ljcut2 = par[P_LJCUT2]
        wrcut2 = par[P_WRCUT2]
        ewa = par[P_EWA]
        twoapi = par[P_TWOAPI]
        q_i = qa[li]

        fx = ft(0.0)
        fy = ft(0.0)
        fz = ft(0.0)
        e_lj = ft(0.0)
        e_co = ft(0.0)

        for j in range(na):
            if j == i:
                continue
            mj = j // NPM
            same_mol = mj == mi
            lj_ = j - NPM * mj

            dx = min_image(x[i, 0] - x[j, 0], boxx, periodic)
            dy = min_image(x[i, 1] - x[j, 1], boxy, periodic)
            dz = min_image(x[i, 2] - x[j, 2], boxz, periodic)
            r2 = dx * dx + dy * dy + dz * dz
            inv_r2 = ft(1.0) / r2
            inv_r = sqrt_(inv_r2)

            # ---------------- 9-6 Lennard-Jones ----------------
            # lj_a = eps_ij, lj_b = r0_ij^3, lj_c = the cutoff shift; the pair tables
            # are precomputed so the innermost loop only reads them
            if not same_mol and (periodic <= ft(0.0) or r2 < ljcut2):
                a_ij = lj_a[li, lj_]
                sr3 = lj_b[li, lj_] * inv_r2 * inv_r  # (r0 / r)^3
                sr6 = sr3 * sr3
                sr9 = sr6 * sr3
                e_lj += a_ij * (ft(2.0) * sr9 - ft(3.0) * sr6 - lj_c[li, lj_])
                coeff = ft(18.0) * a_ij * (sr9 - sr6) * inv_r2
                fx += coeff * dx
                fy += coeff * dy
                fz += coeff * dz

            # ---------------- Coulomb ----------------
            qq = q_i * qa[lj_]
            if periodic > ft(0.0):
                if r2 < wrcut2:
                    r = r2 * inv_r
                    # erfc(alpha r) and the force's gaussian share one exponential
                    ar = ewa * r
                    gauss = exp_(-ar * ar)
                    g = erfc_body(ar, gauss)
                    if same_mol:  # exclusion correction for a bonded pair
                        g = g - ft(1.0)
                    gr = g * inv_r
                    e_co += qq * gr
                    coeff = qq * (gr + twoapi * gauss) * inv_r2
                    fx += coeff * dx
                    fy += coeff * dy
                    fz += coeff * dz
            elif not same_mol:
                e_co += qq * inv_r
                coeff = qq * inv_r * inv_r2
                fx += coeff * dx
                fy += coeff * dy
                fz += coeff * dz

        return (fx, fy, fz, e_lj, e_co)

    @jit
    def structure_factor(k, x, qa, kvec):
        """Real and imaginary parts of ``S(k) = sum_j q_j exp(i k . r_j)``."""

        na = x.shape[0]
        kx = kvec[k, 0]
        ky = kvec[k, 1]
        kz = kvec[k, 2]
        s_re = ft(0.0)
        s_im = ft(0.0)
        for j in range(na):
            phase = kx * x[j, 0] + ky * x[j, 1] + kz * x[j, 2]
            qj = qa[j - NPM * (j // NPM)]
            sn, cs = sincos_(phase)
            s_re += qj * cs
            s_im += qj * sn
        return (s_re, s_im)

    @jit
    def recip_force(i, x, qa, kvec, ak, s_re, s_im):
        """Reciprocal-space Coulomb force on atom ``i``."""

        nk = kvec.shape[0]
        xi = x[i, 0]
        yi = x[i, 1]
        zi = x[i, 2]
        fx = ft(0.0)
        fy = ft(0.0)
        fz = ft(0.0)
        for k in range(nk):
            kx = kvec[k, 0]
            ky = kvec[k, 1]
            kz = kvec[k, 2]
            phase = kx * xi + ky * yi + kz * zi
            sn, cs = sincos_(phase)
            w = ak[k] * (s_im[k] * cs - s_re[k] * sn)
            fx += w * kx
            fy += w * ky
            fz += w * kz
        s = ft(-2.0) * qa[i - NPM * (i // NPM)]
        return (s * fx, s * fy, s * fz)

    @jit
    def bond_terms(i1, ic, x, par):
        """Class2 bond between the outer atom ``i1`` and the central atom ``ic``:
        ``V = K2 dr^2 + K3 dr^3 + K4 dr^4`` with ``dr = r - r0``.

        Returns
        -------
        tuple
            The energy gradient ``(gx, gy, gz)`` with respect to the outer atom (the
            central atom takes the opposite), then the bond energy.
        """

        dx = x[i1, 0] - x[ic, 0]
        dy = x[i1, 1] - x[ic, 1]
        dz = x[i1, 2] - x[ic, 2]
        r = sqrt_(dx * dx + dy * dy + dz * dz)
        dr = r - par[P_R0]
        dr2 = dr * dr
        v = par[P_K2B] * dr2 + par[P_K3B] * dr2 * dr + par[P_K4B] * dr2 * dr2
        dvdr = (
            ft(2.0) * par[P_K2B] * dr
            + ft(3.0) * par[P_K3B] * dr2
            + ft(4.0) * par[P_K4B] * dr2 * dr
        )
        g = dvdr / r
        return (g * dx, g * dy, g * dz, v)

    @jit
    def angle_terms(i1, ic, i3, x, par):
        """Class2 angle ``i1 - ic - i3`` with its bond-bond and bond-angle couplings:
        ``V = K2 dth^2 + K3 dth^3 + K4 dth^4 + M (r1 - r_bb)(r2 - r_bb)
        + N [(r1 - r_ba) + (r2 - r_ba)] dth`` with ``dth = theta - theta0``.

        Returns
        -------
        tuple
            The energy gradients ``(g1x, g1y, g1z)`` and ``(g3x, g3y, g3z)`` with
            respect to the two outer atoms (the central atom takes the opposite of
            their sum), then the angle energy.
        """

        d1x = x[i1, 0] - x[ic, 0]
        d1y = x[i1, 1] - x[ic, 1]
        d1z = x[i1, 2] - x[ic, 2]
        d2x = x[i3, 0] - x[ic, 0]
        d2y = x[i3, 1] - x[ic, 1]
        d2z = x[i3, 2] - x[ic, 2]
        r1sq = d1x * d1x + d1y * d1y + d1z * d1z
        r2sq = d2x * d2x + d2y * d2y + d2z * d2z
        r1 = sqrt_(r1sq)
        r2 = sqrt_(r2sq)
        inv_r1r2 = ft(1.0) / (r1 * r2)

        # the angle from the dot product, with cos(theta) clamped to [-1, 1]
        c = (d1x * d2x + d1y * d2y + d1z * d2z) * inv_r1r2
        if c > ft(1.0):
            c = ft(1.0)
        elif c < ft(-1.0):
            c = ft(-1.0)
        s = sqrt_(ft(1.0) - c * c)
        if s < par[P_SINFLOOR]:
            s = par[P_SINFLOOR]
        inv_s = ft(1.0) / s
        dth = acos_(c) - par[P_THETA0]
        dth2 = dth * dth

        # energy and its partial derivatives with respect to theta, r1 and r2
        b1 = r1 - par[P_RBB]
        b2 = r2 - par[P_RBB]
        ba = (r1 - par[P_RBA]) + (r2 - par[P_RBA])
        v = (
            par[P_K2A] * dth2
            + par[P_K3A] * dth2 * dth
            + par[P_K4A] * dth2 * dth2
            + par[P_MBB] * b1 * b2
            + par[P_NBA] * ba * dth
        )
        dv_dth = (
            ft(2.0) * par[P_K2A] * dth
            + ft(3.0) * par[P_K3A] * dth2
            + ft(4.0) * par[P_K4A] * dth2 * dth
            + par[P_NBA] * ba
        )
        dv_dr1 = par[P_MBB] * b2 + par[P_NBA] * dth
        dv_dr2 = par[P_MBB] * b1 + par[P_NBA] * dth

        # d(theta)/d(d1) = -(d2 / (r1 r2) - c d1 / r1^2) / sin(theta), likewise for d2
        a1 = -dv_dth * inv_s
        a11 = -a1 * c / r1sq
        a12 = a1 * inv_r1r2
        a22 = -a1 * c / r2sq
        e1 = dv_dr1 / r1  # the radial parts, along each bond
        e2 = dv_dr2 / r2
        g1x = a11 * d1x + a12 * d2x + e1 * d1x
        g1y = a11 * d1y + a12 * d2y + e1 * d1y
        g1z = a11 * d1z + a12 * d2z + e1 * d1z
        g3x = a22 * d2x + a12 * d1x + e2 * d2x
        g3y = a22 * d2y + a12 * d1y + e2 * d2y
        g3z = a22 * d2z + a12 * d1z + e2 * d2z
        return (g1x, g1y, g1z, g3x, g3y, g3z, v)

    @jit
    def intramolecular(m, x, F, par):
        """Bonded forces of molecule ``m``, *written* into its rows of ``F``.

        Atom ``NPM m`` is the carbon: every bond joins it to one outer atom, and every
        angle is a pair of outer atoms around it.

        Returns
        -------
        tuple
            The bond energy and the angle energy (with its couplings).
        """

        i0 = NPM * m
        for k in range(NPM):
            F[i0 + k, 0] = ft(0.0)
            F[i0 + k, 1] = ft(0.0)
            F[i0 + k, 2] = ft(0.0)

        stretch = ft(0.0)
        for k in range(1, NPM):
            gx, gy, gz, v = bond_terms(i0 + k, i0, x, par)
            stretch += v
            F[i0 + k, 0] -= gx
            F[i0 + k, 1] -= gy
            F[i0 + k, 2] -= gz
            F[i0, 0] += gx
            F[i0, 1] += gy
            F[i0, 2] += gz

        bend = ft(0.0)
        for j in range(1, NPM):
            for k in range(j + 1, NPM):
                g1x, g1y, g1z, g3x, g3y, g3z, v = angle_terms(
                    i0 + j, i0, i0 + k, x, par
                )
                bend += v
                F[i0 + j, 0] -= g1x
                F[i0 + j, 1] -= g1y
                F[i0 + j, 2] -= g1z
                F[i0 + k, 0] -= g3x
                F[i0 + k, 1] -= g3y
                F[i0 + k, 2] -= g3z
                F[i0, 0] += g1x + g3x
                F[i0, 1] += g1y + g3y
                F[i0, 2] += g1z + g3z

        return (stretch, bend)

    return {
        "pair_forces": pair_forces,
        "structure_factor": structure_factor,
        "recip_force": recip_force,
        "intramolecular": intramolecular,
    }


# --------------------------------------------------------------------------- #
# CPU build: one njit function evaluating the whole force field for one system #
# --------------------------------------------------------------------------- #

_CPU_KERNEL = None


def build_cpu_kernel():
    """
    Build (once per process) the ``njit`` ch4compass force evaluator.

    Returns
    -------
    callable
        ``compute(x, F, efield, s_re, s_im, qa, lj_a, lj_b, lj_c, kvec, ak, par,
        terms)`` returning the potential energy and filling ``terms`` with the stretch
        and the bend energy; all arrays float64.
    """

    global _CPU_KERNEL
    if _CPU_KERNEL is not None:
        return _CPU_KERNEL

    from numba import float64, njit

    @njit
    def _sincos(x):
        return (math.sin(x), math.cos(x))

    terms = _build_terms(
        float64, njit, math.sqrt, math.exp, math.acos, np.rint, _sincos
    )
    pair_forces = terms["pair_forces"]
    structure_factor = terms["structure_factor"]
    recip_force = terms["recip_force"]
    intramolecular = terms["intramolecular"]

    @njit
    def compute(x, F, efield, s_re, s_im, qa, lj_a, lj_b, lj_c, kvec, ak, par, terms):
        na = x.shape[0]
        nm = na // NPM
        nk = kvec.shape[0]
        periodic = par[P_PERIODIC] > 0.0
        inter = par[P_INTER] > 0.0
        potential = 0.0

        # bonded terms: each molecule owns its atoms outright, so they write F
        terms[0] = 0.0
        terms[1] = 0.0
        for m in range(nm):
            stretch, bend = intramolecular(m, x, F, par)
            terms[0] += stretch
            terms[1] += bend
        potential += terms[0] + terms[1]

        # the external field always acts, even on a lone molecule
        for i in range(na):
            qi = qa[i - NPM * (i // NPM)]
            for c in range(3):
                F[i, c] += qi * efield[c]

        if not inter:
            return potential

        if periodic:
            for k in range(nk):
                re, im = structure_factor(k, x, qa, kvec)
                s_re[k] = re
                s_im[k] = im
                potential += ak[k] * (re * re + im * im)
            potential += par[P_ESELF]

        for i in range(na):
            fx, fy, fz, e_lj, e_co = pair_forces(i, x, qa, lj_a, lj_b, lj_c, par)
            if periodic:
                rx, ry, rz = recip_force(i, x, qa, kvec, ak, s_re, s_im)
                fx += rx
                fy += ry
                fz += rz
            F[i, 0] += fx
            F[i, 1] += fy
            F[i, 2] += fz
            potential += 0.5 * (e_lj + e_co)
        return potential

    _CPU_KERNEL = compute
    return compute


# --------------------------------------------------------------------------- #
# GPU build: one CUDA block per system, one thread per atom                    #
# --------------------------------------------------------------------------- #

_CUDA_CACHE = {}
_STATIC_SHARED_BYTES = 48 * 1024  # static shared memory per CUDA block


def build_cuda_kernels(n_atoms, n_kvec, threads_per_block=THREADS_PER_BLOCK):
    """
    Build (once per shape) the CUDA force kernels for a batch of CH4 systems.

    One block handles one system with two kernels:

    ``intra``
        Bonded stretches and bends in float64; *writes* the force array.
    ``nonbonded``
        Lennard-Jones, Ewald and the external field in float32; *adds* to it.

    Parameters
    ----------
    n_atoms, n_kvec : int
        Atoms and half-space k-vectors per system; both become compile-time loop
        bounds and size the per-system work arrays. Those live in static shared
        memory when they fit its 48 KB (the fastest path) and in global memory
        otherwise (see :meth:`CH4COMPASSForceKernels.forces_gpu`), so the system
        size is bounded by the device memory.
    threads_per_block : int, default: 128
        CUDA block size, a multiple of the warp size (32).

    Returns
    -------
    dict
        The kernels, keyed ``"intra"`` and ``"nonbonded"``, and the flag
        ``"work_in_shared"`` telling which non-bonded variant was chosen.

    Raises
    ------
    ImportError
        If numba's CUDA target is unavailable.
    """

    key = (int(n_atoms), int(n_kvec), int(threads_per_block))
    if key in _CUDA_CACHE:
        return _CUDA_CACHE[key]

    try:
        from numba import cuda, float32, float64
        from numba.cuda import libdevice as ld
    except Exception as exc:  # ImportError, or a numba/CUDA runtime failure
        raise ImportError(
            "The GPU batch backend requires numba's CUDA target. Install it "
            "alongside CuPy, e.g. 'pip install maxwelllink[gpu-cuda12]'. On hosts "
            "without CUDA, use backend='cpu' to run the compiled CPU kernels instead."
        ) from exc

    NA = int(n_atoms)
    NM = NA // NPM
    NK = max(int(n_kvec), 1)
    TPB = int(threads_per_block)
    device = cuda.jit(device=True)

    # single precision for the non-bonded sums, double for the bonded term
    f32 = _build_terms(
        float32, device, ld.sqrtf, ld.expf, ld.acosf, ld.rintf, ld.sincosf
    )
    f64 = _build_terms(float64, device, ld.sqrt, ld.exp, ld.acos, ld.rint, ld.sincos)
    pair_forces32 = f32["pair_forces"]
    structure_factor32 = f32["structure_factor"]
    recip_force32 = f32["recip_force"]
    intramolecular64 = f64["intramolecular"]

    @cuda.jit
    def k_intra(x, F, potential, par, terms):
        """Bonded forces, stretch and bend energies of every molecule, in float64."""

        d = cuda.blockIdx.x
        tid = cuda.threadIdx.x
        block_energy = cuda.shared.array(shape=2, dtype=float64)  # stretch, bend
        if tid < 2:
            block_energy[tid] = 0.0
        cuda.syncthreads()

        xd = x[d]
        Fd = F[d]
        stretch = 0.0
        bend = 0.0
        for m in range(tid, NM, TPB):
            st, be = intramolecular64(m, xd, Fd, par)
            stretch += st
            bend += be
        cuda.atomic.add(block_energy, 0, stretch)
        cuda.atomic.add(block_energy, 1, bend)
        cuda.syncthreads()
        if tid == 0:
            terms[d, 0] = block_energy[0]
            terms[d, 1] = block_energy[1]
            potential[d] = block_energy[0] + block_energy[1]

    @device
    def nonbonded_terms(
        d,
        tid,
        x,
        F,
        potential,
        efield,
        qa,
        lj_a,
        lj_b,
        lj_c,
        kvec,
        ak,
        par,
        e_self,
        xs,
        s_re,
        s_im,
    ):
        """
        Lennard-Jones, Ewald and external-field forces of system ``d`` in single
        precision, on the given work arrays: single-precision positions ``xs`` and
        Ewald structure factors ``s_re``/``s_im``, in shared or in global memory.
        """

        block_energy = cuda.shared.array(shape=1, dtype=float64)
        if tid == 0:
            block_energy[0] = 0.0
        # stride over (atom, component) so the global reads coalesce
        for e in range(tid, 3 * NA, TPB):
            i = e // 3
            c = e - 3 * i
            xs[i, c] = float32(x[d, i, c])
        cuda.syncthreads()

        energy = 0.0
        periodic = par[P_PERIODIC] > float32(0.0)
        inter = par[P_INTER] > float32(0.0)

        if inter and periodic:
            for k in range(tid, NK, TPB):
                re, im = structure_factor32(k, xs, qa, kvec)
                s_re[k] = re
                s_im[k] = im
                energy += float64(ak[k] * (re * re + im * im))
            cuda.syncthreads()

        ex = float32(efield[d, 0])
        ey = float32(efield[d, 1])
        ez = float32(efield[d, 2])
        for i in range(tid, NA, TPB):
            qi = qa[i - NPM * (i // NPM)]
            fx = qi * ex
            fy = qi * ey
            fz = qi * ez
            if inter:
                r = pair_forces32(i, xs, qa, lj_a, lj_b, lj_c, par)
                fx += r[0]
                fy += r[1]
                fz += r[2]
                energy += float64(float32(0.5) * (r[3] + r[4]))
                if periodic:
                    rx, ry, rz = recip_force32(i, xs, qa, kvec, ak, s_re, s_im)
                    fx += rx
                    fy += ry
                    fz += rz
            F[d, i, 0] += float64(fx)
            F[d, i, 1] += float64(fy)
            F[d, i, 2] += float64(fz)

        cuda.atomic.add(block_energy, 0, energy)
        cuda.syncthreads()
        if tid == 0:
            potential[d] += block_energy[0] + (e_self if inter and periodic else 0.0)

    @cuda.jit
    def k_nonbonded_shared(
        x, F, potential, efield, qa, lj_a, lj_b, lj_c, kvec, ak, par, e_self
    ):
        """Non-bonded forces with the work arrays in static shared memory: fastest."""

        d = cuda.blockIdx.x
        tid = cuda.threadIdx.x
        xs = cuda.shared.array(shape=(NA, 3), dtype=float32)
        s_re = cuda.shared.array(shape=NK, dtype=float32)
        s_im = cuda.shared.array(shape=NK, dtype=float32)
        nonbonded_terms(
            d,
            tid,
            x,
            F,
            potential,
            efield,
            qa,
            lj_a,
            lj_b,
            lj_c,
            kvec,
            ak,
            par,
            e_self,
            xs,
            s_re,
            s_im,
        )

    @cuda.jit
    def k_nonbonded_global(
        x,
        F,
        potential,
        efield,
        qa,
        lj_a,
        lj_b,
        lj_c,
        kvec,
        ak,
        par,
        e_self,
        xs_all,
        s_re_all,
        s_im_all,
    ):
        """Non-bonded forces with the work arrays in global memory: any system size."""

        d = cuda.blockIdx.x
        tid = cuda.threadIdx.x
        nonbonded_terms(
            d,
            tid,
            x,
            F,
            potential,
            efield,
            qa,
            lj_a,
            lj_b,
            lj_c,
            kvec,
            ak,
            par,
            e_self,
            xs_all[d],
            s_re_all[d],
            s_im_all[d],
        )

    # The work arrays hold (NA, 3) + 2 * NK float32 values plus the 8-byte energy
    # accumulator. Static shared memory is their fastest home but offers 48 KB per
    # block, so larger systems run the same physics on global-memory arrays.
    work_in_shared = (3 * NA + 2 * NK) * 4 + 8 <= _STATIC_SHARED_BYTES
    kernels = {
        "intra": k_intra,
        "nonbonded": k_nonbonded_shared if work_in_shared else k_nonbonded_global,
        "work_in_shared": work_in_shared,
    }
    _CUDA_CACHE[key] = kernels
    return kernels


# --------------------------------------------------------------------------- #
# Force-field set-up and the object the batch driver drives                    #
# --------------------------------------------------------------------------- #


def _ewald_kvectors(box, n_atoms, ewald_wrcut=None):
    """
    Build the half-space Ewald k-vectors and their prefactors.

    Only half of reciprocal space is kept: ``S(-k)`` is the conjugate of ``S(k)``, so
    the energy and force summands are even under ``k -> -k``. Folding the factor two
    into ``a_k`` halves the k-loop and leaves the kernel formulas unchanged.

    Parameters
    ----------
    box : array-like of float, shape (3,)
        Periodic box lengths in Bohr.
    n_atoms : int
        Number of atoms, used for the default real-space cutoff.
    ewald_wrcut : float, optional
        Real-space cutoff in Bohr. ``None`` uses the force field's default.

    Returns
    -------
    kvec : numpy.ndarray of float, shape (nk, 3)
        Half-space k-vectors in inverse Bohr.
    ak : numpy.ndarray of float, shape (nk,)
        Prefactors ``2 (2 pi / V) exp(-k^2 / 4 alpha^2) / k^2``.
    alpha : float
        Ewald screening parameter.
    wrcut : float
        Real-space cutoff used.
    """

    box = np.asarray(box, dtype=float)
    volume = float(box[0] * box[1] * box[2])
    if ewald_wrcut is None:
        wrcut = float(np.min(box)) * min(0.5, 1.2 * n_atoms ** (-1.0 / 6.0))
    else:
        wrcut = float(ewald_wrcut)
    alpha = np.pi / wrcut
    kmax = int(alpha * float(np.max(box)))
    rkmax2 = (2.0 * np.pi * alpha) ** 2

    krange = np.arange(-kmax, kmax + 1)
    nx, ny, nz = np.meshgrid(krange, krange, krange, indexing="ij")
    nx = nx.ravel()
    ny = ny.ravel()
    nz = nz.ravel()
    kvec = (2.0 * np.pi) * np.stack([nx / box[0], ny / box[1], nz / box[2]], axis=1)
    k2 = np.sum(kvec**2, axis=1)
    inside = (k2 > 0.0) & (k2 < rkmax2)
    # keep one member of every (k, -k) pair
    half = (nx > 0) | ((nx == 0) & (ny > 0)) | ((nx == 0) & (ny == 0) & (nz > 0))
    keep = inside & half
    kvec = np.ascontiguousarray(kvec[keep])
    k2 = k2[keep]
    ak = 2.0 * (2.0 * np.pi / volume) * np.exp(-k2 / (4.0 * alpha**2)) / k2
    return kvec, ak, alpha, wrcut


class CH4COMPASSForceKernels:
    """
    Compiled ch4compass forces for a batch of identical systems.

    The batch driver reads ``n_atoms``, ``n_molecules``, ``masses`` and
    ``charges_eff``, then calls :meth:`forces_cpu` or :meth:`forces_gpu` every step.
    Nothing else about this force field leaks out, so another force field only has to
    provide the same attributes and the same two methods.
    """

    #: energy terms reported next to the potential, in this column order
    reported_term_names = ("stretch_au", "bend_au")

    def __init__(self, ff, xp, threads_per_block=THREADS_PER_BLOCK):
        """
        Compile the kernels and upload the constants of one CH4 force field.

        Parameters
        ----------
        ff : ch4compass.CH4COMPASSForceField
            A constructed force field; its geometry, box and cutoffs are read here.
        xp : module
            Array module: ``numpy`` for the CPU path, ``cupy`` for the GPU path.
        threads_per_block : int, default: 128
            CUDA block size, ignored on the CPU path.
        """

        self.xp = xp
        self.on_gpu = getattr(xp, "__name__", "") == "cupy"
        self.n_molecules = ff.n_molecules
        self.n_atoms = ff.n_atoms
        na = self.n_atoms

        # the charge sites are the atoms, so the dipole weights are the charges
        qa = np.array([ff_module._Q_C] + [ff_module._Q_H] * 4, dtype=float)
        massa = np.array([ff_module._M_C] + [ff_module._M_H] * 4, dtype=float)
        self.masses = massa[np.arange(na) % NPM][:, None]
        self.charges_eff = qa[np.arange(na) % NPM][:, None]

        # per-site 9-6 Lennard-Jones pair tables from the per-kind tables
        kinds = ff_module._KINDS
        eps_ij = ff_module._LJ_EPS[kinds[:, None], kinds[None, :]]  # (NPM, NPM)
        r0_ij = ff_module._LJ_R0[kinds[:, None], kinds[None, :]]

        par = np.zeros(N_PARAMS)
        if ff.box is None:
            # a finite cluster has no Ewald sum; one dummy k-vector keeps shapes valid
            kvec = np.zeros((1, 3))
            ak = np.zeros(1)
            lj_cut = 1.0
            par[P_BOXX] = par[P_BOXY] = par[P_BOXZ] = 1.0
        else:
            kvec, ak, ew_alpha, ew_wrcut = _ewald_kvectors(ff.box, na, ff.ewald_wrcut)
            lj_cut = min(ff.rcut, 0.5 * float(np.min(ff.box)))
            par[P_PERIODIC] = 1.0
            par[P_BOXX], par[P_BOXY], par[P_BOXZ] = (float(b) for b in ff.box)
            par[P_LJCUT2] = lj_cut**2
            par[P_EWA] = ew_alpha
            par[P_WRCUT2] = ew_wrcut**2
            par[P_TWOAPI] = 2.0 * ew_alpha / np.sqrt(np.pi)
            par[P_ESELF] = (
                -(ew_alpha / np.sqrt(np.pi)) * self.n_molecules * float(np.sum(qa**2))
            )
        par[P_INTER] = 1.0 if self.n_molecules > 1 else 0.0
        par[P_R0] = ff_module._RCH
        par[P_K2B] = ff_module._K2_BOND
        par[P_K3B] = ff_module._K3_BOND
        par[P_K4B] = ff_module._K4_BOND
        par[P_THETA0] = ff_module._THETA0
        par[P_K2A] = ff_module._K2_ANGLE
        par[P_K3A] = ff_module._K3_ANGLE
        par[P_K4A] = ff_module._K4_ANGLE
        par[P_MBB] = ff_module._M_BB
        par[P_RBB] = ff_module._R_BB
        par[P_NBA] = ff_module._N_BA
        par[P_RBA] = ff_module._R_BA
        par[P_SINFLOOR] = ff_module._SIN_FLOOR

        # pair tables, done once so the pair loop only reads them
        lj_a = eps_ij
        lj_b = r0_ij**3
        lj_c = np.zeros((NPM, NPM))
        if ff.box is not None:
            src6 = (r0_ij / lj_cut) ** 6
            lj_c = 2.0 * src6 * np.sqrt(src6) - 3.0 * src6  # continuous at the cutoff

        self.n_kvec = kvec.shape[0]
        self.e_self = float(par[P_ESELF])

        if self.on_gpu:
            self.kernels = build_cuda_kernels(na, self.n_kvec, threads_per_block)
            self.threads_per_block = int(threads_per_block)
            # single-precision copies for the non-bonded kernel
            self.qa = xp.asarray(qa, dtype=xp.float32)
            self.lj_a = xp.asarray(lj_a, dtype=xp.float32)
            self.lj_b = xp.asarray(lj_b, dtype=xp.float32)
            self.lj_c = xp.asarray(lj_c, dtype=xp.float32)
            self.kvec = xp.asarray(kvec, dtype=xp.float32)
            self.ak = xp.asarray(ak, dtype=xp.float32)
            self.par32 = xp.asarray(par, dtype=xp.float32)
            self.par = xp.asarray(par)  # the bonded kernel stays in float64
            self._efield32 = None  # sized on the first forces_gpu() call
            self._work = None  # the non-bonded kernel's work arrays, likewise
            self._terms = None  # likewise; used when the caller passes no terms array
        else:
            self.compute = build_cpu_kernel()
            self.qa = qa
            self.lj_a = lj_a
            self.lj_b = lj_b
            self.lj_c = lj_c
            self.kvec = kvec
            self.ak = ak
            self.par = par
            self._s_re = np.zeros(max(self.n_kvec, 1))  # scratch reused every call
            self._s_im = np.zeros(max(self.n_kvec, 1))
            self._terms = np.zeros(2)  # used when the caller passes no terms array

    def forces_cpu(self, x, F, efield, terms=None):
        """
        Evaluate the force and potential of one system.

        Parameters
        ----------
        x : numpy.ndarray of float, shape (na, 3)
            Atomic positions in atomic units (Bohr).
        F : numpy.ndarray of float, shape (na, 3)
            Output force array, overwritten in place.
        efield : numpy.ndarray of float, shape (3,)
            Effective electric field in atomic units.
        terms : numpy.ndarray of float, shape (2,), optional
            Output stretch and bend energy, in the order of :attr:`reported_term_names`.

        Returns
        -------
        float
            Mechanical potential energy in atomic units (Hartree).
        """

        if terms is None:
            terms = self._terms
        return self.compute(
            x,
            F,
            efield,
            self._s_re,
            self._s_im,
            self.qa,
            self.lj_a,
            self.lj_b,
            self.lj_c,
            self.kvec,
            self.ak,
            self.par,
            terms,
        )

    def forces_gpu(self, x, F, potential, efield, terms=None):
        """
        Evaluate the force and potential of every system with two kernel launches.

        Parameters
        ----------
        x : cupy.ndarray of float, shape (num, na, 3)
            Atomic positions of every system in atomic units (Bohr).
        F : cupy.ndarray of float, shape (num, na, 3)
            Output force array, overwritten in place.
        potential : cupy.ndarray of float, shape (num,)
            Output potential energy of every system.
        efield : cupy.ndarray of float, shape (num, 3)
            Effective electric field of every system in atomic units.
        terms : cupy.ndarray of float, shape (num, 2), optional
            Output stretch and bend energy of every system, in the order of
            :attr:`reported_term_names`.
        """

        num = x.shape[0]
        tpb = self.threads_per_block
        if self._efield32 is None or self._efield32.shape[0] != num:
            f32 = self.xp.float32
            self._efield32 = self.xp.zeros((num, 3), dtype=f32)
            # a system too large for static shared memory keeps the non-bonded
            # kernel's per-system work arrays (single-precision positions and Ewald
            # structure factors) in global memory
            self._work = ()
            if not self.kernels["work_in_shared"]:
                n_kvec = max(self.n_kvec, 1)
                self._work = (
                    self.xp.zeros((num, self.n_atoms, 3), dtype=f32),
                    self.xp.zeros((num, n_kvec), dtype=f32),
                    self.xp.zeros((num, n_kvec), dtype=f32),
                )
        if terms is None:
            if self._terms is None:
                self._terms = self.xp.zeros((num, 2), dtype=self.xp.float64)
            terms = self._terms
        self._efield32[:] = efield.astype(self.xp.float32)
        self.kernels["intra"][num, tpb](x, F, potential, self.par, terms)
        self.kernels["nonbonded"][num, tpb](
            x,
            F,
            potential,
            self._efield32,
            self.qa,
            self.lj_a,
            self.lj_b,
            self.lj_c,
            self.kvec,
            self.ak,
            self.par32,
            self.e_self,
            *self._work,
        )
