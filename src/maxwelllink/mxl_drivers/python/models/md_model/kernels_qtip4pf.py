# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Compiled q-TIP4P/F forces for the CPU and the GPU.

A few features of q-TIP4P/F are noted:

* the negative charge sits on a virtual M-site,
  ``r_M = alpha r_O + alpha2 (r_H1 + r_H2)``, so the charge sites are ``[M, H, H]`` and
  the force on the M-site is redistributed onto O, H, H by the chain rule;
* Lennard-Jones acts on the oxygens only;
* the bend equilibrium is 107.4 degrees.

Precision: the CPU runs in float64 throughout. The GPU runs the bonded term in float64
and the non-bonded sums in float32, because consumer GPUs run float64 at 1/64 speed.
"""

import math

import numpy as np

from . import qtip4pf as ff_module

# --------------------------------------------------------------------------- #
# Scalar parameter slots. Packing them into one array keeps the kernel         #
# signatures short and lets the GPU hold them at the working precision.        #
# --------------------------------------------------------------------------- #
P_ALPHA = 0  # M-site mixing weight on the oxygen
P_ALPHA2 = 1  # M-site mixing weight on each hydrogen
P_BOXX = 2  # periodic box lengths (Bohr)
P_BOXY = 3
P_BOXZ = 4
P_PERIODIC = 5  # 1.0 for a periodic box, 0.0 for a finite cluster
P_INTER = 6  # 1.0 when the system has more than one molecule
P_LJCUT2 = 7  # squared Lennard-Jones cutoff (periodic only)
P_LJA = 8  # 4 eps of the single O-O Lennard-Jones pair
P_LJB = 9  # sigma^2 of that pair
P_LJC = 10  # its cutoff shift, which keeps the potential continuous
P_EWA = 11  # Ewald screening parameter alpha
P_WRCUT2 = 12  # squared Ewald real-space cutoff
P_TWOAPI = 13  # 2 alpha / sqrt(pi)
P_ESELF = 14  # Ewald self-energy (a geometry-independent constant)
P_DE = 15  # quartic-Morse well-depth prefactor
P_ALP = 16  # quartic-Morse range parameter
P_R0 = 17  # equilibrium O-H length
P_DEB = 18  # harmonic bend force constant
P_THETA0 = 19  # equilibrium H-O-H angle
N_PARAMS = 20

# Abramowitz-Stegun 7.1.26 erfc coefficients. The reference force field uses this
# polynomial rather than the true erfc, so the kernels must use it too.
_ERFC_P = 3.0525860
_ERFC_A1 = 0.254829592
_ERFC_A2 = -0.284496736
_ERFC_A3 = 1.421413741
_ERFC_A4 = -1.453152027
_ERFC_A5 = 1.061405429

# quartic-Morse expansion coefficients
_F1 = 7.0 / 12.0
_F2 = 7.0 / 3.0

_THREADS_PER_BLOCK = 128


def _build_terms(ft, jit, sqrt_, exp_, acos_, rint_, sincos_):
    """
    Build the q-TIP4P/F force terms for one target and one precision.

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
    def site_xyz(i, x, par):
        """Position of charge site ``i``.

        Site ``3m`` is the M-site ``alpha r_O + alpha2 (r_H1 + r_H2)``; the two
        hydrogen sites sit on their atoms.
        """

        m3 = (i // 3) * 3
        if i == m3:
            a = par[P_ALPHA]
            a2 = par[P_ALPHA2]
            return (
                a * x[m3, 0] + a2 * (x[m3 + 1, 0] + x[m3 + 2, 0]),
                a * x[m3, 1] + a2 * (x[m3 + 1, 1] + x[m3 + 2, 1]),
                a * x[m3, 2] + a2 * (x[m3 + 1, 2] + x[m3 + 2, 2]),
            )
        return (x[i, 0], x[i, 1], x[i, 2])

    @jit
    def lj_force(m, x, par):
        """Lennard-Jones force on the oxygen of molecule ``m``.

        Only oxygens carry dispersion in q-TIP4P/F, so this loop visits one atom in
        three rather than every atom, and there is a single pair type.

        Returns
        -------
        tuple
            ``(fx, fy, fz, e_lj)``; the energy is a double-counted row sum.
        """

        nm = x.shape[0] // 3
        i = 3 * m
        periodic = par[P_PERIODIC]
        boxx = par[P_BOXX]
        boxy = par[P_BOXY]
        boxz = par[P_BOXZ]
        ljcut2 = par[P_LJCUT2]
        lj_a = par[P_LJA]
        lj_b = par[P_LJB]
        lj_c = par[P_LJC]

        fx = ft(0.0)
        fy = ft(0.0)
        fz = ft(0.0)
        e_lj = ft(0.0)
        for n in range(nm):
            if n == m:
                continue
            j = 3 * n
            dx = min_image(x[i, 0] - x[j, 0], boxx, periodic)
            dy = min_image(x[i, 1] - x[j, 1], boxy, periodic)
            dz = min_image(x[i, 2] - x[j, 2], boxz, periodic)
            r2 = dx * dx + dy * dy + dz * dz
            if periodic > ft(0.0) and r2 >= ljcut2:
                continue
            inv_r2 = ft(1.0) / r2
            sr2 = lj_b * inv_r2
            sr6 = sr2 * sr2 * sr2
            e_lj += lj_a * (sr6 * (sr6 - ft(1.0)) - lj_c)
            coeff = ft(12.0) * lj_a * sr6 * (sr6 - ft(0.5)) * inv_r2
            fx += coeff * dx
            fy += coeff * dy
            fz += coeff * dz
        return (fx, fy, fz, e_lj)

    @jit
    def coulomb_force(i, sp, q3, par):
        """Real-space Coulomb force on charge site ``i``.

        Bonded pairs carry ``erfc(alpha r) - 1``, which cancels the bare Coulomb that
        the reciprocal sum includes for them.

        Returns
        -------
        tuple
            ``(fx, fy, fz, e_coul)``; the energy is a double-counted row sum.
        """

        ns = sp.shape[0]
        mi = i // 3
        li = i - 3 * mi
        periodic = par[P_PERIODIC]
        boxx = par[P_BOXX]
        boxy = par[P_BOXY]
        boxz = par[P_BOXZ]
        wrcut2 = par[P_WRCUT2]
        ewa = par[P_EWA]
        twoapi = par[P_TWOAPI]
        q_i = q3[li]

        fx = ft(0.0)
        fy = ft(0.0)
        fz = ft(0.0)
        e_co = ft(0.0)
        for j in range(ns):
            if j == i:
                continue
            mj = j // 3
            same_mol = mj == mi
            qq = q_i * q3[j - 3 * mj]

            dx = min_image(sp[i, 0] - sp[j, 0], boxx, periodic)
            dy = min_image(sp[i, 1] - sp[j, 1], boxy, periodic)
            dz = min_image(sp[i, 2] - sp[j, 2], boxz, periodic)
            r2 = dx * dx + dy * dy + dz * dz

            if periodic > ft(0.0):
                if r2 < wrcut2:
                    r = sqrt_(r2)
                    inv_r = ft(1.0) / r
                    # erfc(alpha r) and the force's gaussian share one exponential
                    ar = ewa * r
                    gauss = exp_(-ar * ar)
                    g = erfc_body(ar, gauss)
                    if same_mol:  # exclusion correction for a bonded pair
                        g = g - ft(1.0)
                    gr = g * inv_r
                    e_co += qq * gr
                    coeff = qq * (gr + twoapi * gauss) * inv_r * inv_r
                    fx += coeff * dx
                    fy += coeff * dy
                    fz += coeff * dz
            elif not same_mol:
                r = sqrt_(r2)
                inv_r = ft(1.0) / r
                e_co += qq * inv_r
                coeff = qq * inv_r * inv_r * inv_r
                fx += coeff * dx
                fy += coeff * dy
                fz += coeff * dz
        return (fx, fy, fz, e_co)

    @jit
    def structure_factor(k, sp, q3, kvec):
        """Real and imaginary parts of ``S(k) = sum_j q_j exp(i k . r_j)``."""

        ns = sp.shape[0]
        kx = kvec[k, 0]
        ky = kvec[k, 1]
        kz = kvec[k, 2]
        s_re = ft(0.0)
        s_im = ft(0.0)
        for j in range(ns):
            phase = kx * sp[j, 0] + ky * sp[j, 1] + kz * sp[j, 2]
            qj = q3[j - 3 * (j // 3)]
            sn, cs = sincos_(phase)
            s_re += qj * cs
            s_im += qj * sn
        return (s_re, s_im)

    @jit
    def recip_force(i, sp, q3, kvec, ak, s_re, s_im):
        """Reciprocal-space Coulomb force on charge site ``i``."""

        nk = kvec.shape[0]
        xi = sp[i, 0]
        yi = sp[i, 1]
        zi = sp[i, 2]
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
        s = ft(-2.0) * q3[i - 3 * (i // 3)]
        return (s * fx, s * fy, s * fz)

    @jit
    def redistribute(i, fsite, par):
        """Share the force on charge site ``i`` out to atom ``i``.

        The chain rule sends ``alpha`` of the M-site force to the oxygen and
        ``alpha2`` to each hydrogen, which also keeps its own site force.
        """

        m3 = (i // 3) * 3
        if i == m3:
            a = par[P_ALPHA]
            return (a * fsite[i, 0], a * fsite[i, 1], a * fsite[i, 2])
        a2 = par[P_ALPHA2]
        return (
            fsite[i, 0] + a2 * fsite[m3, 0],
            fsite[i, 1] + a2 * fsite[m3, 1],
            fsite[i, 2] + a2 * fsite[m3, 2],
        )

    @jit
    def intramolecular(m, x, par):
        """Quartic-Morse stretches and harmonic bend of molecule ``m``.

        Atom ``3m`` is the oxygen, so the bonds are ``3m -> 3m+1`` and
        ``3m -> 3m+2``; the bend angle comes from the law of cosines, as in the
        reference force field.

        Returns
        -------
        tuple
            The nine force components ``(f0x .. f2z)``, then the energy.
        """

        i0 = 3 * m
        i1 = i0 + 1
        i2 = i0 + 2

        d1x = x[i1, 0] - x[i0, 0]
        d1y = x[i1, 1] - x[i0, 1]
        d1z = x[i1, 2] - x[i0, 2]
        d2x = x[i2, 0] - x[i0, 0]
        d2y = x[i2, 1] - x[i0, 1]
        d2z = x[i2, 2] - x[i0, 2]
        d3x = x[i2, 0] - x[i1, 0]
        d3y = x[i2, 1] - x[i1, 1]
        d3z = x[i2, 2] - x[i1, 2]

        r1 = sqrt_(d1x * d1x + d1y * d1y + d1z * d1z)
        r2 = sqrt_(d2x * d2x + d2y * d2y + d2z * d2z)
        r3 = sqrt_(d3x * d3x + d3y * d3y + d3z * d3z)

        de = par[P_DE]
        alp = par[P_ALP]
        r0 = par[P_R0]
        deb = par[P_DEB]
        theta0 = par[P_THETA0]

        alp2 = alp * alp
        alp3 = alp2 * alp
        alp4 = alp3 * alp

        # --- stretches (displacement from the equilibrium O-H length) ---
        s1 = r1 - r0
        s2 = r2 - r0
        v = de * (
            alp2 * s1 * s1 - alp3 * s1 * s1 * s1 + ft(_F1) * alp4 * s1 * s1 * s1 * s1
        ) + de * (
            alp2 * s2 * s2 - alp3 * s2 * s2 * s2 + ft(_F1) * alp4 * s2 * s2 * s2 * s2
        )
        a1 = de * (
            ft(2.0) * alp2 * s1
            - ft(3.0) * alp3 * s1 * s1
            + ft(_F2) * alp4 * s1 * s1 * s1
        )
        a2 = de * (
            ft(2.0) * alp2 * s2
            - ft(3.0) * alp3 * s2 * s2
            + ft(_F2) * alp4 * s2 * s2 * s2
        )

        # --- bend (angle from the law of cosines) ---
        u = r1 * r1 + r2 * r2 - r3 * r3
        vv = ft(2.0) * r1 * r2
        v2 = ft(1.0) / (vv * vv)
        arg = u / vv
        if arg > ft(1.0):
            arg = ft(1.0)
        elif arg < ft(-1.0):
            arg = ft(-1.0)
        ang = acos_(arg)
        dang = ang - theta0
        darg = ft(-1.0) / sqrt_(ft(1.0) - arg * arg)
        dtheta_dr1 = darg * ((ft(2.0) * r1) * vv - (ft(2.0) * r2) * u) * v2
        dtheta_dr2 = darg * ((ft(2.0) * r2) * vv - (ft(2.0) * r1) * u) * v2
        dtheta_dr3 = darg * ((ft(-2.0) * r3) * vv) * v2
        v += deb * dang * dang
        a3 = ft(2.0) * deb * dang

        # dV/d(bond length) for each internal coordinate, divided by that length so
        # multiplying by the displacement vector gives the gradient
        dvdr1 = (a1 + a3 * dtheta_dr1) / r1
        dvdr2 = (a2 + a3 * dtheta_dr2) / r2
        dvdr3 = (a3 * dtheta_dr3) / r3
        g1x = dvdr1 * d1x
        g1y = dvdr1 * d1y
        g1z = dvdr1 * d1z
        g2x = dvdr2 * d2x
        g2y = dvdr2 * d2y
        g2z = dvdr2 * d2z
        g3x = dvdr3 * d3x
        g3y = dvdr3 * d3y
        g3z = dvdr3 * d3z

        return (
            g1x + g2x,
            g1y + g2y,
            g1z + g2z,
            -g1x + g3x,
            -g1y + g3y,
            -g1z + g3z,
            -g2x - g3x,
            -g2y - g3y,
            -g2z - g3z,
            v,
        )

    return {
        "site_xyz": site_xyz,
        "lj_force": lj_force,
        "coulomb_force": coulomb_force,
        "structure_factor": structure_factor,
        "recip_force": recip_force,
        "redistribute": redistribute,
        "intramolecular": intramolecular,
    }


# --------------------------------------------------------------------------- #
# CPU build: one njit function evaluating the whole force field for one system #
# --------------------------------------------------------------------------- #

_CPU_KERNEL = None


def build_cpu_kernel():
    """
    Build (once per process) the ``njit`` q-TIP4P/F force evaluator.

    Returns
    -------
    callable
        ``compute(x, F, efield, sites, fsite, s_re, s_im, q3, kvec, ak, par)``
        returning the potential energy; all arrays float64.
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
    site_xyz = terms["site_xyz"]
    lj_force = terms["lj_force"]
    coulomb_force = terms["coulomb_force"]
    structure_factor = terms["structure_factor"]
    recip_force = terms["recip_force"]
    redistribute = terms["redistribute"]
    intramolecular = terms["intramolecular"]

    @njit
    def compute(x, F, efield, sites, fsite, s_re, s_im, q3, kvec, ak, par):
        na = x.shape[0]
        nm = na // 3
        nk = kvec.shape[0]
        periodic = par[P_PERIODIC] > 0.0
        inter = par[P_INTER] > 0.0
        potential = 0.0

        # bonded terms: each molecule owns its three atoms outright, so they write
        for m in range(nm):
            r = intramolecular(m, x, par)
            i0 = 3 * m
            for c in range(3):
                F[i0, c] = r[c]
                F[i0 + 1, c] = r[3 + c]
                F[i0 + 2, c] = r[6 + c]
            potential += r[9]

        # charge-site positions, and the external field acting on their charges
        for i in range(na):
            sx, sy, sz = site_xyz(i, x, par)
            sites[i, 0] = sx
            sites[i, 1] = sy
            sites[i, 2] = sz
            qi = q3[i - 3 * (i // 3)]
            for c in range(3):
                fsite[i, c] = qi * efield[c]

        if inter:
            if periodic:
                for k in range(nk):
                    re, im = structure_factor(k, sites, q3, kvec)
                    s_re[k] = re
                    s_im[k] = im
                    potential += ak[k] * (re * re + im * im)
                potential += par[P_ESELF]

            for m in range(nm):  # Lennard-Jones acts on the oxygens only
                fx, fy, fz, e_lj = lj_force(m, x, par)
                F[3 * m, 0] += fx
                F[3 * m, 1] += fy
                F[3 * m, 2] += fz
                potential += 0.5 * e_lj

            for i in range(na):
                fx, fy, fz, e_co = coulomb_force(i, sites, q3, par)
                if periodic:
                    rx, ry, rz = recip_force(i, sites, q3, kvec, ak, s_re, s_im)
                    fx += rx
                    fy += ry
                    fz += rz
                fsite[i, 0] += fx
                fsite[i, 1] += fy
                fsite[i, 2] += fz
                potential += 0.5 * e_co

        # fold the charge-site forces onto the real atoms
        for i in range(na):
            ax, ay, az = redistribute(i, fsite, par)
            F[i, 0] += ax
            F[i, 1] += ay
            F[i, 2] += az
        return potential

    _CPU_KERNEL = compute
    return compute


# --------------------------------------------------------------------------- #
# GPU build: one CUDA block per system, one thread per atom                    #
# --------------------------------------------------------------------------- #

_CUDA_CACHE = {}


def build_cuda_kernels(n_atoms, n_kvec, threads_per_block=_THREADS_PER_BLOCK):
    """
    Build (once per shape) the CUDA force kernels for a batch of water systems.

    One block handles one system with two kernels:

    ``intra``
        Bonded stretches and bend in float64; *writes* the force array.
    ``nonbonded``
        Lennard-Jones, Ewald and the external field in float32, then the chain-rule
        redistribution of the M-site force; *adds* to the force array.

    Parameters
    ----------
    n_atoms, n_kvec : int
        Atoms and half-space k-vectors per system; both become compile-time
        shared-memory extents.
    threads_per_block : int, default: 128
        CUDA block size, a multiple of the warp size (32).

    Returns
    -------
    dict
        The kernels, keyed ``"intra"`` and ``"nonbonded"``.

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
            "without CUDA, inject xp=numpy to run the compiled CPU kernels instead."
        ) from exc

    NA = int(n_atoms)
    NM = NA // 3
    NK = max(int(n_kvec), 1)
    TPB = int(threads_per_block)
    device = cuda.jit(device=True)

    # single precision for the non-bonded sums, double for the bonded term
    f32 = _build_terms(
        float32, device, ld.sqrtf, ld.expf, ld.acosf, ld.rintf, ld.sincosf
    )
    f64 = _build_terms(float64, device, ld.sqrt, ld.exp, ld.acos, ld.rint, ld.sincos)
    site_xyz32 = f32["site_xyz"]
    lj_force32 = f32["lj_force"]
    coulomb_force32 = f32["coulomb_force"]
    structure_factor32 = f32["structure_factor"]
    recip_force32 = f32["recip_force"]
    redistribute32 = f32["redistribute"]
    intramolecular64 = f64["intramolecular"]

    @cuda.jit
    def k_intra(x, F, potential, par):
        """Bonded forces and energy of every molecule, in double precision."""

        d = cuda.blockIdx.x
        tid = cuda.threadIdx.x
        block_energy = cuda.shared.array(shape=1, dtype=float64)
        if tid == 0:
            block_energy[0] = 0.0
        cuda.syncthreads()

        xd = x[d]
        Fd = F[d]
        energy = 0.0
        for m in range(tid, NM, TPB):
            r = intramolecular64(m, xd, par)
            i0 = 3 * m
            for c in range(3):
                Fd[i0, c] = r[c]
                Fd[i0 + 1, c] = r[3 + c]
                Fd[i0 + 2, c] = r[6 + c]
            energy += r[9]
        cuda.atomic.add(block_energy, 0, energy)
        cuda.syncthreads()
        if tid == 0:
            potential[d] = block_energy[0]

    @cuda.jit
    def k_nonbonded(x, F, potential, efield, q3, kvec, ak, par, e_self):
        """Lennard-Jones, Ewald and external-field forces, in single precision."""

        d = cuda.blockIdx.x
        tid = cuda.threadIdx.x
        xs = cuda.shared.array(shape=(NA, 3), dtype=float32)
        sp = cuda.shared.array(shape=(NA, 3), dtype=float32)
        fsite = cuda.shared.array(shape=(NA, 3), dtype=float32)
        s_re = cuda.shared.array(shape=NK, dtype=float32)
        s_im = cuda.shared.array(shape=NK, dtype=float32)
        block_energy = cuda.shared.array(shape=1, dtype=float64)

        if tid == 0:
            block_energy[0] = 0.0
        # stride over (atom, component) so the global reads coalesce
        for e in range(tid, 3 * NA, TPB):
            i = e // 3
            c = e - 3 * i
            xs[i, c] = float32(x[d, i, c])
        cuda.syncthreads()

        # charge sites, and the external field acting on their charges
        ex = float32(efield[d, 0])
        ey = float32(efield[d, 1])
        ez = float32(efield[d, 2])
        for i in range(tid, NA, TPB):
            sx, sy, sz = site_xyz32(i, xs, par)
            sp[i, 0] = sx
            sp[i, 1] = sy
            sp[i, 2] = sz
            qi = q3[i - 3 * (i // 3)]
            fsite[i, 0] = qi * ex
            fsite[i, 1] = qi * ey
            fsite[i, 2] = qi * ez
        cuda.syncthreads()

        energy = 0.0
        periodic = par[P_PERIODIC] > float32(0.0)
        inter = par[P_INTER] > float32(0.0)

        if inter:
            if periodic:
                for k in range(tid, NK, TPB):
                    re, im = structure_factor32(k, sp, q3, kvec)
                    s_re[k] = re
                    s_im[k] = im
                    energy += float64(ak[k] * (re * re + im * im))
                cuda.syncthreads()

            for m in range(tid, NM, TPB):  # Lennard-Jones, oxygens only
                fx, fy, fz, e_lj = lj_force32(m, xs, par)
                F[d, 3 * m, 0] += float64(fx)
                F[d, 3 * m, 1] += float64(fy)
                F[d, 3 * m, 2] += float64(fz)
                energy += float64(float32(0.5) * e_lj)

            for i in range(tid, NA, TPB):
                fx, fy, fz, e_co = coulomb_force32(i, sp, q3, par)
                if periodic:
                    rx, ry, rz = recip_force32(i, sp, q3, kvec, ak, s_re, s_im)
                    fx += rx
                    fy += ry
                    fz += rz
                fsite[i, 0] += fx
                fsite[i, 1] += fy
                fsite[i, 2] += fz
                energy += float64(float32(0.5) * e_co)
        cuda.syncthreads()

        # fold the charge-site forces onto the real atoms
        for i in range(tid, NA, TPB):
            ax, ay, az = redistribute32(i, fsite, par)
            F[d, i, 0] += float64(ax)
            F[d, i, 1] += float64(ay)
            F[d, i, 2] += float64(az)

        cuda.atomic.add(block_energy, 0, energy)
        cuda.syncthreads()
        if tid == 0:
            potential[d] += block_energy[0] + (e_self if inter and periodic else 0.0)

    kernels = {"intra": k_intra, "nonbonded": k_nonbonded}
    _CUDA_CACHE[key] = kernels
    return kernels


# --------------------------------------------------------------------------- #
# Force-field set-up and the object the batch driver drives                    #
# --------------------------------------------------------------------------- #


def _ewald_kvectors(box, na, ewald_wrcut=None):
    """
    Build the half-space Ewald k-vectors and their prefactors.

    Only half of reciprocal space is kept: ``S(-k)`` is the conjugate of ``S(k)``, so
    the energy and force summands are even under ``k -> -k``. Folding the factor two
    into ``a_k`` halves the k-loop and leaves the kernel formulas unchanged.

    Parameters
    ----------
    box : array-like of float, shape (3,)
        Periodic box lengths in Bohr.
    na : int
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
        wrcut = float(np.min(box)) * min(0.5, 1.2 * na ** (-1.0 / 6.0))
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


class QTIP4PFForceKernels:
    """
    Compiled q-TIP4P/F forces for a batch of identical systems.
    """

    def __init__(self, ff, xp, threads_per_block=_THREADS_PER_BLOCK):
        """
        Compile the kernels and upload the constants of one water force field.

        Parameters
        ----------
        ff : qtip4pf.QTIP4PFForceField
            A constructed force field; its geometry, box and cutoffs are read here.
        xp : module
            Array module: ``numpy`` for the CPU path, ``cupy`` for the GPU path.
        threads_per_block : int, default: 128
            CUDA block size, ignored on the CPU path.
        """

        self.xp = xp
        self.on_gpu = getattr(xp, "__name__", "") == "cupy"
        self.n_molecules = ff.n_molecules
        self.n_atoms = ff.na
        na = self.n_atoms

        alpha = ff_module._ALPHA
        alpha2 = ff_module._ALPHA2
        q3 = np.array([ff_module._Q_O, ff_module._Q_H, ff_module._Q_H], dtype=float)
        mass3 = np.array([ff_module._M_O, ff_module._M_H, ff_module._M_H], dtype=float)
        self.masses = mass3[np.arange(na) % 3][:, None]

        # Effective atomic charges: folding the M-site mixing into the charges turns
        # the dipole, a sum over charge sites, into a plain sum over atoms.
        qeff3 = np.array(
            [q3[0] * alpha, q3[0] * alpha2 + q3[1], q3[0] * alpha2 + q3[2]],
            dtype=float,
        )
        self.charges_eff = qeff3[np.arange(na) % 3][:, None]

        par = np.zeros(N_PARAMS)
        par[P_ALPHA] = alpha
        par[P_ALPHA2] = alpha2
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
                -(ew_alpha / np.sqrt(np.pi)) * self.n_molecules * float(np.sum(q3**2))
            )
        par[P_INTER] = 1.0 if self.n_molecules > 1 else 0.0
        par[P_DE] = ff_module._DE
        par[P_ALP] = ff_module._ALP
        par[P_R0] = ff_module._REOH
        par[P_DEB] = ff_module._DEB
        par[P_THETA0] = ff_module._THETA0

        # one Lennard-Jones pair type (O-O), so its constants are plain scalars
        par[P_LJA] = 4.0 * ff_module._OO_EPS
        par[P_LJB] = ff_module._OO_SIG**2
        if ff.box is not None:
            src6 = (par[P_LJB] / lj_cut**2) ** 3
            par[P_LJC] = src6 * (src6 - 1.0)  # continuous potential at the cutoff

        self.n_kvec = kvec.shape[0]
        self.e_self = float(par[P_ESELF])

        if self.on_gpu:
            self.kernels = build_cuda_kernels(na, self.n_kvec, threads_per_block)
            self.threads_per_block = int(threads_per_block)
            # single-precision copies for the non-bonded kernel
            self.q3 = xp.asarray(q3, dtype=xp.float32)
            self.kvec = xp.asarray(kvec, dtype=xp.float32)
            self.ak = xp.asarray(ak, dtype=xp.float32)
            self.par32 = xp.asarray(par, dtype=xp.float32)
            self.par = xp.asarray(par)  # the bonded kernel stays in float64
            self._efield32 = None  # sized on the first forces_gpu() call
        else:
            self.compute = build_cpu_kernel()
            self.q3 = q3
            self.kvec = kvec
            self.ak = ak
            self.par = par
            self._sites = np.zeros((na, 3))  # scratch reused every call
            self._fsite = np.zeros((na, 3))
            self._s_re = np.zeros(max(self.n_kvec, 1))
            self._s_im = np.zeros(max(self.n_kvec, 1))

    def forces_cpu(self, x, F, efield):
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

        Returns
        -------
        float
            Mechanical potential energy in atomic units (Hartree).
        """

        return self.compute(
            x,
            F,
            efield,
            self._sites,
            self._fsite,
            self._s_re,
            self._s_im,
            self.q3,
            self.kvec,
            self.ak,
            self.par,
        )

    def forces_gpu(self, x, F, potential, efield):
        """
        Evaluate the force and potential of every system, with two kernel launches.

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
        """

        num = x.shape[0]
        tpb = self.threads_per_block
        if self._efield32 is None:
            self._efield32 = self.xp.zeros((num, 3), dtype=self.xp.float32)
        self._efield32[:] = efield.astype(self.xp.float32)
        self.kernels["intra"][num, tpb](x, F, potential, self.par)
        self.kernels["nonbonded"][num, tpb](
            x,
            F,
            potential,
            self._efield32,
            self.q3,
            self.kvec,
            self.ak,
            self.par32,
            self.e_self,
        )
