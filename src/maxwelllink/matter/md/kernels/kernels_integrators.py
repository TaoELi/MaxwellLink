# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
CUDA kernels of the OBABO (velocity-Verlet + Langevin) integrator.

These kernels are the GPU half of :class:`~maxwelllink.matter.md.md_engine.MDEngine`.
"""

# The integrator kernels are compiled once per process and cached here
_INTEGRATOR_KERNELS = None


def build_integrator_kernels():
    """
    Build (once per process) the CUDA kernels of the OBABO integrator.

    One block handles one system. The kernels are force-field independent: the atom
    count comes from the position array and the per-atom masses and effective charges
    are passed in, so no topology is baked in.

    ``pre``
        Langevin half-kick, momentum half-kick, position drift.
    ``post``
        Second momentum half-kick, the dipole and energy reductions the EM solver
        needs, then the trailing Langevin half-kick.

    Returns
    -------
    dict
        The kernels, keyed ``"pre"`` and ``"post"``.

    Raises
    ------
    ImportError
        If numba's CUDA target is unavailable.
    """

    global _INTEGRATOR_KERNELS
    if _INTEGRATOR_KERNELS is not None:
        return _INTEGRATOR_KERNELS

    try:
        from numba import cuda, float64
        from numba.cuda.random import xoroshiro128p_normal_float64
    except Exception as exc:  # ImportError, or a numba/CUDA runtime failure
        raise ImportError(
            "The GPU batch backend requires numba's CUDA target. Install it "
            "alongside CuPy, e.g. 'pip install maxwelllink[gpu-cuda12]'. On hosts "
            "without CUDA, use backend='cpu' to run the compiled CPU kernels instead."
        ) from exc

    @cuda.jit
    def k_pre(x, p, F, mass, noise, dt, c1h, rng, thermostat):
        """Langevin half-kick, momentum half-kick and position drift."""

        d = cuda.blockIdx.x
        tid = cuda.threadIdx.x
        tpb = cuda.blockDim.x
        na = x.shape[1]
        state = d * tpb + tid
        half_dt = 0.5 * dt
        # stride over (atom, component), so neighbouring threads touch neighbouring
        # doubles and the global reads coalesce
        for e in range(tid, 3 * na, tpb):
            i = e // 3
            c = e - 3 * i
            pi = p[d, i, c]
            if thermostat:
                pi = c1h * pi + noise[i] * xoroshiro128p_normal_float64(rng, state)
            pi += half_dt * F[d, i, c]
            p[d, i, c] = pi
            x[d, i, c] += dt * pi / mass[i]

    @cuda.jit
    def k_post(
        x,
        p,
        F,
        mass,
        noise,
        qeff,
        dt,
        c1h,
        rng,
        thermostat,
        potential,
        amp,
        mu_half,
        mu_force,
        energy,
    ):
        """Second half-kick, the dipole and energy reductions, then the thermostat."""

        d = cuda.blockIdx.x
        tid = cuda.threadIdx.x
        tpb = cuda.blockDim.x
        na = x.shape[1]
        state = d * tpb + tid
        half_dt = 0.5 * dt
        acc = cuda.shared.array(shape=10, dtype=float64)
        if tid < 10:
            acc[tid] = 0.0
        cuda.syncthreads()

        for e in range(tid, 3 * na, tpb):  # second momentum half-kick
            i = e // 3
            c = e - 3 * i
            p[d, i, c] += half_dt * F[d, i, c]
        cuda.syncthreads()

        # Dipole and kinetic reductions. Effective atomic charges make the dipole a
        # plain sum over atoms, so each thread walks a contiguous stripe, keeps its
        # partial sums in registers, and commits once.
        dmx = 0.0
        dmy = 0.0
        dmz = 0.0
        hx = 0.0
        hy = 0.0
        hz = 0.0
        mx = 0.0
        my = 0.0
        mz = 0.0
        kinetic = 0.0
        for i in range(tid, na, tpb):
            qe = qeff[i]
            inv_m = 1.0 / mass[i]
            px = p[d, i, 0]
            py = p[d, i, 1]
            pz = p[d, i, 2]
            kinetic += 0.5 * (px * px + py * py + pz * pz) * inv_m
            vhx = (px + half_dt * F[d, i, 0]) * inv_m
            vhy = (py + half_dt * F[d, i, 1]) * inv_m
            vhz = (pz + half_dt * F[d, i, 2]) * inv_m
            x0 = x[d, i, 0]
            x1 = x[d, i, 1]
            x2 = x[d, i, 2]
            dmx += qe * vhx
            dmy += qe * vhy
            dmz += qe * vhz
            hx += qe * (x0 + half_dt * vhx)
            hy += qe * (x1 + half_dt * vhy)
            hz += qe * (x2 + half_dt * vhz)
            mx += qe * x0
            my += qe * x1
            mz += qe * x2
        cuda.atomic.add(acc, 0, dmx)
        cuda.atomic.add(acc, 1, dmy)
        cuda.atomic.add(acc, 2, dmz)
        cuda.atomic.add(acc, 3, hx)
        cuda.atomic.add(acc, 4, hy)
        cuda.atomic.add(acc, 5, hz)
        cuda.atomic.add(acc, 6, mx)
        cuda.atomic.add(acc, 7, my)
        cuda.atomic.add(acc, 8, mz)
        cuda.atomic.add(acc, 9, kinetic)
        cuda.syncthreads()

        if thermostat:  # trailing Langevin half-kick
            for e in range(tid, 3 * na, tpb):
                i = e // 3
                c = e - 3 * i
                p[d, i, c] = c1h * p[d, i, c] + noise[i] * (
                    xoroshiro128p_normal_float64(rng, state)
                )

        if tid == 0:
            amp[d, 0] = acc[0]
            amp[d, 1] = acc[1]
            amp[d, 2] = acc[2]
            mu_half[d, 0] = acc[3]
            mu_half[d, 1] = acc[4]
            mu_half[d, 2] = acc[5]
            mu_force[d, 0] = acc[6]
            mu_force[d, 1] = acc[7]
            mu_force[d, 2] = acc[8]
            energy[d] = acc[9] + potential[d]

    _INTEGRATOR_KERNELS = {"pre": k_pre, "post": k_post}
    return _INTEGRATOR_KERNELS
