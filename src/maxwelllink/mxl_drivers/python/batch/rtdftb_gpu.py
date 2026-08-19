# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
GPU-batched real-time TDDFTB Ehrenfest dynamics, built on the scalar ``RTDFTBModel``.

Thousands of independent DFTB systems can be advanced together, sharing the parameter set and
the basis layout.

This driver runs one execution path on each backend:

- **GPU** (``xp=cupy``): one CUDA block per system.
- **CPU reference** (``xp=numpy``): the same compiled physics through ``numba.njit``.
"""

import math
import multiprocessing
import os
from collections import namedtuple

import numpy as np

from maxwelllink.tools.recorders import (
    PropertyRecorder,
    XYZTrajectoryWriter,
    output_filename,
)
from maxwelllink.units import FS_TO_AU, K_TO_AU
from ..models.rtdftb_model.dftb_params import load_sk_set
from ..models.rtdftb_model.h0_overlap import MAX_INTEGRAL, MAX_ORB
from ..models.rtdftb_model.rtdftb_model import _PRE_NVT_DT_FS, RTDFTBModel
from ..models.rtdftb_model.scc import (
    ShellLayout,
    diis_mix,
    fermi_filling,
    limit_blas_threads,
    shell_charges,
)
from .dummy_gpu import BatchStepResult, DummyBatchModel

# Threads per block for the CUDA launches. A multiple of the warp size (32).
_THREADS_PER_BLOCK = 128

# Below this batch size S^-1 is inverted one system at a time through cuSOLVER
_INVERSE_BATCH_MIN = 16

#: Two-phase SCF (hybrid_precision): iterate in FP32 until the charge error drops
#: below this floor, then finish in FP64 to the requested tolerance. Single-precision
#: eigenvectors floor the Mulliken error at ~1e-6, so the switch sits safely above it.
_SCF_FP32_SWITCH = 1.0e-5
#: FP32 iterations without a factor-of-two error reduction before the FP64 phase is
#: forced -- degenerate frontier orbitals (metals) can stall the FP32 phase.
_SCF_FP32_STALL = 8

# Additional-data field names, in the component order the dipole arrays hold.
_HALF_DIPOLE_KEYS = ("mux_au", "muy_au", "muz_au")
_FORCE_DIPOLE_KEYS = ("mux_m_au", "muy_m_au", "muz_m_au")

# Scalar-driver flags a batch cannot honour, and why:
#   verbose    -- one print per system per step would swamp the run
#   checkpoint/restart -- the scalar driver writes one .npz per molecule ID
_UNSUPPORTED_GPU_FLAGS = ("verbose", "checkpoint", "restart")

# Radial-interpolation stencil width, slakoeqgrid.F90:78, and the difference table
# of the stencil; both size per-thread scratch, so they must be constants.
_N_INTERPOLATION = 8
_N_DELTA = _N_INTERPOLATION - 1

# Kernel argument bundles. numba accepts a namedtuple of device arrays as a kernel
# argument, and bundling matters: with the arrays passed individually the launch spends
# over a millisecond per step re-resolving argument types on the host, which at these
# batch sizes dwarfs the time the device actually needs.
_State = namedtuple(
    "_State",
    "rho rho_old coupling q_orb dq_atom dq_shell v_scc_shell v_shell v_orb h "
    "work_a work_b work_c energy_start e_kin mu_initial",
)
_Shared = namedtuple(
    "_Shared",
    "sk atom_species atom_offset orb_shell orb_atom shell_atom shell_u q0_orb mass",
)
#: Geometry-dependent matrices, indexed per system by the kernels. One shared copy is
#: held while every system sits at the same frozen geometry, and the kernels then read
#: zero-stride views of it; otherwise one copy per system.
_Geometry = namedtuple("_Geometry", "coords h0 overlap s_inv gamma e_repulsive")
_Nuclear = namedtuple("_Nuclear", "velocity half_velocity coords_next accel force")
_Out = namedtuple("_Out", "amp mu_end mu_half energy")

#: Per-system working storage. The Slater-Koster assembly scratch (``h0_*``, ``b_*``)
#: serves the CPU batch path, which steps the systems one after another; the GPU stage
#: kernels give every thread its own local copy instead. The matrices from ``density``
#: on are per system on both backends.
_Scratch = namedtuple(
    "_Scratch",
    "h0_sk_h h0_sk_s h0_block_h h0_block_s h0_core h0_node h0_cc h0_dd h0_delta "
    "h0_y_low h0_y_high "
    "b_sk_h b_sk_s b_dsk_h b_dsk_s b_d_h0 b_d_overlap b_weight b_first b_second "
    "b_radial b_angular b_core b_dcore "
    "density product weight_e weight gradient pair work_r pivot",
)

#: Persistent single-precision mirrors of the dense-algebra operands, allocated only
#: in hybrid mode (``RTDFTBModel(hybrid_precision=True)`` on the CUDA backend). The
#: FP64 arrays stay the state; these hold the per-step downcasts the FP32 cuBLAS/
#: cuSOLVER calls read, so no step allocates. Fields from ``overlap`` on exist only
#: when the nuclei move (``None`` otherwise).
_FP32 = namedtuple(
    "_FP32",
    "h s_inv rho t1 work_b work_c overlap coupling work_r density product weight",
)

#: The per-thread stages of one step. ``geometry`` and ``force`` exist only when the
#: nuclei move; the dense linear algebra between the stages is CuPy's.
_Stages = namedtuple("_Stages", "pre geometry post force")

# The stage kernels are compiled once per process and cached here, keyed by whether the
# nuclei move.
_STEP_KERNELS = {}


def _build_stage_kernels(ehrenfest):
    """
    Compile (once) and return the block-per-system stage kernels of one step.

    Parameters
    ----------
    ehrenfest : bool
        Whether the nuclei move, which decides which stages are built.

    Returns
    -------
    _Stages
        The compiled kernels, ready to launch with ``kernel[num, tpb]``.

    Raises
    ------
    ImportError
        If numba's CUDA target is unavailable.
    """

    if ehrenfest in _STEP_KERNELS:
        return _STEP_KERNELS[ehrenfest]

    try:
        from numba import cuda, float64
    except Exception as exc:  # ImportError, or a numba/CUDA runtime failure
        raise ImportError(
            "The GPU batch backend requires numba's CUDA target. Install it "
            "alongside CuPy, e.g. 'pip install maxwelllink[gpu-cuda12]'. On hosts "
            "without CUDA, inject xp=numpy to run the compiled CPU kernels instead."
        ) from exc

    from ..models.rtdftb_model.ehrenfest import CouplingScratch
    from ..models.rtdftb_model.forces import BandScratch
    from ..models.rtdftb_model.h0_overlap import H0Scratch
    from ..models.rtdftb_model.kernels_dftb import device_kernels

    d = device_kernels()
    scc_potential = d["scc_potential"]
    external_potential = d["external_potential"]
    scc_hamiltonian_row = d["scc_hamiltonian_row"]
    band_energy_row = d["band_energy_row"]
    scc_energy = d["scc_energy"]
    external_energy = d["external_energy"]
    rt_orbital_charge = d["rt_orbital_charge"]
    atom_charges = d["atom_charges"]
    shell_charges_from_orbital = d["shell_charges_from_orbital"]
    h0_overlap_onsite = d["h0_overlap_onsite"]
    h0_overlap_pair = d["h0_overlap_pair"]
    gamma_element = d["gamma_element"]
    repulsive_pair = d["repulsive_pair"]
    coupling_pair = d["coupling_pair"]
    nuclear_step = d["nuclear_step"]
    kinetic_sum = d["kinetic_sum"]
    real_part_row = d["real_part_row"]
    overlap_weight_row = d["overlap_weight_row"]
    band_gradient_pair = d["band_gradient_pair"]
    gamma_gradient_pair = d["gamma_gradient_pair"]
    field_gradient = d["field_gradient"]

    @cuda.jit(device=True)
    def ordered_pair(p, n_atom):
        """The ``p``-th ordered atom pair ``(a, b)``, ``a != b``."""

        a = p // (n_atom - 1)
        b = p - a * (n_atom - 1)
        if b >= a:
            b += 1
        return a, b

    @cuda.jit(device=True)
    def unordered_pair(p):
        """The ``p``-th unordered atom pair ``(a, b)``, ``a < b``, enumerated by ``b``."""

        b = int((1.0 + math.sqrt(1.0 + 8.0 * p)) * 0.5)
        while b * (b - 1) // 2 > p:
            b -= 1
        while (b + 1) * b // 2 <= p:
            b += 1
        return p - b * (b - 1) // 2, b

    @cuda.jit(device=True)
    def potentials(i, st, sh, coords, gamma, field, n_orb, n_shell):
        """Shell and orbital potentials of ONE system from its charges and the field."""

        scc_potential(
            gamma, st.dq_shell[i], sh.orb_shell, st.v_scc_shell[i], st.v_orb[i]
        )
        for s in range(n_shell):
            st.v_shell[i, s] = st.v_scc_shell[i, s]
        external_potential(coords, field[i], sh.shell_atom, st.v_shell[i], n_shell)
        for mu in range(n_orb):
            st.v_orb[i, mu] = st.v_shell[i, sh.orb_shell[mu]]

    @cuda.jit(device=True)
    def hamiltonian_rows(i, st, h0, overlap, n_orb, tid, tpb):
        """``H = H0 + 0.5 S (V_mu + V_nu)`` of ONE system, its rows over the threads."""

        for mu in range(tid, n_orb, tpb):
            scc_hamiltonian_row(h0, overlap, st.v_orb[i], st.h[i], mu)

    @cuda.jit(device=True)
    def band_partial(rho, h0, n_orb, tid, tpb):
        """This thread's rows of ``Tr(rho H0)``."""

        total = 0.0
        for mu in range(tid, n_orb, tpb):
            total += band_energy_row(rho, h0, mu, n_orb)
        return total

    @cuda.jit(device=True)
    def energy_rest(i, st, coords, field, e_rep, e_kin, n_shell, n_atom):
        """The energy of ONE system apart from its band term."""

        return (
            scc_energy(st.v_scc_shell[i], st.dq_shell[i], n_shell)
            + external_energy(st.dq_atom[i], coords, field[i], n_atom)
            + e_rep
            + e_kin
        )

    @cuda.jit(device=True)
    def charges(i, st, sh, coords, mu, n_orb, n_shell, n_atom):
        """Atom and shell charges and the dipole of ONE system, from ``q_orb``."""

        atom_charges(st.q_orb[i], sh.q0_orb, sh.orb_atom, st.dq_atom[i], n_orb, n_atom)
        shell_charges_from_orbital(
            st.q_orb[i], sh.q0_orb, sh.orb_shell, st.dq_shell[i], n_orb, n_shell
        )
        for k in range(3):
            mu[k] = 0.0
        for a in range(n_atom):
            for k in range(3):
                mu[k] -= coords[a, k] * st.dq_atom[i, a]

    @cuda.jit(device=True)
    def report(i, out, st, energy_start, energy_end, mu, dt):
        """Fill the reply the way the external DFTB+ driver builds it.

        Midpoint averages of the dipole and the energy, a finite-difference amplitude,
        and the same dipole reported twice (mxlrtdynamics.F90:96-102).
        """

        for k in range(3):
            end = mu[k] - st.mu_initial[i, k]
            start = out.mu_end[i, k]
            out.amp[i, k] = (end - start) / dt
            out.mu_half[i, k] = 0.5 * (start + end)
            out.mu_end[i, k] = end
        out.energy[i] = 0.5 * (energy_start + energy_end)

    if not ehrenfest:

        @cuda.jit
        def k_pre(st, sh, gm, field):
            """``H(t)`` and the energy at ``t`` of ONE system, at the fixed geometry."""

            i = cuda.blockIdx.x
            tid = cuda.threadIdx.x
            tpb = cuda.blockDim.x
            n_orb = st.rho.shape[1]
            n_shell = st.dq_shell.shape[1]
            n_atom = st.dq_atom.shape[1]
            acc = cuda.shared.array(1, float64)

            if tid == 0:
                acc[0] = 0.0
                potentials(i, st, sh, gm.coords[i], gm.gamma[i], field, n_orb, n_shell)
            cuda.syncthreads()
            hamiltonian_rows(i, st, gm.h0[i], gm.overlap[i], n_orb, tid, tpb)
            cuda.atomic.add(acc, 0, band_partial(st.rho[i], gm.h0[i], n_orb, tid, tpb))
            cuda.syncthreads()
            if tid == 0:
                st.energy_start[i] = acc[0] + energy_rest(
                    i, st, gm.coords[i], field, gm.e_repulsive[i], 0.0, n_shell, n_atom
                )

        @cuda.jit
        def k_post(st, sh, gm, out, field, dt):
            """Charges, dipole and energy at ``t + dt`` of ONE system, and its reply."""

            i = cuda.blockIdx.x
            tid = cuda.threadIdx.x
            tpb = cuda.blockDim.x
            n_orb = st.rho.shape[1]
            n_shell = st.dq_shell.shape[1]
            n_atom = st.dq_atom.shape[1]
            acc = cuda.shared.array(1, float64)
            mu = cuda.local.array(3, float64)

            # rho(t+dt) is in rho_old, where the leapfrog products left it
            for col in range(tid, n_orb, tpb):
                st.q_orb[i, col] = rt_orbital_charge(
                    st.rho_old[i], gm.overlap[i], col, n_orb
                )
            if tid == 0:
                acc[0] = 0.0
            cuda.syncthreads()
            if tid == 0:
                charges(i, st, sh, gm.coords[i], mu, n_orb, n_shell, n_atom)
                potentials(i, st, sh, gm.coords[i], gm.gamma[i], field, n_orb, n_shell)
            cuda.syncthreads()
            hamiltonian_rows(i, st, gm.h0[i], gm.overlap[i], n_orb, tid, tpb)
            cuda.atomic.add(
                acc, 0, band_partial(st.rho_old[i], gm.h0[i], n_orb, tid, tpb)
            )
            cuda.syncthreads()
            if tid == 0:
                energy_end = acc[0] + energy_rest(
                    i, st, gm.coords[i], field, gm.e_repulsive[i], 0.0, n_shell, n_atom
                )
                report(i, out, st, st.energy_start[i], energy_end, mu, dt)

        _STEP_KERNELS[ehrenfest] = _Stages(k_pre, None, k_post, None)
        return _STEP_KERNELS[ehrenfest]

    @cuda.jit
    def k_pre(st, sh, gm, nu, sc, field, dt):
        """Nuclear step, ``H(t)``, energy at ``t`` and ``D(t)`` of ONE system."""

        i = cuda.blockIdx.x
        tid = cuda.threadIdx.x
        tpb = cuda.blockDim.x
        n_orb = st.rho.shape[1]
        n_shell = st.dq_shell.shape[1]
        n_atom = st.dq_atom.shape[1]
        acc = cuda.shared.array(1, float64)
        # per-thread working storage of the coupling assembly
        cs = CouplingScratch(
            cuda.local.array(MAX_INTEGRAL, float64),
            cuda.local.array(MAX_INTEGRAL, float64),
            cuda.local.array((3, MAX_ORB, MAX_ORB), float64),
            cuda.local.array(_N_INTERPOLATION, float64),
            cuda.local.array(_N_INTERPOLATION, float64),
            cuda.local.array(_N_INTERPOLATION, float64),
            cuda.local.array((MAX_ORB, MAX_ORB), float64),
            cuda.local.array((3, MAX_ORB, MAX_ORB), float64),
            cuda.local.array((5, 5), float64),
            cuda.local.array((3, 5, 5), float64),
        )

        if tid == 0:
            acc[0] = 0.0
            # the integrator consumes a(t) and returns r(t+dt) together with v(t)
            nuclear_step(
                nu.coords_next[i],
                nu.half_velocity[i],
                nu.accel[i],
                dt,
                n_atom,
                nu.velocity[i],
            )
            st.e_kin[i] = kinetic_sum(sh.mass, nu.velocity[i], n_atom)
            potentials(i, st, sh, gm.coords[i], gm.gamma[i], field, n_orb, n_shell)
        # D is assembled onto a clean matrix (see coupling_kernel)
        for row in range(tid, n_orb, tpb):
            for col in range(n_orb):
                st.coupling[i, row, col] = 0.0
        cuda.syncthreads()

        hamiltonian_rows(i, st, gm.h0[i], gm.overlap[i], n_orb, tid, tpb)
        cuda.atomic.add(acc, 0, band_partial(st.rho[i], gm.h0[i], n_orb, tid, tpb))
        # D(t) is built at r(t) with v(t), before the geometry is adopted: one ordered
        # atom pair per thread and turn
        for p in range(tid, n_atom * (n_atom - 1), tpb):
            a, b = ordered_pair(p, n_atom)
            coupling_pair(
                sh.sk,
                gm.coords[i],
                sh.atom_species,
                sh.atom_offset,
                a,
                b,
                nu.velocity[i],
                st.coupling[i],
                cs,
            )
        cuda.syncthreads()
        if tid == 0:
            st.energy_start[i] = acc[0] + energy_rest(
                i,
                st,
                gm.coords[i],
                field,
                gm.e_repulsive[i],
                st.e_kin[i],
                n_shell,
                n_atom,
            )

    @cuda.jit
    def k_geometry(st, sh, gm, nu, sc):
        """Adopt ``r(t+dt)`` and rebuild ``H0``, ``S``, gamma and ``E_rep`` there."""

        i = cuda.blockIdx.x
        tid = cuda.threadIdx.x
        tpb = cuda.blockDim.x
        n_orb = st.rho.shape[1]
        n_shell = st.dq_shell.shape[1]
        n_atom = st.dq_atom.shape[1]
        acc = cuda.shared.array(1, float64)
        # per-thread working storage of the H0/S assembly and the repulsive spline
        hs = H0Scratch(
            cuda.local.array(MAX_INTEGRAL, float64),
            cuda.local.array(MAX_INTEGRAL, float64),
            cuda.local.array((MAX_ORB, MAX_ORB), float64),
            cuda.local.array((MAX_ORB, MAX_ORB), float64),
            cuda.local.array((5, 5), float64),
            cuda.local.array(_N_INTERPOLATION, float64),
            cuda.local.array(_N_INTERPOLATION, float64),
            cuda.local.array(_N_INTERPOLATION, float64),
            cuda.local.array(_N_DELTA, float64),
            cuda.local.array(MAX_INTEGRAL, float64),
            cuda.local.array(MAX_INTEGRAL, float64),
        )
        pair = cuda.local.array(2, float64)

        for a in range(tid, n_atom, tpb):
            for k in range(3):
                gm.coords[i, a, k] = nu.coords_next[i, a, k]
        # H0 and S start clean (see build_h0_overlap_kernel)
        for row in range(tid, n_orb, tpb):
            for col in range(n_orb):
                gm.h0[i, row, col] = 0.0
                gm.overlap[i, row, col] = 0.0
        if tid == 0:
            acc[0] = 0.0
        cuda.syncthreads()

        for atom in range(tid, n_atom, tpb):
            h0_overlap_onsite(
                sh.sk, sh.atom_species, sh.atom_offset, atom, gm.h0[i], gm.overlap[i]
            )
        rep = 0.0
        for p in range(tid, n_atom * (n_atom - 1) // 2, tpb):
            a, b = unordered_pair(p)
            h0_overlap_pair(
                sh.sk,
                gm.coords[i],
                sh.atom_species,
                sh.atom_offset,
                a,
                b,
                gm.h0[i],
                gm.overlap[i],
                hs,
            )
            e_pair, g_x, g_y, g_z = repulsive_pair(
                sh.sk, gm.coords[i], sh.atom_species, a, b, pair
            )
            rep += e_pair
        cuda.atomic.add(acc, 0, rep)
        for e in range(tid, n_shell * n_shell, tpb):
            row = e // n_shell
            col = e - row * n_shell
            gm.gamma[i, row, col] = gamma_element(
                gm.coords[i], sh.shell_atom, sh.shell_u, row, col
            )
        cuda.syncthreads()
        if tid == 0:
            gm.e_repulsive[i] = acc[0]

    @cuda.jit
    def k_post(st, sh, gm, sc, out, field, dt):
        """Charges, dipole, energy and reply at ``t + dt`` of ONE system, and ``Re[rho]``."""

        i = cuda.blockIdx.x
        tid = cuda.threadIdx.x
        tpb = cuda.blockDim.x
        n_orb = st.rho.shape[1]
        n_shell = st.dq_shell.shape[1]
        n_atom = st.dq_atom.shape[1]
        acc = cuda.shared.array(1, float64)
        mu = cuda.local.array(3, float64)

        for col in range(tid, n_orb, tpb):
            st.q_orb[i, col] = rt_orbital_charge(
                st.rho_old[i], gm.overlap[i], col, n_orb
            )
        for row in range(tid, n_orb, tpb):
            real_part_row(st.rho_old[i], sc.density[i], row, n_orb)
        if tid == 0:
            acc[0] = 0.0
        cuda.syncthreads()
        if tid == 0:
            charges(i, st, sh, gm.coords[i], mu, n_orb, n_shell, n_atom)
            potentials(i, st, sh, gm.coords[i], gm.gamma[i], field, n_orb, n_shell)
        cuda.syncthreads()
        hamiltonian_rows(i, st, gm.h0[i], gm.overlap[i], n_orb, tid, tpb)
        cuda.atomic.add(acc, 0, band_partial(st.rho_old[i], gm.h0[i], n_orb, tid, tpb))
        cuda.syncthreads()
        if tid == 0:
            energy_end = acc[0] + energy_rest(
                i,
                st,
                gm.coords[i],
                field,
                gm.e_repulsive[i],
                st.e_kin[i],
                n_shell,
                n_atom,
            )
            report(i, out, st, st.energy_start[i], energy_end, mu, dt)

    @cuda.jit
    def k_force(st, sh, gm, nu, sc, field):
        """The Ehrenfest force at ``r(t+dt)`` of ONE system, from the weighted densities."""

        i = cuda.blockIdx.x
        tid = cuda.threadIdx.x
        tpb = cuda.blockDim.x
        n_orb = st.rho.shape[1]
        n_shell = st.dq_shell.shape[1]
        n_atom = st.dq_atom.shape[1]
        # per-thread working storage of the band-gradient assembly and the repulsive
        bs = BandScratch(
            cuda.local.array(MAX_INTEGRAL, float64),
            cuda.local.array(MAX_INTEGRAL, float64),
            cuda.local.array(MAX_INTEGRAL, float64),
            cuda.local.array(MAX_INTEGRAL, float64),
            cuda.local.array((3, MAX_ORB, MAX_ORB), float64),
            cuda.local.array((3, MAX_ORB, MAX_ORB), float64),
            cuda.local.array(_N_INTERPOLATION, float64),
            cuda.local.array(_N_INTERPOLATION, float64),
            cuda.local.array(_N_INTERPOLATION, float64),
            cuda.local.array((MAX_ORB, MAX_ORB), float64),
            cuda.local.array((3, MAX_ORB, MAX_ORB), float64),
            cuda.local.array((5, 5), float64),
            cuda.local.array((3, 5, 5), float64),
        )
        pair = cuda.local.array(2, float64)

        for row in range(tid, n_orb, tpb):
            overlap_weight_row(
                sc.density[i], sc.weight_e[i], st.v_orb[i], sc.weight[i], row
            )
        for a in range(tid, n_atom, tpb):
            for k in range(3):
                sc.gradient[i, a, k] = 0.0
        cuda.syncthreads()

        # the pair sums accumulate onto the atoms with atomics; their order is not
        # fixed, so the gradient differs from the serial sum at round-off only
        for p in range(tid, n_atom * (n_atom - 1), tpb):
            a, b = ordered_pair(p, n_atom)
            g_x, g_y, g_z = band_gradient_pair(
                sh.sk,
                gm.coords[i],
                sh.atom_species,
                sh.atom_offset,
                a,
                b,
                sc.density[i],
                sc.weight[i],
                bs,
            )
            cuda.atomic.add(sc.gradient, (i, a, 0), g_x)
            cuda.atomic.add(sc.gradient, (i, a, 1), g_y)
            cuda.atomic.add(sc.gradient, (i, a, 2), g_z)
        for e in range(tid, n_shell * n_shell, tpb):
            row = e // n_shell
            col = e - row * n_shell
            atom_row = sh.shell_atom[row]
            atom_col = sh.shell_atom[col]
            if atom_row != atom_col:
                f_x, f_y, f_z = gamma_gradient_pair(
                    gm.coords[i], sh.shell_atom, sh.shell_u, st.dq_shell[i], row, col
                )
                cuda.atomic.add(sc.gradient, (i, atom_row, 0), f_x)
                cuda.atomic.add(sc.gradient, (i, atom_row, 1), f_y)
                cuda.atomic.add(sc.gradient, (i, atom_row, 2), f_z)
                cuda.atomic.add(sc.gradient, (i, atom_col, 0), -f_x)
                cuda.atomic.add(sc.gradient, (i, atom_col, 1), -f_y)
                cuda.atomic.add(sc.gradient, (i, atom_col, 2), -f_z)
        for p in range(tid, n_atom * (n_atom - 1) // 2, tpb):
            a, b = unordered_pair(p)
            e_pair, g_x, g_y, g_z = repulsive_pair(
                sh.sk, gm.coords[i], sh.atom_species, a, b, pair
            )
            cuda.atomic.add(sc.gradient, (i, a, 0), g_x)
            cuda.atomic.add(sc.gradient, (i, a, 1), g_y)
            cuda.atomic.add(sc.gradient, (i, a, 2), g_z)
            cuda.atomic.add(sc.gradient, (i, b, 0), -g_x)
            cuda.atomic.add(sc.gradient, (i, b, 1), -g_y)
            cuda.atomic.add(sc.gradient, (i, b, 2), -g_z)
        cuda.syncthreads()
        if tid == 0:
            field_gradient(st.dq_atom[i], field[i], sc.gradient[i], n_atom)
        cuda.syncthreads()
        for a in range(tid, n_atom, tpb):
            for k in range(3):
                nu.force[i, a, k] = -sc.gradient[i, a, k]
                nu.accel[i, a, k] = nu.force[i, a, k] / sh.mass[a]

    _STEP_KERNELS[ehrenfest] = _Stages(k_pre, k_geometry, k_post, k_force)
    return _STEP_KERNELS[ehrenfest]


#: The phase kernels of the wide launch path, cached like :data:`_STEP_KERNELS`.
#: ``pre_start``, ``pre_work`` and everything from ``geo_reset`` on exist only when the
#: nuclei move (the frozen path replaces ``pre_work`` by its own variant).
_WideStages = namedtuple(
    "_WideStages",
    "scc_rows finish_potentials pre_start pre_work energy_start "
    "geo_reset geo_build post_prep post_charges post_ham report "
    "force_weight force_pairs force_finish",
)

_WIDE_STEP_KERNELS = {}


def _build_wide_stage_kernels(ehrenfest):
    """
    Compile (once) the phase-split kernels that spread ONE system over MANY blocks.

    Parameters
    ----------
    ehrenfest : bool
        Whether the nuclei move, which decides which phases are built.

    Returns
    -------
    _WideStages
        The compiled kernels; wide ones launch with ``kernel[(num, bps), tpb]``, narrow
        ones with ``kernel[num, tpb]``.
    """

    if ehrenfest in _WIDE_STEP_KERNELS:
        return _WIDE_STEP_KERNELS[ehrenfest]

    from numba import cuda, float64

    from ..models.rtdftb_model.ehrenfest import CouplingScratch
    from ..models.rtdftb_model.forces import BandScratch
    from ..models.rtdftb_model.h0_overlap import H0Scratch
    from ..models.rtdftb_model.kernels_dftb import device_kernels

    d = device_kernels()
    scc_potential_row = d["scc_potential_row"]
    external_potential = d["external_potential"]
    scc_hamiltonian_row = d["scc_hamiltonian_row"]
    band_energy_row = d["band_energy_row"]
    scc_energy = d["scc_energy"]
    external_energy = d["external_energy"]
    rt_orbital_charge = d["rt_orbital_charge"]
    atom_charges = d["atom_charges"]
    shell_charges_from_orbital = d["shell_charges_from_orbital"]
    h0_overlap_onsite = d["h0_overlap_onsite"]
    h0_overlap_pair = d["h0_overlap_pair"]
    gamma_element = d["gamma_element"]
    repulsive_pair = d["repulsive_pair"]
    coupling_pair = d["coupling_pair"]
    nuclear_step = d["nuclear_step"]
    kinetic_sum = d["kinetic_sum"]
    real_part_row = d["real_part_row"]
    overlap_weight_row = d["overlap_weight_row"]
    band_gradient_pair = d["band_gradient_pair"]
    gamma_gradient_pair = d["gamma_gradient_pair"]
    field_gradient = d["field_gradient"]

    @cuda.jit(device=True)
    def ordered_pair(p, n_atom):
        """The ``p``-th ordered atom pair ``(a, b)``, ``a != b``."""

        a = p // (n_atom - 1)
        b = p - a * (n_atom - 1)
        if b >= a:
            b += 1
        return a, b

    @cuda.jit(device=True)
    def unordered_pair(p):
        """The ``p``-th unordered atom pair ``(a, b)``, ``a < b``, enumerated by ``b``."""

        b = int((1.0 + math.sqrt(1.0 + 8.0 * p)) * 0.5)
        while b * (b - 1) // 2 > p:
            b -= 1
        while (b + 1) * b // 2 <= p:
            b += 1
        return p - b * (b - 1) // 2, b

    @cuda.jit(device=True)
    def band_partial(rho, h0, n_orb, worker, n_workers):
        """This worker's rows of ``Tr(rho H0)``."""

        total = 0.0
        for mu in range(worker, n_orb, n_workers):
            total += band_energy_row(rho, h0, mu, n_orb)
        return total

    @cuda.jit
    def k_scc_rows(st, sh, gm):
        """The ``n_shell^2`` gamma contraction, its rows over ALL the workers."""

        i = cuda.blockIdx.x
        worker = cuda.blockIdx.y * cuda.blockDim.x + cuda.threadIdx.x
        n_workers = cuda.gridDim.y * cuda.blockDim.x
        n_shell = st.dq_shell.shape[1]
        for row in range(worker, n_shell, n_workers):
            scc_potential_row(gm.gamma[i], st.dq_shell[i], st.v_scc_shell[i], row)

    @cuda.jit
    def k_finish_potentials(st, sh, gm, field):
        """External potential and the orbital expansion; narrow, one block per system."""

        i = cuda.blockIdx.x
        tid = cuda.threadIdx.x
        tpb = cuda.blockDim.x
        n_orb = st.rho.shape[1]
        n_shell = st.dq_shell.shape[1]
        for s in range(tid, n_shell, tpb):
            st.v_shell[i, s] = st.v_scc_shell[i, s]
        cuda.syncthreads()
        if tid == 0:
            external_potential(
                gm.coords[i], field[i], sh.shell_atom, st.v_shell[i], n_shell
            )
        cuda.syncthreads()
        for mu in range(tid, n_orb, tpb):
            st.v_orb[i, mu] = st.v_shell[i, sh.orb_shell[mu]]

    @cuda.jit
    def k_energy_start(st, sh, gm, field, acc):
        """``E(t)`` from the accumulated band term; narrow."""

        i = cuda.blockIdx.x
        if cuda.threadIdx.x == 0:
            n_shell = st.dq_shell.shape[1]
            n_atom = st.dq_atom.shape[1]
            st.energy_start[i] = acc[i] + (
                scc_energy(st.v_scc_shell[i], st.dq_shell[i], n_shell)
                + external_energy(st.dq_atom[i], gm.coords[i], field[i], n_atom)
                + gm.e_repulsive[i]
                + st.e_kin[i]
            )

    @cuda.jit
    def k_post_charges(st, sh, gm, mu_out, rep_acc, adopt_rep):
        """Charges and dipole at ``t + dt``; narrow serial bookkeeping."""

        i = cuda.blockIdx.x
        if cuda.threadIdx.x == 0:
            n_orb = st.rho.shape[1]
            n_shell = st.dq_shell.shape[1]
            n_atom = st.dq_atom.shape[1]
            if adopt_rep != 0:
                gm.e_repulsive[i] = rep_acc[i]
            atom_charges(
                st.q_orb[i], sh.q0_orb, sh.orb_atom, st.dq_atom[i], n_orb, n_atom
            )
            shell_charges_from_orbital(
                st.q_orb[i], sh.q0_orb, sh.orb_shell, st.dq_shell[i], n_orb, n_shell
            )
            for k in range(3):
                mu_out[i, k] = 0.0
            for a in range(n_atom):
                for k in range(3):
                    mu_out[i, k] -= st.dq_atom[i, a] * gm.coords[i, a, k]

    @cuda.jit
    def k_report(st, sh, gm, out, field, mu_in, acc, dt):
        """``E(t+dt)`` and the reply to MaxwellLink; narrow."""

        i = cuda.blockIdx.x
        if cuda.threadIdx.x == 0:
            n_shell = st.dq_shell.shape[1]
            n_atom = st.dq_atom.shape[1]
            energy_end = acc[i] + (
                scc_energy(st.v_scc_shell[i], st.dq_shell[i], n_shell)
                + external_energy(st.dq_atom[i], gm.coords[i], field[i], n_atom)
                + gm.e_repulsive[i]
                + st.e_kin[i]
            )
            for k in range(3):
                end = mu_in[i, k] - st.mu_initial[i, k]
                start = out.mu_end[i, k]
                out.amp[i, k] = (end - start) / dt
                out.mu_half[i, k] = 0.5 * (start + end)
                out.mu_end[i, k] = end
            out.energy[i] = 0.5 * (st.energy_start[i] + energy_end)

    @cuda.jit
    def k_geo_build(st, sh, gm, acc):
        """``H0``, ``S``, gamma and the repulsive sum at the current coords; wide.

        Shared by both families: the Ehrenfest step rebuilds the geometry every
        step, and the fully-GPU initialization builds it once for frozen runs too.
        """

        i = cuda.blockIdx.x
        worker = cuda.blockIdx.y * cuda.blockDim.x + cuda.threadIdx.x
        n_workers = cuda.gridDim.y * cuda.blockDim.x
        n_shell = st.dq_shell.shape[1]
        n_atom = st.dq_atom.shape[1]
        hs = H0Scratch(
            cuda.local.array(MAX_INTEGRAL, float64),
            cuda.local.array(MAX_INTEGRAL, float64),
            cuda.local.array((MAX_ORB, MAX_ORB), float64),
            cuda.local.array((MAX_ORB, MAX_ORB), float64),
            cuda.local.array((5, 5), float64),
            cuda.local.array(_N_INTERPOLATION, float64),
            cuda.local.array(_N_INTERPOLATION, float64),
            cuda.local.array(_N_INTERPOLATION, float64),
            cuda.local.array(_N_DELTA, float64),
            cuda.local.array(MAX_INTEGRAL, float64),
            cuda.local.array(MAX_INTEGRAL, float64),
        )
        pair = cuda.local.array(2, float64)

        for atom in range(worker, n_atom, n_workers):
            h0_overlap_onsite(
                sh.sk, sh.atom_species, sh.atom_offset, atom, gm.h0[i], gm.overlap[i]
            )
        rep = 0.0
        for p in range(worker, n_atom * (n_atom - 1) // 2, n_workers):
            a, b = unordered_pair(p)
            h0_overlap_pair(
                sh.sk,
                gm.coords[i],
                sh.atom_species,
                sh.atom_offset,
                a,
                b,
                gm.h0[i],
                gm.overlap[i],
                hs,
            )
            e_pair, g_x, g_y, g_z = repulsive_pair(
                sh.sk, gm.coords[i], sh.atom_species, a, b, pair
            )
            rep += e_pair
        cuda.atomic.add(acc, i, rep)
        for e in range(worker, n_shell * n_shell, n_workers):
            row = e // n_shell
            col = e - row * n_shell
            gm.gamma[i, row, col] = gamma_element(
                gm.coords[i], sh.shell_atom, sh.shell_u, row, col
            )

    if not ehrenfest:

        @cuda.jit
        def k_pre_work_frozen(st, sh, gm, acc):
            """``H(t)`` rows and the band term of ``rho(t)``, over ALL the workers."""

            i = cuda.blockIdx.x
            worker = cuda.blockIdx.y * cuda.blockDim.x + cuda.threadIdx.x
            n_workers = cuda.gridDim.y * cuda.blockDim.x
            n_orb = st.rho.shape[1]
            for mu in range(worker, n_orb, n_workers):
                scc_hamiltonian_row(gm.h0[i], gm.overlap[i], st.v_orb[i], st.h[i], mu)
            cuda.atomic.add(
                acc, i, band_partial(st.rho[i], gm.h0[i], n_orb, worker, n_workers)
            )

        @cuda.jit
        def k_post_prep_frozen(st, sh, gm):
            """Orbital charges of ``rho(t+dt)``, its columns over ALL the workers."""

            i = cuda.blockIdx.x
            worker = cuda.blockIdx.y * cuda.blockDim.x + cuda.threadIdx.x
            n_workers = cuda.gridDim.y * cuda.blockDim.x
            n_orb = st.rho.shape[1]
            for col in range(worker, n_orb, n_workers):
                st.q_orb[i, col] = rt_orbital_charge(
                    st.rho_old[i], gm.overlap[i], col, n_orb
                )

        @cuda.jit
        def k_post_ham_frozen(st, sh, gm, acc):
            """``H(t+dt)`` rows and the band term of ``rho(t+dt)``."""

            i = cuda.blockIdx.x
            worker = cuda.blockIdx.y * cuda.blockDim.x + cuda.threadIdx.x
            n_workers = cuda.gridDim.y * cuda.blockDim.x
            n_orb = st.rho.shape[1]
            for mu in range(worker, n_orb, n_workers):
                scc_hamiltonian_row(gm.h0[i], gm.overlap[i], st.v_orb[i], st.h[i], mu)
            cuda.atomic.add(
                acc, i, band_partial(st.rho_old[i], gm.h0[i], n_orb, worker, n_workers)
            )

        _WIDE_STEP_KERNELS[ehrenfest] = _WideStages(
            k_scc_rows,
            k_finish_potentials,
            None,
            k_pre_work_frozen,
            k_energy_start,
            None,
            k_geo_build,
            k_post_prep_frozen,
            k_post_charges,
            k_post_ham_frozen,
            k_report,
            None,
            None,
            None,
        )
        return _WIDE_STEP_KERNELS[ehrenfest]

    @cuda.jit
    def k_pre_start(st, sh, gm, nu, field, dt):
        """Nuclear step, kinetic energy, gamma rows and a clean ``D``; wide."""

        i = cuda.blockIdx.x
        worker = cuda.blockIdx.y * cuda.blockDim.x + cuda.threadIdx.x
        n_workers = cuda.gridDim.y * cuda.blockDim.x
        n_orb = st.rho.shape[1]
        n_shell = st.dq_shell.shape[1]
        n_atom = st.dq_atom.shape[1]
        if worker == 0:
            nuclear_step(
                nu.coords_next[i],
                nu.half_velocity[i],
                nu.accel[i],
                dt,
                n_atom,
                nu.velocity[i],
            )
            st.e_kin[i] = kinetic_sum(sh.mass, nu.velocity[i], n_atom)
        for row in range(worker, n_shell, n_workers):
            scc_potential_row(gm.gamma[i], st.dq_shell[i], st.v_scc_shell[i], row)
        for e in range(worker, n_orb * n_orb, n_workers):
            row = e // n_orb
            col = e - row * n_orb
            st.coupling[i, row, col] = 0.0

    @cuda.jit
    def k_pre_work(st, sh, gm, nu, acc):
        """``H(t)`` rows, the band term of ``rho(t)`` and ``D(t)``; wide."""

        i = cuda.blockIdx.x
        worker = cuda.blockIdx.y * cuda.blockDim.x + cuda.threadIdx.x
        n_workers = cuda.gridDim.y * cuda.blockDim.x
        n_orb = st.rho.shape[1]
        n_atom = st.dq_atom.shape[1]
        cs = CouplingScratch(
            cuda.local.array(MAX_INTEGRAL, float64),
            cuda.local.array(MAX_INTEGRAL, float64),
            cuda.local.array((3, MAX_ORB, MAX_ORB), float64),
            cuda.local.array(_N_INTERPOLATION, float64),
            cuda.local.array(_N_INTERPOLATION, float64),
            cuda.local.array(_N_INTERPOLATION, float64),
            cuda.local.array((MAX_ORB, MAX_ORB), float64),
            cuda.local.array((3, MAX_ORB, MAX_ORB), float64),
            cuda.local.array((5, 5), float64),
            cuda.local.array((3, 5, 5), float64),
        )
        for mu in range(worker, n_orb, n_workers):
            scc_hamiltonian_row(gm.h0[i], gm.overlap[i], st.v_orb[i], st.h[i], mu)
        cuda.atomic.add(
            acc, i, band_partial(st.rho[i], gm.h0[i], n_orb, worker, n_workers)
        )
        for p in range(worker, n_atom * (n_atom - 1), n_workers):
            a, b = ordered_pair(p, n_atom)
            coupling_pair(
                sh.sk,
                gm.coords[i],
                sh.atom_species,
                sh.atom_offset,
                a,
                b,
                nu.velocity[i],
                st.coupling[i],
                cs,
            )

    @cuda.jit
    def k_geo_reset(st, gm, nu):
        """Adopt ``r(t+dt)`` and clean ``H0`` and ``S``; wide."""

        i = cuda.blockIdx.x
        worker = cuda.blockIdx.y * cuda.blockDim.x + cuda.threadIdx.x
        n_workers = cuda.gridDim.y * cuda.blockDim.x
        n_orb = st.rho.shape[1]
        n_atom = st.dq_atom.shape[1]
        for a in range(worker, n_atom, n_workers):
            for k in range(3):
                gm.coords[i, a, k] = nu.coords_next[i, a, k]
        for e in range(worker, n_orb * n_orb, n_workers):
            row = e // n_orb
            col = e - row * n_orb
            gm.h0[i, row, col] = 0.0
            gm.overlap[i, row, col] = 0.0

    @cuda.jit
    def k_post_prep(st, sh, gm, sc):
        """Orbital charges and ``Re[rho(t+dt)]``; wide."""

        i = cuda.blockIdx.x
        worker = cuda.blockIdx.y * cuda.blockDim.x + cuda.threadIdx.x
        n_workers = cuda.gridDim.y * cuda.blockDim.x
        n_orb = st.rho.shape[1]
        for col in range(worker, n_orb, n_workers):
            st.q_orb[i, col] = rt_orbital_charge(
                st.rho_old[i], gm.overlap[i], col, n_orb
            )
        for row in range(worker, n_orb, n_workers):
            real_part_row(st.rho_old[i], sc.density[i], row, n_orb)

    @cuda.jit
    def k_post_ham(st, sh, gm, acc):
        """``H(t+dt)`` rows and the band term of ``rho(t+dt)``; wide."""

        i = cuda.blockIdx.x
        worker = cuda.blockIdx.y * cuda.blockDim.x + cuda.threadIdx.x
        n_workers = cuda.gridDim.y * cuda.blockDim.x
        n_orb = st.rho.shape[1]
        for mu in range(worker, n_orb, n_workers):
            scc_hamiltonian_row(gm.h0[i], gm.overlap[i], st.v_orb[i], st.h[i], mu)
        cuda.atomic.add(
            acc, i, band_partial(st.rho_old[i], gm.h0[i], n_orb, worker, n_workers)
        )

    @cuda.jit
    def k_force_weight(st, sc):
        """The overlap-weighted density and a clean gradient; wide."""

        i = cuda.blockIdx.x
        worker = cuda.blockIdx.y * cuda.blockDim.x + cuda.threadIdx.x
        n_workers = cuda.gridDim.y * cuda.blockDim.x
        n_orb = st.rho.shape[1]
        n_atom = st.dq_atom.shape[1]
        for row in range(worker, n_orb, n_workers):
            overlap_weight_row(
                sc.density[i], sc.weight_e[i], st.v_orb[i], sc.weight[i], row
            )
        for a in range(worker, n_atom, n_workers):
            for k in range(3):
                sc.gradient[i, a, k] = 0.0

    @cuda.jit
    def k_force_pairs(st, sh, gm, sc):
        """Band, gamma and repulsive gradient sums, atomically onto the atoms; wide."""

        i = cuda.blockIdx.x
        worker = cuda.blockIdx.y * cuda.blockDim.x + cuda.threadIdx.x
        n_workers = cuda.gridDim.y * cuda.blockDim.x
        n_shell = st.dq_shell.shape[1]
        n_atom = st.dq_atom.shape[1]
        bs = BandScratch(
            cuda.local.array(MAX_INTEGRAL, float64),
            cuda.local.array(MAX_INTEGRAL, float64),
            cuda.local.array(MAX_INTEGRAL, float64),
            cuda.local.array(MAX_INTEGRAL, float64),
            cuda.local.array((3, MAX_ORB, MAX_ORB), float64),
            cuda.local.array((3, MAX_ORB, MAX_ORB), float64),
            cuda.local.array(_N_INTERPOLATION, float64),
            cuda.local.array(_N_INTERPOLATION, float64),
            cuda.local.array(_N_INTERPOLATION, float64),
            cuda.local.array((MAX_ORB, MAX_ORB), float64),
            cuda.local.array((3, MAX_ORB, MAX_ORB), float64),
            cuda.local.array((5, 5), float64),
            cuda.local.array((3, 5, 5), float64),
        )
        pair = cuda.local.array(2, float64)

        for p in range(worker, n_atom * (n_atom - 1), n_workers):
            a, b = ordered_pair(p, n_atom)
            g_x, g_y, g_z = band_gradient_pair(
                sh.sk,
                gm.coords[i],
                sh.atom_species,
                sh.atom_offset,
                a,
                b,
                sc.density[i],
                sc.weight[i],
                bs,
            )
            cuda.atomic.add(sc.gradient, (i, a, 0), g_x)
            cuda.atomic.add(sc.gradient, (i, a, 1), g_y)
            cuda.atomic.add(sc.gradient, (i, a, 2), g_z)
        for e in range(worker, n_shell * n_shell, n_workers):
            row = e // n_shell
            col = e - row * n_shell
            atom_row = sh.shell_atom[row]
            atom_col = sh.shell_atom[col]
            if atom_row != atom_col:
                f_x, f_y, f_z = gamma_gradient_pair(
                    gm.coords[i], sh.shell_atom, sh.shell_u, st.dq_shell[i], row, col
                )
                cuda.atomic.add(sc.gradient, (i, atom_row, 0), f_x)
                cuda.atomic.add(sc.gradient, (i, atom_row, 1), f_y)
                cuda.atomic.add(sc.gradient, (i, atom_row, 2), f_z)
                cuda.atomic.add(sc.gradient, (i, atom_col, 0), -f_x)
                cuda.atomic.add(sc.gradient, (i, atom_col, 1), -f_y)
                cuda.atomic.add(sc.gradient, (i, atom_col, 2), -f_z)
        for p in range(worker, n_atom * (n_atom - 1) // 2, n_workers):
            a, b = unordered_pair(p)
            e_pair, g_x, g_y, g_z = repulsive_pair(
                sh.sk, gm.coords[i], sh.atom_species, a, b, pair
            )
            cuda.atomic.add(sc.gradient, (i, a, 0), g_x)
            cuda.atomic.add(sc.gradient, (i, a, 1), g_y)
            cuda.atomic.add(sc.gradient, (i, a, 2), g_z)
            cuda.atomic.add(sc.gradient, (i, b, 0), -g_x)
            cuda.atomic.add(sc.gradient, (i, b, 1), -g_y)
            cuda.atomic.add(sc.gradient, (i, b, 2), -g_z)

    @cuda.jit
    def k_force_finish(st, sh, nu, sc, field):
        """Field force, then the total force and acceleration; narrow."""

        i = cuda.blockIdx.x
        tid = cuda.threadIdx.x
        tpb = cuda.blockDim.x
        n_atom = st.dq_atom.shape[1]
        if tid == 0:
            field_gradient(st.dq_atom[i], field[i], sc.gradient[i], n_atom)
        cuda.syncthreads()
        for a in range(tid, n_atom, tpb):
            for k in range(3):
                nu.force[i, a, k] = -sc.gradient[i, a, k]
                nu.accel[i, a, k] = nu.force[i, a, k] / sh.mass[a]

    _WIDE_STEP_KERNELS[ehrenfest] = _WideStages(
        k_scc_rows,
        k_finish_potentials,
        k_pre_start,
        k_pre_work,
        k_energy_start,
        k_geo_reset,
        k_geo_build,
        k_post_prep,
        k_post_charges,
        k_post_ham,
        k_report,
        k_force_weight,
        k_force_pairs,
        k_force_finish,
    )
    return _WIDE_STEP_KERNELS[ehrenfest]


class RTDFTBGPUBatchModel(DummyBatchModel):
    """
    Vectorized real-time TD-DFTB batch model with or without Ehrenfest motion.
    """

    def __init__(
        self,
        *,
        num,
        driver_kwargs,
        xp,
        driver_args=None,
        store_additional_data=False,
        blocks_per_system=None,
    ):
        """
        Build the batch model from one scalar-driver template.

        Parameters
        ----------
        num : int
            Number of systems in the batch.
        driver_kwargs : dict
            Keyword arguments of the scalar :class:`RTDFTBModel` template.
        xp : module
            Array module: ``numpy`` for the CPU path, ``cupy`` for the GPU path.
        driver_args : tuple, optional
            Positional arguments of the template.
        blocks_per_system : int, optional
            CUDA blocks each system's stage kernels are spread over. ``None`` (the
            default) resolves automatically: one block per system while the batch
            alone fills the GPU (thousands of systems), and enough blocks to fill it
            when ``num`` is small and the systems are large. Set explicitly
            only for benchmarking or testing.
        store_additional_data : bool, default: False
            Accepted for parity with the other batch models. The extras are always
            built, because the cavity solvers read the dipole from them.
        """

        if int(num) <= 0:
            raise ValueError("num must be a positive integer.")
        self.xp = xp
        self.num = int(num)

        self._driver_args = tuple(driver_args or ())
        self._driver_kwargs = dict(driver_kwargs or {})
        template = RTDFTBModel(*self._driver_args, **self._driver_kwargs)
        for flag in _UNSUPPORTED_GPU_FLAGS:
            if getattr(template, flag, False):
                raise ValueError(
                    f"{flag}=True is not supported by the GPU batch RT-DFTB model."
                )
        if template.propagator != "leapfrog":
            raise ValueError(
                "the GPU batch RT-DFTB model implements the leapfrog propagator only; "
                f"got propagator={template.propagator!r}."
            )
        self._template = template
        self.ehrenfest = template.ehrenfest
        self.reset_dipole = template.reset_dipole
        self.hybrid_precision = template.hybrid_precision
        self.gpu_init = template.gpu_init
        if blocks_per_system is not None:
            blocks_per_system = int(blocks_per_system)
            if blocks_per_system < 1:
                raise ValueError("blocks_per_system must be a positive integer.")
        #: CUDA blocks per system for the stage kernels. ``None`` picks automatically:
        #: one block per system when the batch alone fills the GPU, many otherwise
        #: (a small batch of huge systems, e.g. one nanoparticle on one GPU).
        self.blocks_per_system = blocks_per_system
        # run-time output, as the scalar driver configures it; one file for the whole
        # batch, opened in initialize()
        self.property_filename = template.property_filename
        self.traj_filename = template.traj_filename
        self.record_every_steps = template.record_every_steps
        self.record_max_steps = template.record_max_steps
        self.record_names = template.record_names
        self.symbols = template.elements
        self._recorder = None
        self._trajectory = None
        self._step_index = 0
        # the batch writes for everyone: the template and the per-molecule scalar
        # initializations must not open files of their own
        template.property_filename = template.traj_filename = None
        self._driver_kwargs.update(property_filename=None, traj_filename=None)

        # state, all set in initialize()
        self.dt = 0.0  # shared time step in a.u.
        self.t = 0.0  # current time in a.u.
        self.molecule_ids = ()
        self.n_orb = 0
        self.n_atom = 0
        self.n_shell = 0
        self._on_gpu = False
        self._hybrid = False  # hybrid_precision resolved against the backend
        self._fp32 = None  # the _FP32 mirrors, allocated only in hybrid mode
        self._wide_bps = 1  # blocks per system resolved in initialize()
        self._gpu_layout = None  # ShellLayout of the fully-GPU initialization
        self._scf_iterations = None  # (n_fp32, n_fp64) of the last GPU ground state
        self._wide_kernels = None
        self._wide_acc = None  # (num,) reduction target of the wide phase kernels
        self._wide_mu = None  # (num, 3) dipole hand-over between wide phases
        self._shared_geometry = True  # one geometry for all, until initialize() says
        self._kernels = None
        self._bundles = None
        self._field = None
        self._coords = None
        self._rho = None  # (num, n_orb, n_orb) complex128, the current density
        self._rho_old = None  # the previous one; the two are swapped every step
        self._velocity = None  # nuclear state, allocated only when ehrenfest
        self._amp = None  # (num, 3) reusable output buffers
        self._mu_end = None
        self._mu_half = None
        self._energy = None
        self._stepped = False

    # ----------------------- heavy-load initialization ------------------------------
    def initialize(self, dt_au, molecule_ids):
        """
        Initialize every system as its scalar driver would and allocate the batch state.

        Parameters
        ----------
        dt_au : float
            The time step in atomic units, shared by every system.
        molecule_ids : array-like of int
            Molecule IDs assigned by the hub, one per system.
        """

        xp, num = self.xp, self.num
        self.molecule_ids = tuple(int(mid) for mid in molecule_ids)
        if len(self.molecule_ids) != num:
            raise ValueError(
                f"got {len(self.molecule_ids)} molecule IDs for a batch of {num}."
            )
        self.dt = float(dt_au)
        self.t = 0.0
        self._on_gpu = getattr(xp, "__name__", "") == "cupy"
        self._hybrid = bool(self.hybrid_precision and self._on_gpu)
        if self.hybrid_precision:
            print(
                "[RTDFTBGPUBatchModel] hybrid FP32/FP64 precision "
                + (
                    "enabled: FP32 dense algebra, FP64 state."
                    if self._hybrid
                    else "requested, but the CPU backend is FP64 only; ignored."
                )
            )

        # Every system starts as the scalar driver of its molecule ID would: ground
        # state, pre-NVT, velocities, kick and bootstrap all come from
        # RTDFTBModel.initialize(). With identical initial conditions one template is
        # run and copied into every row; when the template declares per-molecule
        # conditions -- batch_xyz, pre_nvt, sampled velocities -- every molecule ID is
        # initialized on its own, so the batch is exact rather than merely similar.
        template = self._template
        # None means "the driver decides": on for the CUDA backend, off elsewhere.
        wanted = self.gpu_init if self.gpu_init is not None else self._on_gpu
        gpu_boot = bool(
            wanted and self._on_gpu and not (template.pre_nvt and not self.ehrenfest)
        )
        if wanted and not gpu_boot:
            reason = (
                "pre_nvt without ehrenfest keeps the CPU ground state"
                if self._on_gpu
                else "the backend is numpy"
            )
            print(
                f"[RTDFTBGPUBatchModel] gpu_init requested, but {reason}; "
                "using the CPU initialization."
            )
        per_system = (
            template.batch_frames is not None
            or template.pre_nvt
            or (template.ehrenfest and template.init_velocities)
        )
        if gpu_boot:
            # topology and geometry only; the ground state, the kick and the
            # bootstrap run on the device once the batch arrays exist
            template._prepare(self.dt, self.molecule_ids[0])
            self._gpu_layout = ShellLayout(template.system, shell_resolved=False)
            source_ids = self.molecule_ids if per_system else (self.molecule_ids[0],)
            rows = [self._topology_row(template, mid) for mid in source_ids]
        elif per_system:
            rows = _initial_states(
                self._driver_args, self._driver_kwargs, self.dt, self.molecule_ids
            )
        else:
            template.initialize(self.dt, self.molecule_ids[0])
            rows = [_scalar_state(template)]
        first = rows[0]

        self.n_orb = n = first["n_orb"]
        self.n_atom = first["n_atom"]
        self.n_shell = n_shell = first["n_shell"]
        self._sk_set = load_sk_set(
            template.sk_path,
            sorted(set(template.elements)),
            template.max_angular_momentum,
        )
        self._mass_host = first["mass"]
        # topology, shared by construction
        self._shared_host = {name: first[name] for name in _Shared._fields[1:]}

        def batched(key, dtype=None):
            """The batch array of one state field: stacked rows, or one row spread."""

            if per_system:
                return xp.asarray(np.stack([row[key] for row in rows]), dtype=dtype)
            one = np.asarray(first[key])
            # np.array, not np.ascontiguousarray: for num == 1 the broadcast view is
            # already contiguous and would be handed on read-only
            spread = np.broadcast_to(one, (num,) + one.shape)
            return xp.asarray(np.array(spread, order="C"), dtype=dtype)

        # geometry-dependent matrices: one shared copy while every system sits at the
        # same frozen geometry (the kernels then read zero-stride views of it), one per
        # system otherwise
        self._shared_geometry = not per_system and not self.ehrenfest
        if self._shared_geometry:
            self._coords = xp.asarray(first["coords"], dtype=xp.float64)
            self._h0 = xp.asarray(first["h0"], dtype=xp.float64)
            self._overlap = xp.asarray(first["overlap"], dtype=xp.float64)
            self._s_inv = xp.asarray(first["s_inv"], dtype=xp.float64)
            self._gamma = xp.asarray(first["gamma"], dtype=xp.float64)
        else:
            self._coords = batched("coords", xp.float64)
            self._h0 = batched("h0", xp.float64)
            self._overlap = batched("overlap", xp.float64)
            self._s_inv = batched("s_inv", xp.float64)
            self._gamma = batched("gamma", xp.float64)
        self._e_repulsive = batched("e_repulsive", xp.float64)

        # per-system electronic state, from the post-bootstrap values
        self._rho = batched("rho", xp.complex128)
        self._rho_old = batched("rho_old", xp.complex128)
        self._coupling = batched("coupling", xp.float64)
        self._q_orb = batched("q_orb", xp.float64)
        self._dq_atom = batched("dq_atom", xp.float64)
        self._dq_shell = batched("dq_shell", xp.float64)
        self._v_scc_shell = xp.zeros((num, n_shell), dtype=xp.float64)
        self._v_shell = xp.zeros((num, n_shell), dtype=xp.float64)
        self._v_orb = xp.zeros((num, n), dtype=xp.float64)
        self._h = xp.zeros((num, n, n), dtype=xp.float64)
        self._work_a = xp.zeros((num, n, n), dtype=xp.complex128)
        self._work_b = xp.zeros((num, n, n), dtype=xp.complex128)
        self._work_c = xp.zeros((num, n, n), dtype=xp.complex128)
        self._fp32 = self._allocate_fp32() if self._hybrid else None
        # per-system scalars carried from one stage kernel to the next
        self._energy_start = xp.zeros(num, dtype=xp.float64)
        self._e_kin = xp.zeros(num, dtype=xp.float64)

        # nuclear integrator state, also from the bootstrap
        if self.ehrenfest:
            self._velocity = batched("velocity", xp.float64)
            self._half_velocity = batched("half_velocity", xp.float64)
            self._coords_next = batched("coords_next", xp.float64)
            self._accel = batched("accel", xp.float64)
            self._force = batched("force", xp.float64)
            self._scratch = self._allocate_scratch()
        else:
            self._velocity = self._half_velocity = self._coords_next = None
            self._accel = self._force = self._scratch = None

        # reusable output buffers. _mu_end carries the end-of-step dipole into the next
        # step as its start, seeded with the post-bootstrap value; _mu_half is the
        # midpoint average that is reported, and the force-time dipole is the same value
        # again, because that is what the external DFTB+ driver sends. _mu_initial is
        # every system's own baseline.
        self._mu_initial = batched("mu_initial", xp.float64)
        self._amp = xp.zeros((num, 3), dtype=xp.float64)
        self._mu_end = batched("mu_end", xp.float64)
        self._mu_half = xp.zeros((num, 3), dtype=xp.float64)
        self._energy = xp.zeros(num, dtype=xp.float64)
        self._field = xp.zeros((num, 3), dtype=xp.float64)
        self._stepped = False

        output = dict(
            record_every_steps=self.record_every_steps,
            record_max_steps=self.record_max_steps,
            append=bool(template.restart and template.checkpoint),
        )
        if self.property_filename is not None:
            self._recorder = PropertyRecorder(
                output_filename(self.property_filename, self.molecule_ids[0]),
                self.record_names,
                self.molecule_ids,
                self.dt,
                **output,
            )
            print(
                f"[RTDFTBGPUBatchModel] Recording {self.record_names} to "
                f"{self._recorder.path}"
            )
        if self.traj_filename is not None:
            self._trajectory = XYZTrajectoryWriter(
                output_filename(self.traj_filename, self.molecule_ids[0]),
                self.symbols,
                self.molecule_ids,
                self.dt,
                per_atom=("dq",),
                **output,
            )
            print(
                f"[RTDFTBGPUBatchModel] Writing the trajectory to "
                f"{self._trajectory.path}"
            )

        if self._on_gpu:
            # resolved here, where the system sizes of this batch are known
            self._wide_bps = self._resolve_blocks_per_system()
            if self._wide_bps > 1 or gpu_boot:
                # the fully-GPU initialization always drives the wide phase kernels,
                # whatever the step dispatch is
                self._wide_kernels = _build_wide_stage_kernels(self.ehrenfest)
                self._wide_acc = xp.zeros(num, dtype=xp.float64)
                self._wide_mu = xp.zeros((num, 3), dtype=xp.float64)
                if self._wide_bps > 1:
                    print(
                        f"[RTDFTBGPUBatchModel] wide launch: {self._wide_bps} blocks "
                        f"per system for {num} system(s)."
                    )
            if self._wide_bps == 1:
                self._kernels = _build_stage_kernels(self.ehrenfest)
        self._bundles = self._build_bundles()
        if gpu_boot:
            self._initialize_on_device(template, per_system)

    def _allocate_fp32(self):
        """The persistent FP32 mirrors of hybrid mode, see :data:`_FP32`."""

        xp, num, n = self.xp, self.num, self.n_orb

        def real():
            return xp.zeros((num, n, n), dtype=xp.float32)

        def cplx():
            return xp.zeros((num, n, n), dtype=xp.complex64)

        ehrenfest_only = (
            (real(), real(), real(), real(), real(), real())
            if self.ehrenfest
            else (None,) * 6
        )
        return _FP32(real(), real(), cplx(), cplx(), cplx(), cplx(), *ehrenfest_only)

    def _topology_row(self, template, molecule_id):
        """
        Topology of one system for the GPU init.
        """

        system, layout, sk_set = template.system, self._gpu_layout, template.sk_set
        n, n_atom, n_shell = system.n_orb, system.n_atom, layout.n_shell
        coords = (
            template.batch_frames[molecule_id].copy()
            if template.batch_frames is not None
            else np.array(system.coords, dtype=float)
        )
        mass = np.array([sk_set.mass[sp] for sp in system.atom_species], dtype=float)
        row = dict(
            n_orb=n,
            n_atom=n_atom,
            n_shell=n_shell,
            atom_species=system.atom_species,
            atom_offset=system.atom_offset,
            orb_shell=layout.orb_shell,
            orb_atom=layout.orb_atom,
            shell_atom=layout.shell_atom,
            shell_u=layout.shell_u,
            q0_orb=layout.q0_orb,
            mass=mass,
            coords=coords,
            h0=np.zeros((n, n)),
            overlap=np.zeros((n, n)),
            s_inv=np.zeros((n, n)),
            gamma=np.zeros((n_shell, n_shell)),
            e_repulsive=0.0,
            rho=np.zeros((n, n), dtype=np.complex128),
            rho_old=np.zeros((n, n), dtype=np.complex128),
            coupling=np.zeros((n, n)),
            q_orb=np.zeros(n),
            dq_atom=np.zeros(n_atom),
            dq_shell=np.zeros(n_shell),
            mu_initial=np.zeros(3),
            mu_end=np.zeros(3),
        )
        if template.ehrenfest:
            velocity = np.zeros((n_atom, 3))
            # with pre_nvt the BOMD both consumes the random stream and supplies the
            # velocities, exactly as the scalar driver orders it
            if template.init_velocities and not template.pre_nvt:
                # the same draw the scalar driver makes, from the same stream
                rng = np.random.default_rng(template.seed + molecule_id)
                velocity = np.sqrt(template.kT / mass)[:, None] * rng.standard_normal(
                    (n_atom, 3)
                )
                velocity -= (mass[:, None] * velocity).sum(axis=0) / mass.sum()
            elif template.velocities is not None:
                velocity = np.array(template.velocities, dtype=float)
            row.update(
                velocity=velocity,
                half_velocity=velocity.copy(),
                coords_next=coords.copy(),
                accel=np.zeros((n_atom, 3)),
                force=np.zeros((n_atom, 3)),
            )
        return row

    def _eigh_general_on_device(self, h, overlap, dtype=None):
        """
        Solve the generalized eigenvalue problem ``H c = eps S c`` on the GPU device,
        the Cholesky route of ``solve_generalised``.
        """

        xp = self.xp
        if dtype is not None and h.dtype != dtype:
            h = h.astype(dtype)
            overlap = overlap.astype(dtype)
        chol = xp.linalg.cholesky(overlap)
        inv_chol = xp.linalg.inv(chol)
        transformed = inv_chol @ h @ inv_chol.T
        transformed = 0.5 * (transformed + transformed.T)
        eigenvalues, vectors = xp.linalg.eigh(transformed)
        return eigenvalues, inv_chol.T @ vectors

    def _scf_on_device(
        self, i, template, dq_start=None, store_force_state=False, announce=True
    ):
        """
        The SCC loop of ``scc.scf`` for system ``i`` with its dense algebra on the GPU device.

        Parameters
        ----------
        dq_start : numpy.ndarray, optional
            Warm-start shell charges, as ``scc.scf(dq_shell_start=...)``: the
            pre-NVT BOMD passes the previous geometry's charges.
        store_force_state : bool, default: False
            Also seed everything the ground-state force kernels read: the orbital and
            shell charges of row ``i`` and, with moving nuclei, the real density and
            the energy-weighted density (``scc``'s ``edm``) in the force scratch.
        announce : bool, default: True
            Print the one-line convergence summary.

        Returns
        -------
        numpy.ndarray
            The converged shell charges, for warm-starting the next geometry.
        """

        xp, layout = self.xp, self._gpu_layout
        n_shell = self.n_shell

        def slice_of(array):
            return array if array.ndim == 2 else array[i]

        h0 = slice_of(self._h0)
        overlap = slice_of(self._overlap)
        gamma = slice_of(self._gamma)
        orb_shell_dev = xp.asarray(layout.orb_shell)

        n_electron = template.system.n_electrons() - template.charge
        tolerance = template.scc_tolerance
        mixing, history, max_iterations = 0.2, 8, 500
        electronic_temperature_au = template.electronic_temperature_au
        dq_shell = np.zeros(n_shell)
        if dq_start is not None:
            dq_shell[:] = dq_start
        dq_new = np.zeros(n_shell)
        history_in = np.zeros((history, n_shell))
        history_residual = np.zeros((history, n_shell))
        filling = np.zeros(self.n_orb)
        converged, n_filled, n_iteration = False, 0, 0

        def hamiltonian_and_density(dtype):
            v_shell = gamma @ xp.asarray(dq_shell)
            h = h0 + 0.5 * overlap * (
                v_shell[orb_shell_dev][:, None] + v_shell[orb_shell_dev][None, :]
            )
            eigenvalues, vectors = self._eigh_general_on_device(h, overlap, dtype)
            eig_host = xp.asnumpy(eigenvalues).astype(np.float64)
            fermi_filling(eig_host, n_electron, electronic_temperature_au, filling)
            rho = (vectors * xp.asarray(filling).astype(vectors.dtype)) @ vectors.T
            s_here = (
                overlap if rho.dtype == overlap.dtype else overlap.astype(rho.dtype)
            )
            q_orb = xp.asnumpy(xp.einsum("ij,ji->i", rho, s_here)).astype(np.float64)
            return v_shell, rho, q_orb, vectors, eig_host

        # the two-phase schedule: with hybrid_precision the bulk of the iterations
        # runs in FP32 and a short FP64 tail finishes to the requested tolerance,
        # converging the same fixed point; without it, FP64 throughout (the default)
        fp32_active = bool(self.hybrid_precision)
        n_fp32 = 0
        best_error = np.inf
        stalled = 0
        for iteration in range(max_iterations):
            n_iteration = iteration + 1
            dtype = xp.float32 if fp32_active else xp.float64
            try:
                v_shell, rho, q_orb, _, _ = hamiltonian_and_density(dtype)
            except np.linalg.LinAlgError:
                if not fp32_active:
                    raise
                # ill-conditioned in single precision: hand over to FP64 for good
                fp32_active = False
                n_filled = 0
                v_shell, rho, q_orb, _, _ = hamiltonian_and_density(xp.float64)
            shell_charges(q_orb, layout.q0_orb, layout.orb_shell, dq_new)
            error = float(np.max(np.abs(dq_new - dq_shell)))
            if fp32_active:
                n_fp32 += 1
                if error < 0.5 * best_error:
                    best_error = error
                    stalled = 0
                else:
                    stalled += 1
                if error < _SCF_FP32_SWITCH or stalled >= _SCF_FP32_STALL:
                    # switch to the FP64 tail; the single-precision noise correlated
                    # across the FP32-era residuals misleads the DIIS least squares
                    # (measured: keeping them costs more than the restart), so the
                    # history starts clean
                    fp32_active = False
                    n_filled = 0
            elif error < tolerance:
                dq_shell[:] = dq_new
                converged = True
                break
            # the DIIS history handling of scc.scf, verbatim
            if n_filled < history:
                slot = n_filled
                n_filled += 1
            else:
                for j in range(history - 1):
                    history_in[j] = history_in[j + 1]
                    history_residual[j] = history_residual[j + 1]
                slot = history - 1
            history_in[slot] = dq_shell
            history_residual[slot] = dq_new - dq_shell
            diis_mix(history_in, history_residual, n_filled, mixing, dq_shell)
        if not converged:
            raise RuntimeError(
                "[molecule ID %d] the SCC ground state did not converge."
                % self.molecule_ids[i]
            )

        # one rebuild from the converged charges, as scc.scf reports it; FP64 always,
        # so the seeded density and force state are full precision whatever the path
        self._scf_iterations = (n_fp32, n_iteration - n_fp32)
        v_shell, rho, q_orb, vectors, eig_host = hamiltonian_and_density(xp.float64)
        self._rho[i] = rho.astype(xp.complex128)
        if store_force_state:
            # what forces.total_gradient reads: charges for the gamma term, the real
            # density and the energy-weighted density for the band and Pulay terms
            self._q_orb[i] = xp.asarray(q_orb)
            self._dq_shell[i] = xp.asarray(dq_shell)
            if self.ehrenfest:
                sc = self._scratch
                sc.density[i] = rho
                sc.weight_e[i] = (vectors * xp.asarray(filling * eig_host)) @ vectors.T
        if announce:
            energy_h0 = float(xp.sum(rho * h0))
            energy_scc = 0.5 * float(xp.asnumpy(v_shell) @ dq_shell)
            energy_total = energy_h0 + energy_scc + float(self._e_repulsive[i])
            phases = (
                f"{n_fp32} FP32 + {n_iteration - n_fp32} FP64"
                if n_fp32
                else f"{n_iteration} FP64"
            )
            print(
                f"[RTDFTBGPUBatchModel] GPU ground state of molecule "
                f"{self.molecule_ids[i]}: {phases} iterations, "
                f"E = {energy_total:.10f} Ha."
            )
        return dq_shell

    def _start_on_device(self, template):
        """``RTDynamics.start`` for the whole batch on the GPU device.

        Charges and Hamiltonian of the unkicked ground state, the Ehrenfest force and
        the first nuclear half-move, the delta kick, the coupling at ``(r(0), v(0))``,
        the Euler bootstrap into the first leapfrog interval, and the observation at
        ``t = dt`` -- the exact sequence of the CPU bootstrap, through the wide phase
        kernels and the dense operations of the step path (FP64 here; the caller holds
        ``hybrid_precision`` off during initialization).
        """

        from numba import cuda

        xp, dt = self.xp, self.dt
        state, shared, geometry, nuclear, scratch, out, field = self._bundles
        wide = ((self.num, self._wide_bps), _THREADS_PER_BLOCK)
        narrow = (self.num, _THREADS_PER_BLOCK)
        k, acc, mu = self._wide_kernels, self._wide_acc, self._wide_mu

        def observe(adopt_rep):
            """Charges, potentials and H of rho held in ``rho_old`` (t = 0 and t = dt)."""

            if self.ehrenfest:
                k.post_prep[wide](state, shared, geometry, scratch)
            else:
                k.post_prep[wide](state, shared, geometry)
            k.post_charges[narrow](state, shared, geometry, mu, acc, adopt_rep)
            k.scc_rows[wide](state, shared, geometry)
            k.finish_potentials[narrow](state, shared, geometry, field)
            acc[...] = 0.0
            k.post_ham[wide](state, shared, geometry, acc)

        def forces():
            self._energy_weighted_density_on_device()
            k.force_weight[wide](state, scratch)
            k.force_pairs[wide](state, shared, geometry, scratch)
            k.force_finish[narrow](state, shared, nuclear, scratch, field)

        # charges, H and (Ehrenfest) force of the unkicked ground state
        self._rho_old[...] = self._rho
        observe(0)
        if self.ehrenfest:
            self._half_velocity[...] = self._velocity
            self._coords_next[...] = self._coords + self._velocity * dt
            forces()

        # the delta kick, timeprop.F90:1861-1876 through cuBLAS
        if template.delta_kick_au != 0.0:
            orb_atom = self._gpu_layout.orb_atom
            if self._coords.ndim == 2:
                phase = (
                    template.delta_kick_au
                    * self._coords[orb_atom, template.kick_direction]
                )
            else:
                phase = (
                    template.delta_kick_au
                    * self._coords[:, orb_atom, template.kick_direction]
                )
            work = (xp.exp(-1j * phase)[..., :, None] * self._rho) @ self._overlap
            work = (work * xp.exp(1j * phase)[..., None, :]) @ self._s_inv
            self._rho[...] = 0.5 * (work + xp.conj(work.swapaxes(-1, -2)))

        # D(r(0), v(0)); the Hamiltonian rows it re-runs are unchanged
        if self.ehrenfest:
            self._coupling[...] = 0.0
            acc[...] = 0.0
            k.pre_work[wide](state, shared, geometry, nuclear, acc)

        # the Euler bootstrap: rho(dt) lands in rho_old, as one step leaves it
        self._rho_old[...] = self._rho
        self._leapfrog_on_device(dt)

        if self.ehrenfest:
            k.geo_reset[wide](state, geometry, nuclear)
            acc[...] = 0.0
            k.geo_build[wide](state, shared, geometry, acc)
            self._invert_overlap_on_device()
        observe(1 if self.ehrenfest else 0)

        cuda.synchronize()
        if self.reset_dipole:
            self._mu_initial[...] = mu
        else:
            self._mu_initial[...] = 0.0
        self._mu_end[...] = mu - self._mu_initial
        if self.ehrenfest:
            forces()
        self._swap_density()
        cuda.synchronize()

    def _prenvt_on_device(self, template):
        """
        ``dynamics.bomd_equilibrate`` for the whole batch on the GPU device.
        """

        xp = self.xp
        state, shared, geometry, nuclear, scratch, out, field = self._bundles
        wide = ((self.num, self._wide_bps), _THREADS_PER_BLOCK)
        narrow = (self.num, _THREADS_PER_BLOCK)
        k, acc, mu = self._wide_kernels, self._wide_acc, self._wide_mu

        dt = _PRE_NVT_DT_FS * FS_TO_AU
        n_steps = int(round(template.pre_nvt_duration_ps * 1000.0 * FS_TO_AU / dt))
        friction = template.friction_fs * FS_TO_AU
        mass = self._mass_host[:, None]
        c1h = float(np.exp(-0.5 * dt / friction))
        noise = np.sqrt(template.kT / mass * (1.0 - c1h**2))
        rngs = [np.random.default_rng(template.seed + mid) for mid in self.molecule_ids]
        velocity = np.stack(
            [
                np.sqrt(template.kT / mass) * rng.standard_normal((self.n_atom, 3))
                for rng in rngs
            ]
        )
        velocity -= (mass * velocity).sum(axis=1, keepdims=True) / mass.sum()
        velocity_dev = xp.asarray(velocity)
        warm = [None] * self.num

        def acceleration():
            """Geometry, warm-started SCC and the ground-state force of every system."""

            self._h0[...] = 0.0
            self._overlap[...] = 0.0
            acc[...] = 0.0
            k.geo_build[wide](state, shared, geometry, acc)
            for i in range(self.num):
                warm[i] = self._scf_on_device(
                    i,
                    template,
                    dq_start=warm[i],
                    store_force_state=True,
                    announce=False,
                )
            k.post_charges[narrow](state, shared, geometry, mu, acc, 0)
            k.scc_rows[wide](state, shared, geometry)
            k.finish_potentials[narrow](state, shared, geometry, field)
            k.force_weight[wide](state, scratch)
            k.force_pairs[wide](state, shared, geometry, scratch)
            k.force_finish[narrow](state, shared, nuclear, scratch, field)

        acceleration()
        for _ in range(n_steps):
            xi = np.stack([rng.standard_normal((self.n_atom, 3)) for rng in rngs])
            velocity_dev = c1h * velocity_dev + xp.asarray(noise * xi)  # O
            velocity_dev += (0.5 * dt) * self._accel  # B
            self._coords += dt * velocity_dev  # A
            acceleration()
            velocity_dev += (0.5 * dt) * self._accel  # B
            xi = np.stack([rng.standard_normal((self.n_atom, 3)) for rng in rngs])
            velocity_dev = c1h * velocity_dev + xp.asarray(noise * xi)  # O
        self._velocity[...] = velocity_dev
        print(
            f"[RTDFTBGPUBatchModel] GPU pre-NVT: {n_steps} BOMD steps of "
            f"{_PRE_NVT_DT_FS} fs for {self.num} system(s)."
        )

    def _initialize_on_device(self, template, per_system):
        """Geometry, ground state, kick and bootstrap of every system on the GPU."""

        from numba import cuda

        xp = self.xp
        hybrid_saved, self._hybrid = self._hybrid, False  # FP64 init, always
        state, shared, geometry, nuclear, scratch, out, field = self._bundles
        k, acc = self._wide_kernels, self._wide_acc

        # thermalize first, so r(0) and v(0) below are the pre-NVT endpoint
        if template.pre_nvt:
            self._prenvt_on_device(template)

        # H0, S, gamma and the repulsive energy at r(0)
        n_geo = 1 if self._shared_geometry else self.num
        self._h0[...] = 0.0
        self._overlap[...] = 0.0
        acc[...] = 0.0
        k.geo_build[(n_geo, self._wide_bps), _THREADS_PER_BLOCK](
            state, shared, geometry, acc
        )
        cuda.synchronize()
        if self._shared_geometry:
            self._e_repulsive[...] = acc[0]
        else:
            self._e_repulsive[...] = acc

        # S^-1 at r(0), FP64
        if self._shared_geometry:
            self._s_inv[...] = xp.linalg.inv(self._overlap)
        else:
            self._invert_overlap_on_device()

        # one SCC ground state per distinct starting point
        if per_system:
            for i in range(self.num):
                self._scf_on_device(i, template)
        else:
            self._scf_on_device(0, template)
            if self.num > 1:
                self._rho[...] = self._rho[0:1]

        self._start_on_device(template)
        self._hybrid = hybrid_saved

    def _allocate_scratch(self):
        """Per-system working storage, see :data:`_Scratch`."""

        xp, num, n, n_atom = self.xp, self.num, self.n_orb, self.n_atom

        def zeros(*shape):
            return xp.zeros((num,) + shape, dtype=xp.float64)

        return _Scratch(
            # build_h0_overlap_kernel
            zeros(MAX_INTEGRAL),
            zeros(MAX_INTEGRAL),
            zeros(MAX_ORB, MAX_ORB),
            zeros(MAX_ORB, MAX_ORB),
            zeros(5, 5),
            zeros(_N_INTERPOLATION),
            zeros(_N_INTERPOLATION),
            zeros(_N_INTERPOLATION),
            zeros(_N_INTERPOLATION - 1),
            zeros(MAX_INTEGRAL),
            zeros(MAX_INTEGRAL),
            # band_gradient_kernel; the coupling kernel reuses the overlap-only subset,
            # which is safe because the two are never live at the same time
            zeros(MAX_INTEGRAL),
            zeros(MAX_INTEGRAL),
            zeros(MAX_INTEGRAL),
            zeros(MAX_INTEGRAL),
            zeros(3, MAX_ORB, MAX_ORB),
            zeros(3, MAX_ORB, MAX_ORB),
            zeros(_N_INTERPOLATION),
            zeros(_N_INTERPOLATION),
            zeros(_N_INTERPOLATION),
            zeros(MAX_ORB, MAX_ORB),
            zeros(3, MAX_ORB, MAX_ORB),
            zeros(5, 5),
            zeros(3, 5, 5),
            # the Ehrenfest gradient itself
            zeros(n, n),
            zeros(n, n),
            zeros(n, n),
            zeros(n, n),
            zeros(n_atom, 3),
            zeros(2),
            zeros(n, n),
            xp.zeros((num, n), dtype=xp.int64),
        )

    def _build_bundles(self):
        """Wrap the state in the kernel's argument bundles once.

        A raw CuPy array inside a namedtuple is untypeable by numba, so on the GPU every
        field goes through ``cuda.as_cuda_array``, which is a zero-copy view.
        """

        if self._on_gpu:
            from numba import cuda

            def wrap(array):
                return cuda.as_cuda_array(array)

            def upload(array):
                return cuda.to_device(np.ascontiguousarray(array))

        else:

            def wrap(array):
                return array

            def upload(array):
                return np.ascontiguousarray(array)

        tables = self._sk_set.tables()
        shared = _Shared(
            tables._replace(
                **{name: upload(getattr(tables, name)) for name in tables._fields}
            ),
            *[upload(self._shared_host[name]) for name in _Shared._fields[1:]],
        )
        state = _State(*[wrap(getattr(self, "_" + name)) for name in _State._fields])
        if self._shared_geometry:
            # one geometry for all: the kernels index per system, so hand them views
            # that repeat the shared arrays along the batch axis without copying them
            def view(array):
                return wrap(self.xp.broadcast_to(array, (self.num,) + array.shape))

        else:
            view = wrap
        geometry = _Geometry(
            view(self._coords),
            view(self._h0),
            view(self._overlap),
            view(self._s_inv),
            view(self._gamma),
            wrap(self._e_repulsive),
        )
        out = _Out(
            wrap(self._amp),
            wrap(self._mu_end),
            wrap(self._mu_half),
            wrap(self._energy),
        )
        nuclear = scratch = None
        if self.ehrenfest:
            nuclear = _Nuclear(
                wrap(self._velocity),
                wrap(self._half_velocity),
                wrap(self._coords_next),
                wrap(self._accel),
                wrap(self._force),
            )
            scratch = _Scratch(*[wrap(field) for field in self._scratch])
        return state, shared, geometry, nuclear, scratch, out, wrap(self._field)

    # ---------------------------- one FDTD step under E-field -----------------------
    def step(self, efield_au):
        """
        Advance every system by one RT-TDDFTB step under its own effective field.

        Parameters
        ----------
        efield_au : numpy.ndarray of float, shape (num, 3)
            Effective electric field of every system in atomic units, rows in the
            molecule-ID order :meth:`initialize` was given.

        Returns
        -------
        BatchStepResult
            Amplitude, both dipoles and the energy, as host arrays.
        """

        if self._rho is None:
            raise RuntimeError("RTDFTBGPUBatchModel.step() before initialize().")
        field = np.ascontiguousarray(efield_au, dtype=np.float64)
        if field.shape != (self.num, 3):
            raise ValueError(
                f"efield_au must have shape ({self.num}, 3); got {field.shape}."
            )
        self._field[...] = self.xp.asarray(field)  # host -> device (one copy)

        if self._on_gpu:
            self._step_on_gpu()
        else:
            self._step_on_cpu()
        self._swap_density()
        self.t += self.dt
        self._stepped = True

        h = self._to_host
        # the run-time record, as RTDFTBModel takes it: T = 2 K / (3 N k_B) of the nuclei
        # (zero when frozen), the energies and the dipole reported to MaxwellLink; the
        # geometry with the Mulliken charge deviation of every atom
        self._step_index += 1
        if self._step_index % self.record_every_steps == 0:
            if self._recorder is not None:
                e_kin = h(self._e_kin)
                temperature = 2.0 * e_kin / (3.0 * self.n_atom) / K_TO_AU
                self._recorder.record(
                    self._step_index,
                    self.t,
                    np.column_stack(
                        (temperature, h(self._energy), e_kin, h(self._mu_half))
                    ),
                )
            if self._trajectory is not None:
                self._trajectory.write(
                    self._step_index,
                    self.t,
                    self.coordinates(),
                    {"dq": h(self._dq_atom)},
                )
        return BatchStepResult(
            amplitude_au=h(self._amp),
            dipole_half_au=h(self._mu_half),
            dipole_force_au=h(self._mu_half),
            energy_au=h(self._energy),
        )

    def _swap_density(self):
        """Exchange the two leapfrog buffers, and the views the bundles hold."""

        self._rho, self._rho_old = self._rho_old, self._rho
        bundles = list(self._bundles)
        state = bundles[0]
        bundles[0] = state._replace(rho=state.rho_old, rho_old=state.rho)
        self._bundles = tuple(bundles)

    def _resolve_blocks_per_system(self):
        """How many CUDA blocks each system's stage work is spread over.

        One block per system (the narrow path) once the batch alone oversubscribes
        every SM; otherwise enough blocks to fill the device ~4 deep, capped by the
        largest strided loop so no block is left without work. An explicit
        ``blocks_per_system`` from the constructor wins.

        Returns
        -------
        int
            ``1`` selects the narrow block-per-system kernels.
        """

        if self.blocks_per_system is not None:
            return self.blocks_per_system
        from numba import cuda

        sm_count = cuda.get_current_device().MULTIPROCESSOR_COUNT
        want = -(-4 * sm_count // max(self.num, 1))  # ceil: blocks to fill the GPU
        max_work = max(
            self.n_orb,
            self.n_atom * max(self.n_atom - 1, 1),
            self.n_shell * self.n_shell,
        )
        cap = -(-max_work // _THREADS_PER_BLOCK)
        return max(1, min(want, cap))

    def _step_on_gpu_wide(self):
        """One step of a SMALL batch of LARGE systems: the phase-split kernels.

        The same physics as :meth:`_step_on_gpu`, with each stage's parallel sections
        launched on a ``(num, blocks_per_system)`` grid and its serial bookkeeping in
        narrow ``(num,)`` kernels between them; the kernel boundaries provide the
        grid-wide ordering that ``syncthreads`` provides inside one block. All the
        launches and the CuPy zero-fills queue on the default stream, so the one
        synchronization at the end is for the device->host copies of the results.
        """

        from numba import cuda

        state, shared, geometry, nuclear, scratch, out, field = self._bundles
        wide = ((self.num, self._wide_bps), _THREADS_PER_BLOCK)
        narrow = (self.num, _THREADS_PER_BLOCK)
        k, dt = self._wide_kernels, self.dt
        acc, mu = self._wide_acc, self._wide_mu

        acc[...] = 0.0
        if self.ehrenfest:
            k.pre_start[wide](state, shared, geometry, nuclear, field, dt)
            k.finish_potentials[narrow](state, shared, geometry, field)
            k.pre_work[wide](state, shared, geometry, nuclear, acc)
            k.energy_start[narrow](state, shared, geometry, field, acc)
            self._leapfrog_on_device(2.0 * dt)
            acc[...] = 0.0
            k.geo_reset[wide](state, geometry, nuclear)
            k.geo_build[wide](state, shared, geometry, acc)
            self._invert_overlap_on_device()
            k.post_prep[wide](state, shared, geometry, scratch)
            k.post_charges[narrow](state, shared, geometry, mu, acc, 1)
            acc[...] = 0.0
            k.scc_rows[wide](state, shared, geometry)
            k.finish_potentials[narrow](state, shared, geometry, field)
            k.post_ham[wide](state, shared, geometry, acc)
            k.report[narrow](state, shared, geometry, out, field, mu, acc, dt)
            self._energy_weighted_density_on_device()
            k.force_weight[wide](state, scratch)
            k.force_pairs[wide](state, shared, geometry, scratch)
            k.force_finish[narrow](state, shared, nuclear, scratch, field)
        else:
            k.scc_rows[wide](state, shared, geometry)
            k.finish_potentials[narrow](state, shared, geometry, field)
            k.pre_work[wide](state, shared, geometry, acc)
            k.energy_start[narrow](state, shared, geometry, field, acc)
            self._leapfrog_on_device(2.0 * dt)
            k.post_prep[wide](state, shared, geometry)
            k.post_charges[narrow](state, shared, geometry, mu, acc, 0)
            acc[...] = 0.0
            k.scc_rows[wide](state, shared, geometry)
            k.finish_potentials[narrow](state, shared, geometry, field)
            k.post_ham[wide](state, shared, geometry, acc)
            k.report[narrow](state, shared, geometry, out, field, mu, acc, dt)
        cuda.synchronize()

    def _step_on_gpu(self):
        """One step of the whole batch: stage kernels around the dense linear algebra.

        The stage kernels run one block per system; between them CuPy applies the
        leapfrog products, ``S^-1`` and the energy-weighted density to every system at
        once through cuBLAS/cuSOLVER. Both libraries queue on the default CUDA stream,
        which keeps the stages in order; the one synchronization at the end is for the
        device->host copies of the results.
        """

        if self._wide_bps > 1:
            return self._step_on_gpu_wide()
        from numba import cuda

        state, shared, geometry, nuclear, scratch, out, field = self._bundles
        launch = (self.num, _THREADS_PER_BLOCK)  # one block per system
        stages, dt = self._kernels, self.dt
        if self.ehrenfest:
            stages.pre[launch](state, shared, geometry, nuclear, scratch, field, dt)
            self._leapfrog_on_device(2.0 * dt)
            stages.geometry[launch](state, shared, geometry, nuclear, scratch)
            self._invert_overlap_on_device()
            stages.post[launch](state, shared, geometry, scratch, out, field, dt)
            self._energy_weighted_density_on_device()
            stages.force[launch](state, shared, geometry, nuclear, scratch, field)
        else:
            stages.pre[launch](state, shared, geometry, field)
            self._leapfrog_on_device(2.0 * dt)
            stages.post[launch](state, shared, geometry, out, field, dt)
        cuda.synchronize()

    def _leapfrog_on_device(self, step):
        """
        The leapfrog products of ``rt.leapfrog_step`` for the whole batch, with cuBLAS.

        ``rho(t+dt) = rho(t-dt) - step (T1 rho + rho T1^dagger)`` with
        ``T1 = S^-1 (D + iH)``, written into ``rho_old`` as the kernel does; the host
        swaps the two buffers afterwards. ``T1`` is assembled from two real products
        rather than one complex one, and ``rho T1^dagger`` is the adjoint of ``T1 rho``
        because ``rho`` is Hermitian, so one complex product serves both terms (the
        kernel computes both, as DFTB+ does; the two agree to round-off).
        """

        xp = self.xp
        if self._hybrid:
            # the same products in FP32: the operands of this step are downcast into
            # the persistent mirrors, the FP64 density accumulates the increment.
            # Round-off enters only the increment (~1e-7 of it per step), which keeps
            # the energy drift and the electron count at FP64 quality; see
            # ``RTDFTBModel(hybrid_precision=...)``.
            fp = self._fp32
            t1, work_b, work_c = fp.t1, fp.work_b, fp.work_c  # augmented assignment
            fp.h[...] = self._h
            fp.s_inv[...] = self._s_inv
            fp.rho[...] = self._rho
            xp.multiply(xp.matmul(fp.s_inv, fp.h), xp.complex64(1j), out=t1)  # i S^-1 H
            if self.ehrenfest:
                fp.coupling[...] = self._coupling
                xp.matmul(fp.s_inv, fp.coupling, out=fp.work_r)
                t1 += fp.work_r  # + S^-1 D
            xp.matmul(t1, fp.rho, out=work_b)  # T1 rho
            xp.conj(work_b.transpose(0, 2, 1), out=work_c)  # rho T1^dagger
            work_b += work_c
            work_b *= xp.complex64(step)
            self._rho_old -= work_b  # upcast accumulate into the FP64 state
            return
        t1, work_b, work_c = self._work_a, self._work_b, self._work_c
        xp.multiply(xp.matmul(self._s_inv, self._h), 1j, out=t1)  # i S^-1 H
        if self.ehrenfest:
            t1 += xp.matmul(self._s_inv, self._coupling)  # + S^-1 D
        xp.matmul(t1, self._rho, out=work_b)  # T1 rho
        xp.conj(work_b.transpose(0, 2, 1), out=work_c)  # rho T1^dagger
        work_b += work_c
        work_b *= step
        self._rho_old -= work_b

    def _invert_overlap_on_device(self):
        """``S^-1`` at the new geometry, the LU inverse ``rt.lu_invert`` takes."""

        xp = self.xp
        if self._hybrid:
            # the FP32 LU inverse; upcast into the FP64 ``s_inv`` every reader shares
            fp = self._fp32
            fp.overlap[...] = self._overlap
            if self.num < _INVERSE_BATCH_MIN:
                for i in range(self.num):
                    self._s_inv[i] = xp.linalg.inv(fp.overlap[i])
            else:
                self._s_inv[...] = xp.linalg.inv(fp.overlap)
            return
        if self.num < _INVERSE_BATCH_MIN:
            for i in range(self.num):
                self._s_inv[i] = xp.linalg.inv(self._overlap[i])
        else:
            self._s_inv[...] = xp.linalg.inv(self._overlap)

    def _energy_weighted_density_on_device(self):
        """``W = 0.5 (S^-1 H P + P H S^-1)`` as ``ehrenfest.energy_weighted_density``."""

        xp, sc = self.xp, self._scratch
        if self._hybrid:
            # W feeds only the nuclear forces, so FP32 throughout; ``h`` and ``s_inv``
            # are re-downcast because both changed since the leapfrog stage
            fp = self._fp32
            weight = fp.weight  # augmented assignment below; a namedtuple field
            fp.density[...] = sc.density
            fp.h[...] = self._h
            fp.s_inv[...] = self._s_inv
            xp.matmul(fp.density, fp.h, out=fp.product)  # P H
            xp.matmul(fp.s_inv, fp.product.transpose(0, 2, 1), out=weight)
            xp.matmul(fp.product, fp.s_inv.transpose(0, 2, 1), out=fp.work_r)
            weight += fp.work_r
            weight *= xp.float32(0.5)
            sc.weight_e[...] = weight  # upcast into the array the force kernel reads
            return
        product, weight_e = sc.product, sc.weight_e
        xp.matmul(sc.density, self._h, out=product)  # P H
        xp.matmul(self._s_inv, product.transpose(0, 2, 1), out=weight_e)
        weight_e += xp.matmul(product, self._s_inv.transpose(0, 2, 1))
        weight_e *= 0.5

    def _step_on_cpu(self):
        """The same physics through the njit kernels, one system at a time.

        This is a production path, not a stub -- it is what a CUDA-less host runs -- so
        it drives exactly the same compiled kernels in exactly the same order as the
        CUDA kernel above.
        """

        state, shared, geometry, nuclear, scratch, out, field = self._bundles
        for i in range(self.num):
            if self.ehrenfest:
                _cpu_step_ehrenfest(
                    i,
                    state,
                    shared,
                    geometry,
                    nuclear,
                    scratch,
                    out,
                    field,
                    2.0 * self.dt,
                    self.dt,
                )
            else:
                _cpu_step_frozen(
                    i, state, shared, geometry, out, field, 2.0 * self.dt, self.dt
                )

    # ----------------------- internal helper method ---------------------------------
    def _to_host(self, array):
        """Return a host copy of ``array``, whichever backend holds it."""

        asnumpy = getattr(self.xp, "asnumpy", None)
        if asnumpy is not None:
            return asnumpy(array)
        return np.array(array)  # numpy: force a copy so buffer reuse cannot mutate it

    # ------------ optional data / trajectory read-out --------------
    def append_additional_data(self):
        """
        Append additional data for each system to send back to MaxwellLink.

        Returns
        -------
        list of dict
            One dictionary per system, carrying exactly the keys the external DFTB+
            driver sends. ``mux_au`` and ``mux_m_au`` hold the same value, as they do
            in the scalar driver.
        """

        if not self._stepped:
            raise RuntimeError(
                "RTDFTBGPUBatchModel.append_additional_data() before the first step()."
            )
        mu = self._to_host(self._mu_half)
        energy = self._to_host(self._energy)
        kinetic = self.kinetic_energies()
        rows = []
        for i in range(self.num):
            row = {"time_au": self.t, "energy_au": float(energy[i])}
            row["energy_kin_au"] = float(kinetic[i])
            for k, key in enumerate(_HALF_DIPOLE_KEYS):
                row[key] = float(mu[i, k])
            for k, key in enumerate(_FORCE_DIPOLE_KEYS):
                row[key] = float(mu[i, k])
            rows.append(row)
        return rows

    def additional_data_columns(self, keys):
        """
        Return the requested additional-data fields as one contiguous block.

        Parameters
        ----------
        keys : sequence of str
            Field names to return, in column order.

        Returns
        -------
        numpy.ndarray of float, shape (num, len(keys))
            The requested fields, one row per system.
        """

        if not self._stepped:
            raise RuntimeError(
                "RTDFTBGPUBatchModel.additional_data_columns() before the first step()."
            )
        columns = {}
        if any(key in _HALF_DIPOLE_KEYS + _FORCE_DIPOLE_KEYS for key in keys):
            mu = self._to_host(self._mu_half).T
            columns.update(zip(_HALF_DIPOLE_KEYS, mu))
            columns.update(zip(_FORCE_DIPOLE_KEYS, mu))
        if "energy_au" in keys:
            columns["energy_au"] = self._to_host(self._energy)
        if "energy_kin_au" in keys:
            columns["energy_kin_au"] = self.kinetic_energies()
        if "time_au" in keys:
            columns["time_au"] = np.full(self.num, self.t)
        return np.ascontiguousarray(np.column_stack([columns[key] for key in keys]))

    def kinetic_energies(self):
        """
        Nuclear kinetic energy of every system in Hartree, zero when the nuclei are
        frozen.

        Returns
        -------
        numpy.ndarray of float, shape (num,)
        """

        if not self.ehrenfest:
            return np.zeros(self.num)
        velocity = self._to_host(self._velocity)
        return 0.5 * np.einsum("a,dak,dak->d", self._mass_host, velocity, velocity)

    def coordinates(self):
        """
        Current geometry of every system, in Bohr.

        Returns
        -------
        numpy.ndarray of float, shape (num, n_atom, 3)
            With the nuclei frozen and one shared starting geometry every row is that
            geometry.
        """

        if self._coords is None:
            raise RuntimeError("RTDFTBGPUBatchModel.coordinates() before initialize().")
        coords = self._to_host(self._coords)
        if not self._shared_geometry:
            return coords
        return np.ascontiguousarray(np.broadcast_to(coords, (self.num,) + coords.shape))

    def velocities(self):
        """
        Current nuclear velocities of every system, in atomic units.

        Returns
        -------
        numpy.ndarray of float, shape (num, n_atom, 3)
            Zero while the nuclei are frozen.
        """

        if self._velocity is None:
            return np.zeros((self.num, self.n_atom, 3))
        return self._to_host(self._velocity)

    def close(self):
        """Close the output files and release the device state."""

        for name in ("_recorder", "_trajectory"):
            writer = getattr(self, name)
            if writer is not None:
                writer.close()
                setattr(self, name, None)
        for name in (
            "_rho",
            "_rho_old",
            "_coupling",
            "_h0",
            "_overlap",
            "_s_inv",
            "_gamma",
            "_coords",
            "_q_orb",
            "_dq_atom",
            "_dq_shell",
            "_v_scc_shell",
            "_v_shell",
            "_v_orb",
            "_h",
            "_work_a",
            "_work_b",
            "_work_c",
            "_amp",
            "_mu_end",
            "_mu_half",
            "_energy",
            "_energy_start",
            "_e_kin",
            "_mu_initial",
            "_kernels",
            "_field",
            "_bundles",
            "_velocity",
            "_half_velocity",
            "_coords_next",
            "_accel",
            "_force",
            "_scratch",
        ):
            setattr(self, name, None)


# ---------------------------------------------------------------------------- #
# the starting state of every system, from the scalar driver                   #
# ---------------------------------------------------------------------------- #
def _scalar_state(model):
    """The batch-relevant state of one initialized scalar driver, as host arrays."""

    dynamics = model.dynamics
    state = dynamics.state
    system = model.system
    layout = model.ground.layout
    row = dict(
        n_orb=system.n_orb,
        n_atom=system.n_atom,
        n_shell=layout.n_shell,
        atom_species=system.atom_species,
        atom_offset=system.atom_offset,
        orb_shell=layout.orb_shell,
        orb_atom=layout.orb_atom,
        shell_atom=layout.shell_atom,
        shell_u=layout.shell_u,
        q0_orb=layout.q0_orb,
        mass=np.array(dynamics.mass, dtype=float),
        coords=np.array(system.coords, dtype=float),
        h0=state.h0,
        overlap=state.overlap,
        s_inv=state.s_inv,
        gamma=state.gamma,
        e_repulsive=float(state.energies()[3]),
        rho=state.rho,
        rho_old=state.rho_old,
        coupling=state.coupling,
        q_orb=state.q_orb,
        dq_atom=state.dq_atom,
        dq_shell=state.dq_shell,
        mu_initial=np.array(model.mu_initial, dtype=float),
        mu_end=np.array(state.dipole - model.mu_initial, dtype=float),
    )
    if model.ehrenfest:
        row.update(
            velocity=dynamics.velocity,
            half_velocity=dynamics.half_velocity,
            coords_next=dynamics.coords_next,
            accel=dynamics.accel,
            force=dynamics.force_end,
        )
    return row


def _initial_state(driver_args, driver_kwargs, dt_au, molecule_id):
    """Initialize the scalar driver of one molecule ID and return its state."""

    limit_blas_threads(1)  # many of these may run side by side
    model = RTDFTBModel(*driver_args, **driver_kwargs)
    model.initialize(dt_au, molecule_id)
    return _scalar_state(model)


def _initial_states(driver_args, driver_kwargs, dt_au, molecule_ids):
    """
    One starting state per molecule ID, over the CPU cores when that pays.

    A pre-NVT equilibration is seconds to minutes per molecule, so it is spread over
    forked workers, one per core the process may use; the plain SCC ground state is
    milliseconds and runs in place. The workers do CPU work only, so forking after the
    device was set up is safe.
    """

    jobs = [(driver_args, driver_kwargs, dt_au, mid) for mid in molecule_ids]
    workers = min(len(jobs) - 1, _cpu_count())
    if driver_kwargs.get("pre_nvt", False) and workers > 1:
        # the first molecule runs here, which compiles every kernel once; the forked
        # workers inherit the compiled code instead of each compiling it again
        rows = [_initial_state(*jobs[0])]
        with multiprocessing.get_context("fork").Pool(workers) as pool:
            rows.extend(pool.starmap(_initial_state, jobs[1:]))
        return rows
    return [_initial_state(*job) for job in jobs]


def _cpu_count():
    """CPU cores this process may run on (the SLURM allocation, not the whole node)."""

    try:
        return len(os.sched_getaffinity(0))
    except (AttributeError, OSError):
        return os.cpu_count() or 1


# ---------------------------------------------------------------------------- #
# the CPU builds of the same two step bodies                                   #
# ---------------------------------------------------------------------------- #
def _rebuild_h(i, st, sh, coords, h0, overlap, gamma, field, n_orb, n_shell):
    """Rebuild ``H`` of one system from its charges and this step's field."""

    from ..models.rtdftb_model import rt, scc

    scc.scc_potential(
        gamma, st.dq_shell[i], sh.orb_shell, st.v_scc_shell[i], st.v_orb[i]
    )
    st.v_shell[i] = st.v_scc_shell[i]
    rt.external_potential(coords, field[i], sh.shell_atom, st.v_shell[i], n_shell)
    for mu in range(n_orb):
        st.v_orb[i, mu] = st.v_shell[i, sh.orb_shell[mu]]
    scc.scc_hamiltonian(h0, overlap, st.v_orb[i], st.h[i])


def _total_energy(i, st, rho, coords, h0, field, e_rep, e_kin, n_orb, n_shell, n_atom):
    """Total energy of one system, matching ``RTState.energies`` for our scope."""

    from ..models.rtdftb_model import rt

    return (
        rt.band_energy(rho[i], h0, n_orb)
        + rt.scc_energy(st.v_scc_shell[i], st.dq_shell[i], n_shell)
        + rt.external_energy(st.dq_atom[i], coords, field[i], n_atom)
        + e_rep
        + e_kin
    )


def _recharge(i, st, sh, rho, overlap, coords, mu, n_orb, n_shell, n_atom):
    """Mulliken charges and the dipole of one system, after propagation."""

    from ..models.rtdftb_model import rt

    rt.rt_orbital_charges(rho[i], overlap, st.q_orb[i], n_orb)
    rt.atom_charges(st.q_orb[i], sh.q0_orb, sh.orb_atom, st.dq_atom[i], n_orb, n_atom)
    rt.shell_charges_from_orbital(
        st.q_orb[i], sh.q0_orb, sh.orb_shell, st.dq_shell[i], n_orb, n_shell
    )
    rt.rt_dipole(st.dq_atom[i], coords, mu, n_atom)


def _report(i, out, st, energy_start, energy_end, mu, dt):
    """Fill the reply the way the external DFTB+ driver builds it."""

    end = mu - st.mu_initial[i]
    start = out.mu_end[i].copy()
    out.amp[i] = (end - start) / dt
    out.mu_half[i] = 0.5 * (start + end)
    out.mu_end[i] = end
    out.energy[i] = 0.5 * (energy_start + energy_end)


def _cpu_step_frozen(i, st, sh, gm, out, field, step, dt):
    """One RT-TDDFTB step of system ``i``, at a fixed geometry."""

    from ..models.rtdftb_model import rt

    n_orb, n_shell = st.rho.shape[1], st.dq_shell.shape[1]
    n_atom = st.dq_atom.shape[1]
    mu = np.zeros(3)

    _rebuild_h(
        i,
        st,
        sh,
        gm.coords[i],
        gm.h0[i],
        gm.overlap[i],
        gm.gamma[i],
        field,
        n_orb,
        n_shell,
    )
    energy_start = _total_energy(
        i,
        st,
        st.rho,
        gm.coords[i],
        gm.h0[i],
        field,
        gm.e_repulsive[i],
        0.0,
        n_orb,
        n_shell,
        n_atom,
    )
    rt.leapfrog_step(
        st.rho_old[i],
        st.rho[i],
        st.h[i],
        gm.s_inv[i],
        st.coupling[i],
        step,
        st.work_a[i],
        st.work_b[i],
        st.work_c[i],
        n_orb,
    )
    _recharge(
        i, st, sh, st.rho_old, gm.overlap[i], gm.coords[i], mu, n_orb, n_shell, n_atom
    )
    _rebuild_h(
        i,
        st,
        sh,
        gm.coords[i],
        gm.h0[i],
        gm.overlap[i],
        gm.gamma[i],
        field,
        n_orb,
        n_shell,
    )
    energy_end = _total_energy(
        i,
        st,
        st.rho_old,
        gm.coords[i],
        gm.h0[i],
        field,
        gm.e_repulsive[i],
        0.0,
        n_orb,
        n_shell,
        n_atom,
    )
    _report(i, out, st, energy_start, energy_end, mu, dt)


def _cpu_step_ehrenfest(i, st, sh, gm, nu, sc, out, field, step, dt):
    """One RT-TDDFTB-Ehrenfest step of system ``i``."""

    from ..models.rtdftb_model import ehrenfest as ehr
    from ..models.rtdftb_model import forces, h0_overlap, rt, scc

    n_orb, n_shell = st.rho.shape[1], st.dq_shell.shape[1]
    n_atom = st.dq_atom.shape[1]
    mu = np.zeros(3)

    ehr.nuclear_step(
        nu.coords_next[i], nu.half_velocity[i], nu.accel[i], dt, n_atom, nu.velocity[i]
    )
    e_kin = ehr.kinetic_sum(sh.mass, nu.velocity[i], n_atom)
    st.e_kin[i] = e_kin  # recorded by step(), as the stage kernel stores it

    _rebuild_h(
        i,
        st,
        sh,
        gm.coords[i],
        gm.h0[i],
        gm.overlap[i],
        gm.gamma[i],
        field,
        n_orb,
        n_shell,
    )
    energy_start = _total_energy(
        i,
        st,
        st.rho,
        gm.coords[i],
        gm.h0[i],
        field,
        gm.e_repulsive[i],
        e_kin,
        n_orb,
        n_shell,
        n_atom,
    )
    ehr.coupling_kernel(
        sh.sk,
        gm.coords[i],
        sh.atom_species,
        sh.atom_offset,
        n_atom,
        nu.velocity[i],
        st.coupling[i],
        _coupling_scratch(sc, i),
    )
    rt.leapfrog_step(
        st.rho_old[i],
        st.rho[i],
        st.h[i],
        gm.s_inv[i],
        st.coupling[i],
        step,
        st.work_a[i],
        st.work_b[i],
        st.work_c[i],
        n_orb,
    )

    gm.coords[i] = nu.coords_next[i]
    h0_overlap.build_h0_overlap_kernel(
        sh.sk,
        gm.coords[i],
        sh.atom_species,
        sh.atom_offset,
        n_atom,
        gm.h0[i],
        gm.overlap[i],
        _h0_scratch(sc, i),
    )
    rt.lu_invert(gm.overlap[i], n_orb, sc.work_r[i], sc.pivot[i], gm.s_inv[i])
    scc.build_gamma(gm.coords[i], sh.shell_atom, sh.shell_u, gm.gamma[i])
    gm.e_repulsive[i] = scc.repulsive_sum(
        sh.sk, gm.coords[i], sh.atom_species, n_atom, scc.RepulsiveScratch(sc.pair[i])
    )

    _recharge(
        i, st, sh, st.rho_old, gm.overlap[i], gm.coords[i], mu, n_orb, n_shell, n_atom
    )
    _rebuild_h(
        i,
        st,
        sh,
        gm.coords[i],
        gm.h0[i],
        gm.overlap[i],
        gm.gamma[i],
        field,
        n_orb,
        n_shell,
    )
    energy_end = _total_energy(
        i,
        st,
        st.rho_old,
        gm.coords[i],
        gm.h0[i],
        field,
        gm.e_repulsive[i],
        e_kin,
        n_orb,
        n_shell,
        n_atom,
    )
    _report(i, out, st, energy_start, energy_end, mu, dt)

    ehr.real_part(st.rho_old[i], sc.density[i], n_orb)
    ehr.energy_weighted_density(
        sc.density[i], st.h[i], gm.s_inv[i], sc.product[i], sc.weight_e[i], n_orb
    )
    forces.overlap_weight(sc.density[i], sc.weight_e[i], st.v_orb[i], sc.weight[i])
    sc.gradient[i] = 0.0
    forces.band_gradient_kernel(
        sh.sk,
        gm.coords[i],
        sh.atom_species,
        sh.atom_offset,
        n_atom,
        sc.density[i],
        sc.weight[i],
        sc.gradient[i],
        _band_scratch(sc, i),
    )
    forces.gamma_gradient_kernel(
        gm.coords[i], sh.shell_atom, sh.shell_u, st.dq_shell[i], n_shell, sc.gradient[i]
    )
    forces.repulsive_gradient_kernel(
        sh.sk, gm.coords[i], sh.atom_species, n_atom, sc.gradient[i], sc.pair[i]
    )
    ehr.field_gradient(st.dq_atom[i], field[i], sc.gradient[i], n_atom)
    nu.force[i] = -sc.gradient[i]
    nu.accel[i] = nu.force[i] / sh.mass[:, None]


def _h0_scratch(sc, i):
    """The H0/S assembly scratch of one system, sliced out of the batch."""

    from ..models.rtdftb_model.h0_overlap import H0Scratch

    return H0Scratch(
        sc.h0_sk_h[i],
        sc.h0_sk_s[i],
        sc.h0_block_h[i],
        sc.h0_block_s[i],
        sc.h0_core[i],
        sc.h0_node[i],
        sc.h0_cc[i],
        sc.h0_dd[i],
        sc.h0_delta[i],
        sc.h0_y_low[i],
        sc.h0_y_high[i],
    )


def _band_scratch(sc, i):
    """The band-gradient scratch of one system."""

    from ..models.rtdftb_model.forces import BandScratch

    return BandScratch(
        sc.b_sk_h[i],
        sc.b_sk_s[i],
        sc.b_dsk_h[i],
        sc.b_dsk_s[i],
        sc.b_d_h0[i],
        sc.b_d_overlap[i],
        sc.b_weight[i],
        sc.b_first[i],
        sc.b_second[i],
        sc.b_radial[i],
        sc.b_angular[i],
        sc.b_core[i],
        sc.b_dcore[i],
    )


def _coupling_scratch(sc, i):
    """The non-adiabatic-coupling scratch of one system."""

    from ..models.rtdftb_model.ehrenfest import CouplingScratch

    return CouplingScratch(
        sc.b_sk_s[i],
        sc.b_dsk_s[i],
        sc.b_d_overlap[i],
        sc.b_weight[i],
        sc.b_first[i],
        sc.b_second[i],
        sc.b_radial[i],
        sc.b_angular[i],
        sc.b_core[i],
        sc.b_dcore[i],
    )
