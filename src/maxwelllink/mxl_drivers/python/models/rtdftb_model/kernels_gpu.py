# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
CUDA kernels of the batched real-time step, for the GPU batch driver.

This is the GPU form of the RT-TDDFTB physics, the counterpart of the md path's
``kernels_co2.py``: the scalar-loop bodies of the other modules are compiled as CUDA
device functions by :mod:`jit`, and here they are composed into the kernels
:class:`~maxwelllink.mxl_drivers.python.batch.rtdftb_gpu.RTDFTBGPUBatchModel` launches.
Nothing here touches the CUDA runtime at import time, so the module imports on a
CUDA-less host like every other module of the package.

One real-time step of one system is a fixed sequence of *phases* -- the nuclear move,
the SCC potential rows, the Hamiltonian rows, the coupling pairs, the charges, the
geometry rebuild, the force pairs, ... -- with the dense linear algebra (the leapfrog
products, ``S^-1``, the energy-weighted density) done by CuPy in between. Every phase is
written **once** (:func:`_phases`) as a device function striding its work over
``worker = 0 ... n_workers - 1``. Two kernel families are then assembled from the same
phases:

- the **narrow** family (:func:`narrow_kernels`), one CUDA block per system, fuses the
  phases of one stage into one launch with ``syncthreads`` between them; it is the
  default when the batch alone fills the GPU (thousands of small systems);
- the **wide** family (:func:`wide_kernels`) spreads one system over
  ``blocks_per_system`` blocks, one launch per parallel phase on a
  ``(num, blocks_per_system)`` grid and the serial bookkeeping in narrow launches
  between; kernel boundaries provide the grid-wide ordering ``syncthreads`` gives inside
  one block. It serves a small batch of large systems, and the fully-GPU initialization.

The kernels take their arrays as namedtuple bundles (:data:`State`, :data:`Shared`, ...):
numba accepts a namedtuple of device arrays as a kernel argument, and bundling matters --
with the arrays passed individually the launch spends over a millisecond per step
re-resolving argument types on the host, which at these batch sizes dwarfs the time the
device actually needs.
"""

import math
from collections import namedtuple

from .dftb_params import MAX_INTEGRAL, MAX_ORB, N_INTERPOLATION
from .h0_overlap import PairScratch

#: Threads per block for the CUDA launches. A multiple of the warp size (32).
THREADS_PER_BLOCK = 128

# The difference table of the interpolation stencil sizes per-thread scratch; a local
# array shape must be a constant name, not an expression written inside the kernel.
_N_DELTA = N_INTERPOLATION - 1

# ---------------------------------------------------------------------------- #
# kernel argument bundles                                                      #
# ---------------------------------------------------------------------------- #
#: Per-system electronic state and the scalars carried between the stages of a step.
State = namedtuple(
    "State",
    "rho rho_old coupling q_orb dq_atom dq_shell v_scc_shell v_orb h "
    "energy_start e_kin mu_initial",
)
#: Topology shared by every system: the parameter tables, the basis and shell layout.
Shared = namedtuple(
    "Shared",
    "sk atom_species atom_offset orb_shell orb_atom shell_atom shell_u q0_orb mass",
)
#: Geometry-dependent matrices, indexed per system by the kernels. One shared copy is
#: held while every system sits at the same frozen geometry, and the kernels then read
#: zero-stride views of it; otherwise one copy per system.
Geometry = namedtuple("Geometry", "coords h0 overlap s_inv gamma e_repulsive")
#: Nuclear integrator state, only with moving nuclei.
Nuclear = namedtuple("Nuclear", "velocity half_velocity coords_next accel force")
#: Per-system dense work of the Ehrenfest force: ``Re[rho]``, ``P H``, the energy-
#: weighted density, the overlap weight and the gradient.
Forces = namedtuple("Forces", "density product weight_e weight gradient")
#: The reply of a step: ``dmu/dt``, the end-of-step dipole carried into the next step as
#: its start, the midpoint dipole, and the midpoint energy.
Out = namedtuple("Out", "amp mu_end mu_half energy")

#: The narrow (block-per-system) family: ``geometry`` and ``force`` exist only with
#: moving nuclei (``None`` otherwise).
NarrowKernels = namedtuple(
    "NarrowKernels", "pre post geometry force", defaults=(None, None)
)
#: The wide (phase-split) family; ``nuclear``, ``coupling``, ``adopt`` and the three
#: force kernels exist only with moving nuclei (``None`` otherwise).
WideKernels = namedtuple(
    "WideKernels",
    "scc_rows potentials hamiltonian energy_start geometry charge_columns charges "
    "report nuclear coupling adopt force_weight force_pairs force_finish",
    defaults=(None,) * 6,
)

_PHASES = None
_NARROW = {}
_WIDE = {}


def _cuda():
    """Import numba's CUDA target, raising a clear error on a CUDA-less host."""

    try:
        from numba import cuda, float64
    except Exception as exc:  # ImportError, or a numba/CUDA runtime failure
        raise ImportError(
            "The GPU batch backend requires numba's CUDA target. Install it "
            "alongside CuPy, e.g. 'pip install maxwelllink[gpu-cuda12]'. On hosts "
            "without CUDA, inject xp=numpy to run the scalar drivers instead."
        ) from exc
    return cuda, float64


# ---------------------------------------------------------------------------- #
# the phases                                                                   #
# ---------------------------------------------------------------------------- #
def _phases():
    """
    Compile (once per process) the device-side phases both kernel families use.

    Returns
    -------
    namedtuple
        The phases as CUDA device functions. Parallel phases take ``(worker,
        n_workers)`` and stride their loops; serial ones are meant for one thread.
    """

    global _PHASES
    if _PHASES is not None:
        return _PHASES

    cuda, float64 = _cuda()
    from .jit import device_kernels

    d = device_kernels()
    nuclear_step = d["nuclear_step"]
    kinetic_sum = d["kinetic_sum"]
    scc_potential_row = d["scc_potential_row"]
    orbital_potential = d["orbital_potential"]
    scc_hamiltonian_row = d["scc_hamiltonian_row"]
    band_energy_row = d["band_energy_row"]
    scc_energy = d["scc_energy"]
    external_energy = d["external_energy"]
    orbital_charge = d["orbital_charge"]
    atom_charges = d["atom_charges"]
    shell_charges = d["shell_charges"]
    dipole_from_charges = d["dipole_from_charges"]
    h0_overlap_onsite = d["h0_overlap_onsite"]
    h0_overlap_pair = d["h0_overlap_pair"]
    gamma_element = d["gamma_element"]
    repulsive_pair = d["repulsive_pair"]
    coupling_pair = d["coupling_pair"]
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

    # -- the nuclei -------------------------------------------------------------
    @cuda.jit(device=True)
    def nuclear(i, st, sh, nu, dt):
        """Move the nuclei of ONE system: ``a(t)`` in, ``r(t+dt)`` and ``v(t)`` out
        (serial), and its kinetic energy."""

        n_atom = st.dq_atom.shape[1]
        nuclear_step(
            nu.coords_next[i],
            nu.half_velocity[i],
            nu.accel[i],
            dt,
            n_atom,
            nu.velocity[i],
        )
        st.e_kin[i] = kinetic_sum(sh.mass, nu.velocity[i], n_atom)

    # -- potentials, Hamiltonian, energy ------------------------------------------
    @cuda.jit(device=True)
    def scc_rows(i, st, gm, worker, n_workers):
        """The ``n_shell^2`` gamma contraction, its rows over the workers."""

        for row in range(worker, st.dq_shell.shape[1], n_workers):
            scc_potential_row(gm.gamma[i], st.dq_shell[i], st.v_scc_shell[i], row)

    @cuda.jit(device=True)
    def potentials(i, st, sh, gm, field, worker, n_workers):
        """The orbital potentials, SCC plus field, over the workers."""

        for mu in range(worker, st.rho.shape[1], n_workers):
            st.v_orb[i, mu] = orbital_potential(
                st.v_scc_shell[i], gm.coords[i], field[i], sh.orb_shell, sh.orb_atom, mu
            )

    @cuda.jit(device=True)
    def hamiltonian_rows(i, st, gm, worker, n_workers):
        """``H = H0 + 0.5 S (V_mu + V_nu)``, its rows over the workers."""

        for mu in range(worker, st.rho.shape[1], n_workers):
            scc_hamiltonian_row(gm.h0[i], gm.overlap[i], st.v_orb[i], st.h[i], mu)

    @cuda.jit(device=True)
    def band_partial(rho, h0, worker, n_workers):
        """This worker's rows of ``Tr(rho H0)``."""

        n_orb = rho.shape[0]
        total = 0.0
        for mu in range(worker, n_orb, n_workers):
            total += band_energy_row(rho, h0, mu, n_orb)
        return total

    @cuda.jit(device=True)
    def energy_rest(i, st, gm, field):
        """The energy of ONE system apart from its band term: SCC, field, repulsive,
        kinetic."""

        return (
            scc_energy(st.v_scc_shell[i], st.dq_shell[i])
            + external_energy(st.dq_atom[i], gm.coords[i], field[i])
            + gm.e_repulsive[i]
            + st.e_kin[i]
        )

    # -- the coupling -------------------------------------------------------------
    @cuda.jit(device=True)
    def coupling_pairs(i, st, sh, gm, nu, worker, n_workers):
        """``D(t)`` at ``r(t)`` with ``v(t)``, one ordered atom pair per worker and turn,
        added to a zeroed ``coupling``."""

        # this thread's PairScratch in local memory; the shapes mirror
        # h0_overlap.PAIR_SCRATCH_SHAPES field for field (a device function may not
        # return local arrays, so every phase that walks pairs allocates its own)
        s = PairScratch(
            cuda.local.array(MAX_INTEGRAL, float64),  # sk_h
            cuda.local.array(MAX_INTEGRAL, float64),  # sk_s
            cuda.local.array(MAX_INTEGRAL, float64),  # dsk_h
            cuda.local.array(MAX_INTEGRAL, float64),  # dsk_s
            cuda.local.array((MAX_ORB, MAX_ORB), float64),  # block_h
            cuda.local.array((MAX_ORB, MAX_ORB), float64),  # block_s
            cuda.local.array((5, 5), float64),  # core
            cuda.local.array(N_INTERPOLATION, float64),  # node
            cuda.local.array(N_INTERPOLATION, float64),  # cc
            cuda.local.array(N_INTERPOLATION, float64),  # dd
            cuda.local.array(_N_DELTA, float64),  # delta
            cuda.local.array(MAX_INTEGRAL, float64),  # y_low
            cuda.local.array(MAX_INTEGRAL, float64),  # y_high
            cuda.local.array(N_INTERPOLATION, float64),  # weight
            cuda.local.array(N_INTERPOLATION, float64),  # first
            cuda.local.array(N_INTERPOLATION, float64),  # second
            cuda.local.array((3, MAX_ORB, MAX_ORB), float64),  # d_h0
            cuda.local.array((3, MAX_ORB, MAX_ORB), float64),  # d_overlap
            cuda.local.array((MAX_ORB, MAX_ORB), float64),  # radial
            cuda.local.array((3, MAX_ORB, MAX_ORB), float64),  # angular
            cuda.local.array((3, 5, 5), float64),  # dcore
            cuda.local.array(2, float64),  # pair
        )
        n_atom = st.dq_atom.shape[1]
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
                s,
            )

    # -- the geometry -------------------------------------------------------------
    @cuda.jit(device=True)
    def adopt_geometry(i, st, gm, nu, worker, n_workers):
        """Take on ``r(t+dt)`` and clean ``H0`` and ``S`` for the rebuild."""

        n_orb = st.rho.shape[1]
        for a in range(worker, st.dq_atom.shape[1], n_workers):
            for k in range(3):
                gm.coords[i, a, k] = nu.coords_next[i, a, k]
        for e in range(worker, n_orb * n_orb, n_workers):
            row = e // n_orb
            col = e - row * n_orb
            gm.h0[i, row, col] = 0.0
            gm.overlap[i, row, col] = 0.0

    @cuda.jit(device=True)
    def geometry_pairs(i, st, sh, gm, worker, n_workers):
        """``H0``, ``S`` and gamma at the current coordinates, and this worker's share
        of the repulsive sum; the matrices must come in clean."""

        # this thread's PairScratch in local memory; the shapes mirror
        # h0_overlap.PAIR_SCRATCH_SHAPES field for field (a device function may not
        # return local arrays, so every phase that walks pairs allocates its own)
        s = PairScratch(
            cuda.local.array(MAX_INTEGRAL, float64),  # sk_h
            cuda.local.array(MAX_INTEGRAL, float64),  # sk_s
            cuda.local.array(MAX_INTEGRAL, float64),  # dsk_h
            cuda.local.array(MAX_INTEGRAL, float64),  # dsk_s
            cuda.local.array((MAX_ORB, MAX_ORB), float64),  # block_h
            cuda.local.array((MAX_ORB, MAX_ORB), float64),  # block_s
            cuda.local.array((5, 5), float64),  # core
            cuda.local.array(N_INTERPOLATION, float64),  # node
            cuda.local.array(N_INTERPOLATION, float64),  # cc
            cuda.local.array(N_INTERPOLATION, float64),  # dd
            cuda.local.array(_N_DELTA, float64),  # delta
            cuda.local.array(MAX_INTEGRAL, float64),  # y_low
            cuda.local.array(MAX_INTEGRAL, float64),  # y_high
            cuda.local.array(N_INTERPOLATION, float64),  # weight
            cuda.local.array(N_INTERPOLATION, float64),  # first
            cuda.local.array(N_INTERPOLATION, float64),  # second
            cuda.local.array((3, MAX_ORB, MAX_ORB), float64),  # d_h0
            cuda.local.array((3, MAX_ORB, MAX_ORB), float64),  # d_overlap
            cuda.local.array((MAX_ORB, MAX_ORB), float64),  # radial
            cuda.local.array((3, MAX_ORB, MAX_ORB), float64),  # angular
            cuda.local.array((3, 5, 5), float64),  # dcore
            cuda.local.array(2, float64),  # pair
        )
        n_atom = st.dq_atom.shape[1]
        n_shell = st.dq_shell.shape[1]
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
                s,
            )
            e_pair, g_x, g_y, g_z = repulsive_pair(
                sh.sk, gm.coords[i], sh.atom_species, a, b, s.pair
            )
            rep += e_pair
        for e in range(worker, n_shell * n_shell, n_workers):
            row = e // n_shell
            col = e - row * n_shell
            gm.gamma[i, row, col] = gamma_element(
                gm.coords[i], sh.shell_atom, sh.shell_u, row, col
            )
        return rep

    # -- the charges --------------------------------------------------------------
    @cuda.jit(device=True)
    def charge_columns(i, st, gm, worker, n_workers):
        """Orbital populations of ``rho(t+dt)``, held in ``rho_old`` where the leapfrog
        products left it, its columns over the workers."""

        n_orb = st.rho.shape[1]
        for col in range(worker, n_orb, n_workers):
            st.q_orb[i, col] = orbital_charge(st.rho_old[i], gm.overlap[i], col, n_orb)

    @cuda.jit(device=True)
    def density_rows(i, st, fw, worker, n_workers):
        """``Re[rho(t+dt)]`` for the force, its rows over the workers."""

        n_orb = st.rho.shape[1]
        for row in range(worker, n_orb, n_workers):
            real_part_row(st.rho_old[i], fw.density[i], row, n_orb)

    @cuda.jit(device=True)
    def charges(i, st, sh, gm, mu):
        """Atom and shell charges and the dipole of ONE system from ``q_orb`` (serial)."""

        atom_charges(st.q_orb[i], sh.q0_orb, sh.orb_atom, st.dq_atom[i])
        shell_charges(st.q_orb[i], sh.q0_orb, sh.orb_shell, st.dq_shell[i])
        dipole_from_charges(st.dq_atom[i], gm.coords[i], mu)

    @cuda.jit(device=True)
    def report(i, out, st, energy_end, mu, dt):
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
        out.energy[i] = 0.5 * (st.energy_start[i] + energy_end)

    # -- the force ----------------------------------------------------------------
    @cuda.jit(device=True)
    def force_weight(i, st, fw, worker, n_workers):
        """The overlap-weighted density, its rows over the workers, and a clean gradient."""

        n_orb = st.rho.shape[1]
        for row in range(worker, n_orb, n_workers):
            overlap_weight_row(
                fw.density[i], fw.weight_e[i], st.v_orb[i], fw.weight[i], row
            )
        for a in range(worker, st.dq_atom.shape[1], n_workers):
            for k in range(3):
                fw.gradient[i, a, k] = 0.0

    @cuda.jit(device=True)
    def gradient_pairs(i, st, sh, gm, fw, worker, n_workers):
        """Band, gamma and repulsive gradient sums, atomically onto the atoms.

        The pair sums accumulate in an order that is not fixed, so the gradient differs
        from the serial sum at round-off only.
        """

        # this thread's PairScratch in local memory; the shapes mirror
        # h0_overlap.PAIR_SCRATCH_SHAPES field for field (a device function may not
        # return local arrays, so every phase that walks pairs allocates its own)
        s = PairScratch(
            cuda.local.array(MAX_INTEGRAL, float64),  # sk_h
            cuda.local.array(MAX_INTEGRAL, float64),  # sk_s
            cuda.local.array(MAX_INTEGRAL, float64),  # dsk_h
            cuda.local.array(MAX_INTEGRAL, float64),  # dsk_s
            cuda.local.array((MAX_ORB, MAX_ORB), float64),  # block_h
            cuda.local.array((MAX_ORB, MAX_ORB), float64),  # block_s
            cuda.local.array((5, 5), float64),  # core
            cuda.local.array(N_INTERPOLATION, float64),  # node
            cuda.local.array(N_INTERPOLATION, float64),  # cc
            cuda.local.array(N_INTERPOLATION, float64),  # dd
            cuda.local.array(_N_DELTA, float64),  # delta
            cuda.local.array(MAX_INTEGRAL, float64),  # y_low
            cuda.local.array(MAX_INTEGRAL, float64),  # y_high
            cuda.local.array(N_INTERPOLATION, float64),  # weight
            cuda.local.array(N_INTERPOLATION, float64),  # first
            cuda.local.array(N_INTERPOLATION, float64),  # second
            cuda.local.array((3, MAX_ORB, MAX_ORB), float64),  # d_h0
            cuda.local.array((3, MAX_ORB, MAX_ORB), float64),  # d_overlap
            cuda.local.array((MAX_ORB, MAX_ORB), float64),  # radial
            cuda.local.array((3, MAX_ORB, MAX_ORB), float64),  # angular
            cuda.local.array((3, 5, 5), float64),  # dcore
            cuda.local.array(2, float64),  # pair
        )
        n_atom = st.dq_atom.shape[1]
        n_shell = st.dq_shell.shape[1]
        for p in range(worker, n_atom * (n_atom - 1), n_workers):
            a, b = ordered_pair(p, n_atom)
            g_x, g_y, g_z = band_gradient_pair(
                sh.sk,
                gm.coords[i],
                sh.atom_species,
                sh.atom_offset,
                a,
                b,
                fw.density[i],
                fw.weight[i],
                s,
            )
            cuda.atomic.add(fw.gradient, (i, a, 0), g_x)
            cuda.atomic.add(fw.gradient, (i, a, 1), g_y)
            cuda.atomic.add(fw.gradient, (i, a, 2), g_z)
        for e in range(worker, n_shell * n_shell, n_workers):
            row = e // n_shell
            col = e - row * n_shell
            atom_row = sh.shell_atom[row]
            atom_col = sh.shell_atom[col]
            if atom_row != atom_col:
                f_x, f_y, f_z = gamma_gradient_pair(
                    gm.coords[i], sh.shell_atom, sh.shell_u, st.dq_shell[i], row, col
                )
                cuda.atomic.add(fw.gradient, (i, atom_row, 0), f_x)
                cuda.atomic.add(fw.gradient, (i, atom_row, 1), f_y)
                cuda.atomic.add(fw.gradient, (i, atom_row, 2), f_z)
                cuda.atomic.add(fw.gradient, (i, atom_col, 0), -f_x)
                cuda.atomic.add(fw.gradient, (i, atom_col, 1), -f_y)
                cuda.atomic.add(fw.gradient, (i, atom_col, 2), -f_z)
        for p in range(worker, n_atom * (n_atom - 1) // 2, n_workers):
            a, b = unordered_pair(p)
            e_pair, g_x, g_y, g_z = repulsive_pair(
                sh.sk, gm.coords[i], sh.atom_species, a, b, s.pair
            )
            cuda.atomic.add(fw.gradient, (i, a, 0), g_x)
            cuda.atomic.add(fw.gradient, (i, a, 1), g_y)
            cuda.atomic.add(fw.gradient, (i, a, 2), g_z)
            cuda.atomic.add(fw.gradient, (i, b, 0), -g_x)
            cuda.atomic.add(fw.gradient, (i, b, 1), -g_y)
            cuda.atomic.add(fw.gradient, (i, b, 2), -g_z)

    @cuda.jit(device=True)
    def field_force(i, st, fw, field):
        """The field term ``dq_A E`` of the gradient (serial)."""

        field_gradient(st.dq_atom[i], field[i], fw.gradient[i])

    @cuda.jit(device=True)
    def force_accel(i, sh, nu, fw, worker, n_workers):
        """The total force and acceleration from the finished gradient."""

        for a in range(worker, nu.force.shape[1], n_workers):
            for k in range(3):
                nu.force[i, a, k] = -fw.gradient[i, a, k]
                nu.accel[i, a, k] = nu.force[i, a, k] / sh.mass[a]

    Phases = namedtuple(
        "Phases",
        "nuclear scc_rows potentials hamiltonian_rows band_partial energy_rest "
        "coupling_pairs adopt_geometry geometry_pairs charge_columns density_rows "
        "charges report force_weight gradient_pairs field_force force_accel",
    )
    _PHASES = Phases(
        nuclear,
        scc_rows,
        potentials,
        hamiltonian_rows,
        band_partial,
        energy_rest,
        coupling_pairs,
        adopt_geometry,
        geometry_pairs,
        charge_columns,
        density_rows,
        charges,
        report,
        force_weight,
        gradient_pairs,
        field_force,
        force_accel,
    )
    return _PHASES


# ---------------------------------------------------------------------------- #
# the narrow family: one block per system                                      #
# ---------------------------------------------------------------------------- #
def narrow_kernels(ehrenfest):
    """
    Compile (once) and return the block-per-system stage kernels of one step.

    Every kernel launches as ``kernel[num, THREADS_PER_BLOCK]``. The stages of a step
    are ``pre`` (the nuclear move, ``H(t)``, ``E(t)``, ``D(t)``), then the leapfrog
    products, ``geometry`` (adopt ``r(t+dt)`` and rebuild the matrices there), then
    ``S^-1``, ``post`` (charges, ``H(t+dt)``, ``E(t+dt)`` and the reply), then the
    energy-weighted density, and ``force``. Reductions go through shared memory.

    Parameters
    ----------
    ehrenfest : bool
        Whether the nuclei move, which decides which stages are built.

    Returns
    -------
    NarrowKernels
        The compiled kernels.
    """

    if ehrenfest in _NARROW:
        return _NARROW[ehrenfest]
    cuda, float64 = _cuda()
    ph = _phases()

    if not ehrenfest:

        @cuda.jit
        def k_pre(st, sh, gm, field, first):
            """``H(t)`` and, on the first sub-step, ``E(t)`` of ONE system."""

            i = cuda.blockIdx.x
            tid = cuda.threadIdx.x
            tpb = cuda.blockDim.x
            acc = cuda.shared.array(1, float64)
            if tid == 0:
                acc[0] = 0.0
            ph.scc_rows(i, st, gm, tid, tpb)
            cuda.syncthreads()
            ph.potentials(i, st, sh, gm, field, tid, tpb)
            cuda.syncthreads()
            ph.hamiltonian_rows(i, st, gm, tid, tpb)
            cuda.atomic.add(acc, 0, ph.band_partial(st.rho[i], gm.h0[i], tid, tpb))
            cuda.syncthreads()
            if tid == 0 and first:
                st.energy_start[i] = acc[0] + ph.energy_rest(i, st, gm, field)

        @cuda.jit
        def k_post(st, sh, gm, out, field, dt, last):
            """Charges, dipole, ``H`` and energy at ``t + dt`` of ONE system, and on
            the last sub-step its reply."""

            i = cuda.blockIdx.x
            tid = cuda.threadIdx.x
            tpb = cuda.blockDim.x
            acc = cuda.shared.array(1, float64)
            mu = cuda.local.array(3, float64)
            ph.charge_columns(i, st, gm, tid, tpb)
            cuda.syncthreads()
            if tid == 0:
                acc[0] = 0.0
                ph.charges(i, st, sh, gm, mu)
            cuda.syncthreads()
            ph.scc_rows(i, st, gm, tid, tpb)
            cuda.syncthreads()
            ph.potentials(i, st, sh, gm, field, tid, tpb)
            cuda.syncthreads()
            ph.hamiltonian_rows(i, st, gm, tid, tpb)
            cuda.atomic.add(acc, 0, ph.band_partial(st.rho_old[i], gm.h0[i], tid, tpb))
            cuda.syncthreads()
            if tid == 0 and last:
                energy_end = acc[0] + ph.energy_rest(i, st, gm, field)
                ph.report(i, out, st, energy_end, mu, dt)

        _NARROW[ehrenfest] = NarrowKernels(pre=k_pre, post=k_post)
        return _NARROW[ehrenfest]

    @cuda.jit
    def k_pre(st, sh, gm, nu, field, dt, first):
        """Nuclear step, ``H(t)``, ``D(t)`` and, on the first sub-step, ``E(t)`` of ONE
        system."""

        i = cuda.blockIdx.x
        tid = cuda.threadIdx.x
        tpb = cuda.blockDim.x
        n_orb = st.rho.shape[1]
        acc = cuda.shared.array(1, float64)
        if tid == 0:
            acc[0] = 0.0
            ph.nuclear(i, st, sh, nu, dt)
        # D is assembled onto a clean matrix (see coupling_kernel)
        for e in range(tid, n_orb * n_orb, tpb):
            st.coupling[i, e // n_orb, e % n_orb] = 0.0
        ph.scc_rows(i, st, gm, tid, tpb)
        cuda.syncthreads()
        ph.potentials(i, st, sh, gm, field, tid, tpb)
        cuda.syncthreads()
        ph.hamiltonian_rows(i, st, gm, tid, tpb)
        cuda.atomic.add(acc, 0, ph.band_partial(st.rho[i], gm.h0[i], tid, tpb))
        # D(t) is built at r(t) with v(t), before the geometry is adopted
        ph.coupling_pairs(i, st, sh, gm, nu, tid, tpb)
        cuda.syncthreads()
        if tid == 0 and first:
            st.energy_start[i] = acc[0] + ph.energy_rest(i, st, gm, field)

    @cuda.jit
    def k_geometry(st, sh, gm, nu):
        """Adopt ``r(t+dt)`` and rebuild ``H0``, ``S``, gamma and ``E_rep`` there."""

        i = cuda.blockIdx.x
        tid = cuda.threadIdx.x
        tpb = cuda.blockDim.x
        acc = cuda.shared.array(1, float64)
        if tid == 0:
            acc[0] = 0.0
        ph.adopt_geometry(i, st, gm, nu, tid, tpb)
        cuda.syncthreads()
        cuda.atomic.add(acc, 0, ph.geometry_pairs(i, st, sh, gm, tid, tpb))
        cuda.syncthreads()
        if tid == 0:
            gm.e_repulsive[i] = acc[0]

    @cuda.jit
    def k_post(st, sh, gm, fw, out, field, dt, last):
        """Charges, dipole, ``H``, energy and ``Re[rho]`` at ``t + dt`` of ONE system,
        and on the last sub-step its reply."""

        i = cuda.blockIdx.x
        tid = cuda.threadIdx.x
        tpb = cuda.blockDim.x
        acc = cuda.shared.array(1, float64)
        mu = cuda.local.array(3, float64)
        ph.charge_columns(i, st, gm, tid, tpb)
        ph.density_rows(i, st, fw, tid, tpb)
        cuda.syncthreads()
        if tid == 0:
            acc[0] = 0.0
            ph.charges(i, st, sh, gm, mu)
        cuda.syncthreads()
        ph.scc_rows(i, st, gm, tid, tpb)
        cuda.syncthreads()
        ph.potentials(i, st, sh, gm, field, tid, tpb)
        cuda.syncthreads()
        ph.hamiltonian_rows(i, st, gm, tid, tpb)
        cuda.atomic.add(acc, 0, ph.band_partial(st.rho_old[i], gm.h0[i], tid, tpb))
        cuda.syncthreads()
        if tid == 0 and last:
            energy_end = acc[0] + ph.energy_rest(i, st, gm, field)
            ph.report(i, out, st, energy_end, mu, dt)

    @cuda.jit
    def k_force(st, sh, gm, nu, fw, field):
        """The Ehrenfest force at ``r(t+dt)`` of ONE system, from the weighted densities."""

        i = cuda.blockIdx.x
        tid = cuda.threadIdx.x
        tpb = cuda.blockDim.x
        ph.force_weight(i, st, fw, tid, tpb)
        cuda.syncthreads()
        ph.gradient_pairs(i, st, sh, gm, fw, tid, tpb)
        cuda.syncthreads()
        if tid == 0:
            ph.field_force(i, st, fw, field)
        cuda.syncthreads()
        ph.force_accel(i, sh, nu, fw, tid, tpb)

    _NARROW[ehrenfest] = NarrowKernels(
        pre=k_pre, geometry=k_geometry, post=k_post, force=k_force
    )
    return _NARROW[ehrenfest]


# ---------------------------------------------------------------------------- #
# the wide family: one launch per phase                                        #
# ---------------------------------------------------------------------------- #
def wide_kernels(ehrenfest):
    """
    Compile (once) the phase-split kernels that spread ONE system over MANY blocks.

    Wide kernels launch as ``kernel[(num, blocks_per_system), THREADS_PER_BLOCK]`` and
    stride their work over every thread of every block of the system; narrow ones
    launch as ``kernel[num, THREADS_PER_BLOCK]`` for the serial bookkeeping. The band
    energy reductions go through a ``(num,)`` accumulator the host zeroes.

    Parameters
    ----------
    ehrenfest : bool
        Whether the nuclei move, which decides which phases are built.

    Returns
    -------
    WideKernels
        The compiled kernels.
    """

    if ehrenfest in _WIDE:
        return _WIDE[ehrenfest]
    cuda, float64 = _cuda()
    ph = _phases()

    @cuda.jit(device=True)
    def wide_index():
        """This thread's worker index over every block of its system, and the count."""

        return (
            cuda.blockIdx.y * cuda.blockDim.x + cuda.threadIdx.x,
            cuda.gridDim.y * cuda.blockDim.x,
        )

    @cuda.jit
    def k_scc_rows(st, gm):
        worker, n_workers = wide_index()
        ph.scc_rows(cuda.blockIdx.x, st, gm, worker, n_workers)

    @cuda.jit
    def k_potentials(st, sh, gm, field):
        worker, n_workers = wide_index()
        ph.potentials(cuda.blockIdx.x, st, sh, gm, field, worker, n_workers)

    @cuda.jit
    def k_hamiltonian(st, gm, acc, use_old):
        """``H`` rows and the band term of ``rho`` (``use_old == 0``) or of ``rho_old``."""

        i = cuda.blockIdx.x
        worker, n_workers = wide_index()
        ph.hamiltonian_rows(i, st, gm, worker, n_workers)
        rho = st.rho_old[i] if use_old else st.rho[i]
        cuda.atomic.add(acc, i, ph.band_partial(rho, gm.h0[i], worker, n_workers))

    @cuda.jit
    def k_energy_start(st, gm, field, acc, first):
        """``E(t)`` from the accumulated band term, on the first sub-step; narrow."""

        i = cuda.blockIdx.x
        if cuda.threadIdx.x == 0 and first:
            st.energy_start[i] = acc[i] + ph.energy_rest(i, st, gm, field)

    @cuda.jit
    def k_geometry(st, sh, gm):
        """``H0``, ``S``, gamma and the repulsive sum at the current coordinates; wide.

        The matrices and ``e_repulsive`` must come in clean: the host zeroes them.
        Shared by both families: the Ehrenfest step rebuilds the geometry every step,
        and the fully-GPU initialization builds it once for frozen runs too.
        """

        i = cuda.blockIdx.x
        worker, n_workers = wide_index()
        rep = ph.geometry_pairs(i, st, sh, gm, worker, n_workers)
        cuda.atomic.add(gm.e_repulsive, i, rep)

    @cuda.jit
    def k_charges(st, sh, gm, mu_out):
        """Charges and dipole of the density held in ``rho_old``; narrow."""

        i = cuda.blockIdx.x
        if cuda.threadIdx.x == 0:
            ph.charges(i, st, sh, gm, mu_out[i])

    @cuda.jit
    def k_report(st, gm, out, field, mu, acc, dt, last):
        """``E(t+dt)`` and, on the last sub-step, the reply to MaxwellLink; narrow."""

        i = cuda.blockIdx.x
        if cuda.threadIdx.x == 0 and last:
            energy_end = acc[i] + ph.energy_rest(i, st, gm, field)
            ph.report(i, out, st, energy_end, mu[i], dt)

    if not ehrenfest:

        @cuda.jit
        def k_charge_columns(st, gm):
            worker, n_workers = wide_index()
            ph.charge_columns(cuda.blockIdx.x, st, gm, worker, n_workers)

        _WIDE[ehrenfest] = WideKernels(
            scc_rows=k_scc_rows,
            potentials=k_potentials,
            hamiltonian=k_hamiltonian,
            energy_start=k_energy_start,
            geometry=k_geometry,
            charge_columns=k_charge_columns,
            charges=k_charges,
            report=k_report,
        )
        return _WIDE[ehrenfest]

    @cuda.jit
    def k_nuclear(st, sh, nu, dt):
        """The nuclear move and kinetic energy; narrow."""

        if cuda.threadIdx.x == 0:
            ph.nuclear(cuda.blockIdx.x, st, sh, nu, dt)

    @cuda.jit
    def k_coupling(st, sh, gm, nu):
        """``D(t)`` onto the zeroed coupling; wide."""

        worker, n_workers = wide_index()
        ph.coupling_pairs(cuda.blockIdx.x, st, sh, gm, nu, worker, n_workers)

    @cuda.jit
    def k_adopt(st, gm, nu):
        """Adopt ``r(t+dt)`` and clean ``H0`` and ``S``; wide."""

        worker, n_workers = wide_index()
        ph.adopt_geometry(cuda.blockIdx.x, st, gm, nu, worker, n_workers)

    @cuda.jit
    def k_charge_columns(st, gm, fw):
        """Orbital charges and ``Re[rho(t+dt)]``; wide."""

        i = cuda.blockIdx.x
        worker, n_workers = wide_index()
        ph.charge_columns(i, st, gm, worker, n_workers)
        ph.density_rows(i, st, fw, worker, n_workers)

    @cuda.jit
    def k_force_weight(st, fw):
        worker, n_workers = wide_index()
        ph.force_weight(cuda.blockIdx.x, st, fw, worker, n_workers)

    @cuda.jit
    def k_force_pairs(st, sh, gm, fw):
        worker, n_workers = wide_index()
        ph.gradient_pairs(cuda.blockIdx.x, st, sh, gm, fw, worker, n_workers)

    @cuda.jit
    def k_force_finish(st, sh, nu, fw, field):
        """Field force, then the total force and acceleration; narrow."""

        i = cuda.blockIdx.x
        tid = cuda.threadIdx.x
        if tid == 0:
            ph.field_force(i, st, fw, field)
        cuda.syncthreads()
        ph.force_accel(i, sh, nu, fw, tid, cuda.blockDim.x)

    _WIDE[ehrenfest] = WideKernels(
        scc_rows=k_scc_rows,
        potentials=k_potentials,
        hamiltonian=k_hamiltonian,
        energy_start=k_energy_start,
        geometry=k_geometry,
        charge_columns=k_charge_columns,
        charges=k_charges,
        report=k_report,
        nuclear=k_nuclear,
        coupling=k_coupling,
        adopt=k_adopt,
        force_weight=k_force_weight,
        force_pairs=k_force_pairs,
        force_finish=k_force_finish,
    )
    return _WIDE[ehrenfest]
