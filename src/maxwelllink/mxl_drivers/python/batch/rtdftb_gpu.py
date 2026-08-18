# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
GPU-batched real-time TDDFTB Ehrenfest dynamics.

Many independent DFTB systems are advanced together, sharing the parameter set and
the initial conditions (geometry and density matrix); only the field, and with
``ehrenfest=True`` the trajectory, differ between them.

This driver runs one execution path on each backend:

- **GPU kernel** (``xp=cupy``): one thread per system, calling the same scalar-loop
  device functions the CPU path compiles.
- **CPU reference** (``xp=numpy``): the same compiled physics through ``numba.njit``.
"""

from collections import namedtuple

import numpy as np

from ..models.rtdftb_model.dftb_params import DFTBSystem, load_sk_set
from ..models.rtdftb_model.h0_overlap import MAX_INTEGRAL, MAX_ORB, build_h0_overlap
from ..models.rtdftb_model.rtdftb_model import RTDFTBModel
from ..models.rtdftb_model.scc import scf
from .dummy_gpu import BatchStepResult, DummyBatchModel

# Threads per block for the CUDA launch. A multiple of the warp size (32).
_THREADS_PER_BLOCK = 128

# Additional-data field names, in the component order the dipole arrays hold.
_HALF_DIPOLE_KEYS = ("mux_au", "muy_au", "muz_au")
_FORCE_DIPOLE_KEYS = ("mux_m_au", "muy_m_au", "muz_m_au")

# Scalar-driver flags a batch cannot honour, and why:
#   verbose    -- one print per system per step would swamp the run
#   checkpoint/restart -- the scalar driver writes one .npz per molecule ID
_UNSUPPORTED_GPU_FLAGS = ("verbose", "checkpoint", "restart")

# Radial-interpolation stencil width, slakoeqgrid.F90:78.
_N_INTERPOLATION = 8

# Kernel argument bundles. numba accepts a namedtuple of device arrays as a kernel
# argument, and bundling matters: with the arrays passed individually the launch spends
# over a millisecond per step re-resolving argument types on the host, which at these
# batch sizes dwarfs the time the device actually needs.
_State = namedtuple(
    "_State",
    "rho rho_old coupling q_orb dq_atom dq_shell v_scc_shell v_shell v_orb h "
    "work_a work_b work_c",
)
_Shared = namedtuple(
    "_Shared",
    "sk atom_species atom_offset orb_shell orb_atom shell_atom shell_u q0_orb mass "
    "mu_initial",
)
#: Geometry-dependent matrices. Shared (2-D) with the nuclei frozen, per system (3-D)
#: once they move; the device functions take the per-system view either way.
_Geometry = namedtuple("_Geometry", "coords h0 overlap s_inv gamma e_repulsive")
_Nuclear = namedtuple("_Nuclear", "velocity half_velocity coords_next accel force")
_Out = namedtuple("_Out", "amp mu_end mu_half energy")

#: Per-system working storage for the matrix-assembly kernels. On the scalar path one
#: copy is shared across calls; here every system needs its own, because on the GPU all
#: of them are in flight at once.
_Scratch = namedtuple(
    "_Scratch",
    "h0_sk_h h0_sk_s h0_block_h h0_block_s h0_core h0_node h0_cc h0_dd h0_delta "
    "h0_y_low h0_y_high "
    "b_sk_h b_sk_s b_dsk_h b_dsk_s b_d_h0 b_d_overlap b_weight b_first b_second "
    "b_radial b_angular b_core b_dcore "
    "density product weight_e weight gradient pair work_r pivot",
)

# The step kernels are compiled once per process and cached here, keyed by whether the
# nuclei move.
_STEP_KERNELS = {}


def _build_step_kernel(ehrenfest):
    """Compile (once) and return the fused RT-TDDFTB CUDA kernel.

    The kernel is one ``doTdStep`` per thread, assembled from the device builds of the
    validated scalar kernels.

    Parameters
    ----------
    ehrenfest : bool
        Whether the nuclei move, which decides which of the two kernels is built.

    Returns
    -------
    numba.cuda.dispatcher.CUDADispatcher
        The compiled kernel, ready to launch with ``kernel[blocks, tpb]``.

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
    from ..models.rtdftb_model.scc import RepulsiveScratch

    d = device_kernels()
    scc_potential = d["scc_potential"]
    scc_hamiltonian = d["scc_hamiltonian"]
    external_potential = d["external_potential"]
    leapfrog_step = d["leapfrog_step"]
    rt_orbital_charges = d["rt_orbital_charges"]
    atom_charges = d["atom_charges"]
    shell_charges_from_orbital = d["shell_charges_from_orbital"]
    band_energy = d["band_energy"]
    scc_energy = d["scc_energy"]
    external_energy = d["external_energy"]
    build_h0_overlap_kernel = d["build_h0_overlap_kernel"]
    build_gamma = d["build_gamma"]
    lu_invert = d["lu_invert"]
    repulsive_sum = d["repulsive_sum"]
    coupling_kernel = d["coupling_kernel"]
    nuclear_step = d["nuclear_step"]
    kinetic_sum = d["kinetic_sum"]
    real_part = d["real_part"]
    energy_weighted_density = d["energy_weighted_density"]
    overlap_weight = d["overlap_weight"]
    band_gradient_kernel = d["band_gradient_kernel"]
    gamma_gradient_kernel = d["gamma_gradient_kernel"]
    repulsive_gradient_kernel = d["repulsive_gradient_kernel"]
    field_gradient = d["field_gradient"]

    @cuda.jit(device=True)
    def rebuild_h(i, st, sh, coords, h0, overlap, gamma, field, n_orb, n_shell):
        """Rebuild ``H`` of ONE system from its charges and this step's field."""

        scc_potential(
            gamma, st.dq_shell[i], sh.orb_shell, st.v_scc_shell[i], st.v_orb[i]
        )
        for s in range(n_shell):
            st.v_shell[i, s] = st.v_scc_shell[i, s]
        external_potential(coords, field[i], sh.shell_atom, st.v_shell[i], n_shell)
        for mu in range(n_orb):
            st.v_orb[i, mu] = st.v_shell[i, sh.orb_shell[mu]]
        scc_hamiltonian(h0, overlap, st.v_orb[i], st.h[i])

    @cuda.jit(device=True)
    def total_energy(
        i, st, rho, coords, h0, field, e_rep, e_kin, n_orb, n_shell, n_atom
    ):
        """Total energy of ONE system, matching ``RTState.energies`` for our scope."""

        return (
            band_energy(rho[i], h0, n_orb)
            + scc_energy(st.v_scc_shell[i], st.dq_shell[i], n_shell)
            + external_energy(st.dq_atom[i], coords, field[i], n_atom)
            + e_rep
            + e_kin
        )

    @cuda.jit(device=True)
    def recharge(i, st, sh, rho, overlap, coords, mu, n_orb, n_shell, n_atom):
        """Mulliken charges and the dipole of ONE system, after propagation."""

        rt_orbital_charges(rho[i], overlap, st.q_orb[i], n_orb)
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
    def report(i, out, sh, energy_start, energy_end, mu, dt):
        """Fill the reply the way the external DFTB+ driver builds it.

        Midpoint averages of the dipole and the energy, a finite-difference amplitude,
        and the same dipole reported twice (mxlrtdynamics.F90:96-102).
        """

        for k in range(3):
            end = mu[k] - sh.mu_initial[k]
            start = out.mu_end[i, k]
            out.amp[i, k] = (end - start) / dt
            out.mu_half[i, k] = 0.5 * (start + end)
            out.mu_end[i, k] = end
        out.energy[i] = 0.5 * (energy_start + energy_end)

    @cuda.jit
    def k_step_frozen(st, sh, gm, out, field, step, dt):
        """One RT-TDDFTB step per thread, at a fixed geometry."""

        i = cuda.grid(1)
        if i >= st.rho.shape[0]:
            return
        n_orb = st.rho.shape[1]
        n_shell = st.dq_shell.shape[1]
        n_atom = st.dq_atom.shape[1]
        mu = cuda.local.array(3, float64)

        rebuild_h(
            i, st, sh, gm.coords, gm.h0, gm.overlap, gm.gamma, field, n_orb, n_shell
        )
        energy_start = total_energy(
            i,
            st,
            st.rho,
            gm.coords,
            gm.h0,
            field,
            gm.e_repulsive,
            0.0,
            n_orb,
            n_shell,
            n_atom,
        )

        # leapfrog_step writes rho(t+dt) into rho_old; the host swaps the two buffers
        leapfrog_step(
            st.rho_old[i],
            st.rho[i],
            st.h[i],
            gm.s_inv,
            st.coupling[i],
            step,
            st.work_a[i],
            st.work_b[i],
            st.work_c[i],
            n_orb,
        )

        recharge(
            i, st, sh, st.rho_old, gm.overlap, gm.coords, mu, n_orb, n_shell, n_atom
        )
        rebuild_h(
            i, st, sh, gm.coords, gm.h0, gm.overlap, gm.gamma, field, n_orb, n_shell
        )
        energy_end = total_energy(
            i,
            st,
            st.rho_old,
            gm.coords,
            gm.h0,
            field,
            gm.e_repulsive,
            0.0,
            n_orb,
            n_shell,
            n_atom,
        )
        report(i, out, sh, energy_start, energy_end, mu, dt)

    @cuda.jit
    def k_step_ehrenfest(st, sh, gm, nu, sc, out, field, step, dt):
        """One RT-TDDFTB-Ehrenfest step per thread: the nuclei move with the density."""

        i = cuda.grid(1)
        if i >= st.rho.shape[0]:
            return
        n_orb = st.rho.shape[1]
        n_shell = st.dq_shell.shape[1]
        n_atom = st.dq_atom.shape[1]
        mu = cuda.local.array(3, float64)

        # the integrator consumes a(t) and returns r(t+dt) together with v(t)
        nuclear_step(
            nu.coords_next[i],
            nu.half_velocity[i],
            nu.accel[i],
            dt,
            n_atom,
            nu.velocity[i],
        )
        e_kin = kinetic_sum(sh.mass, nu.velocity[i], n_atom)

        rebuild_h(
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
        energy_start = total_energy(
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

        # D(t) is built at r(t) with v(t), before the geometry is adopted
        coupling_kernel(
            sh.sk,
            gm.coords[i],
            sh.atom_species,
            sh.atom_offset,
            n_atom,
            nu.velocity[i],
            st.coupling[i],
            CouplingScratch(
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
            ),
        )
        leapfrog_step(
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

        # adopt r(t+dt) and rebuild every geometry-dependent matrix there
        for a in range(n_atom):
            for k in range(3):
                gm.coords[i, a, k] = nu.coords_next[i, a, k]
        build_h0_overlap_kernel(
            sh.sk,
            gm.coords[i],
            sh.atom_species,
            sh.atom_offset,
            n_atom,
            gm.h0[i],
            gm.overlap[i],
            H0Scratch(
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
            ),
        )
        lu_invert(gm.overlap[i], n_orb, sc.work_r[i], sc.pivot[i], gm.s_inv[i])
        build_gamma(gm.coords[i], sh.shell_atom, sh.shell_u, gm.gamma[i])
        gm.e_repulsive[i] = repulsive_sum(
            sh.sk, gm.coords[i], sh.atom_species, n_atom, RepulsiveScratch(sc.pair[i])
        )

        recharge(
            i,
            st,
            sh,
            st.rho_old,
            gm.overlap[i],
            gm.coords[i],
            mu,
            n_orb,
            n_shell,
            n_atom,
        )
        rebuild_h(
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
        energy_end = total_energy(
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
        report(i, out, sh, energy_start, energy_end, mu, dt)

        # the Ehrenfest force at r(t+dt), which drives the next nuclear step
        real_part(st.rho_old[i], sc.density[i], n_orb)
        energy_weighted_density(
            sc.density[i], st.h[i], gm.s_inv[i], sc.product[i], sc.weight_e[i], n_orb
        )
        overlap_weight(sc.density[i], sc.weight_e[i], st.v_orb[i], sc.weight[i])
        for a in range(n_atom):
            for k in range(3):
                sc.gradient[i, a, k] = 0.0
        band_gradient_kernel(
            sh.sk,
            gm.coords[i],
            sh.atom_species,
            sh.atom_offset,
            n_atom,
            sc.density[i],
            sc.weight[i],
            sc.gradient[i],
            BandScratch(
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
            ),
        )
        gamma_gradient_kernel(
            gm.coords[i],
            sh.shell_atom,
            sh.shell_u,
            st.dq_shell[i],
            n_shell,
            sc.gradient[i],
        )
        repulsive_gradient_kernel(
            sh.sk, gm.coords[i], sh.atom_species, n_atom, sc.gradient[i], sc.pair[i]
        )
        field_gradient(st.dq_atom[i], field[i], sc.gradient[i], n_atom)
        for a in range(n_atom):
            for k in range(3):
                nu.force[i, a, k] = -sc.gradient[i, a, k]
                nu.accel[i, a, k] = nu.force[i, a, k] / sh.mass[a]

    _STEP_KERNELS[ehrenfest] = k_step_ehrenfest if ehrenfest else k_step_frozen
    return _STEP_KERNELS[ehrenfest]


class RTDFTBGPUBatchModel(DummyBatchModel):
    """
    Vectorized real-time TD-DFTB batch model with or without Ehrenfest motion.

    Notes
    -----
    Every system shares the parameter set, the basis layout and the starting geometry.
    With the nuclei frozen the geometry-dependent matrices follow from that geometry and
    are held once rather than ``num`` times; with ``ehrenfest=True`` the trajectories
    diverge from the first step, so they become per system and are rebuilt every step.
    """

    def __init__(
        self,
        *,
        num,
        driver_kwargs,
        xp,
        driver_args=None,
        store_additional_data=False,
    ):
        """
        Build the batch model from one scalar-driver template.

        One template validates the arguments, reads the .skf files and converges the
        ground state; no per-system Python objects are ever created.

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
        store_additional_data : bool, default: False
            Accepted for parity with the other batch models. The extras are always
            built, because the cavity solvers read the dipole from them.
        """

        if int(num) <= 0:
            raise ValueError("num must be a positive integer.")
        self.xp = xp
        self.num = int(num)

        template = RTDFTBModel(*tuple(driver_args or ()), **dict(driver_kwargs or {}))
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

        # state, all set in initialize()
        self.dt = 0.0  # shared time step in a.u.
        self.t = 0.0  # current time in a.u.
        self.molecule_ids = ()
        self.n_orb = 0
        self.n_atom = 0
        self.n_shell = 0
        self._on_gpu = False
        self._kernel = None
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
        Converge the shared ground state and allocate the contiguous batch state.

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

        # One scalar driver reproduces the reference trajectory. Run its whole
        # initialization once -- ground state, kick, bootstrap step -- and copy the
        # resulting state into every system, which is what makes the batch exact rather
        # than merely similar.
        template = self._template
        sk_set = load_sk_set(
            template.sk_path,
            sorted(set(template.elements)),
            template.max_angular_momentum,
        )
        system = DFTBSystem(template.elements, template.positions, sk_set, units="bohr")
        h0, overlap = build_h0_overlap(system)
        ground = scf(
            system,
            h0,
            overlap,
            tolerance=template.scc_tolerance,
            charge=template.charge,
        )
        if not ground.converged:
            raise RuntimeError("the shared SCC ground state did not converge.")

        self.n_orb = n = system.n_orb
        self.n_atom = system.n_atom
        self.n_shell = n_shell = ground.layout.n_shell
        self._sk_set = sk_set

        template.initialize(self.dt, self.molecule_ids[0])
        dynamics = template.dynamics
        state = dynamics.state
        self._mu_initial_host = np.array(template.mu_initial, dtype=float)
        self._mass_host = np.array(dynamics.mass, dtype=float)

        # topology, shared by construction
        layout = ground.layout
        self._shared_host = dict(
            atom_species=system.atom_species,
            atom_offset=system.atom_offset,
            orb_shell=layout.orb_shell,
            orb_atom=layout.orb_atom,
            shell_atom=layout.shell_atom,
            shell_u=layout.shell_u,
            q0_orb=layout.q0_orb,
            mass=self._mass_host,
            mu_initial=self._mu_initial_host,
        )

        def spread(array, dtype=None):
            """Copy one system's array into every row of a contiguous batch array."""

            batched = np.broadcast_to(np.asarray(array), (num,) + np.shape(array))
            return xp.asarray(np.ascontiguousarray(batched), dtype=dtype)

        # geometry-dependent matrices: one copy while the nuclei are frozen, one per
        # system once they move and the trajectories diverge
        if self.ehrenfest:
            self._coords = spread(system.coords, xp.float64)
            self._h0 = spread(state.h0, xp.float64)
            self._overlap = spread(state.overlap, xp.float64)
            self._s_inv = spread(state.s_inv, xp.float64)
            self._gamma = spread(state.gamma, xp.float64)
            self._e_repulsive = xp.full(num, float(state.energies()[3]))
        else:
            self._coords = xp.asarray(system.coords, dtype=xp.float64)
            self._h0 = xp.asarray(state.h0, dtype=xp.float64)
            self._overlap = xp.asarray(state.overlap, dtype=xp.float64)
            self._s_inv = xp.asarray(state.s_inv, dtype=xp.float64)
            self._gamma = xp.asarray(state.gamma, dtype=xp.float64)
            self._e_repulsive = float(state.energies()[3])

        # per-system electronic state, seeded from the template's post-bootstrap values
        self._rho = spread(state.rho, xp.complex128)
        self._rho_old = spread(state.rho_old, xp.complex128)
        self._coupling = spread(state.coupling, xp.float64)
        self._q_orb = spread(state.q_orb, xp.float64)
        self._dq_atom = spread(state.dq_atom, xp.float64)
        self._dq_shell = spread(state.dq_shell, xp.float64)
        self._v_scc_shell = xp.zeros((num, n_shell), dtype=xp.float64)
        self._v_shell = xp.zeros((num, n_shell), dtype=xp.float64)
        self._v_orb = xp.zeros((num, n), dtype=xp.float64)
        self._h = xp.zeros((num, n, n), dtype=xp.float64)
        self._work_a = xp.zeros((num, n, n), dtype=xp.complex128)
        self._work_b = xp.zeros((num, n, n), dtype=xp.complex128)
        self._work_c = xp.zeros((num, n, n), dtype=xp.complex128)

        # nuclear integrator state, also seeded from the template's bootstrap
        if self.ehrenfest:
            self._velocity = spread(dynamics.velocity, xp.float64)
            self._half_velocity = spread(dynamics.half_velocity, xp.float64)
            self._coords_next = spread(dynamics.coords_next, xp.float64)
            self._accel = spread(dynamics.accel, xp.float64)
            self._force = spread(dynamics.force_end, xp.float64)
            self._scratch = self._allocate_scratch()
        else:
            self._velocity = self._half_velocity = self._coords_next = None
            self._accel = self._force = self._scratch = None

        # reusable output buffers. _mu_end carries the end-of-step dipole into the next
        # step as its start, seeded with the template's post-bootstrap value; _mu_half
        # is the midpoint average that is reported, and the force-time dipole is the
        # same value again, because that is what the external DFTB+ driver sends.
        self._amp = xp.zeros((num, 3), dtype=xp.float64)
        self._mu_end = spread(state.dipole - self._mu_initial_host, xp.float64)
        self._mu_half = xp.zeros((num, 3), dtype=xp.float64)
        self._energy = xp.zeros(num, dtype=xp.float64)
        self._field = xp.zeros((num, 3), dtype=xp.float64)
        self._stepped = False

        if self._on_gpu:
            self._kernel = _build_step_kernel(self.ehrenfest)
        self._bundles = self._build_bundles()

    def _allocate_scratch(self):
        """Per-system working storage for the matrix-assembly kernels.

        One copy per system, not one shared copy: on the GPU every thread runs a whole
        system at once, so a shared buffer would be written by every thread in flight.
        """

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
        """Wrap the state in the kernel's argument bundles, once.

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
        geometry = _Geometry(
            wrap(self._coords),
            wrap(self._h0),
            wrap(self._overlap),
            wrap(self._s_inv),
            wrap(self._gamma),
            wrap(self._e_repulsive) if self.ehrenfest else self._e_repulsive,
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

    def _step_on_gpu(self):
        """Launch the fused kernel, one thread per system."""

        from numba import cuda

        state, shared, geometry, nuclear, scratch, out, field = self._bundles
        blocks = (self.num + _THREADS_PER_BLOCK - 1) // _THREADS_PER_BLOCK
        if self.ehrenfest:
            self._kernel[blocks, _THREADS_PER_BLOCK](
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
            self._kernel[blocks, _THREADS_PER_BLOCK](
                state, shared, geometry, out, field, 2.0 * self.dt, self.dt
            )
        # numba and CuPy may schedule on different CUDA streams; synchronize once here
        # so the subsequent device->host copies see the finished results.
        cuda.synchronize()

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
            With the nuclei frozen every row is the shared starting geometry.
        """

        if self._coords is None:
            raise RuntimeError("RTDFTBGPUBatchModel.coordinates() before initialize().")
        coords = self._to_host(self._coords)
        if self.ehrenfest:
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
        """Release the device state so CuPy can free the memory."""

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
            "_kernel",
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


def _report(i, out, sh, energy_start, energy_end, mu, dt):
    """Fill the reply the way the external DFTB+ driver builds it."""

    end = mu - sh.mu_initial
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

    _rebuild_h(i, st, sh, gm.coords, gm.h0, gm.overlap, gm.gamma, field, n_orb, n_shell)
    energy_start = _total_energy(
        i,
        st,
        st.rho,
        gm.coords,
        gm.h0,
        field,
        gm.e_repulsive,
        0.0,
        n_orb,
        n_shell,
        n_atom,
    )
    rt.leapfrog_step(
        st.rho_old[i],
        st.rho[i],
        st.h[i],
        gm.s_inv,
        st.coupling[i],
        step,
        st.work_a[i],
        st.work_b[i],
        st.work_c[i],
        n_orb,
    )
    _recharge(i, st, sh, st.rho_old, gm.overlap, gm.coords, mu, n_orb, n_shell, n_atom)
    _rebuild_h(i, st, sh, gm.coords, gm.h0, gm.overlap, gm.gamma, field, n_orb, n_shell)
    energy_end = _total_energy(
        i,
        st,
        st.rho_old,
        gm.coords,
        gm.h0,
        field,
        gm.e_repulsive,
        0.0,
        n_orb,
        n_shell,
        n_atom,
    )
    _report(i, out, sh, energy_start, energy_end, mu, dt)


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
    _report(i, out, sh, energy_start, energy_end, mu, dt)

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
