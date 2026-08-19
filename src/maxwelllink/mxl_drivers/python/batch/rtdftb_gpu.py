# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
GPU-batched real-time TDDFTB Ehrenfest dynamics, built on the scalar ``RTDFTBModel``.

Thousands of independent DFTB systems can be advanced together, sharing the parameter
set and the basis layout.

This driver runs one execution path on each backend:

- **GPU** (``xp=cupy``): the CUDA kernels of the batched step
  (``models/rtdftb_model/kernels_gpu.py``) around CuPy's dense linear algebra, one
  CUDA block per system (or several for a small batch of large systems).
- **CPU reference** (``xp=numpy``): the scalar drivers themselves, one per system,
  stepped in turn through the same compiled physics kernels.
"""

import contextlib
import multiprocessing
import os
from collections import namedtuple

import numpy as np

from maxwelllink.tools.recorders import (
    PropertyRecorder,
    XYZTrajectoryWriter,
    output_filename,
)
from maxwelllink.units import K_TO_AU
from ..models.rtdftb_model.dynamics import (
    langevin_coefficients,
    maxwell_boltzmann_velocities,
)
from ..models.rtdftb_model.rtdftb_model import PRE_NVT_DT_FS, RTDFTBModel
from ..models.rtdftb_model.scc import scc_loop
from .dummy_gpu import BatchStepResult, DummyBatchModel
from ..models.rtdftb_model.kernels_gpu import (
    THREADS_PER_BLOCK,
    Forces,
    Geometry,
    Nuclear,
    Out,
    Shared,
    State,
    narrow_kernels,
    wide_kernels,
)

# Below this batch size S^-1 is inverted one system at a time through cuSOLVER
_INVERSE_BATCH_MIN = 16

# Additional-data field names, in the component order the dipole arrays hold.
_HALF_DIPOLE_KEYS = ("mux_au", "muy_au", "muz_au")
_FORCE_DIPOLE_KEYS = ("mux_m_au", "muy_m_au", "muz_m_au")

# Scalar-driver flags a batch cannot honour, and why:
#   verbose    -- one print per system per step would swamp the run
#   checkpoint/restart -- the scalar driver writes one .npz per molecule ID
_UNSUPPORTED_FLAGS = ("verbose", "checkpoint", "restart")

#: The kernel argument bundles of one batch, built once in ``initialize()``.
Bundles = namedtuple("Bundles", "state shared geometry nuclear forces out field")

#: Persistent single-precision mirrors of the dense-algebra operands, allocated only
#: in hybrid mode (``RTDFTBModel(hybrid_precision=True)`` on the CUDA backend). The
#: FP64 arrays stay the state; these hold the per-step downcasts the FP32 cuBLAS/
#: cuSOLVER calls read, so no step allocates. Fields from ``overlap`` on exist only
#: when the nuclei move (``None`` otherwise).
_FP32 = namedtuple(
    "_FP32",
    "h s_inv rho t1 work_b work_c overlap coupling work_r density product weight",
)


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
        store_additional_data : bool, default: False
            Accepted for parity with the other batch models. The extras are always
            built, because the cavity solvers read the dipole from them.
        blocks_per_system : int, optional
            CUDA blocks each system's stage kernels are spread over. ``None`` (the
            default) resolves automatically: one block per system while the batch
            alone fills the GPU (thousands of systems), and enough blocks to fill it
            when ``num`` is small and the systems are large. Set explicitly
            only for benchmarking or testing.

        Raises
        ------
        ValueError
            If ``num`` is not positive, the template asks for a flag the batch cannot
            honour (``verbose``, ``checkpoint``, ``restart``), or a propagator other
            than the leapfrog.
        """

        if int(num) <= 0:
            raise ValueError("num must be a positive integer.")
        self.xp = xp
        self.num = int(num)
        self.store_additional_data = bool(store_additional_data)

        # One template validates the arguments and carries the settings; the batch
        # writes the output files for everyone, so the template and the per-molecule
        # scalar initializations must not open files of their own.
        self._driver_args = tuple(driver_args or ())
        self._driver_kwargs = dict(driver_kwargs or {})
        self._driver_kwargs.update(property_filename=None, traj_filename=None)
        template = RTDFTBModel(*self._driver_args, **dict(driver_kwargs or {}))
        for flag in _UNSUPPORTED_FLAGS:
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

        # shared bookkeeping, set in initialize()
        self.dt = 0.0  # the EM time step in a.u.
        self.dt_rt = 0.0  # the electronic sub-step, dt / n_substeps
        self.n_substeps = 1
        self.t = 0.0  # current time in a.u.
        self.molecule_ids = ()
        self.n_orb = self.n_atom = self.n_shell = 0
        self._on_gpu = False
        self._stepped = False

        # the CPU backend: one scalar driver per system
        self._drivers = None

        # the GPU backend: the batch arrays, their bundles and the kernels
        self._hybrid = False  # hybrid_precision resolved against the backend
        self._fp32 = None  # the _FP32 mirrors, allocated only in hybrid mode
        self._wide_bps = 1  # blocks per system resolved in initialize()
        self._kernels = None  # the narrow family, when _wide_bps == 1
        self._wide_kernels = None  # the wide family, when _wide_bps > 1 or gpu_init
        self._acc = None  # (num,) reduction target of the wide kernels
        self._mu = None  # (num, 3) dipole hand-over between wide phases
        self._shared_geometry = True  # one geometry for all, until initialize() says
        # diagnostics of the fully-GPU initialization (read by the tests)
        self._gpu_initialized = False  # whether the GPU converged the ground state
        self._scf_iterations = None  # (n_fp32, n_fp64) of the last GPU ground state
        self._layout = None  # the ShellLayout of the GPU initialization
        self._n_electron = 0.0  # its electron count
        self._sync = None  # numba's device synchronization, set with the kernels
        self._mass_host = None
        self._bundles = None
        self._rho = None  # (num, n_orb, n_orb) complex128, the current density
        self._rho_old = None  # the previous one; the two are swapped every sub-step
        self._velocity = None  # nuclear state, allocated only when ehrenfest
        self._forces = None  # the Forces work, allocated only when ehrenfest
        # the reply buffers; on the CPU backend they are filled from the drivers
        self._amp = self._mu_half = self._energy = self._e_kin = None

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

        Raises
        ------
        ValueError
            If the number of molecule IDs does not match ``num``.
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
        template = self._template
        # one RT step per EM step by default, as the scalar driver sub-steps
        self.n_substeps = 1
        if template.dt_rtdftb_au is not None:
            self.n_substeps = max(1, int(round(self.dt / template.dt_rtdftb_au)))
        self.dt_rt = self.dt / self.n_substeps

        # Every system starts as the scalar driver of its molecule ID would: ground
        # state, pre-NVT, velocities, kick and bootstrap all come from
        # RTDFTBModel.initialize(). With identical initial conditions one template is
        # run and copied into every row; when the template declares per-molecule
        # conditions -- batch_xyz, pre_nvt, sampled velocities -- every molecule ID is
        # initialized on its own, so the batch is exact rather than merely similar.
        per_system = (
            template.batch_frames is not None
            or template.pre_nvt
            or (template.ehrenfest and template.init_velocities)
        )
        if not self._on_gpu:
            # the CPU backend IS the scalar drivers: no second copy of the physics
            self._drivers = _initialize_drivers(
                self._driver_args, self._driver_kwargs, self.dt, self.molecule_ids
            )
            first = self._drivers[0]
            self.n_orb, self.n_atom = first.system.n_orb, first.system.n_atom
            self.n_shell = first.ground.layout.n_shell
            self._mass_host = first.system.masses
            self._amp = np.zeros((num, 3))
            self._mu_half = np.zeros((num, 3))
            self._energy = np.zeros(num)
            self._e_kin = np.zeros(num)
            self._open_outputs()
            return

        # None means "the driver decides": on for the CUDA backend, off elsewhere.
        wanted = self.gpu_init if self.gpu_init is not None else True
        gpu_boot = bool(wanted and not (template.pre_nvt and not self.ehrenfest))
        if wanted and not gpu_boot:
            print(
                "[RTDFTBGPUBatchModel] gpu_init requested, but pre_nvt without "
                "ehrenfest keeps the CPU ground state; using the CPU initialization."
            )
        if gpu_boot:
            # topology and geometry only; the ground state, the kick and the bootstrap
            # run on the device once the batch arrays exist
            system = template.build_system(self.molecule_ids[0])
            self._layout = system.layout
            self._n_electron = system.n_electrons() - template.charge
            source_ids = self.molecule_ids if per_system else self.molecule_ids[:1]
            rows = [_topology_row(template, mid) for mid in source_ids]
            shared = _shared_arrays(system, self._layout)
        else:
            drivers = _initialize_drivers(
                self._driver_args,
                self._driver_kwargs,
                self.dt,
                self.molecule_ids if per_system else self.molecule_ids[:1],
            )
            rows = [_driver_row(model) for model in drivers]
            shared = _shared_arrays(drivers[0].system, drivers[0].ground.layout)
        self._allocate(shared, rows, per_system)
        self._resolve_kernels(gpu_boot)
        if gpu_boot:
            self._initialize_on_device(per_system)
        self._open_outputs()

    def _allocate(self, shared, rows, per_system):
        """Allocate the device arrays of the batch from the starting rows and bundle them.

        Parameters
        ----------
        shared : dict
            The topology every system shares (see :func:`_shared_arrays`).
        rows : list of dict
            The starting state of every system (``per_system``), or of the one system
            every row starts from.
        per_system : bool
            Whether every system has its own starting state.
        """

        xp, num = self.xp, self.num
        first = rows[0]
        self.n_orb = n = shared["orb_shell"].shape[0]
        self.n_atom = shared["atom_species"].shape[0]
        self.n_shell = n_shell = shared["shell_atom"].shape[0]
        self._mass_host = shared["mass"]

        def batched(key, dtype=None):
            """The batch array of one state field: stacked rows, or one row spread."""

            if per_system:
                return xp.asarray(np.stack([row[key] for row in rows]), dtype=dtype)
            one = np.asarray(first[key])
            return xp.asarray(np.broadcast_to(one, (num,) + one.shape), dtype=dtype)

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
        self._v_orb = xp.zeros((num, n), dtype=xp.float64)
        self._h = xp.zeros((num, n, n), dtype=xp.float64)
        self._work_a = xp.zeros((num, n, n), dtype=xp.complex128)
        self._work_b = xp.zeros((num, n, n), dtype=xp.complex128)
        self._work_c = xp.zeros((num, n, n), dtype=xp.complex128)
        self._work_r = xp.zeros((num, n, n), dtype=xp.float64)
        # per-system scalars carried from one stage kernel to the next
        self._energy_start = xp.zeros(num, dtype=xp.float64)
        self._e_kin = xp.zeros(num, dtype=xp.float64)

        # nuclear integrator state and the force work, also from the bootstrap
        if self.ehrenfest:
            self._velocity = batched("velocity", xp.float64)
            self._half_velocity = batched("half_velocity", xp.float64)
            self._coords_next = batched("coords_next", xp.float64)
            self._accel = batched("accel", xp.float64)
            self._force = batched("force", xp.float64)
            self._forces = Forces(
                *(xp.zeros((num, n, n), dtype=xp.float64) for _ in range(4)),
                xp.zeros((num, self.n_atom, 3), dtype=xp.float64),
            )
        self._fp32 = self._allocate_fp32() if self._hybrid else None

        # the reply buffers. _mu_end carries the end-of-step dipole into the next step
        # as its start, seeded with the post-bootstrap value; _mu_half is the midpoint
        # average that is reported, and the force-time dipole is the same value again,
        # because that is what the external DFTB+ driver sends. _mu_initial is every
        # system's own baseline.
        self._mu_initial = batched("mu_initial", xp.float64)
        self._mu_end = batched("mu_end", xp.float64)
        self._amp = xp.zeros((num, 3), dtype=xp.float64)
        self._mu_half = xp.zeros((num, 3), dtype=xp.float64)
        self._energy = xp.zeros(num, dtype=xp.float64)
        self._field = xp.zeros((num, 3), dtype=xp.float64)
        self._acc = xp.zeros(num, dtype=xp.float64)
        self._mu = xp.zeros((num, 3), dtype=xp.float64)
        self._stepped = False
        self._bundles = self._build_bundles(shared)

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

    def _build_bundles(self, shared):
        """Wrap the batch arrays in the kernels' argument bundles once.

        A raw CuPy array inside a namedtuple is untypeable by numba, so every field
        goes through ``cuda.as_cuda_array``, which is a zero-copy view.
        """

        from numba import cuda

        wrap = cuda.as_cuda_array
        tables = shared["sk"]

        def upload(array):
            return cuda.to_device(np.ascontiguousarray(array))

        shared_bundle = Shared(
            tables._replace(
                **{name: upload(getattr(tables, name)) for name in tables._fields}
            ),
            *[upload(shared[name]) for name in Shared._fields[1:]],
        )
        state = State(*[wrap(getattr(self, "_" + name)) for name in State._fields])
        if self._shared_geometry:
            # one geometry for all: the kernels index per system, so hand them views
            # that repeat the shared arrays along the batch axis without copying them
            def view(array):
                return wrap(self.xp.broadcast_to(array, (self.num,) + array.shape))

        else:
            view = wrap
        geometry = Geometry(
            view(self._coords),
            view(self._h0),
            view(self._overlap),
            view(self._s_inv),
            view(self._gamma),
            wrap(self._e_repulsive),
        )
        out = Out(
            wrap(self._amp), wrap(self._mu_end), wrap(self._mu_half), wrap(self._energy)
        )
        nuclear = forces = None
        if self.ehrenfest:
            nuclear = Nuclear(
                wrap(self._velocity),
                wrap(self._half_velocity),
                wrap(self._coords_next),
                wrap(self._accel),
                wrap(self._force),
            )
            forces = Forces(*[wrap(field) for field in self._forces])
        return Bundles(
            state, shared_bundle, geometry, nuclear, forces, out, wrap(self._field)
        )

    def _resolve_kernels(self, gpu_boot):
        """Pick the launch family of this batch and compile its kernels.

        One block per system (the narrow family) once the batch alone oversubscribes
        every SM; otherwise enough blocks to fill the device ~4 deep, capped by the
        largest strided loop so no block is left without work. An explicit
        ``blocks_per_system`` from the constructor wins. The fully-GPU initialization
        always drives the wide family, whatever the step dispatch is.
        """

        from numba import cuda

        if self.blocks_per_system is not None:
            self._wide_bps = self.blocks_per_system
        else:
            sm_count = cuda.get_current_device().MULTIPROCESSOR_COUNT
            want = -(-4 * sm_count // max(self.num, 1))  # ceil: blocks to fill the GPU
            max_work = max(
                self.n_orb,
                self.n_atom * max(self.n_atom - 1, 1),
                self.n_shell * self.n_shell,
            )
            cap = -(-max_work // THREADS_PER_BLOCK)
            self._wide_bps = max(1, min(want, cap))
        if self._wide_bps > 1 or gpu_boot:
            self._wide_kernels = wide_kernels(self.ehrenfest)
            if self._wide_bps > 1:
                print(
                    f"[RTDFTBGPUBatchModel] wide launch: {self._wide_bps} blocks "
                    f"per system for {self.num} system(s)."
                )
        if self._wide_bps == 1:
            self._kernels = narrow_kernels(self.ehrenfest)
        self._sync = cuda.synchronize

    def _open_outputs(self):
        """Open the property record and the trajectory file, if the template asked."""

        output = dict(
            record_every_steps=self.record_every_steps,
            record_max_steps=self.record_max_steps,
            append=False,
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

    # ----------------------- the fully-GPU initialization ---------------------------
    def _initialize_on_device(self, per_system):
        """Geometry, ground state, kick and bootstrap of every system on the GPU.

        The dense operations run in FP64 whatever ``hybrid_precision`` says; the SCC
        ground state alone follows the two-phase schedule of :func:`scc.scc_loop`.
        """

        template = self._template
        with self._full_precision() as hybrid:
            # thermalize first, so r(0) and v(0) below are the pre-NVT endpoint
            if template.pre_nvt:
                self._equilibrate_on_device(hybrid)

            # H0, S, S^-1, gamma and the repulsive energy at r(0)
            self._rebuild_geometry()
            self._sync()
            if self._shared_geometry:
                self._s_inv[...] = self.xp.linalg.inv(self._overlap)
            else:
                self._invert_overlap()

            # one SCC ground state per distinct starting point
            if per_system:
                for i in range(self.num):
                    self._ground_state(i, hybrid=hybrid)
            else:
                self._ground_state(0, hybrid=hybrid)
                if self.num > 1:
                    self._rho[...] = self._rho[0:1]

            self._start_on_device()
        self._gpu_initialized = True

    @contextlib.contextmanager
    def _full_precision(self):
        """Run the dense algebra of the block in FP64, whatever ``hybrid_precision``
        says; yields whether hybrid mode is on, for the SCC loop's own schedule."""

        hybrid, self._hybrid = self._hybrid, False
        try:
            yield hybrid
        finally:
            self._hybrid = hybrid

    def _geometry_of(self, i):
        """``(h0, overlap, gamma)`` of system ``i``: its own, or the one shared copy."""

        if self._shared_geometry:
            return self._h0, self._overlap, self._gamma
        return self._h0[i], self._overlap[i], self._gamma[i]

    def _ground_state(
        self, i, hybrid=False, dq_start=None, seed_forces=False, energy_repulsive=None
    ):
        """
        Converge the SCC ground state of system ``i`` with its matrices on the device.

        Parameters
        ----------
        i : int
            Row of the system.
        hybrid : bool
            Whether to run the two-phase FP32/FP64 schedule of :func:`scc.scc_loop`.
        dq_start : numpy.ndarray, optional
            Warm-start shell charges: the pre-NVT BOMD passes the previous geometry's.
        seed_forces : bool
            Also seed everything the ground-state force kernels read: the orbital and
            shell charges of row ``i`` and, with moving nuclei, the real density and
            the energy-weighted density in the force work.
        energy_repulsive : float, optional
            The repulsive energy of the geometry, when the caller already holds it on
            the host; read from the device otherwise.

        Returns
        -------
        numpy.ndarray
            The converged shell charges, for warm-starting the next geometry.
        """

        xp, template = self.xp, self._template
        h0, overlap, gamma = self._geometry_of(i)
        result = scc_loop(
            self._layout,
            self._n_electron,
            h0,
            overlap,
            gamma,
            xp=xp,
            tolerance=template.scc_tolerance,
            electronic_temperature_au=template.electronic_temperature_au,
            dq_shell_start=dq_start,
            hybrid_precision=hybrid,
            energy_repulsive=(
                float(self._e_repulsive[i])
                if energy_repulsive is None
                else float(energy_repulsive)
            ),
        )
        if not result.converged:
            raise RuntimeError(
                "[molecule ID %d] the SCC ground state did not converge."
                % self.molecule_ids[i]
            )
        self._rho[i] = result.rho
        if seed_forces:
            # what the force kernels read: charges for the gamma term, the real
            # density and the energy-weighted density for the band and Pulay terms
            self._q_orb[i] = xp.asarray(result.q_orb)
            self._dq_shell[i] = xp.asarray(result.dq_shell)
            if self.ehrenfest:
                self._forces.density[i] = result.rho
                self._forces.weight_e[i] = result.edm
        n_fp32 = result.n_iteration_fp32
        self._scf_iterations = (n_fp32, result.n_iteration - n_fp32)
        if dq_start is None:  # the first ground state of a system, not a BOMD step
            phases = (
                f"{n_fp32} FP32 + {result.n_iteration - n_fp32} FP64"
                if n_fp32
                else f"{result.n_iteration} FP64"
            )
            print(
                f"[RTDFTBGPUBatchModel] GPU ground state of molecule "
                f"{self.molecule_ids[i]}: {phases} iterations, "
                f"E = {result.energy_total:.10f} Ha."
            )
        return result.dq_shell

    def _start_on_device(self):
        """``RTDynamics.start`` for the whole batch on the GPU device.

        Charges and Hamiltonian of the unkicked ground state, the Ehrenfest force and
        the first nuclear half-move, the delta kick, the coupling at ``(r(0), v(0))``,
        the Euler bootstrap into the first leapfrog interval, and the observation at
        ``t = dt`` -- the exact sequence of the CPU bootstrap, through the wide phase
        kernels and the dense operations of the step path.
        """

        xp, dt, template = self.xp, self.dt_rt, self._template
        b, k = self._bundles, self._wide_kernels
        wide = ((self.num, self._wide_bps), THREADS_PER_BLOCK)

        # charges, H and (Ehrenfest) force of the unkicked ground state
        self._rho_old[...] = self._rho
        self._observe_on_device()
        if self.ehrenfest:
            self._half_velocity[...] = self._velocity
            self._coords_next[...] = self._coords + self._velocity * dt
            self._forces_on_device()

        # the delta kick, timeprop.F90:1861-1876 through cuBLAS
        if template.delta_kick_au != 0.0:
            phase = template.delta_kick_au * (
                self._coords[..., self._layout.orb_atom, template.kick_direction]
            )
            work = (xp.exp(-1j * phase)[..., :, None] * self._rho) @ self._overlap
            work = (work * xp.exp(1j * phase)[..., None, :]) @ self._s_inv
            self._rho[...] = 0.5 * (work + xp.conj(work.swapaxes(-1, -2)))

        # D(r(0), v(0))
        if self.ehrenfest:
            self._coupling[...] = 0.0
            k.coupling[wide](b.state, b.shared, b.geometry, b.nuclear)

        # the Euler bootstrap: rho(dt) lands in rho_old, as one step leaves it
        self._rho_old[...] = self._rho
        self._leapfrog(dt)

        if self.ehrenfest:
            k.adopt[wide](b.state, b.geometry, b.nuclear)
            self._rebuild_geometry()
            self._invert_overlap()
        self._observe_on_device()

        self._sync()
        if self.reset_dipole:
            self._mu_initial[...] = self._mu
        else:
            self._mu_initial[...] = 0.0
        self._mu_end[...] = self._mu - self._mu_initial
        if self.ehrenfest:
            self._forces_on_device()
        self._swap_density()
        self._sync()

    def _equilibrate_on_device(self, hybrid):
        """``dynamics.bomd_equilibrate`` for the whole batch on the GPU device.

        The same OBABO draws, charges and forces as the CPU BOMD: every system's
        random stream is ``seed + molecule_id``, as the scalar driver's is.

        Parameters
        ----------
        hybrid : bool
            Whether the SCC ground states run the two-phase FP32/FP64 schedule.
        """

        xp, template, mass = self.xp, self._template, self._mass_host
        n_steps, dt, friction = template.pre_nvt_schedule()
        c1h, noise = langevin_coefficients(dt, template.kT, friction, mass)
        rngs = [np.random.default_rng(template.seed + mid) for mid in self.molecule_ids]
        velocity_dev = xp.asarray(
            np.stack(
                [maxwell_boltzmann_velocities(template.kT, mass, rng) for rng in rngs]
            )
        )
        warm = [None] * self.num

        def acceleration():
            """Geometry, warm-started SCC and the ground-state force of every system."""

            self._rebuild_geometry()
            e_rep = self._to_host(self._e_repulsive)
            for i in range(self.num):
                warm[i] = self._ground_state(
                    i,
                    hybrid=hybrid,
                    dq_start=warm[i],
                    seed_forces=True,
                    energy_repulsive=e_rep[i],
                )
            self._potentials_on_device()
            self._forces_on_device(real_time=False)

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
            f"{PRE_NVT_DT_FS} fs for {self.num} system(s)."
        )

    # -- the wide phases the initialization composes --------------------------------
    def _rebuild_geometry(self):
        """``H0``, ``S``, gamma and ``E_rep`` at the current coordinates, through the
        wide geometry kernel (for the one shared geometry, or for every system)."""

        b, k = self._bundles, self._wide_kernels
        n_geo = 1 if self._shared_geometry else self.num
        self._h0[...] = 0.0
        self._overlap[...] = 0.0
        self._e_repulsive[...] = 0.0
        k.geometry[(n_geo, self._wide_bps), THREADS_PER_BLOCK](
            b.state, b.shared, b.geometry
        )
        if self._shared_geometry:  # the one geometry's sum, for every system
            self._e_repulsive[...] = self._e_repulsive[0]

    def _potentials_on_device(self):
        """Charges and orbital potentials of every system from its ``q_orb``."""

        b, k = self._bundles, self._wide_kernels
        wide = ((self.num, self._wide_bps), THREADS_PER_BLOCK)
        narrow = (self.num, THREADS_PER_BLOCK)
        k.charges[narrow](b.state, b.shared, b.geometry, self._mu)
        k.scc_rows[wide](b.state, b.geometry)
        k.potentials[wide](b.state, b.shared, b.geometry, b.field)

    def _observe_on_device(self):
        """Charges, potentials and ``H`` of the density held in ``rho_old``, and its
        band energy into the accumulator."""

        b, k, acc = self._bundles, self._wide_kernels, self._acc
        wide = ((self.num, self._wide_bps), THREADS_PER_BLOCK)
        narrow = (self.num, THREADS_PER_BLOCK)
        if self.ehrenfest:
            k.charge_columns[wide](b.state, b.geometry, b.forces)
        else:
            k.charge_columns[wide](b.state, b.geometry)
        k.charges[narrow](b.state, b.shared, b.geometry, self._mu)
        k.scc_rows[wide](b.state, b.geometry)
        k.potentials[wide](b.state, b.shared, b.geometry, b.field)
        acc[...] = 0.0
        k.hamiltonian[wide](b.state, b.geometry, acc, 1)

    def _forces_on_device(self, real_time=True):
        """The force and acceleration of every system, through the wide force kernels.

        Parameters
        ----------
        real_time : bool, default: True
            Whether the energy-weighted density is the real-time one, built here from
            ``H`` and ``S^-1``; the Born-Oppenheimer pre-NVT passes ``False``, its
            ground-state ``edm`` having been seeded by the SCC loop.
        """

        b, k = self._bundles, self._wide_kernels
        wide = ((self.num, self._wide_bps), THREADS_PER_BLOCK)
        narrow = (self.num, THREADS_PER_BLOCK)
        if real_time:
            self._energy_weighted_density()
        k.force_weight[wide](b.state, b.forces)
        k.force_pairs[wide](b.state, b.shared, b.geometry, b.forces)
        k.force_finish[narrow](b.state, b.shared, b.nuclear, b.forces, b.field)

    # ---------------------------- one FDTD step under E-field -----------------------
    def step(self, efield_au):
        """
        Advance every system by one EM step under its own effective field.

        Parameters
        ----------
        efield_au : numpy.ndarray of float, shape (num, 3)
            Effective electric field of every system in atomic units, rows in the
            molecule-ID order :meth:`initialize` was given.

        Returns
        -------
        BatchStepResult
            Amplitude, both dipoles and the energy, as host arrays.

        Raises
        ------
        RuntimeError
            If called before :meth:`initialize`.
        ValueError
            If ``efield_au`` does not have shape ``(num, 3)``.
        """

        if self._drivers is None and self._rho is None:
            raise RuntimeError("RTDFTBGPUBatchModel.step() before initialize().")
        field = np.ascontiguousarray(efield_au, dtype=np.float64)
        if field.shape != (self.num, 3):
            raise ValueError(
                f"efield_au must have shape ({self.num}, 3); got {field.shape}."
            )

        if self._on_gpu:
            self._field[...] = self.xp.asarray(field)  # host -> device (one copy)
            for k in range(self.n_substeps):
                self._step_on_gpu(first=k == 0, last=k == self.n_substeps - 1)
                self._swap_density()
        else:
            self._step_on_cpu(field)
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
                    {"dq": self._charge_deviations()},
                )
        return BatchStepResult(
            amplitude_au=h(self._amp),
            dipole_half_au=h(self._mu_half),
            dipole_force_au=h(self._mu_half),
            energy_au=h(self._energy),
        )

    def _step_on_cpu(self, field):
        """One EM step of every scalar driver, and their replies into the batch arrays."""

        for i, model in enumerate(self._drivers):
            model.propagate(field[i])
            self._amp[i] = model.calc_amp_vector()
            self._mu_half[i] = model.dipole_vec
            self._energy[i] = model.energy
            self._e_kin[i] = model.energy_kin

    def _step_on_gpu(self, first, last):
        """One electronic sub-step of the whole batch.

        The stage kernels run around CuPy's dense linear algebra -- the leapfrog
        products, ``S^-1`` and the energy-weighted density, applied to every system at
        once through cuBLAS/cuSOLVER. Everything queues on the default CUDA stream,
        which keeps the stages in order; the one synchronization at the end is for the
        device->host copies of the results.

        Parameters
        ----------
        first, last : bool
            Whether this is the first or the last sub-step of the EM step: ``E(t)`` is
            kept on the first, the reply is built on the last.
        """

        if self._wide_bps > 1:
            self._step_wide(first, last)
        else:
            self._step_narrow(first, last)
        self._sync()

    def _step_narrow(self, first, last):
        """One sub-step through the block-per-system kernels."""

        b, k, dt = self._bundles, self._kernels, self.dt_rt
        launch = (self.num, THREADS_PER_BLOCK)
        if self.ehrenfest:
            k.pre[launch](b.state, b.shared, b.geometry, b.nuclear, b.field, dt, first)
            self._leapfrog(2.0 * dt)
            k.geometry[launch](b.state, b.shared, b.geometry, b.nuclear)
            self._invert_overlap()
            k.post[launch](
                b.state, b.shared, b.geometry, b.forces, b.out, b.field, self.dt, last
            )
            self._energy_weighted_density()
            k.force[launch](b.state, b.shared, b.geometry, b.nuclear, b.forces, b.field)
        else:
            k.pre[launch](b.state, b.shared, b.geometry, b.field, first)
            self._leapfrog(2.0 * dt)
            k.post[launch](b.state, b.shared, b.geometry, b.out, b.field, self.dt, last)

    def _step_wide(self, first, last):
        """One sub-step through the phase-split kernels, a SMALL batch of LARGE systems."""

        b, k, dt = self._bundles, self._wide_kernels, self.dt_rt
        acc, mu = self._acc, self._mu
        wide = ((self.num, self._wide_bps), THREADS_PER_BLOCK)
        narrow = (self.num, THREADS_PER_BLOCK)

        # H(t), E(t) and D(t)
        acc[...] = 0.0
        if self.ehrenfest:
            k.nuclear[narrow](b.state, b.shared, b.nuclear, dt)
            self._coupling[...] = 0.0
        k.scc_rows[wide](b.state, b.geometry)
        k.potentials[wide](b.state, b.shared, b.geometry, b.field)
        k.hamiltonian[wide](b.state, b.geometry, acc, 0)
        if self.ehrenfest:
            k.coupling[wide](b.state, b.shared, b.geometry, b.nuclear)
        k.energy_start[narrow](b.state, b.geometry, b.field, acc, first)

        self._leapfrog(2.0 * dt)

        # r(t+dt) and the matrices there; charges, H(t+dt), E(t+dt) and the reply
        if self.ehrenfest:
            self._e_repulsive[...] = 0.0
            k.adopt[wide](b.state, b.geometry, b.nuclear)
            k.geometry[wide](b.state, b.shared, b.geometry)
            self._invert_overlap()
        self._observe_on_device()
        k.report[narrow](b.state, b.geometry, b.out, b.field, mu, acc, self.dt, last)
        if self.ehrenfest:
            self._forces_on_device()

    # ---------------------------- dense linear algebra ------------------------------
    def _leapfrog(self, step):
        """
        The leapfrog products of ``rt.leapfrog_step`` for the whole batch, with cuBLAS.

        ``rho(t+dt) = rho(t-dt) - step (T1 rho + rho T1^dagger)`` with
        ``T1 = S^-1 (D + iH)``, written into ``rho_old`` as the kernel does; the host
        swaps the two buffers afterwards. ``T1`` is assembled from two real products
        rather than one complex one, and ``rho T1^dagger`` is the adjoint of ``T1 rho``
        because ``rho`` is Hermitian, so one complex product serves both terms (the
        kernel computes both, as DFTB+ does; the two agree to round-off).

        In hybrid mode the same products run in FP32 on the persistent mirrors and the
        FP64 density accumulates the increment: round-off enters only the increment
        (~1e-7 of it per step), which keeps the energy drift and the electron count at
        FP64 quality.
        """

        xp = self.xp
        if self._hybrid:
            fp = self._fp32
            fp.h[...] = self._h
            fp.s_inv[...] = self._s_inv
            fp.rho[...] = self._rho
            if self.ehrenfest:
                fp.coupling[...] = self._coupling
            h, s_inv, rho, coupling = fp.h, fp.s_inv, fp.rho, fp.coupling
            t1, work, work_adj, work_r = fp.t1, fp.work_b, fp.work_c, fp.work_r
            unit, scale = xp.complex64(1j), xp.complex64(step)
        else:
            h, s_inv, rho, coupling = self._h, self._s_inv, self._rho, self._coupling
            t1, work, work_adj = self._work_a, self._work_b, self._work_c
            work_r = self._work_r
            unit, scale = 1j, step
        xp.multiply(xp.matmul(s_inv, h), unit, out=t1)  # i S^-1 H
        if self.ehrenfest:
            xp.matmul(s_inv, coupling, out=work_r)
            t1 += work_r  # + S^-1 D
        xp.matmul(t1, rho, out=work)  # T1 rho
        xp.conj(work.transpose(0, 2, 1), out=work_adj)  # rho T1^dagger
        work += work_adj
        work *= scale
        self._rho_old -= work  # (upcast) accumulate into the FP64 state

    def _invert_overlap(self):
        """``S^-1`` at the new geometry, the LU inverse ``rt.lu_invert`` takes."""

        xp = self.xp
        overlap = self._overlap
        if self._hybrid:  # the FP32 LU inverse, upcast into the FP64 s_inv all read
            self._fp32.overlap[...] = overlap
            overlap = self._fp32.overlap
        if self.num < _INVERSE_BATCH_MIN:
            for i in range(self.num):
                self._s_inv[i] = xp.linalg.inv(overlap[i])
        else:
            self._s_inv[...] = xp.linalg.inv(overlap)

    def _energy_weighted_density(self):
        """``W = 0.5 (S^-1 H P + P H S^-1)`` as ``forces.energy_weighted_density``.

        ``W`` feeds only the nuclear forces, so in hybrid mode it runs in FP32
        throughout; ``h`` and ``s_inv`` are re-downcast because both changed since the
        leapfrog stage.
        """

        xp, fw = self.xp, self._forces
        if self._hybrid:
            fp = self._fp32
            fp.density[...] = fw.density
            fp.h[...] = self._h
            fp.s_inv[...] = self._s_inv
            density, h, s_inv = fp.density, fp.h, fp.s_inv
            product, weight, work_r = fp.product, fp.weight, fp.work_r
            half = xp.float32(0.5)
        else:
            density, h, s_inv = fw.density, self._h, self._s_inv
            product, weight, work_r = fw.product, fw.weight_e, self._work_r
            half = 0.5
        xp.matmul(density, h, out=product)  # P H
        xp.matmul(s_inv, product.transpose(0, 2, 1), out=weight)
        xp.matmul(product, s_inv.transpose(0, 2, 1), out=work_r)
        weight += work_r
        weight *= half
        if self._hybrid:
            fw.weight_e[...] = weight  # upcast into the array the force kernel reads

    def _swap_density(self):
        """Exchange the two leapfrog buffers, and the views the bundles hold."""

        self._rho, self._rho_old = self._rho_old, self._rho
        state = self._bundles.state
        self._bundles = self._bundles._replace(
            state=state._replace(rho=state.rho_old, rho_old=state.rho)
        )

    # ----------------------- internal helper method ---------------------------------
    def _to_host(self, array):
        """Return a host copy of ``array``, whichever backend holds it."""

        asnumpy = getattr(self.xp, "asnumpy", None)
        if asnumpy is not None:
            return asnumpy(array)
        return np.array(array)  # numpy: force a copy so buffer reuse cannot mutate it

    def _charge_deviations(self):
        """Mulliken charge deviation ``dq`` of every atom of every system, on the host."""

        if self._on_gpu:
            return self._to_host(self._dq_atom)
        return np.array([model.dynamics.state.dq_atom for model in self._drivers])

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

        Raises
        ------
        RuntimeError
            If called before the first :meth:`step`.
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

        Raises
        ------
        RuntimeError
            If called before the first :meth:`step`.
        KeyError
            If a requested field is not one this driver reports.
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
        velocity = self.velocities()
        return 0.5 * np.einsum("a,dak,dak->d", self._mass_host, velocity, velocity)

    def coordinates(self):
        """
        Current geometry of every system, in Bohr.

        Returns
        -------
        numpy.ndarray of float, shape (num, n_atom, 3)
            With the nuclei frozen and one shared starting geometry every row is that
            geometry.

        Raises
        ------
        RuntimeError
            If called before :meth:`initialize`.
        """

        if self._drivers is not None:
            return np.array([model.system.coords for model in self._drivers])
        if self._rho is None:
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

        if not self.ehrenfest:
            return np.zeros((self.num, self.n_atom, 3))
        if self._drivers is not None:
            return np.array([model.dynamics.velocity for model in self._drivers])
        return self._to_host(self._velocity)

    def close(self):
        """Close the output files and release the device state."""

        for name in ("_recorder", "_trajectory"):
            writer = getattr(self, name)
            if writer is not None:
                writer.close()
                setattr(self, name, None)
        if self._drivers is not None:
            for model in self._drivers:
                model.close()
            self._drivers = None
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
            "_v_orb",
            "_h",
            "_work_a",
            "_work_b",
            "_work_c",
            "_work_r",
            "_amp",
            "_mu_end",
            "_mu_half",
            "_energy",
            "_energy_start",
            "_e_kin",
            "_mu_initial",
            "_field",
            "_acc",
            "_mu",
            "_bundles",
            "_kernels",
            "_wide_kernels",
            "_velocity",
            "_half_velocity",
            "_coords_next",
            "_accel",
            "_force",
            "_forces",
            "_fp32",
        ):
            setattr(self, name, None)


# ---------------------------------------------------------------------------- #
# the starting state of every system, from the scalar driver                   #
# ---------------------------------------------------------------------------- #
def _shared_arrays(system, layout):
    """The topology every system of a batch shares, keyed like :data:`Shared`."""

    return dict(
        sk=system.tables,
        atom_species=system.atom_species,
        atom_offset=system.atom_offset,
        orb_shell=layout.orb_shell,
        orb_atom=layout.orb_atom,
        shell_atom=layout.shell_atom,
        shell_u=layout.shell_u,
        q0_orb=layout.q0_orb,
        mass=system.masses,
    )


def _driver_row(model):
    """The batch-relevant state of one initialized scalar driver, as host arrays."""

    dynamics = model.dynamics
    state = dynamics.state
    row = dict(
        coords=model.system.coords,
        h0=state.h0,
        overlap=state.overlap,
        s_inv=state.s_inv,
        gamma=state.gamma,
        e_repulsive=state.e_repulsive,
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


def _topology_row(template, molecule_id):
    """The starting point of one system for the fully-GPU initialization: its
    geometry and velocity, and zeros for everything the device will compute."""

    system = template.build_system(molecule_id)
    n, n_atom, n_shell = system.n_orb, system.n_atom, system.layout.n_shell
    coords = system.coords
    row = dict(
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
        # the velocity the scalar driver would start from: the same draw from the same
        # stream; with pre_nvt the BOMD supplies it instead, on the device
        velocity = np.zeros((n_atom, 3))
        if not template.pre_nvt:
            rng = np.random.default_rng(template.seed + molecule_id)
            drawn = template.initial_velocities(system, rng)
            if drawn is not None:
                velocity = np.array(drawn, dtype=float)
        row.update(
            velocity=velocity,
            half_velocity=velocity.copy(),
            coords_next=coords.copy(),
            accel=np.zeros((n_atom, 3)),
            force=np.zeros((n_atom, 3)),
        )
    return row


def _initialize_driver(driver_args, driver_kwargs, dt_au, molecule_id):
    """Construct and initialize the scalar driver of one molecule ID."""

    model = RTDFTBModel(*driver_args, **driver_kwargs)
    model.initialize(dt_au, molecule_id)
    return model


def _initialize_snapshot(driver_args, driver_kwargs, dt_au, molecule_id):
    """Initialize the scalar driver of one molecule ID in a worker; return its state."""

    model = _initialize_driver(driver_args, driver_kwargs, dt_au, molecule_id)
    return model._snapshot(), np.array(model.mu_initial, dtype=float)


def _driver_from_snapshot(driver_args, driver_kwargs, dt_au, molecule_id, snapshot):
    """Rebuild an initialized driver from a worker's snapshot.

    The driver is initialized without the pre-equilibration (a cheap SCC at the
    constructor geometry) and the worker's state is restored into it, exactly as a
    checkpoint restart does.
    """

    state, mu_initial = snapshot
    model = RTDFTBModel(*driver_args, **{**driver_kwargs, "pre_nvt": False})
    model.initialize(dt_au, molecule_id)
    model._restore(state)
    model.mu_initial = mu_initial
    return model


def _initialize_drivers(driver_args, driver_kwargs, dt_au, molecule_ids):
    """
    One initialized scalar driver per molecule ID, over the CPU cores when that pays.

    A pre-NVT equilibration is seconds to minutes per molecule, so it is spread over
    forked workers, one per core the process may use, which hand their state back;
    the plain SCC ground state is milliseconds and runs in place. The workers do CPU
    work only, so forking after the device was set up is safe.
    """

    jobs = [(driver_args, driver_kwargs, dt_au, mid) for mid in molecule_ids]
    workers = min(len(jobs) - 1, _cpu_count())
    if driver_kwargs.get("pre_nvt", False) and workers > 1:
        # the first molecule runs here, which compiles every kernel once; the forked
        # workers inherit the compiled code instead of each compiling it again
        drivers = [_initialize_driver(*jobs[0])]
        with multiprocessing.get_context("fork").Pool(workers) as pool:
            snapshots = pool.starmap(_initialize_snapshot, jobs[1:])
        drivers.extend(
            _driver_from_snapshot(*job, snapshot)
            for job, snapshot in zip(jobs[1:], snapshots)
        )
        return drivers
    return [_initialize_driver(*job) for job in jobs]


def _cpu_count():
    """CPU cores this process may run on (the SLURM allocation, not the whole node)."""

    try:
        return len(os.sched_getaffinity(0))
    except (AttributeError, OSError):
        return os.cpu_count() or 1
