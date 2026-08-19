# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
GPU-batched classical molecular dynamics, built on the scalar ``MDModel``.

Thousands of independent MD systems can be advanced together, with the topology and
force field the same for every system.

This driver runs one execution path on each backend:

- **GPU kernel** (``xp=cupy``): one CUDA block per system and one thread per atom.
- **CPU reference** (``xp=numpy``): the same compiled physics through ``numba.njit``.
"""

import os

import numpy as np

from maxwelllink.units import FS_TO_AU, K_TO_AU
from ..models.md_model.md_model import MDModel, _PRE_NVT_FRICTION_FS
from maxwelllink.tools.recorders import (
    PropertyRecorder,
    XYZTrajectoryWriter,
    output_filename,
)
from .dummy_gpu import BatchStepResult, DummyBatchModel

# Threads per block for the CUDA launches. A multiple of the warp size (32).
_THREADS_PER_BLOCK = 128

# Additional-data field names, in the component order the dipole arrays hold.
_HALF_DIPOLE_KEYS = ("mux_au", "muy_au", "muz_au")
_FORCE_DIPOLE_KEYS = ("mux_m_au", "muy_m_au", "muz_m_au")

# The integrator kernels are compiled once per process and cached here
_INTEGRATOR_KERNELS = None


def _build_integrator_kernels():
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
            "without CUDA, inject xp=numpy to run the compiled CPU kernels instead."
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


class MDGPUBatchModel(DummyBatchModel):
    """
    Vectorized classical-MD batch model for the ``co2jcp2021`` and ``qtip4pf`` force
    fields.
    """

    def __init__(
        self, *, num, driver_kwargs, xp, driver_args=None, store_additional_data=False
    ):
        """
        Initialize a batch of identical MD systems from one template driver.

        Parameters
        ----------
        num : int
            Number of MD systems in the batch; must be positive.
        driver_kwargs : mapping
            Keyword arguments for the template ``MDModel``, e.g.
            ``{"ff": "co2jcp2021", "thermostat": "nve", "pre_nvt": True}``.
        xp : module
            Array module exposing the NumPy API (``numpy`` or ``cupy``).
        driver_args : sequence, optional
            Positional arguments for the template ``MDModel``.
        store_additional_data : bool, default: False
            Accepted for parity with the other batch models. The extras are always
            built, because the cavity solvers read the dipole from them.

        Raises
        ------
        ValueError
            If ``num`` is not positive, or ``verbose`` is enabled.
        """

        self.xp = xp
        self.num = int(num)
        if self.num <= 0:
            raise ValueError("num must be a positive integer.")
        self.store_additional_data = bool(store_additional_data)

        # One template validates the arguments and supplies the force field; no
        # per-system Python objects are ever created.
        template = MDModel(*tuple(driver_args or ()), **dict(driver_kwargs or {}))
        if template.verbose:
            raise ValueError(
                "GPU MD backend does not support verbose=True; per-system log "
                "streams defeat the purpose of batching."
            )
        self.ff = template.ff
        self.ff_name = template.ff_name
        self.na = template.na
        self.n_molecules = self.ff.n_molecules

        # the scalar driver's settings, copied so the template can be dropped
        self.x0 = np.ascontiguousarray(template.x, dtype=float)  # shared geometry
        self.batch_frames = template.batch_frames  # or one frame per molecule ID
        self.thermostat = template.thermostat
        self.temperature_K = template.temperature_K
        self.friction_fs = template.friction_fs
        self.kT = template.kT
        self.init_velocities = template.init_velocities
        self.pre_nvt = template.pre_nvt
        self.pre_nvt_duration_ps = template.pre_nvt_duration_ps
        self.reset_dipole = template.reset_dipole
        self.seed = template.seed
        self.checkpoint = template.checkpoint
        self.restart = template.restart
        # run-time trajectory output, as the scalar driver configures it; one file for
        # the whole batch, opened in initialize()
        self.property_filename = template.property_filename
        self.traj_filename = template.traj_filename
        self.record_every_steps = template.record_every_steps
        self.record_max_steps = template.record_max_steps
        self.record_names = template.record_names
        self.symbols = template.symbols
        self.n_terms = len(template.ff.term_names)
        self._recorder = None
        self._trajectory = None
        self._step_index = 0

        self.dt = 0.0  # shared time step in a.u.
        self.t = 0.0  # current time in a.u.
        self.molecule_ids = ()  # molecule IDs, set in initialize()
        self._rngs = ()  # one host generator per molecule, set in initialize()
        self.x = self.p = self.F = None  # system state, set in initialize()
        self._terms = None  # (num, n_terms) force-field energy terms of the last forces
        self.c1h = 1.0  # Langevin O half-step scaling
        self.mu_initial = None  # dipole baseline, set in initialize()
        self._on_gpu = False  # True when xp is CuPy (use the CUDA kernels)
        self.force_kernels = None  # the force field's kernels, built in initialize()

    # ----------------------- heavy-load initialization ------------------------------

    def initialize(self, dt_au, molecule_ids):
        """
        Allocate the batch state and optionally pre-equilibrate.

        Parameters
        ----------
        dt_au : float
            The shared time step in atomic units (a.u.).
        molecule_ids : array-like of int
            Molecule IDs assigned by the hub; must number exactly ``num``.

        Raises
        ------
        ValueError
            If the number of molecule IDs does not match ``num``.
        """

        xp, n, na = self.xp, self.num, self.na
        self.molecule_ids = tuple(int(mid) for mid in molecule_ids)
        if len(self.molecule_ids) != n:
            raise ValueError(
                f"MDGPUBatchModel expected {n} molecule ids, "
                f"got {len(self.molecule_ids)}."
            )
        self.dt = float(dt_au)
        self.t = 0.0

        # Backend choice: CuPy means a real CUDA device (use the CUDA kernels);
        # NumPy means a CUDA-less host (use the compiled CPU kernel).
        self._on_gpu = getattr(xp, "__name__", "") == "cupy"
        self.force_kernels = self.ff.build_force_kernels(xp, _THREADS_PER_BLOCK)
        mass = self.force_kernels.masses  # (na, 1), per atom
        qeff = self.force_kernels.charges_eff  # (na, 1), per atom

        # Langevin O half-step coefficients, as in MDModel.initialize()
        if self.thermostat == "nvt":
            gamma = 1.0 / (self.friction_fs * FS_TO_AU)
            self.c1h = float(np.exp(-0.5 * gamma * self.dt))
        else:
            self.c1h = 1.0
        self.noise = np.sqrt(mass * self.kT * (1.0 - self.c1h**2))  # (na, 1)

        # One host generator per molecule, seeded like MDModel.initialize(). It draws
        # the initial momenta on both backends and, on the CPU backend, the Langevin
        # noise as well; the GPU backend seeds its device streams the same way below.
        self._rngs = [
            np.random.default_rng(self.seed + mid) for mid in self.molecule_ids
        ]

        # positions: one geometry replicated, or every molecule's own frame; momenta:
        # one draw per molecule
        if self.batch_frames is None:
            x_host = np.broadcast_to(self.x0, (n, na, 3)).copy()
        else:
            if max(self.molecule_ids) >= len(self.batch_frames):
                raise ValueError(
                    f"batch_xyz holds {len(self.batch_frames)} frames, fewer than "
                    f"molecule ID {max(self.molecule_ids)} needs."
                )
            x_host = np.array([self.batch_frames[mid] for mid in self.molecule_ids])
        p_host = np.zeros((n, na, 3))
        if self.init_velocities:
            sigma_p = np.sqrt(mass * self.kT)
            for row, rng in enumerate(self._rngs):
                p = sigma_p * rng.standard_normal((na, 3))
                p -= mass * (p.sum(axis=0) / mass.sum())  # no center-of-mass drift
                p_host[row] = p

        self.x = xp.asarray(x_host)
        self.p = xp.asarray(p_host)
        self.F = xp.zeros((n, na, 3), dtype=xp.float64)
        self.mass = xp.asarray(mass)
        self.qeff = xp.asarray(qeff)
        self.potential = xp.zeros(n, dtype=xp.float64)
        self._terms = xp.zeros((n, self.n_terms), dtype=xp.float64)
        self.zero_field = xp.zeros((n, 3), dtype=xp.float64)
        self._amp = xp.zeros((n, 3), dtype=xp.float64)  # dmu/dt
        self._mu_half = xp.zeros((n, 3), dtype=xp.float64)  # mu half a step later
        self._mu_force = xp.zeros((n, 3), dtype=xp.float64)  # mu at the force time
        self._energy = xp.zeros(n, dtype=xp.float64)

        if self._on_gpu:
            from numba import cuda
            from numba.cuda.random import init_xoroshiro128p_states, xoroshiro128p_dtype

            self.kernels = _build_integrator_kernels()
            self.mass_flat = xp.asarray(mass.ravel())  # the kernels index by atom
            self.qeff_flat = xp.asarray(qeff.ravel())
            # Langevin noise on the device: one block of thread streams per system,
            # seeded from seed + molecule_id so the block is a property of the
            # molecule, not of its row in this batch
            tpb = _THREADS_PER_BLOCK
            self.rng_states = cuda.device_array(n * tpb, dtype=xoroshiro128p_dtype)
            for row, mid in enumerate(self.molecule_ids):
                init_xoroshiro128p_states(
                    self.rng_states[row * tpb : (row + 1) * tpb],
                    seed=np.uint64(self.seed + mid),
                )

        if self.restart and self.checkpoint:
            self.load_checkpoint()

        # forces at the starting geometry, as MDModel.initialize() does
        if self._on_gpu:
            self.force_kernels.forces_gpu(
                self.x, self.F, self.potential, self.zero_field, self._terms
            )
        else:
            self._forces_on_cpu(self.zero_field)

        if self.pre_nvt and not (self.restart and self.checkpoint):
            self.equilibrate(self.pre_nvt_duration_ps, _PRE_NVT_FRICTION_FS)
            self.t = 0.0  # reset the clock so production dynamics start fresh

        # Baseline subtracted from the reported dipoles, as the LAMMPS fix's
        # reset_dipole does. It is captured here, after any equilibration or
        # restart, and is what keeps the permanent dipole of the one shared
        # starting geometry from adding coherently over the whole batch.
        if self.mu_initial is None:
            if self.reset_dipole:
                self.mu_initial = xp.sum(self.qeff * self.x, axis=1)
            else:
                self.mu_initial = xp.zeros((n, 3), dtype=xp.float64)

        output = dict(
            record_every_steps=self.record_every_steps,
            record_max_steps=self.record_max_steps,
            append=bool(self.restart and self.checkpoint),
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
                f"[MDGPUBatchModel] Recording {self.record_names} to "
                f"{self._recorder.path}"
            )
        if self.traj_filename is not None:
            self._trajectory = XYZTrajectoryWriter(
                output_filename(self.traj_filename, self.molecule_ids[0]),
                self.symbols,
                self.molecule_ids,
                self.dt,
                **output,
            )
            print(
                f"[MDGPUBatchModel] Writing the trajectory to {self._trajectory.path}"
            )

    # ----------------------- one FDTD step under E-field ----------------------------

    def _forces_on_cpu(self, efield):
        """
        Fill ``self.F`` and ``self.potential``, one system at a time.

        Parameters
        ----------
        efield : numpy.ndarray of float, shape (num, 3)
            Effective electric field vector of every system in a.u.
        """

        for d in range(self.num):
            self.potential[d] = self.force_kernels.forces_cpu(
                self.x[d],
                self.F[d],
                np.ascontiguousarray(efield[d], dtype=float),
                self._terms[d],
            )

    def _step_on_cpu(self, efield, c1h, noise):
        """
        Advance every system by one OBABO step vectorized over the batch.

        Mirrors ``MDModel.propagate``.

        Parameters
        ----------
        efield : numpy.ndarray of float, shape (num, 3)
            Effective electric field vector of every system in a.u.
        c1h : float
            Langevin O half-step scaling; ``1.0`` disables the thermostat.
        noise : numpy.ndarray of float, shape (na, 1)
            Momentum noise amplitude of the Langevin half-step per atom.
        """

        dt, mass = self.dt, self.mass

        if c1h < 1.0:  # O
            self._langevin_half_kick_on_cpu(c1h, noise)
        self.p += 0.5 * dt * self.F  # B
        self.x += dt * (self.p / mass)  # A
        self._forces_on_cpu(efield)  # force at the E-field time
        self.p += 0.5 * dt * self.F  # B

        # the two dipole snapshots and the energy the EM solver needs
        p_half = self.p + 0.5 * dt * self.F
        v_half = p_half / mass
        x_half = self.x + 0.5 * dt * v_half
        self._amp[:] = np.sum(self.qeff * v_half, axis=1)
        self._mu_half[:] = np.sum(self.qeff * x_half, axis=1)
        self._mu_force[:] = np.sum(self.qeff * self.x, axis=1)
        kinetic = 0.5 * np.sum(self.p**2 / mass, axis=(1, 2))
        self._energy[:] = kinetic + self.potential

        if c1h < 1.0:  # O
            self._langevin_half_kick_on_cpu(c1h, noise)

    def _langevin_half_kick_on_cpu(self, c1h, noise):
        """
        Apply one Ornstein-Uhlenbeck half-step to the momenta of every system.

        Each system draws from its own generator, in the order ``MDModel`` draws, so a
        batch member and the scalar driver with the same molecule ID stay identical.

        Parameters
        ----------
        c1h : float
            Langevin O half-step scaling.
        noise : numpy.ndarray of float, shape (na, 1)
            Momentum noise amplitude of the Langevin half-step, per atom.
        """

        shape = (self.na, 3)
        for d, rng in enumerate(self._rngs):
            self.p[d] = c1h * self.p[d] + noise * rng.standard_normal(shape)

    def _step_on_gpu(self, efield, c1h, noise):
        """
        Advance every system by one OBABO step with four kernel launches.

        Parameters
        ----------
        efield : cupy.ndarray of float, shape (num, 3)
            Effective electric field vector of every system in a.u., on the device.
        c1h : float
            Langevin O half-step scaling; ``1.0`` disables the thermostat.
        noise : numpy.ndarray of float, shape (na, 1)
            Momentum noise amplitude of the Langevin half-step per atom.
        """

        noise_dev = self.xp.asarray(noise.ravel())
        thermostat = c1h < 1.0
        self.kernels["pre"][self.num, _THREADS_PER_BLOCK](
            self.x,
            self.p,
            self.F,
            self.mass_flat,
            noise_dev,
            self.dt,
            c1h,
            self.rng_states,
            thermostat,
        )
        self.force_kernels.forces_gpu(
            self.x, self.F, self.potential, efield, self._terms
        )
        self.kernels["post"][self.num, _THREADS_PER_BLOCK](
            self.x,
            self.p,
            self.F,
            self.mass_flat,
            noise_dev,
            self.qeff_flat,
            self.dt,
            c1h,
            self.rng_states,
            thermostat,
            self.potential,
            self._amp,
            self._mu_half,
            self._mu_force,
            self._energy,
        )

    def step(self, efield_au):
        """
        Advance every system by one OBABO velocity-Verlet step under the field.

        Parameters
        ----------
        efield_au : numpy.ndarray of float, shape (num, 3)
            Contiguous host array of effective electric field vectors in a.u.

        Returns
        -------
        BatchStepResult
            Columnar amplitude (``dmu/dt``), half-step dipole, force-time dipole,
            and energy for every system.

        Raises
        ------
        RuntimeError
            If called before :meth:`initialize`.
        ValueError
            If ``efield_au`` does not have shape ``(num, 3)``.
        """

        if self.x is None:
            raise RuntimeError("MDGPUBatchModel.step() called before initialize().")
        field = np.ascontiguousarray(efield_au, dtype=np.float64)
        if field.shape != (self.num, 3):
            raise ValueError(
                f"efield_au must have shape ({self.num}, 3); got {tuple(field.shape)}."
            )

        field = self.xp.asarray(field)  # host -> device (one copy)
        if self._on_gpu:
            self._step_on_gpu(field, self.c1h, self.noise)
        else:
            self._step_on_cpu(field, self.c1h, self.noise)
        self._mu_half -= self.mu_initial  # report relative to time zero
        self._mu_force -= self.mu_initial
        self.t += self.dt

        h = self._to_host
        # the trajectory record, as MDModel takes it: T = 2 K / (3 N k_B) with
        # K = E - U the kinetic energy at the force time, E, then the force field's terms
        self._step_index += 1
        if self._step_index % self.record_every_steps == 0:
            if self._recorder is not None:
                energy, potential = h(self._energy), h(self.potential)
                temperature = 2.0 * (energy - potential) / (3.0 * self.na) / K_TO_AU
                self._recorder.record(
                    self._step_index,
                    self.t,
                    np.column_stack((temperature, energy, h(self._terms))),
                )
            if self._trajectory is not None:
                self._trajectory.write(self._step_index, self.t, h(self.x))
        return BatchStepResult(
            amplitude_au=h(self._amp),
            dipole_half_au=h(self._mu_half),
            dipole_force_au=h(self._mu_force),
            energy_au=h(self._energy),
        )

    def equilibrate(self, duration_ps, friction_fs):
        """
        Run a field-free Langevin trajectory to thermalize every system.

        Each system carries its own random stream and remains uncorrelated with other systems.

        Parameters
        ----------
        duration_ps : float
            Equilibration length in picoseconds.
        friction_fs : float
            Langevin relaxation time in femtoseconds.

        Returns
        -------
        int
            Number of MD steps taken.
        """

        gamma = 1.0 / (float(friction_fs) * FS_TO_AU)
        c1h = float(np.exp(-0.5 * gamma * self.dt))
        noise = np.sqrt(self.force_kernels.masses * self.kT * (1.0 - c1h**2))
        n_steps = int(round(float(duration_ps) * 1000.0 * FS_TO_AU / self.dt))
        for _ in range(n_steps):
            if self._on_gpu:
                self._step_on_gpu(self.zero_field, c1h, noise)
            else:
                self._step_on_cpu(self.zero_field, c1h, noise)
            self.t += self.dt
        return n_steps

    # ----------------------- internal helper method ---------------------------------

    def _to_host(self, array):
        """
        Return a fresh host NumPy snapshot of an array-module array.

        Parameters
        ----------
        array : numpy.ndarray or cupy.ndarray
            Array from ``self.xp`` to copy to host memory.

        Returns
        -------
        numpy.ndarray
            A host copy, so reused buffers cannot mutate returned data.
        """

        asnumpy = getattr(self.xp, "asnumpy", None)
        if asnumpy is not None:  # cupy: device -> host copy
            return asnumpy(array)
        return np.array(array)  # numpy: force a copy

    # ------------ optional data / checkpoint --------------

    def append_additional_data(self):
        """
        Return per-system dicts matching ``MDModel.append_additional_data``.

        Returns
        -------
        list of dict
            One dictionary per system with ``time_au``, ``energy_au``,
            ``potential_au``, the half-step dipole (``mux_au``/``muy_au``/``muz_au``)
            and the force-time dipole (``mux_m_au``/``muy_m_au``/``muz_m_au``).

        Raises
        ------
        RuntimeError
            If called before the first :meth:`step`.
        """

        if self.x is None:
            raise RuntimeError(
                "append_additional_data() called before the first step()."
            )
        mu_half = self._to_host(self._mu_half)
        mu_force = self._to_host(self._mu_force)
        energy = self._to_host(self._energy)
        potential = self._to_host(self.potential)
        return [
            {
                "time_au": self.t,
                "energy_au": float(energy[i]),
                "potential_au": float(potential[i]),
                "mux_au": float(mu_half[i, 0]),
                "muy_au": float(mu_half[i, 1]),
                "muz_au": float(mu_half[i, 2]),
                "mux_m_au": float(mu_force[i, 0]),
                "muy_m_au": float(mu_force[i, 1]),
                "muz_m_au": float(mu_force[i, 2]),
            }
            for i in range(self.num)
        ]

    def additional_data_columns(self, keys):
        """
        Return the requested additional-data fields as one contiguous block.

        The batch already holds every field as an array, so this copies only the
        columns the hub asked for and never builds a dictionary per system.

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
        KeyError
            If a requested field is not one this driver reports.
        """

        columns = {}
        if any(key in _HALF_DIPOLE_KEYS for key in keys):
            columns.update(zip(_HALF_DIPOLE_KEYS, self._to_host(self._mu_half).T))
        if any(key in _FORCE_DIPOLE_KEYS for key in keys):
            columns.update(zip(_FORCE_DIPOLE_KEYS, self._to_host(self._mu_force).T))
        if "energy_au" in keys:
            columns["energy_au"] = self._to_host(self._energy)
        if "potential_au" in keys:
            columns["potential_au"] = self._to_host(self.potential)
        if "time_au" in keys:
            columns["time_au"] = np.full(self.num, self.t)
        return np.ascontiguousarray(np.column_stack([columns[key] for key in keys]))

    def save_checkpoint(self, filename=None):
        """
        Write the whole batch state to one ``.npz`` file.

        The scalar driver writes one checkpoint per molecule; the batch stores every
        system in a single array instead.

        Parameters
        ----------
        filename : str, optional
            Target path. Defaults to a name built from the first molecule ID.
        """

        np.savez(
            filename or self._checkpoint_filename(),
            time=self.t,
            x=self._to_host(self.x),
            p=self._to_host(self.p),
            F=self._to_host(self.F),
            mu_initial=self._to_host(self.mu_initial),
            molecule_ids=np.asarray(self.molecule_ids),
        )

    def load_checkpoint(self, filename=None):
        """
        Restore the batch state written by :meth:`save_checkpoint`.

        Starts fresh (printing a note) when the file is absent, as the scalar driver
        does.

        Parameters
        ----------
        filename : str, optional
            Source path. Defaults to a name built from the first molecule ID.

        Raises
        ------
        ValueError
            If the checkpoint holds a differently shaped batch.
        """

        path = filename or self._checkpoint_filename()
        if not os.path.exists(path):
            print(
                "[checkpoint] No batch checkpoint file %r found, starting fresh." % path
            )
            return
        data = np.load(path)
        if data["x"].shape != (self.num, self.na, 3):
            raise ValueError(
                f"Checkpoint {path!r} holds {data['x'].shape} state, expected "
                f"{(self.num, self.na, 3)}."
            )
        self.t = float(data["time"])
        self.x = self.xp.asarray(data["x"])
        self.p = self.xp.asarray(data["p"])
        self.F = self.xp.asarray(data["F"])
        if "mu_initial" in data:  # keep reporting against the original baseline
            self.mu_initial = self.xp.asarray(data["mu_initial"])

    def _checkpoint_filename(self):
        """Default checkpoint filename, built from the first molecule ID."""

        first = self.molecule_ids[0] if self.molecule_ids else 0
        return "md_batch_checkpoint_id_%d.npz" % first

    def close(self):
        """
        Drop the batch state, writing a checkpoint first when one was requested.
        """

        if self.x is not None and self.checkpoint:
            self.save_checkpoint()
        for name in ("_recorder", "_trajectory"):
            writer = getattr(self, name)
            if writer is not None:
                writer.close()
                setattr(self, name, None)
        self.x = self.p = self.F = None
        self._terms = None
        self._amp = self._mu_half = self._mu_force = self._energy = None
        self.mu_initial = None
        self.potential = None
        self.force_kernels = None
