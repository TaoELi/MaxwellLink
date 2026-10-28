# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Central built-in classical molecular-dynamics (MD) engine of MaxwellLink.
"""

from dataclasses import dataclass

import numpy as np

from maxwelllink.units import BOHR_PER_ANG, FS_TO_AU, K_TO_AU
from maxwelllink.tools.xyz_helper import read_xyz, read_xyz_frames
from maxwelllink.tools.recorders import (
    PropertyRecorder,
    XYZTrajectoryWriter,
    output_filename,
)

from ..helpers import THREADS_PER_BLOCK, to_host
from .force_fields import DummyForceField, _FORCE_FIELDS, replicate_cell
from .kernels.kernels_integrators import build_integrator_kernels

# The per-molecule properties every MD driver records.
_BASE_RECORD_NAMES = ("temperature_K", "energy_au", "stretch_au", "bend_au",)

_ROTATION_COMPONENT_NAMES = (
    "kinetic_rot_1_au",
    "kinetic_rot_2_au",
    "kinetic_rot_3_au",
)

_FULL_RECORD_NAMES = (
    "temperature_K",
    "energy_au",
    "stretch_au",
    "bend_au",
    "kinetic_au",
    "potential_au",
    "kinetic_trans_au",
    "kinetic_rot_au",
    *_ROTATION_COMPONENT_NAMES,
    "kinetic_vib_au",
    "energy_vib_au",
)
# Properties needed to calculate each derived quantity.
_PROPERTY_DEPENDENCIES = {
    "temperature_K": ("kinetic_au",),
    "energy_au": ("kinetic_au", "potential_au"),
    "energy_vib_au": (
        "kinetic_vib_au",
        "stretch_au",
        "bend_au",
    ),
}

@dataclass(frozen=True, eq=False)
class MDStepResult:
    """
    Columnar physics outputs of one engine step (contiguous host arrays).

    All arrays are host (NumPy) arrays in atomic units for a batch of ``num``
    systems; the field names match the aggregated hub's ``BatchStepResult`` so
    the batch driver can forward them unchanged.

    Attributes
    ----------
    amplitude_au : numpy.ndarray, shape (num, 3)
        Source amplitude ``dmu/dt`` consumed by the EM solver.
    dipole_half_au : numpy.ndarray, shape (num, 3)
        Dipole half a step after the force time (relative to the baseline).
    dipole_force_au : numpy.ndarray, shape (num, 3)
        Dipole at the force time (relative to the baseline).
    energy_au : numpy.ndarray, shape (num,)
        Total (kinetic + potential) energy of every system.
    """

    amplitude_au: np.ndarray
    dipole_half_au: np.ndarray
    dipole_force_au: np.ndarray
    energy_au: np.ndarray


class MDEngine:
    """
    Batch-first classical-MD propagator with a pluggable force field. Not used directly by users.
    """

    def __init__(
        self,
        ff="qtip4pf",
        n_molecules=None,
        positions=None,
        xyz=None,
        batch_xyz=None,
        box=None,
        replicate=None,
        rcut=None,
        ewald_wrcut=None,
        thermostat: str = "nve",
        temperature_K: float = 300.0,
        friction_fs: float = 100.0,
        init_velocities: bool = True,
        pre_nvt: bool = False,
        pre_nvt_duration_ps: float = 20.0,
        reset_dipole: bool = True,
        seed: int = 0,
        force_evaluator: str = "auto",
        property_filename=None,
        traj_filename=None,
        record_every_steps: int = 10,
        record_max_steps=None,
        checkpoint: bool = False,
        restart: bool = False,
        verbose: bool = False,
        *,
        property_list="base",
        traj_content="positions",
        pre_nvt_timestep_fs: float | None = None,
        pre_nvt_friction_fs: float = 100.0,
        num: int = 1,
        xp=np,
        label: str = "MDEngine",
    ):
        """
        Validate the run parameters and build the force field.

        Parameters
        ----------
        ff : str or DummyForceField, default: 'qtip4pf'
            Name of a registered force field (``'qtip4pf'`` for flexible water,
            ``'co2jcp2021'`` for flexible CO2, ``'cs2compass'`` for flexible CS2,
            ``'ch4compass'`` for flexible CH4), or an already constructed
            :class:`DummyForceField` instance.
        n_molecules : int, optional
            Number of molecules to build when ``positions`` is not given (before any
            ``replicate``). If ``None`` (the default) each force field applies its own
            default count (32 for water, 36 for CO2 and CS2, 100 for CH4, which load
            the bundled bulk boxes).
        positions : array-like of float, shape (n_atoms, 3), optional
            Initial atomic positions in Bohr. If ``None`` the force field builds a
            default geometry from ``n_molecules`` (and ``box``).
        xyz : str, optional
            Path to an XYZ file (Angstrom) to take the starting geometry from, with the
            atoms in the order the force field expects (``[C, O, O, ...]`` for CO2,
            ``[O, H, H, ...]`` for water, ``[C, S, S, ...]`` for CS2,
            ``[C, H, H, H, H, ...]`` for CH4). Overrides ``positions``; but this also gives
            coordinates only, so a periodic system still needs ``box``.
        batch_xyz : str, optional
            Path to a multi-frame XYZ file (Angstrom) that gives every molecule its own
            starting geometry: molecule ``m`` starts from frame ``m``, so a batch of
            drivers, or several batches, read consecutive frames and the same molecule
            ID always gets the same frame. Every frame must list the same atoms; there
            must be more frames than the largest molecule ID. Frame 0 doubles as the
            template geometry when neither ``xyz`` nor ``positions`` is given.
        box : float or array-like of float, shape (3,), optional
            Periodic box lengths in Bohr. ``None`` treats the system as a finite
            cluster.
        replicate : int or array-like of int, shape (3,), optional
            Replicate the periodic cell. The molecules and the box lengths grow together, 
            so the density is unchanged.
        rcut : float, optional
            Real-space cutoff in Bohr for the periodic non-bonded sums.
        ewald_wrcut : float, optional
            Real-space cutoff in Bohr for the Ewald sum (periodic electrostatics).
        thermostat : {'nve', 'nvt'}, default: 'nve'
            Ensemble. ``'nve'`` is plain velocity Verlet; ``'nvt'`` adds a Langevin
            (Ornstein-Uhlenbeck) thermostat.
        temperature_K : float, default: 300.0
            Target temperature in Kelvin (NVT) and temperature for the initial
            Maxwell-Boltzmann velocities.
        friction_fs : float, default: 100.0
            Langevin relaxation time in femtoseconds (``gamma = 1 / tau``). Only used
            when ``thermostat == 'nvt'``.
        init_velocities : bool, default: True
            Whether to sample initial velocities from a Maxwell-Boltzmann distribution
            at ``temperature_K``. If ``False`` the system starts at rest.
        pre_nvt : bool, default: False
            Whether to thermally equilibrate the system inside :meth:`initialize`
            before any production dynamics. The random seed is offset by the molecule
            ID so that different molecules are decorrelated even at the same ``seed``.
        pre_nvt_duration_ps : float, default: 20.0
            Duration of the pre-NVT equilibration in picoseconds. It uses a Langevin
            thermostat at ``temperature_K`` with ``pre_nvt_friction_fs`` damping,
            regardless of the production ``thermostat``.
        pre_nvt_friction_fs : float, default: 100.0
            Pre-NVT Langevin relaxation time in femtoseconds; must be finite and
            positive. Independent of the production ``friction_fs``.
        pre_nvt_timestep_fs : float, optional
            Pre-NVT timestep in femtoseconds; None uses the coupling timestep.
            The coupling timestep is restored after equilibration.
        reset_dipole : bool, default: True
            Whether to report the dipole moment relative to its value at time zero.
        seed : int, default: 0
            Base seed; system ``molecule_id`` draws from ``seed + molecule_id``.
        force_evaluator : {'auto', 'numba', 'numpy'}, default: 'auto'
            Which force evaluator to use. ``'numba'`` uses the compiled loop-form
            kernels, which are typically several times faster than the array-at-a-time
            NumPy reference and allocate no ``(na, na, 3)`` temporaries; ``'numpy'``
            forces the reference implementation. ``'auto'`` picks ``'numba'`` whenever
            the force field provides a compiled-kernel description and silently falls
            back to ``'numpy'`` when it does not. The GPU backend always runs the
            compiled kernels.
        property_filename : str, optional
            Turns on a run-time record of per-molecule properties written to this file every
            ``record_every_steps`` production steps: HDF5, streamed while the run goes,
            unless the name ends in ``.npz`` or ``h5py`` is missing.
        property_list : str or list or tuple, default: "base"
            Properties written to property_filename. Only used when
            property_filename is set. "base" selects _BASE_RECORD_NAMES;
            "all" selects _FULL_RECORD_NAMES. Other strings use semicolons
            between property names. Lists and tuples are also accepted.
            Duplicate names are removed while preserving their first occurrence.
        traj_filename : str, optional
            Extended-XYZ output with one frame per independent system every
            ``record_every_steps`` production steps. Positions are in Angstrom,
            atomic velocities in Bohr per atomic time, and body-frame angular
            velocities in inverse atomic time. Angular output automatically
            includes the three corresponding principal moments of inertia in
            electron-mass times Bohr-squared units. A system may contain multiple
            physical molecules; each molecule's six values are repeated for all
            its atoms.
        traj_content : str, default: "positions"
            Content to include in the trajectory file: "positions", "velocities",
            "angular_velocity_body", or "all". Angular output contains three
            angular-velocity components followed by their three principal moments
            of inertia. "all" includes positions, atomic velocities, and these
            six angular-output columns.
            Only used when ``traj_filename`` is set. Velocities and angular
            velocities are sampled before the final Langevin O half-step, at the
            same state as the kinetic properties. Body-frame components use
            instantaneous principal axes ordered by decreasing moment of inertia;
            their signs and degenerate directions are not tracked between frames.
        record_every_steps : int, default: 10
            Record every this many production steps, for both files.
        record_max_steps : int, optional
            Stop recording after this many records; ``None`` for no cap.
        checkpoint : bool, default: False
            Whether the owning front end checkpoints this engine's state.
        restart : bool, default: False
            Whether :meth:`initialize` restores a checkpointed state (handed in by the
            front end) instead of pre-equilibrating.
        verbose : bool, default: False
            Kept for the front ends; the engine itself never prints per step.
        num : int, keyword-only, default: 1
            Number of independent systems advanced together.
        xp : module, keyword-only, default: numpy
            Array module exposing the NumPy API (``numpy`` or ``cupy``).
        label : str, keyword-only, default: 'MDEngine'
            Name used in printed messages, so each front end reports as itself.
        """

        self.xp = xp
        self.num = int(num)
        if self.num <= 0:
            raise ValueError("num must be a positive integer.")
        self._on_gpu = getattr(xp, "__name__", "") == "cupy"
        self._label = str(label)

        self.thermostat = str(thermostat).lower()
        if self.thermostat not in ("nve", "nvt"):
            raise ValueError("thermostat must be 'nve' or 'nvt'.")
        self.temperature_K = float(temperature_K)
        self.friction_fs = float(friction_fs)
        self.init_velocities = bool(init_velocities)
        self.pre_nvt = bool(pre_nvt)
        self.pre_nvt_duration_ps = float(pre_nvt_duration_ps)
        self.pre_nvt_friction_fs = float(pre_nvt_friction_fs)
        if not np.isfinite(self.pre_nvt_friction_fs) or self.pre_nvt_friction_fs <= 0:
            raise ValueError("pre_nvt_friction_fs must be finite and positive.")
        self.pre_nvt_timestep_fs = (
            None if pre_nvt_timestep_fs is None else float(pre_nvt_timestep_fs)
        )
        if self.pre_nvt_timestep_fs is not None and (
            not np.isfinite(self.pre_nvt_timestep_fs) or self.pre_nvt_timestep_fs <= 0
        ):
            raise ValueError("pre_nvt_timestep_fs must be finite and positive.")
        self.reset_dipole = bool(reset_dipole)
        self.seed = int(seed)
        self.kT = K_TO_AU * self.temperature_K
        self.checkpoint = bool(checkpoint)
        self.restart = bool(restart)
        self.verbose = bool(verbose)

        # run-time trajectory output, opened in initialize() once the molecule IDs and
        # the time step are known
        self.property_filename = (
            None if property_filename is None else str(property_filename)
        )
        self.traj_filename = None if traj_filename is None else str(traj_filename)
        self.traj_content = str(traj_content).strip().lower()
        if self.traj_content not in ("positions", "velocities", "angular_velocity_body", "all"):
            raise ValueError("traj_content must be positions, velocities, angular_velocity_body, or all.")
        self._write_trajectory_velocities = (
            self.traj_filename is not None
            and self.traj_content in ("velocities", "all")
        )
        self._write_angular_velocity_body = (
            self.traj_filename is not None
            and self.traj_content in ("angular_velocity_body", "all")
        )
        self.record_every_steps = int(record_every_steps)
        self.record_max_steps = record_max_steps
        self._recorder = None
        self._trajectory = None
        self._step_index = 0  # production steps taken; not part of a snapshot

        # the starting geometry from XYZ files: one for all, or one frame per molecule
        # (picked by molecule ID in initialize()); Angstrom in the files, Bohr here
        self.batch_frames = None
        elements = None
        if batch_xyz is not None:
            elements, frames = read_xyz_frames(batch_xyz)
            self.batch_frames = frames * BOHR_PER_ANG
            if xyz is None and positions is None:
                positions = self.batch_frames[0]
        if xyz is not None:
            elements, positions = read_xyz(xyz)
            positions = positions * BOHR_PER_ANG

        # build the force field: a registered name, or a ready-made instance
        if isinstance(ff, DummyForceField):
            if any(
                option is not None
                for option in (
                    n_molecules,
                    positions,
                    box,
                    replicate,
                    rcut,
                    ewald_wrcut,
                )
            ):
                raise ValueError(
                    "a force-field instance carries its own geometry; pass "
                    "n_molecules/positions/xyz/batch_xyz/box/replicate/rcut/"
                    "ewald_wrcut to its constructor instead."
                )
            self.ff = ff
        else:
            key = str(ff).lower()
            if key not in _FORCE_FIELDS:
                raise ValueError(
                    "Unknown force field %r. Available: %s"
                    % (ff, ", ".join(sorted(_FORCE_FIELDS)))
                )
            # the universal MD parameters are the common force-field constructor API;
            # forward n_molecules only when set so each force field can apply its own
            # default (e.g. the 32-molecule water box) when it is left unspecified
            ff_params = dict(
                positions=positions, box=box, rcut=rcut, ewald_wrcut=ewald_wrcut
            )
            if n_molecules is not None:
                ff_params["n_molecules"] = n_molecules
            if replicate is not None:
                ff_params["replicate"] = replicate
            self.ff = _FORCE_FIELDS[key](**ff_params)
        self.ff_name = self.ff.name
        self.n_molecules = getattr(self.ff, "n_molecules", 1)
        # the XYZ files describe the cell before replication; the per-molecule
        # starting geometries of batch_xyz are tiled the same way as the template
        images = int(np.prod(getattr(self.ff, "replicate", 1)))
        expected = list(self.ff.molecule_symbols) * (self.n_molecules // images)
        if elements is not None and expected and list(elements) != expected:
            raise ValueError(
                f"the XYZ atoms do not follow the {self.ff_name} force field's order "
                f"{list(self.ff.molecule_symbols)} per molecule."
            )
        if self.batch_frames is not None and images > 1:
            self.batch_frames, _, _ = replicate_cell(
                self.batch_frames, self.ff.box / self.ff.replicate, self.ff.replicate
            )

        # Pick the CPU force evaluator; the GPU path always runs the compiled kernels
        evaluator = str(force_evaluator).lower()
        if evaluator not in ("auto", "numba", "numpy"):
            raise ValueError("force_evaluator must be 'auto', 'numba' or 'numpy'.")
        self.force_evaluator = "numpy"
        if (evaluator != "numpy" or self._on_gpu) and self.ff.has_compiled_kernels:
            self.force_evaluator = "numba"
        elif evaluator == "numba" or self._on_gpu:
            raise NotImplementedError(
                f"Force field {self.ff.name!r} provides no compiled kernels; "
                f"use force_evaluator='numpy' on the CPU."
            )

        # geometry template and bookkeeping shared by every front end
        self.n_atoms = self.ff.n_atoms
        self.x0 = np.ascontiguousarray(self.ff.positions, dtype=float)
        self.n_terms = len(self.ff.reported_term_names)

        # Disable property recording unless a filename is provided.
        self.record_names = ()
        self.compute_names = ()

        if self.property_filename is not None:
            # Parse the requested output properties.
            if isinstance(property_list, str):
                option = property_list.strip()
                if option.lower() == "base": requested_names = _BASE_RECORD_NAMES
                elif option.lower() == "all": requested_names = _FULL_RECORD_NAMES
                else: requested_names = option.split(";")
            elif isinstance(property_list, (list, tuple)):
                requested_names = property_list
            else:
                raise TypeError("property_list must be a string, a list or a tuple.")

            # Check types before stripping whitespace and removing duplicates.
            if any(not isinstance(name, str) for name in requested_names):
                raise TypeError("Every property name must be a string.")

            # Preserve the first occurrence of each property.
            requested_names = tuple(dict.fromkeys(name.strip() for name in requested_names))

            if not requested_names or any(not name for name in requested_names):
                raise ValueError("property_list contains no name or an empty name.")

            unknown_names = tuple(name for name in requested_names if name not in _FULL_RECORD_NAMES)
            if unknown_names:
                raise ValueError(
                    f"Unknown properties: {unknown_names}. "
                    f"Allowed properties: {_FULL_RECORD_NAMES}"
                )

            # Store the output tuple in the requested order.
            self.record_names = requested_names

            # Add each property's dependencies before the property itself.
            compute_names = ()
            for name in self.record_names:
                compute_names += ( _PROPERTY_DEPENDENCIES.get(name, ()) + (name,))

            # Store the calculation tuple with ordered deduplication.
            self.compute_names = tuple(dict.fromkeys(compute_names))

        # element symbols of the geometry, for the trajectory file
        self.symbols = (
            list(self.ff.molecule_symbols) * self.n_molecules or ["X"] * self.n_atoms
        )

        self.dt = 0.0  # shared time step in a.u.
        self.t = 0.0  # current time in a.u.
        self.molecule_ids = ()  # molecule IDs, set in initialize()
        self._rngs = ()  # one host generator per molecule, set in initialize()
        self.x = self.p = self.F = None  # system state, set in initialize()
        self._terms = None  # (num, n_terms) force-field energy terms of the last forces
        self.c1h = 1.0  # Langevin O half-step scaling
        self.mu_initial = None  # dipole baseline, set in initialize()
        self.force_kernels = None  # the force field's kernels, built in initialize()

    # ----------------------- heavy-load initialization ------------------------------

    def initialize(self, dt_au, molecule_ids, restart_state=None):
        """
        Allocate the batch state, then restore a checkpoint or pre-equilibrate.

        Parameters
        ----------
        dt_au : float
            The shared time step in atomic units (a.u.).
        molecule_ids : array-like of int
            Molecule IDs assigned by the front end; must number exactly ``num``.
        restart_state : dict, optional
            A state produced by :meth:`state_dict` (front ends read it from their
            checkpoint files). Only applied when the engine was built with
            ``restart=True`` and ``checkpoint=True``; a restart skips the pre-NVT
            equilibration even when no state could be read.

        Raises
        ------
        ValueError
            If the number of molecule IDs does not match ``num``, or the restart
            state holds a differently shaped batch.
        """

        xp, n, na = self.xp, self.num, self.n_atoms
        self.molecule_ids = tuple(int(mid) for mid in molecule_ids)
        if len(self.molecule_ids) != n:
            raise ValueError(
                f"{self._label} expected {n} molecule ids, "
                f"got {len(self.molecule_ids)}."
            )
        self.dt = float(dt_au)
        self.t = 0.0

        # Force evaluator: the compiled kernels of the force field, or its NumPy
        # reference compute() when force_evaluator='numpy' (CPU only).
        if self.force_evaluator == "numba":
            self.force_kernels = self.ff.build_force_kernels(xp, THREADS_PER_BLOCK)
            mass = self.force_kernels.masses  # (na, 1), per atom
            qeff = self.force_kernels.charges_eff  # (na, 1), per atom
        else:
            mass = self.ff.masses  # (na, 1), per atom
            qeff = None  # dipoles then come from ff.dipole()/ff.dipole_velocity()
        self._mass_host = mass

        # Langevin O half-step coefficients (exp(-0.5 * gamma * dt))
        if self.thermostat == "nvt":
            gamma = 1.0 / (self.friction_fs * FS_TO_AU)
            self.c1h = float(np.exp(-0.5 * gamma * self.dt))
        else:
            self.c1h = 1.0
        self.noise = np.sqrt(mass * self.kT * (1.0 - self.c1h**2))  # (na, 1)

        # One host generator per molecule, seeded from seed + molecule_id. It draws
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
        self.qeff = None if qeff is None else xp.asarray(qeff)
        self.potential = xp.zeros(n, dtype=xp.float64)
        self._terms = xp.zeros((n, self.n_terms), dtype=xp.float64)
        self.zero_field = xp.zeros((n, 3), dtype=xp.float64)
        self._amplitude = xp.zeros((n, 3), dtype=xp.float64)  # dmu/dt
        self._dipole_half = xp.zeros((n, 3), dtype=xp.float64)  # mu half a step later
        self._dipole_force = xp.zeros((n, 3), dtype=xp.float64)  # mu at the force time
        self._energy = xp.zeros(n, dtype=xp.float64)

        # Columns used to store calculated properties.
        self._property_columns = {name: column for column, name in enumerate(self.compute_names)}
        self._property_values = xp.empty((n, len(self.compute_names)), dtype=xp.float64)
        # Columns selected for output, in the requested order.
        self._record_columns = tuple(self._property_columns[name] for name in self.record_names)
        # Columns of the existing force-field energy terms.
        self._term_columns = {name: column for column, name in enumerate(self.ff.reported_term_names)}

        self._gpu_property_momenta = None
        if self._on_gpu:
            from numba import cuda
            from numba.cuda.random import init_xoroshiro128p_states, xoroshiro128p_dtype
            if any(
                name in self._property_columns
                for name in (
                    "kinetic_trans_au",
                    "kinetic_rot_au",
                    "kinetic_vib_au",
                    *_ROTATION_COMPONENT_NAMES,
                )
            ) or self._write_trajectory_velocities or self._write_angular_velocity_body:
                self._gpu_property_momenta = xp.empty_like(self.p)

            self.kernels = build_integrator_kernels()
            self.mass_flat = xp.asarray(mass.ravel())  # the kernels index by atom
            self.qeff_flat = xp.asarray(qeff.ravel())
            # Langevin noise on the device: one block of thread streams per system,
            # seeded from seed + molecule_id so the block is a property of the
            # molecule, not of its row in this batch
            tpb = THREADS_PER_BLOCK
            self.rng_states = cuda.device_array(n * tpb, dtype=xoroshiro128p_dtype)
            for row, mid in enumerate(self.molecule_ids):
                init_xoroshiro128p_states(
                    self.rng_states[row * tpb : (row + 1) * tpb],
                    seed=np.uint64(self.seed + mid),
                )

        restarting = self.restart and self.checkpoint
        if restarting and restart_state is not None:
            self.load_state_dict(restart_state)

        # forces at the starting geometry (a pure function of it, so this reproduces
        # a checkpointed force array bit for bit)
        if self._on_gpu:
            self.force_kernels.forces_gpu(
                self.x, self.F, self.potential, self.zero_field, self._terms
            )
        else:
            self._forces_on_cpu(self.zero_field)

        if self.pre_nvt and not restarting:
            coupling_dt = self.dt
            try:
                if self.pre_nvt_timestep_fs is not None:
                    self.dt = self.pre_nvt_timestep_fs * FS_TO_AU
                self.equilibrate(self.pre_nvt_duration_ps, self.pre_nvt_friction_fs)
            finally:
                self.dt = coupling_dt
            self.t = 0.0  # reset the clock so production dynamics start fresh

        # Baseline subtracted from the reported dipoles, as the LAMMPS fix's
        # reset_dipole does. It is captured here, after any equilibration or
        # restart, and is what keeps the permanent dipole of the one shared
        # starting geometry from adding coherently over a whole batch.
        if self.mu_initial is None:
            if self.reset_dipole:
                self.mu_initial = self._dipoles(self.x)
            else:
                self.mu_initial = xp.zeros((n, 3), dtype=xp.float64)

        output = dict(
            record_every_steps=self.record_every_steps,
            record_max_steps=self.record_max_steps,
            append=bool(restarting),
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
                f"[{self._label}] Recording {self.record_names} to "
                f"{self._recorder.path}"
            )
        if self.traj_filename is not None:
            self._trajectory = XYZTrajectoryWriter(
                output_filename(self.traj_filename, self.molecule_ids[0]),
                self.symbols,
                self.molecule_ids,
                self.dt,
                traj_content=self.traj_content,
                **output,
            )
            print(f"[{self._label}] Writing the trajectory to {self._trajectory.path}")

    def _should_record_properties(self, step_index):
        """Check whether this step needs a property record."""
        recorder = self._recorder
        return (
            recorder is not None
            and step_index % self.record_every_steps == 0
            and (
                recorder.record_max_steps is None
                or recorder.n_records < recorder.record_max_steps
            )
        )

    def _should_record_trajectories(self, step_index):
        """Check whether this step needs a trajectory record."""
        trajectory = self._trajectory
        return (
            trajectory is not None
            and step_index % self.record_every_steps == 0
            and (
                trajectory.record_max_steps is None
                or trajectory.n_records < trajectory.record_max_steps
            )
        )

    def _complete_property_values(self):
        """Fill existing energy results and derived properties."""
        values = self._property_values
        columns = self._property_columns

        for name in self.compute_names:
            column = columns[name]

            if name in (
                "kinetic_au",
                "kinetic_trans_au",
                "kinetic_rot_au",
                "kinetic_vib_au",
                *_ROTATION_COMPONENT_NAMES,
            ):
                # These columns are filled during integration.
                continue

            elif name == "potential_au":
                values[:, column] = self.potential

            elif name == "energy_au":
                values[:, column] = self._energy

            elif name in self._term_columns:
                values[:, column] = self._terms[:, self._term_columns[name]]

            elif name == "temperature_K":
                values[:, column] = (
                    2.0 * values[:, columns["kinetic_au"]]
                    / (3.0 * self.n_atoms)
                    / K_TO_AU
                )

            elif name == "energy_vib_au":
                values[:, column] = (
                    values[:, columns["kinetic_vib_au"]]
                    + values[:, columns["stretch_au"]]
                    + values[:, columns["bend_au"]]
                )

    def _gather_property_values(self):
        """Select output columns and return a NumPy array."""
        return to_host(self._property_values[:, list(self._record_columns)])

    # ----------------------- one OBABO step under E-field ---------------------------

    def _dipoles(self, x):
        """
        Return every system's dipole moment for the given positions.

        Parameters
        ----------
        x : numpy.ndarray or cupy.ndarray of float, shape (num, na, 3)
            Atomic positions in atomic units (Bohr).

        Returns
        -------
        numpy.ndarray or cupy.ndarray of float, shape (num, 3)
            With compiled kernels the dipole is the effective-charge sum
            ``sum_i q_eff_i x_i``; the NumPy reference path asks the force field.
        """

        if self.qeff is not None:
            return self.xp.sum(self.qeff * x, axis=1)
        return np.stack([self.ff.dipole(x[d]) for d in range(self.num)])

    def _forces_on_cpu(self, efield):
        """
        Fill ``self.F`` and ``self.potential``, one system at a time.

        Parameters
        ----------
        efield : numpy.ndarray of float, shape (num, 3)
            Effective electric field vector of every system in a.u.
        """

        for d in range(self.num):
            if self.force_kernels is not None:
                self.potential[d] = self.force_kernels.forces_cpu(
                    self.x[d],
                    self.F[d],
                    np.ascontiguousarray(efield[d], dtype=float),
                    self._terms[d],
                )
            else:  # the force field's NumPy reference implementation
                forces, potential = self.ff.compute(
                    self.x[d], efield[d], self._terms[d]
                )
                self.F[d] = forces
                self.potential[d] = potential

    def _step_on_cpu(self, efield, c1h, noise, sample_properties=False, sample_trajectory=False):
        """
        Advance every system by one OBABO step vectorized over the batch.

        Parameters
        ----------
        efield : numpy.ndarray of float, shape (num, 3)
            Effective electric field vector of every system in a.u.
        c1h : float
            Langevin O half-step scaling; ``1.0`` disables the thermostat.
        noise : numpy.ndarray of float, shape (na, 1)
            Momentum noise amplitude of the Langevin half-step per atom.
        sample_properties : bool, default: False
            Fill selected kinetic-property columns before the final O half-step.
        sample_trajectory : bool, default: False
            Prepare the selected trajectory arrays before the final O half-step.

        Returns
        -------
        trajectory_velocities : numpy.ndarray or None
            Pre-O atomic velocities, shape ``(num, n_atoms, 3)``; ``None`` when
            atomic velocities are not selected for this step.
        angular_velocity_body : numpy.ndarray or None
            Pre-O molecular trajectory data, shape ``(num, n_molecules, 6)``:
            three body-frame angular velocities followed by their three principal
            moments of inertia. ``None`` when angular output is not selected.
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
        if self.qeff is not None:
            self._amplitude[:] = np.sum(self.qeff * v_half, axis=1)
            self._dipole_half[:] = np.sum(self.qeff * x_half, axis=1)
            self._dipole_force[:] = np.sum(self.qeff * self.x, axis=1)
        else:  # no effective charges: ask the force field system by system
            for d in range(self.num):
                self._amplitude[d] = self.ff.dipole_velocity(v_half[d])
                self._dipole_half[d] = self.ff.dipole(x_half[d])
                self._dipole_force[d] = self.ff.dipole(self.x[d])
        kinetic = 0.5 * np.sum(self.p**2 / mass, axis=(1, 2))
        self._energy[:] = kinetic + self.potential

        trajectory_velocities = None
        angular_velocity_body = None

        if sample_properties:
            if "kinetic_au" in self._property_columns:
                self._property_values[:, self._property_columns["kinetic_au"]] = kinetic
            
        if sample_properties or (sample_trajectory and self._write_angular_velocity_body):
            angular_velocity_body = self._compute_molecular_kinetic(
                self.p,
                sample_trajectory=sample_trajectory,
            )

        if sample_trajectory and self._write_trajectory_velocities:
            trajectory_velocities = self.p / mass
        
        if c1h < 1.0:  # O
            self._langevin_half_kick_on_cpu(c1h, noise)

        return trajectory_velocities, angular_velocity_body
    
    def _langevin_half_kick_on_cpu(self, c1h, noise):
        """
        Apply one Ornstein-Uhlenbeck half-step to the momenta of every system.

        Each system draws from its own generator in a fixed order, so a batch
        member and a single system with the same molecule ID stay identical.

        Parameters
        ----------
        c1h : float
            Langevin O half-step scaling.
        noise : numpy.ndarray of float, shape (na, 1)
            Momentum noise amplitude of the Langevin half-step, per atom.
        """

        shape = (self.n_atoms, 3)
        for d, rng in enumerate(self._rngs):
            self.p[d] = c1h * self.p[d] + noise * rng.standard_normal(shape)

    def _step_on_gpu(self, efield, c1h, noise, sample_properties=False, sample_trajectory=False):
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
        sample_properties : bool, default: False
            Fill selected property columns using the state before the final
            Langevin O half-step.
        sample_trajectory : bool, default: False
            Prepare selected trajectory arrays from the pre-O momentum copy.

        Returns
        -------
        trajectory_velocities : cupy.ndarray or None
            Pre-O atomic velocities, shape ``(num, n_atoms, 3)``; ``None`` when
            atomic velocities are not selected for this step.
        angular_velocity_body : cupy.ndarray or None
            Pre-O molecular trajectory data, shape ``(num, n_molecules, 6)``:
            three body-frame angular velocities followed by their three principal
            moments of inertia. ``None`` when angular output is not selected.
        """

        noise_dev = self.xp.asarray(noise.ravel())
        thermostat = c1h < 1.0
        
        kinetic_column = (self._property_columns.get("kinetic_au", -1) if sample_properties else -1)
        sample_molecular = ((sample_properties or sample_trajectory) and self._gpu_property_momenta is not None)
        property_momenta = (self._gpu_property_momenta if sample_molecular else self.p)

        self.kernels["pre"][self.num, THREADS_PER_BLOCK](
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
        self.kernels["post"][self.num, THREADS_PER_BLOCK](
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
            self._amplitude,
            self._dipole_half,
            self._dipole_force,
            self._energy,
            kinetic_column,
            self._property_values,
            sample_molecular,
            property_momenta,
        )

        trajectory_velocities = None
        angular_velocity_body = None

        if sample_properties or sample_trajectory:
            from numba import cuda

            cuda.synchronize()

            if sample_properties or (sample_trajectory and self._write_angular_velocity_body):
                angular_velocity_body = self._compute_molecular_kinetic(
                    property_momenta,
                    sample_trajectory=sample_trajectory,
                )
            if sample_trajectory and self._write_trajectory_velocities:
                trajectory_velocities = property_momenta / self.mass

        return trajectory_velocities, angular_velocity_body
    
    def _compute_molecular_kinetic(self, momenta, sample_trajectory=False):
        """Fill kinetic properties and optionally return angular velocities with inertia.

        Parameters
        ----------
        momenta : numpy.ndarray or cupy.ndarray
            Atomic momenta before the final Langevin O half-step, with shape
            ``(num, n_atoms, 3)``. This array is only read.
        sample_trajectory : bool, default: False
            Whether this step needs a trajectory frame. Angular velocities and
            principal moments are returned only when angular output is also
            selected by ``traj_content``.

        Returns
        -------
        numpy.ndarray or cupy.ndarray or None
            Molecular trajectory data, shape ``(num, n_molecules, 6)``: the first
            three columns are body-frame angular velocities in inverse atomic
            time, and the last three are principal moments of inertia in
            electron-mass times Bohr-squared units. Both follow instantaneous
            principal axes ordered by decreasing moment of inertia. Inactive
            axes have zero angular velocity; the moments retain their original
            eigenvalues. The basis is not made continuous between frames.
            Returns ``None`` when angular output is not selected for this step.
        """

        xp = self.xp
        columns = self._property_columns
        values = self._property_values

        need_trans = "kinetic_trans_au" in columns
        need_rot_components = any(name in columns for name in _ROTATION_COMPONENT_NAMES)
        need_rot = (
            "kinetic_rot_au" in columns
            or need_rot_components
            or (sample_trajectory and self._write_angular_velocity_body)
        )
        need_vib = "kinetic_vib_au" in columns

        if not (need_trans or need_rot or need_vib):
            return

        atoms_per_molecule = self.n_atoms // self.n_molecules
        shape = (self.num, self.n_molecules, atoms_per_molecule, 3)

        momentum = momenta.reshape(shape)
        mass = self.mass.reshape(self.n_molecules, atoms_per_molecule)
        total_mass = mass.sum(axis=1)

        # Molecular center-of-mass velocity.
        velocity_com = (momentum.sum(axis=2) / total_mass[None, :, None])

        if need_trans:
            energy_molecule = (0.5 * total_mass[None, :] * xp.sum(velocity_com**2, axis=-1))
            values[:, columns["kinetic_trans_au"]] = energy_molecule.sum(axis=1)

        if not (need_rot or need_vib):
            return

        position = self.x.reshape(shape)

        # Reconstruct molecules without changing the simulation positions.
        if self.ff.box is not None:
            box = xp.asarray(self.ff.box).reshape(1, 1, 1, 3)
            anchor = position[:, :, :1, :]
            displacement = position - anchor
            position = anchor + (displacement - box * xp.rint(displacement / box))

        mass_weights = mass[None, :, :, None]
        position_com = xp.sum(position * mass_weights, axis=2) / total_mass[None, :, None]
        relative_position = position - position_com[:, :, None, :]
        relative_velocity = momentum / mass_weights - velocity_com[:, :, None, :]

        x = relative_position[..., 0]
        y = relative_position[..., 1]
        z = relative_position[..., 2]
        weights = mass[None, :, :]

        inertia = xp.empty((self.num, self.n_molecules, 3, 3), dtype=xp.float64)
        inertia[..., 0, 0] = xp.sum(weights * (y*y + z*z), axis=-1)
        inertia[..., 1, 1] = xp.sum(weights * (x*x + z*z), axis=-1)
        inertia[..., 2, 2] = xp.sum(weights * (x*x + y*y), axis=-1)
        inertia[..., 0, 1] = inertia[..., 1, 0] = -xp.sum(weights * x * y, axis=-1)
        inertia[..., 0, 2] = inertia[..., 2, 0] = -xp.sum(weights * x * z, axis=-1)
        inertia[..., 1, 2] = inertia[..., 2, 1] = -xp.sum(weights * y * z, axis=-1)

        angular_momentum = xp.sum(xp.cross(relative_position, mass_weights * relative_velocity), axis=2)

        # Principal axes ordered by decreasing moment of inertia.
        principal_moments, principal_axes = xp.linalg.eigh(inertia)
        principal_moments = principal_moments[..., ::-1]
        principal_axes = principal_axes[..., :, ::-1]

        # Use the same relative cutoff as the previous pseudoinverse.
        valid = principal_moments > 1e-10 * xp.maximum(principal_moments[..., :1], 0.0)
        safe_moments = xp.where(valid, principal_moments, 1.0)

        # Angular momentum and angular velocity in the principal-axis frame.
        angular_momentum_body = (xp.swapaxes(principal_axes, -1, -2) @ angular_momentum[..., None])[..., 0]

        angular_velocity_body = xp.where(valid, angular_momentum_body / safe_moments, 0.0)

        # Transform back for the existing vibrational-velocity calculation.
        angular_velocity = (principal_axes @ angular_velocity_body[..., None])[..., 0]

        if need_rot_components:
            component_energy = 0.5 * angular_momentum_body * angular_velocity_body
            for axis, name in enumerate(_ROTATION_COMPONENT_NAMES):
                if name in columns:
                    values[:, columns[name]] = component_energy[..., axis].sum(axis=1)

        if "kinetic_rot_au" in columns:
            energy_molecule = 0.5 * xp.sum(angular_velocity * angular_momentum, axis=-1)
            values[:, columns["kinetic_rot_au"]] = energy_molecule.sum(axis=1)

        if need_vib:
            rotational_velocity = xp.cross(angular_velocity[:, :, None, :], relative_position)
            vibrational_velocity = relative_velocity - rotational_velocity
            energy_molecule = 0.5 * xp.sum(weights * xp.sum(vibrational_velocity**2, axis=-1), axis=-1)
            values[:, columns["kinetic_vib_au"]] = energy_molecule.sum(axis=1)

        if sample_trajectory and self._write_angular_velocity_body:
            # Pack only the trajectory output; energy calculations above use 3-component omega.
            return xp.concatenate((angular_velocity_body, principal_moments), axis=-1)

    def step(self, efield_au):
        """
        Advance every system by one OBABO velocity-Verlet step under the field.

        Parameters
        ----------
        efield_au : numpy.ndarray of float, shape (num, 3)
            Contiguous host array of effective electric field vectors in a.u.

        Returns
        -------
        MDStepResult
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
            raise RuntimeError(f"{self._label}.step() called before initialize().")
        field = np.ascontiguousarray(efield_au, dtype=np.float64)
        if field.shape != (self.num, 3):
            raise ValueError(
                f"efield_au must have shape ({self.num}, 3); got {tuple(field.shape)}."
            )

        next_step = self._step_index + 1
        sample_properties = self._should_record_properties(next_step)
        sample_trajectory = self._should_record_trajectories(next_step)

        field = self.xp.asarray(field)
        step_method = self._step_on_gpu if self._on_gpu else self._step_on_cpu
        trajectory_velocities, angular_velocity_body = step_method(
            field,
            self.c1h,
            self.noise,
            sample_properties=sample_properties,
            sample_trajectory=sample_trajectory,
        )

        self._dipole_half -= self.mu_initial  # report relative to time zero
        self._dipole_force -= self.mu_initial
        self.t += self.dt

        self._step_index = next_step

        # Selected properties, shared by CPU and GPU.
        if sample_properties:
            self._complete_property_values()
            values = self._gather_property_values()

            self._recorder.record(
                self._step_index,
                self.t,
                values,
            )

        # Independent trajectory output from the pre-O state.
        if sample_trajectory:
            positions = None
            velocities = None
            angular_velocity_atoms = None

            if self.traj_content in ("positions", "all"):
                positions = to_host(self.x)

            if self.traj_content in ("velocities", "all"):
                velocities = to_host(trajectory_velocities)

            if angular_velocity_body is not None:
                # Repeat each molecule's [omega_1, omega_2, omega_3, I_1, I_2, I_3].
                atoms_per_molecule = self.n_atoms // self.n_molecules
                angular_velocity_atoms = np.repeat(
                    to_host(angular_velocity_body),
                    atoms_per_molecule,
                    axis=1,
                )

            self._trajectory.write(
                self._step_index,
                self.t,
                positions_bohr=positions,
                velocities_au=velocities,
                angular_velocity_body_au=angular_velocity_atoms,
            )

        return MDStepResult(
            amplitude_au=to_host(self._amplitude),
            dipole_half_au=to_host(self._dipole_half),
            dipole_force_au=to_host(self._dipole_force),
            energy_au=to_host(self._energy),
        )

    def equilibrate(self, duration_ps, friction_fs, temperature_K=None):
        """
        Run a field-free Langevin trajectory to thermalize every system.

        Each system carries its own random stream and remains uncorrelated with
        other systems. Nothing is recorded and the dipole baseline is untouched.

        Parameters
        ----------
        duration_ps : float
            Equilibration length in picoseconds.
        friction_fs : float
            Langevin relaxation time in femtoseconds.
        temperature_K : float, optional
            Bath temperature; the engine's own ``temperature_K`` by default.

        Returns
        -------
        int
            Number of MD steps taken.
        """

        kT = self.kT if temperature_K is None else K_TO_AU * float(temperature_K)
        gamma = 1.0 / (float(friction_fs) * FS_TO_AU)
        c1h = float(np.exp(-0.5 * gamma * self.dt))
        noise = np.sqrt(self._mass_host * kT * (1.0 - c1h**2))
        n_steps = int(round(float(duration_ps) * 1000.0 * FS_TO_AU / self.dt))
        for _ in range(n_steps):
            if self._on_gpu:
                self._step_on_gpu(self.zero_field, c1h, noise)
            else:
                self._step_on_cpu(self.zero_field, c1h, noise)
            self.t += self.dt
        return n_steps

    def set_thermostat(self, thermostat, temperature_K=None, friction_fs=None):
        """
        Switch the production ensemble after :meth:`initialize`.

        Parameters
        ----------
        thermostat : {'nve', 'nvt'}
            The ensemble subsequent :meth:`step` calls integrate.
        temperature_K : float, optional
            New target temperature in Kelvin; unchanged by default.
        friction_fs : float, optional
            New Langevin relaxation time in femtoseconds; unchanged by default.
        """

        thermostat = str(thermostat).lower()
        if thermostat not in ("nve", "nvt"):
            raise ValueError("thermostat must be 'nve' or 'nvt'.")
        if self.dt <= 0.0:
            raise RuntimeError("set_thermostat() requires initialize() first.")
        self.thermostat = thermostat
        if temperature_K is not None:
            self.temperature_K = float(temperature_K)
            self.kT = K_TO_AU * self.temperature_K
        if friction_fs is not None:
            self.friction_fs = float(friction_fs)
        if self.thermostat == "nvt":
            gamma = 1.0 / (self.friction_fs * FS_TO_AU)
            self.c1h = float(np.exp(-0.5 * gamma * self.dt))
        else:
            self.c1h = 1.0
        self.noise = np.sqrt(self._mass_host * self.kT * (1.0 - self.c1h**2))

    # --------------------- snapshot / checkpoint state ------------------------------

    def snapshot(self):
        """
        Return a deep-copied snapshot of the propagating state.

        Returns
        -------
        dict
            The clock, positions, momenta, forces, and random-generator states.
        """

        snap = {
            "time": self.t,
            "x": self.x.copy(),
            "p": self.p.copy(),
            "F": self.F.copy(),
            "rng": [rng.bit_generator.state for rng in self._rngs],
        }
        if self._on_gpu:  # the device streams draw the GPU Langevin noise
            snap["rng_states"] = self.rng_states.copy_to_host()
        return snap

    def restore(self, snapshot):
        """
        Restore the propagating state from a snapshot.

        Parameters
        ----------
        snapshot : dict
            A snapshot produced by :meth:`snapshot`.
        """

        self.t = snapshot["time"]
        self.x[:] = snapshot["x"]
        self.p[:] = snapshot["p"]
        self.F[:] = snapshot["F"]
        for rng, state in zip(self._rngs, snapshot["rng"]):
            rng.bit_generator.state = state
        if self._on_gpu and "rng_states" in snapshot:
            from numba import cuda

            self.rng_states = cuda.to_device(snapshot["rng_states"])

    def state_dict(self):
        """
        Return the checkpointable state as host arrays.

        Returns
        -------
        dict
            ``time`` (float), ``x``/``p``/``F`` of shape ``(num, na, 3)``,
            ``mu_initial`` of shape ``(num, 3)`` and the ``molecule_ids``.
        """

        return {
            "time": self.t,
            "x": to_host(self.x),
            "p": to_host(self.p),
            "F": to_host(self.F),
            "mu_initial": to_host(self.mu_initial),
            "molecule_ids": np.asarray(self.molecule_ids),
        }

    def load_state_dict(self, state):
        """
        Restore a checkpointed state produced by :meth:`state_dict`.

        Parameters
        ----------
        state : dict
            ``time``, ``x``, ``p``, ``F`` and optionally ``mu_initial`` (an old
            checkpoint without it keeps ``mu_initial`` unset, so the baseline is
            recaptured from the restored geometry).

        Raises
        ------
        ValueError
            If the state holds a differently shaped batch.
        """

        if self.x is None:
            raise RuntimeError("load_state_dict() requires initialize() first.")
        x = np.asarray(state["x"])
        if x.shape != (self.num, self.n_atoms, 3):
            raise ValueError(
                f"Checkpoint holds {x.shape} state, expected "
                f"{(self.num, self.n_atoms, 3)}."
            )
        self.t = float(state["time"])
        self.x[:] = self.xp.asarray(x)
        self.p[:] = self.xp.asarray(state["p"])
        self.F[:] = self.xp.asarray(state["F"])
        if "mu_initial" in state:  # keep reporting against the original baseline
            if self.mu_initial is None:
                self.mu_initial = self.xp.asarray(state["mu_initial"])
            else:
                self.mu_initial[:] = self.xp.asarray(state["mu_initial"])

    # ----------------------- internal helper / teardown -----------------------------

    def close_outputs(self):
        """Close the output files if any are being written."""

        for name in ("_recorder", "_trajectory"):
            writer = getattr(self, name)
            if writer is not None:
                writer.close()
                setattr(self, name, None)

    def close(self):
        """
        Close the output files and drop the batch state (device buffers included).
        """

        self.close_outputs()
        self.x = self.p = self.F = None
        self._terms = None
        self._property_values = None
        self._gpu_property_momenta = None
        self._amplitude = self._dipole_half = self._dipole_force = self._energy = None
        self.mu_initial = None
        self.potential = None
        self.force_kernels = None
