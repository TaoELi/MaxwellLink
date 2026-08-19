# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Conventional classical molecular-dynamics (MD) driver for MaxwellLink.

Universal MD driver, ``MDModel``, can be connected to different force fields (``ff``)
for simulating a wide range of simple condensed-phase molecules.

This built-in feature avoids calling external LAMMPS for MD simulations and reduces the
memory requirement for MaxwellLink connecting to MD drivers.
"""

import os

import numpy as np

from maxwelllink.tools.xyz_helper import read_xyz, read_xyz_frames
from maxwelllink.units import BOHR_PER_ANG, FS_TO_AU, K_TO_AU
from maxwelllink.tools.recorders import (
    PropertyRecorder,
    XYZTrajectoryWriter,
    output_filename,
)

try:
    from ..dummy_model import DummyModel
    from .qtip4pf import QTIP4PFForceField
    from .co2jcp2021 import CO2JCP2021ForceField
except (ImportError, ValueError):  # allow running as a stand-alone script
    from dummy_model import DummyModel
    from qtip4pf import QTIP4PFForceField
    from co2jcp2021 import CO2JCP2021ForceField


# registry of available force fields, keyed by their short string name
_FORCE_FIELDS = {
    QTIP4PFForceField.name: QTIP4PFForceField,
    CO2JCP2021ForceField.name: CO2JCP2021ForceField,
}

# Langevin relaxation time (fs) used for the optional pre-NVT equilibration
_PRE_NVT_FRICTION_FS = 100.0

#: The per-molecule properties every MD driver records, before the force field's own.
_BASE_RECORD_NAMES = ("temperature_K", "energy_au")


class MDModel(DummyModel):
    """
    Conventional classical molecular-dynamics driver with a pluggable force field.

    Notes
    -----
    This is a minimal implementation of a classical MD driver for MaxwellLink.
    It is not meant to be a full-featured MD engine.
    """

    def __init__(
        self,
        ff="qtip4pf",
        n_molecules=None,
        positions=None,
        xyz=None,
        batch_xyz=None,
        box=None,
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
        force_backend: str = "auto",
        property_filename=None,
        traj_filename=None,
        record_every_steps: int = 10,
        record_max_steps=None,
        checkpoint: bool = False,
        restart: bool = False,
        verbose: bool = False,
    ):
        """
        Initialize the necessary parameters for the classical MD driver.

        Parameters
        ----------
        ff : str, default: 'qtip4pf'
            Name of the force field to integrate. Registered force fields are
            ``'qtip4pf'`` (flexible water) and ``'co2jcp2021'`` (flexible CO2).
        n_molecules : int, optional
            Number of molecules to build when ``positions`` is not given. If ``None``
            (the default) each force field applies its own default count (216 for
            water and 36 for CO2, which load the bundled equilibrated bulk boxes).
        positions : array-like of float, shape (n_atoms, 3), optional
            Initial atomic positions in Bohr. If ``None`` the force field builds a
            default geometry from ``n_molecules`` (and ``box``).
        xyz : str, optional
            Path to an XYZ file (Angstrom) to take the starting geometry from, with the
            atoms in the order the force field expects (``[C, O, O, ...]`` for CO2,
            ``[O, H, H, ...]`` for water). Overrides ``positions``; like it, it gives
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
            before any EM coupled dynamics. The random seed is offset by ``molecule_id``
            so that different molecules are decorrelated even at the same ``seed``.
        pre_nvt_duration_ps : float, default: 20.0
            Duration of the pre-NVT equilibration in picoseconds. It uses a Langevin
            thermostat with a 100 fs relaxation time at ``temperature_K``, regardless
            of the production ``thermostat``.
        reset_dipole : bool, default: True
            Whether to report the dipole moment relative to its value at time zero.
        seed : int, default: 0
            Seed for the random-number generator in EM-coupled dynamics.
        force_backend : {'auto', 'numba', 'numpy'}, default: 'auto'
            Which force evaluator to use. ``'numba'`` uses the compiled loop-form
            kernels, which are typically several times faster than the array-at-a-time
            NumPy reference and allocate no ``(na, na, 3)`` temporaries; ``'numpy'``
            forces the reference implementation. ``'auto'`` picks ``'numba'`` whenever
            the force field provides a compiled-kernel description and silently falls
            back to ``'numpy'`` when it does not.
        property_filename : str, optional
            Turns on a run-time record of per-molecule properties -- the temperature,
            the total energy and the force field's own energy terms (``stretch_au`` and
            ``bend_au`` for CO2 and water) -- written to this file every
            ``record_every_steps`` production steps: HDF5, streamed while the run goes,
            unless the name ends in ``.npz`` or ``h5py`` is missing. ``_id_<molecule
            ID>`` is inserted before the extension, so each driver process writes its
            own file; a batch writes one file with one column per molecule. Pre-NVT is
            not recorded; a restart appends.
        traj_filename : str, optional
            Turns on a geometry trajectory in extended XYZ (Angstrom), one frame per
            molecule every ``record_every_steps`` steps, with the same ``_id_`` suffix;
            see :class:`maxwelllink.tools.XYZTrajectoryWriter` for the frame order of a
            batch and :func:`maxwelllink.tools.read_xyz_trajectory` to read it back.
        record_every_steps : int, default: 10
            Record every this many production steps, for both files.
        record_max_steps : int, optional
            Stop recording after this many records; ``None`` for no cap.
        checkpoint : bool, default: False
            Whether to enable checkpointing.
        restart : bool, default: False
            Whether to restart from a checkpoint if available.
        verbose : bool, default: False
            Whether to print verbose output.
        """

        super().__init__(verbose, checkpoint, restart)

        self.thermostat = str(thermostat).lower()
        if self.thermostat not in ("nve", "nvt"):
            raise ValueError("thermostat must be 'nve' or 'nvt'.")
        self.temperature_K = float(temperature_K)
        self.friction_fs = float(friction_fs)
        self.init_velocities = bool(init_velocities)
        self.pre_nvt = bool(pre_nvt)
        self.pre_nvt_duration_ps = float(pre_nvt_duration_ps)
        self.reset_dipole = bool(reset_dipole)
        self.seed = int(seed)
        self.kT = K_TO_AU * self.temperature_K

        # run-time trajectory output, opened in initialize() once the molecule ID and
        # the time step are known
        self.property_filename = (
            None if property_filename is None else str(property_filename)
        )
        self.traj_filename = None if traj_filename is None else str(traj_filename)
        self.record_every_steps = int(record_every_steps)
        self.record_max_steps = record_max_steps
        self._recorder = None
        self._trajectory = None
        self._step_index = 0  # production steps taken; not part of the snapshot

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

        # build the force field
        key = str(ff).lower()
        if key not in _FORCE_FIELDS:
            raise ValueError(
                "Unknown force field %r. Available: %s"
                % (ff, ", ".join(sorted(_FORCE_FIELDS)))
            )
        # the universal MD parameters are the common force-field constructor API;
        # forward n_molecules only when set so each force field can apply its own
        # default (e.g. the 216-molecule water box) when it is left unspecified
        ff_params = dict(
            positions=positions, box=box, rcut=rcut, ewald_wrcut=ewald_wrcut
        )
        if n_molecules is not None:
            ff_params["n_molecules"] = n_molecules
        self.ff = _FORCE_FIELDS[key](**ff_params)
        self.ff_name = self.ff.name
        expected = list(self.ff.molecule_symbols) * self.ff.n_molecules
        if elements is not None and expected and list(elements) != expected:
            raise ValueError(
                f"the XYZ atoms do not follow the {self.ff_name} force field's order "
                f"{list(self.ff.molecule_symbols)} per molecule."
            )

        # Pick the force evaluator
        backend = str(force_backend).lower()
        if backend not in ("auto", "numba", "numpy"):
            raise ValueError("force_backend must be 'auto', 'numba' or 'numpy'.")
        self.force_backend = "numpy"
        self._compute = self.ff.compute
        if backend != "numpy" and self.ff.has_compiled_kernels:
            self.force_backend = "numba"
            self._compute = self.ff.compute_fast
        elif backend == "numba":
            raise NotImplementedError(
                f"Force field {self.ff.name!r} provides no compiled kernels; "
                f"use force_backend='numpy'."
            )

        # state pulled from the force field
        self.na = self.ff.na
        self.mass = self.ff.masses  # shape (na, 1)
        self.x = self.ff.positions.copy()  # positions (na, 3)
        self.p = np.zeros((self.na, 3))  # momenta
        self.F = np.zeros((self.na, 3))  # forces (updated each step)

        # integration helpers filled in during initialize()
        self.rng = None
        self.c1h = 1.0  # Langevin O half-step scaling

        # data returned to MaxwellLink
        self.mu_initial = None  # dipole baseline, set in initialize()
        self._amp = np.zeros(3)  # d(mu)/dt
        self.dipole_vec = np.zeros(3)  # mu half a step after force time (mu_half)
        self.dipole_force = np.zeros(3)  # mu at force-evaluation position (mu_force)
        self.energy = 0.0  # kinetic + potential
        self.potential = 0.0
        # the force field's energy terms, filled with every force evaluation
        self.record_names = _BASE_RECORD_NAMES + tuple(self.ff.term_names)
        # element symbols of the geometry, for the trajectory file
        self.symbols = (
            list(self.ff.molecule_symbols) * self.ff.n_molecules or ["X"] * self.na
        )
        self._terms = np.zeros(len(self.ff.term_names))

    # ----------------------- heavy-load initialization ------------------------------
    def initialize(self, dt_new, molecule_id):
        """
        Initialize the MD simulation and possibly pre-equilibrate.

        This is called during the INIT stage of the socket communication, once the
        molecule ID has been assigned. When ``pre_nvt`` is enabled (and this is not a
        checkpoint restart), the system is thermally equilibrated here before any EM
        coupling.

        Parameters
        ----------
        dt_new : float
            The new time step in atomic units (a.u.).
        molecule_id : int
            The ID of the molecule assigned by SocketHub.
        """

        self.dt = float(dt_new)
        self.molecule_id = int(molecule_id)
        self.checkpoint_filename = "md_checkpoint_id_%d.npz" % self.molecule_id
        if self.batch_frames is not None:
            if self.molecule_id >= len(self.batch_frames):
                raise ValueError(
                    "[molecule ID %d] batch_xyz holds %d frames, so it has no frame "
                    "for this molecule." % (self.molecule_id, len(self.batch_frames))
                )
            self.x = self.batch_frames[self.molecule_id].copy()

        # random generator (deterministic given the seed and molecule id)
        self.rng = np.random.default_rng(self.seed + self.molecule_id)

        # Langevin O half-step coefficient exp(-0.5 * gamma * dt)
        if self.thermostat == "nvt":
            gamma = 1.0 / (self.friction_fs * FS_TO_AU)
            self.c1h = float(np.exp(-0.5 * gamma * self.dt))
        else:
            self.c1h = 1.0

        # initial momenta from a Maxwell-Boltzmann distribution
        if self.init_velocities:
            sigma_p = np.sqrt(self.mass * self.kT)  # per-DOF momentum spread
            self.p = sigma_p * self.rng.standard_normal((self.na, 3))
            self._remove_com_momentum()
        else:
            self.p = np.zeros((self.na, 3))

        # forces at the initial geometry
        self.F, self.potential = self._compute(self.x, np.zeros(3), self._terms)

        if self.restart and self.checkpoint:
            self._reset_from_checkpoint()
        elif self.pre_nvt:
            # thermally equilibrate before production; the per-molecule random stream
            # decorrelates drivers that share the same initial geometry
            self._equilibrate(self.pre_nvt_duration_ps, _PRE_NVT_FRICTION_FS)
            self.t = 0.0  # reset the clock so production dynamics start fresh

        # Baseline subtracted from the reported dipole, as the LAMMPS fix's
        # reset_dipole does. It is captured here, after any equilibration or
        # restart, so the driver reports how its dipole changes rather than the
        # permanent dipole frozen into the starting geometry.
        if self.mu_initial is None:
            self.mu_initial = (
                self.ff.dipole(self.x) if self.reset_dipole else np.zeros(3)
            )

        output = dict(
            record_every_steps=self.record_every_steps,
            record_max_steps=self.record_max_steps,
            append=bool(self.restart and self.checkpoint),
        )
        if self.property_filename is not None:
            self._recorder = PropertyRecorder(
                output_filename(self.property_filename, self.molecule_id),
                self.record_names,
                [self.molecule_id],
                self.dt,
                **output,
            )
            print(f"[MDModel] Recording {self.record_names} to {self._recorder.path}")
        if self.traj_filename is not None:
            self._trajectory = XYZTrajectoryWriter(
                output_filename(self.traj_filename, self.molecule_id),
                self.symbols,
                [self.molecule_id],
                self.dt,
                **output,
            )
            print(f"[MDModel] Writing the trajectory to {self._trajectory.path}")

    def close(self):
        """Close the output files, if any are being written."""

        for name in ("_recorder", "_trajectory"):
            writer = getattr(self, name)
            if writer is not None:
                writer.close()
                setattr(self, name, None)

    def _thermostat_half_step(self):
        """
        Apply one Ornstein-Uhlenbeck (Langevin) half-step to the momenta.
        """

        if self.c1h >= 1.0:
            return
        noise = np.sqrt(self.mass * self.kT * (1.0 - self.c1h**2))
        self.p = self.c1h * self.p + noise * self.rng.standard_normal((self.na, 3))

    def _equilibrate(self, duration_ps, friction_fs):
        """
        Run a bare NVT (Langevin) trajectory in place to equilibrate the state.

        This reuses the OBABO integrator with a field-free force but skips the dipole
        and amplitude bookkeeping of :meth:`propagate`. The production thermostat
        coefficient is restored on return.

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

        n_steps = int(round(duration_ps * 1000.0 * FS_TO_AU / self.dt))
        gamma = 1.0 / (friction_fs * FS_TO_AU)
        saved_c1h = self.c1h
        self.c1h = float(np.exp(-0.5 * gamma * self.dt))  # force NVT for equilibration
        zero = np.zeros(3)
        try:
            for _ in range(n_steps):
                self._thermostat_half_step()
                self.p += 0.5 * self.dt * self.F
                self.x += self.dt * (self.p / self.mass)
                self.F, self.potential = self._compute(self.x, zero, self._terms)
                self.p += 0.5 * self.dt * self.F
                self._thermostat_half_step()
        finally:
            self.c1h = saved_c1h  # restore the production thermostat coefficient
        return n_steps

    def _remove_com_momentum(self):
        """Subtract the center-of-mass momentum so the system does not drift."""

        total_p = self.p.sum(axis=0)
        total_m = self.mass.sum()
        self.p -= self.mass * (total_p / total_m)

    # ---------------------------- one EM (FDTD) step --------------------------------
    def propagate(self, effective_efield_vec):
        """
        Advance the system by one OBABO velocity-Verlet step under the field.

        The step recomputes the force at the new geometry, then records the source
        amplitude and two dipole snapshots (at the force-evaluation position and half a
        time step later) that the SingleModeSimulation EM solver needs.

        Parameters
        ----------
        effective_efield_vec : array-like of float, shape (3,)
            Effective electric field vector in the form ``[E_x, E_y, E_z]`` in atomic
            units.
        """

        efield = np.asarray(effective_efield_vec, dtype=float)

        # O: leading Langevin half-step (identity for NVE)
        self._thermostat_half_step()
        # B: half momentum kick using the force carried from the previous step
        self.p += 0.5 * self.dt * self.F
        # A: full drift of the positions
        self.x += self.dt * (self.p / self.mass)
        # recompute the force at the new geometry with the current field
        self.F, self.potential = self._compute(self.x, efield, self._terms)
        # B: second half momentum kick
        self.p += 0.5 * self.dt * self.F

        # Two dipole snapshots the SingleModeSimulation EM solver needs (as in
        # SHOModel): mu at the force-evaluation position (mu_force -> mux_m_au) and mu
        # half a step later (mu_half -> mux_au). The solver drifts the dipole to the
        # next force time via 2 * mu_half - mu_force.
        p_half = self.p + 0.5 * self.dt * self.F
        v_half = p_half / self.mass
        x_half = self.x + 0.5 * self.dt * v_half
        self._amp = self.ff.dipole_velocity(v_half)
        self.dipole_force = self.ff.dipole(self.x) - self.mu_initial
        self.dipole_vec = self.ff.dipole(x_half) - self.mu_initial

        kinetic = 0.5 * float(np.sum(self.p**2 / self.mass))
        self.energy = kinetic + self.potential

        # O: Langevin half-step, then advance the clock
        self._thermostat_half_step()
        self.t += self.dt

        # the trajectory record: T = 2 K / (3 N k_B) over all 3 N degrees of freedom,
        # the total energy, then the force field's terms
        self._step_index += 1
        if self._recorder is not None:
            temperature = 2.0 * kinetic / (3.0 * self.na) / K_TO_AU
            self._recorder.record(
                self._step_index,
                self.t,
                np.concatenate(([temperature, self.energy], self._terms)),
            )
        if self._trajectory is not None:
            self._trajectory.write(self._step_index, self.t, self.x[None])

        if self.verbose:
            print(
                f"[molecule ID {self.molecule_id}] t={self.t:.3f} a.u. "
                f"E_tot={self.energy:.8f} a.u. mu={self.dipole_vec}"
            )

    def calc_amp_vector(self):
        """
        Update the source amplitude vector after propagating this molecule for one time
        step.

        Returns
        -------
        numpy.ndarray of float, shape (3,)
            Amplitude vector in the form
            :math:`[\\mathrm{d}\\mu_x/\\mathrm{d}t,\\ \\mathrm{d}\\mu_y/\\mathrm{d}t,\\ \\mathrm{d}\\mu_z/\\mathrm{d}t]`.
        """

        return self._amp

    # --------------------- optional data / checkpoint -------------------------------
    def append_additional_data(self):
        """
        Append additional data to be sent back to MaxwellLink.

        Returns
        -------
        dict
            A dictionary with ``time_au``, ``energy_au``, ``potential_au`` and the
            dipole components ``mux_au``/``muy_au``/``muz_au`` (half a step after the
            force time) and ``mux_m_au``/``muy_m_au``/``muz_m_au`` (at the force time),
            all in atomic units.
        """

        data = {}
        data["time_au"] = self.t
        data["energy_au"] = self.energy
        data["potential_au"] = self.potential
        # mu half a step after the force time (used with mu_force for the drift)
        data["mux_au"] = self.dipole_vec[0]
        data["muy_au"] = self.dipole_vec[1]
        data["muz_au"] = self.dipole_vec[2]
        # mu at the force-evaluation position (needed by the EM solver's dipole drift)
        data["mux_m_au"] = self.dipole_force[0]
        data["muy_m_au"] = self.dipole_force[1]
        data["muz_m_au"] = self.dipole_force[2]
        return data

    def _dump_to_checkpoint(self):
        """
        Dump the internal MD state to a checkpoint.

        Notes
        -----
        ``self.checkpoint_filename`` includes the molecule ID (set in
        :meth:`initialize`).
        """

        np.savez(
            self.checkpoint_filename,
            time=self.t,
            x=self.x,
            p=self.p,
            F=self.F,
            mu_initial=self.mu_initial,
        )

    def _reset_from_checkpoint(self):
        """
        Restore the internal MD state from a checkpoint.

        Notes
        -----
        Starts fresh (printing a note) when no checkpoint file is found for this
        molecule ID.
        """

        if not os.path.exists(self.checkpoint_filename):
            print(
                "[checkpoint] No checkpoint file found for molecule ID %d, "
                "starting fresh." % self.molecule_id
            )
            return
        data = np.load(self.checkpoint_filename)
        self.t = float(data["time"])
        self.x = data["x"]
        self.p = data["p"]
        self.F = data["F"]
        if "mu_initial" in data:  # keep reporting against the original baseline
            self.mu_initial = data["mu_initial"]

    # --------------------------------- stage / commit protocol (deep-copied state) --
    def _snapshot(self):
        """
        Return a deep-copied snapshot of the propagating state.

        Used by the stage-commit protocol so a proposed step can be previewed and then
        either committed or discarded.

        Returns
        -------
        dict
            A dictionary with the clock, positions, momenta, forces, and
            random-generator state.
        """

        return {
            "time": self.t,
            "x": self.x.copy(),
            "p": self.p.copy(),
            "F": self.F.copy(),
            "rng": self.rng.bit_generator.state,
        }

    def _restore(self, snapshot):
        """
        Restore the internal state from a snapshot.

        Parameters
        ----------
        snapshot : dict
            A snapshot produced by :meth:`_snapshot`.
        """

        self.t = snapshot["time"]
        self.x = snapshot["x"].copy()
        self.p = snapshot["p"].copy()
        self.F = snapshot["F"].copy()
        self.rng.bit_generator.state = snapshot["rng"]
