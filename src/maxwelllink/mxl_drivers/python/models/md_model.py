# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Built-in classical molecular-dynamics (MD) driver for MaxwellLink.
"""

import os

import numpy as np

from maxwelllink.matter.helpers import resolve_backend
from maxwelllink.matter.md.md_engine import MDEngine

try:
    from .dummy_model import DummyModel
except (ImportError, ValueError):  # allow running as a stand-alone script
    from dummy_model import DummyModel


class MDModel(DummyModel):
    """
    Classical molecular-dynamics socket driver: the scalar (``num=1``) front end
    of the shared :class:`~maxwelllink.matter.md.md_engine.MDEngine`.
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
        backend: str = "cpu",
        *,
        property_list="base",
        traj_content="positions",
    ):
        """
        Initialize the classical MD driver.

        All keywords are forwarded unchanged to the shared
        :class:`~maxwelllink.matter.md.md_engine.MDEngine`; they are documented
        in full here for user convenience.

        Parameters
        ----------
        ff : str or DummyForceField, default: 'qtip4pf'
            Name of a registered force field (``'qtip4pf'`` for flexible water,
            ``'co2jcp2021'`` for flexible CO2, ``'cs2compass'`` for flexible CS2,
            ``'ch4compass'`` for flexible CH4), or an already constructed
            :class:`~maxwelllink.matter.md.force_fields.DummyForceField`
            instance.
        n_molecules : int, optional
            Number of molecules to build when ``positions`` is not given (before any
            ``replicate``). If ``None`` (the default) each force field applies its own
            default count (32 for water and 36 for CO2, which load the bundled
            equilibrated bulk boxes).
        positions : array-like of float, shape (n_atoms, 3), optional
            Initial atomic positions in Bohr. If ``None`` the force field builds a
            default geometry from ``n_molecules`` (and ``box``).
        xyz : str, optional
            Path to an XYZ file (Angstrom) to take the starting geometry from, with the
            atoms in the order the force field expects (``[C, O, O, ...]`` for CO2,
            ``[O, H, H, ...]`` for water). Overrides ``positions``; but this also gives
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
            before any EM-coupled dynamics. The random seed is offset by
            ``molecule_id`` so that different molecules are decorrelated even at the
            same ``seed``.
        pre_nvt_duration_ps : float, default: 20.0
            Duration of the pre-NVT equilibration in picoseconds. It uses a Langevin
            thermostat with a 100 fs relaxation time at ``temperature_K``, regardless
            of the production ``thermostat``.
        reset_dipole : bool, default: True
            Whether to report the dipole moment relative to its value at time zero.
        seed : int, default: 0
            Seed for the random-number generator in EM-coupled dynamics; this
            molecule draws from ``seed + molecule_id``.
        force_evaluator : {'auto', 'numba', 'numpy'}, default: 'auto'
            Which force evaluator to use. ``'numba'`` uses the compiled loop-form
            kernels, which are typically several times faster than the array-at-a-time
            NumPy reference and allocate no ``(na, na, 3)`` temporaries; ``'numpy'``
            forces the reference implementation. ``'auto'`` picks ``'numba'`` whenever
            the force field provides a compiled-kernel description and silently falls
            back to ``'numpy'`` when it does not.
        property_filename : str, optional
            Turns on a run-time record of per-molecule properties written to this file every
            ``record_every_steps`` production steps: HDF5, streamed while the run goes,
            unless the name ends in ``.npz`` or ``h5py`` is missing. 
        traj_filename : str, optional
            Runtime extended-XYZ output with one frame per independent system
            every ``record_every_steps`` production steps. Positions are in
            Angstrom, atomic velocities in Bohr per atomic time, and body-frame
            angular velocities in inverse atomic time. Angular output automatically
            includes the corresponding principal moments of inertia in electron-mass
            times Bohr-squared units. Each molecule's six angular-output values
            are repeated for all its atoms.
        record_every_steps : int, default: 10
            Record every this many production steps, for both files.
        record_max_steps : int, optional
            Stop recording after this many records; ``None`` for no cap.
        checkpoint : bool, default: False
            Whether to enable checkpointing (one
            ``md_checkpoint_id_<molecule ID>.npz`` file per molecule).
        restart : bool, default: False
            Whether to restart from a checkpoint if available.
        verbose : bool, default: False
            Whether to print verbose output.
        backend : {'cpu', 'gpu', None}, default: 'cpu'
            ``'cpu'`` runs the compiled (numba) kernels; ``'gpu'`` runs the
            CUDA kernels through CuPy; ``None`` (auto) picks the GPU when CuPy
            reaches a CUDA device. The GPU integrator assigns one CUDA block
            per system, so a single socket driver usually runs fastest on the
            CPU.
        property_list : str or list or tuple, default: "base"
            Select "base", "all", or semicolon-separated property names.
            Lists and tuples are also accepted. Only used when
            ``property_filename`` is set.
        traj_content : str, default: "positions"
            Content to record: "positions", "velocities", "angular_velocity_body",
            or "all". Angular output contains three angular velocities followed
            by three principal moments of inertia. "all" includes positions,
            atomic velocities, and these six angular-output columns. Velocities
            and angular data are sampled before the final Langevin O half-step.
        """

        super().__init__(verbose, checkpoint, restart)

        self.backend, xp = resolve_backend(backend)
        self.engine = MDEngine(
            ff=ff,
            n_molecules=n_molecules,
            positions=positions,
            xyz=xyz,
            batch_xyz=batch_xyz,
            box=box,
            replicate=replicate,
            rcut=rcut,
            ewald_wrcut=ewald_wrcut,
            thermostat=thermostat,
            temperature_K=temperature_K,
            friction_fs=friction_fs,
            init_velocities=init_velocities,
            pre_nvt=pre_nvt,
            pre_nvt_duration_ps=pre_nvt_duration_ps,
            reset_dipole=reset_dipole,
            seed=seed,
            force_evaluator=force_evaluator,
            property_filename=property_filename,
            property_list=property_list,
            traj_filename=traj_filename,
            traj_content=traj_content,
            record_every_steps=record_every_steps,
            record_max_steps=record_max_steps,
            checkpoint=checkpoint,
            restart=restart,
            verbose=verbose,
            num=1,
            xp=xp,
            label="MDModel",
        )

        # The engine owns every run setting; only the names user scripts read
        # stay mirrored here. Everything else is available as
        # ``model.engine.<name>``.
        engine = self.engine
        self.ff = engine.ff
        self.n_atoms = engine.n_atoms

        # State before initialize(): the template geometry (editing it in place
        # changes the starting geometry), momenta and forces at rest. After
        # initialize() these are rebound to the live engine state by
        # _bind_engine_state().
        self.x = engine.x0  # positions (n_atoms, 3)
        self.p = np.zeros((self.n_atoms, 3))  # momenta
        self.F = np.zeros((self.n_atoms, 3))  # forces (updated each step)
        self.mu_initial = None  # dipole baseline, set in initialize()

        # data returned to MaxwellLink, refreshed by every propagate()
        self._amp = np.zeros(3)  # d(mu)/dt
        self.dipole_vec = np.zeros(3)  # mu half a step after force time (mu_half)
        self.dipole_force = np.zeros(3)  # mu at force-evaluation position (mu_force)
        self.energy = 0.0  # kinetic + potential
        self.potential = 0.0

    def _bind_engine_state(self):
        """
        Point the public state attributes at the live engine arrays.

        ``self.x``/``self.p``/``self.F`` become views of row 0 of the engine's
        ``(1, na, 3)`` batch arrays. 
        """

        engine = self.engine
        self.x = engine.x[0]
        self.p = engine.p[0]
        self.F = engine.F[0]
        self.mu_initial = engine.mu_initial[0]
        self.potential = float(engine.potential[0])
        self.t = engine.t

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

        restart_state = None
        if self.restart and self.checkpoint:
            restart_state = self._read_checkpoint_state()
        self.engine.initialize(self.dt, [self.molecule_id], restart_state=restart_state)
        self._bind_engine_state()

    def close(self):
        """Close the output files, if any are being written."""

        self.engine.close_outputs()

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
        out = self.engine.step(efield.reshape(1, 3))

        # Two dipole snapshots the SingleModeSimulation EM solver needs (as in
        # SHOModel): mu at the force-evaluation position (mu_force -> mux_m_au) and mu
        # half a step later (mu_half -> mux_au). The solver drifts the dipole to the
        # next force time via 2 * mu_half - mu_force.
        self._amp = out.amplitude_au[0]
        self.dipole_vec = out.dipole_half_au[0]
        self.dipole_force = out.dipole_force_au[0]
        self.energy = float(out.energy_au[0])
        self.potential = float(self.engine.potential[0])
        self.t = self.engine.t

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
        :meth:`initialize`); the arrays keep the historical single-system shapes
        and are copied to the host first, so the GPU backend checkpoints too.
        """

        state = self.engine.state_dict()
        np.savez(
            self.checkpoint_filename,
            time=state["time"],
            x=state["x"][0],
            p=state["p"][0],
            F=state["F"][0],
            mu_initial=state["mu_initial"][0],
        )

    def _read_checkpoint_state(self):
        """
        Read this molecule's checkpoint file into an engine ``restart_state``.

        Returns ``None`` (printing a note) when no checkpoint file is found for
        this molecule ID, so the run starts fresh.
        """

        if not os.path.exists(self.checkpoint_filename):
            print(
                "[checkpoint] No checkpoint file found for molecule ID %d, "
                "starting fresh." % self.molecule_id
            )
            return None
        data = np.load(self.checkpoint_filename)
        state = {
            "time": float(data["time"]),
            "x": np.asarray(data["x"])[None],
            "p": np.asarray(data["p"])[None],
            "F": np.asarray(data["F"])[None],
        }
        if "mu_initial" in data:  # keep reporting against the original baseline
            state["mu_initial"] = np.asarray(data["mu_initial"])[None]
        return state

    def _reset_from_checkpoint(self):
        """
        Restore the internal MD state from a checkpoint.

        Notes
        -----
        Starts fresh (printing a note) when no checkpoint file is found for this
        molecule ID.
        """

        state = self._read_checkpoint_state()
        if state is not None:
            self.engine.load_state_dict(state)  # in place: self.x/p/F stay views
            self.t = self.engine.t

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

        return self.engine.snapshot()

    def _restore(self, snapshot):
        """
        Restore the internal state from a snapshot.

        Parameters
        ----------
        snapshot : dict
            A snapshot produced by :meth:`_snapshot`.
        """

        self.engine.restore(snapshot)  # in place: self.x/p/F stay views
        self.t = self.engine.t
