# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Real-time TD-DFTB driver for MaxwellLink, with or without moving nuclei.

``RTDFTBModel`` runs the same physics as an external DFTB+ ``ElectronDynamics`` block
driven over the socket, but in process: the .skf files are read directly, the SCC ground
state is converged here, and one EM step drives one ``doTdStep``. Setting
``ehrenfest=True`` lets the nuclei move, which is RT-TDDFTB-Ehrenfest dynamics.

This built-in feature avoids calling external DFTB+ and, like ``MDModel``, keeps the
per-driver memory small enough that many drivers can share one cavity.
"""

import os

import numpy as np

from maxwelllink.tools.recorders import (
    PropertyRecorder,
    XYZTrajectoryWriter,
    output_filename,
)
from maxwelllink.tools.slko import resolve as resolve_slko
from maxwelllink.tools.xyz_helper import read_xyz, read_xyz_frames
from maxwelllink.units import FS_TO_AU, K_TO_AU

try:
    from ..dummy_model import DummyModel
    from .dftb_params import AA_TO_BOHR, DFTBSystem, load_sk_set
    from .dynamics import PROPAGATORS, RTDynamics, bomd_equilibrate
    from .h0_overlap import build_h0_overlap
    from .scc import limit_blas_threads, scf
except (ImportError, ValueError):  # allow running as a stand-alone script
    from dummy_model import DummyModel
    from dftb_params import AA_TO_BOHR, DFTBSystem, load_sk_set
    from dynamics import PROPAGATORS, RTDynamics, bomd_equilibrate
    from h0_overlap import build_h0_overlap
    from scc import limit_blas_threads, scf

# kick and field polarisations, as the DFTB+ input names them
_DIRECTIONS = {"x": 0, "y": 1, "z": 2}

# Time step of the Born-Oppenheimer pre-equilibration in fs: a classical MD step, not
# the electronic one, which is what makes the thermalization affordable.
_PRE_NVT_DT_FS = 0.5

#: The per-molecule properties the driver records: what it reports to MaxwellLink, plus
#: the nuclear temperature when the nuclei move.
_RECORD_NAMES = (
    "temperature_K",
    "energy_au",
    "energy_kin_au",
    "mux_au",
    "muy_au",
    "muz_au",
)


class RTDFTBModel(DummyModel):
    """
    Real-time TD-DFTB driver, optionally with Ehrenfest nuclear dynamics.

    Notes
    -----
    The reply follows the external DFTB+ driver's convention rather than the classical
    MD one (``mxlrtdynamics.F90:96-102``): the reported dipole and energy are midpoint
    averages over the step, the amplitude is a finite difference across it, and the
    half-step and force-time dipoles are the same value.
    """

    def __init__(
        self,
        sk_path="3ob",
        elements=None,
        positions=None,
        xyz=None,
        units="angstrom",
        max_angular_momentum=None,
        charge=0.0,
        scc_tolerance=1.0e-10,
        ehrenfest: bool = False,
        propagator: str = "leapfrog",
        hybrid_precision: bool = False,
        dt_rtdftb_au=None,
        delta_kick_au: float = 0.0,
        kick_direction: str = "z",
        velocities=None,
        init_velocities: bool = False,
        temperature_K: float = 300.0,
        batch_xyz=None,
        pre_nvt: bool = False,
        pre_nvt_duration_ps: float = 1.0,
        friction_fs: float = 100.0,
        reset_dipole: bool = True,
        seed: int = 0,
        property_filename=None,
        traj_filename=None,
        record_every_steps: int = 10,
        record_max_steps=None,
        checkpoint: bool = False,
        restart: bool = False,
        verbose: bool = False,
    ):
        """
        Initialize the necessary parameters for the real-time TD-DFTB driver.

        Parameters
        ----------
        sk_path : str, default: '3ob'
            Either a directory holding the ``<A>-<B>.skf`` files of a Slater-Koster
            parameter set, or the name of one of the sets published at
            ``github.com/dftbparams`` -- ``'3ob'``, ``'mio'``, ``'auorg'``, ``'znorg'``
            and twenty more, listed by
            :func:`maxwelllink.tools.slko.available_sets`. A name that is not already
            installed is downloaded once, beside the installed package, and reused by
            every later run.
        elements : sequence of str, optional
            Element symbol of every atom, in input order. Required unless ``xyz`` is
            given.
        positions : array-like of float, shape (n_atom, 3), optional
            Atomic positions in ``units``.
        xyz : str, optional
            Path to an XYZ file to take ``elements`` and ``positions`` from.
        units : {'angstrom', 'bohr'}, default: 'angstrom'
            Units of ``positions``. XYZ files are always Angstrom.
        max_angular_momentum : dict, optional
            Maximum angular momentum per element, as ``'s'``, ``'p'`` or ``'d'``.
            Missing entries fall back to the parameter sets' usual choice.
        charge : float, default: 0.0
            Net charge of the system in units of ``+e``.
        scc_tolerance : float, default: 1e-10
            Convergence threshold of the ground-state SCC loop, on the shell charges.
        ehrenfest : bool, default: False
            Whether the nuclei move. ``False`` is plain RT-TDDFTB at a frozen geometry;
            ``True`` is RT-TDDFTB-Ehrenfest.
        propagator : {'leapfrog', 'cayley', 'cayley-midpoint'}, default: 'leapfrog'
            Electronic propagator. The default is what DFTB+ itself runs, so it is the
            one that reproduces an external DFTB+ driver step for step.
        hybrid_precision : bool, default: False
            Opt-in hybrid FP32/FP64 arithmetic of the GPU batch driver: the dense
            linear algebra of every step, including the leapfrog products, ``S^-1`` and the
            energy-weighted density, runs in single precision through cuBLAS/
            cuSOLVER, while the density matrix, every accumulator and all assembly
            kernels stay in double precision. Roughly halves the step time of large
            batches on consumer GPUs (whose FP64 rate is 1/64 of FP32) and keeps the
            conservation laws at FP64 quality.
        dt_rtdftb_au : float, optional
            Electronic time step in atomic units. ``None`` (the default) takes one RT
            step per EM step, as the external DFTB+ driver does. A smaller value makes
            the driver sub-step internally; it is rounded so that an integer number of
            sub-steps fills one EM step exactly.
        delta_kick_au : float, default: 0.0
            Strength of a delta kick applied once during :meth:`initialize`, in atomic
            units. Zero (the default) starts from the unperturbed ground state, which is
            what a cavity simulation wants.
        kick_direction : {'x', 'y', 'z'}, default: 'z'
            Polarisation of that kick.
        velocities : array-like of float, shape (n_atom, 3), optional
            Initial nuclear velocities in atomic units. Only used when ``ehrenfest``.
        init_velocities : bool, default: False
            Whether to sample the initial velocities from a Maxwell-Boltzmann
            distribution at ``temperature_K`` instead. The random seed is offset by
            ``molecule_id`` so that different molecules are decorrelated.
        temperature_K : float, default: 300.0
            Temperature of those initial velocities and of the pre-NVT thermostat, in
            Kelvin.
        batch_xyz : str, optional
            Path to a multi-frame XYZ file (Angstrom) that gives every molecule its own
            starting geometry: molecule ``m`` starts from frame ``m``, so a batch of
            drivers, or several batches, read consecutive frames and the same molecule
            ID always gets the same frame. Every frame must list the same atoms; there
            must be more frames than the largest molecule ID. Frame 0 doubles as the
            template geometry when neither ``xyz`` nor ``positions`` is given.
        pre_nvt : bool, default: False
            Whether to thermalize the geometry (and, with ``ehrenfest``, the velocities)
            before the real-time dynamics with Langevin Born-Oppenheimer MD at
            ``temperature_K`` -- SCC ground state and forces at every step, a
            classical time step of 0.5 fs -- for ``pre_nvt_duration_ps``. Every
            molecule follows its own random stream (``seed + molecule_id``), so a batch
            of drivers starts from as many different geometries. Skipped on a restart
            from a checkpoint.
        pre_nvt_duration_ps : float, default: 1.0
            Length of that pre-equilibration in picoseconds.
        friction_fs : float, default: 100.0
            Langevin relaxation time of the pre-equilibration in femtoseconds.
        reset_dipole : bool, default: True
            Whether to report the dipole moment relative to its value at time zero.
        seed : int, default: 0
            Seed for the random-number generator.
        property_filename : str, optional
            Turns on a run-time record of per-molecule properties -- the nuclear
            temperature (zero with frozen nuclei), the total and the nuclear kinetic
            energy and the dipole, i.e. what the driver reports to MaxwellLink --
            written to this file every ``record_every_steps`` steps: HDF5, streamed
            while the run goes, unless the name ends in ``.npz`` or ``h5py`` is
            missing. ``_id_<molecule ID>`` is inserted before the extension, so each
            driver process writes its own file; a batch writes one file with one column
            per molecule. A restart appends.
        traj_filename : str, optional
            Turns on a geometry trajectory in extended XYZ (Angstrom) with the Mulliken
            charge deviation ``dq`` of every atom as an extra column, one frame per
            molecule every ``record_every_steps`` steps, with the same ``_id_`` suffix;
            see :class:`maxwelllink.tools.XYZTrajectoryWriter` for the frame order of a
            batch and :func:`maxwelllink.tools.read_xyz_trajectory` to read it back.
        record_every_steps : int, default: 10
            Record every this many steps, for both files.
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

        # every molecule's own starting geometry, picked by molecule ID in initialize()
        self.batch_frames = None
        if batch_xyz is not None:
            frame_elements, frames = read_xyz_frames(batch_xyz)
            self.batch_frames = frames * AA_TO_BOHR
            if xyz is None and (elements is None or positions is None):
                elements, positions, units = frame_elements, frames[0], "angstrom"
        if xyz is not None:
            elements, positions = read_xyz(xyz)
            units = "angstrom"
        if elements is None or positions is None:
            raise ValueError(
                "provide xyz=..., batch_xyz=..., or both elements and positions."
            )
        if self.batch_frames is not None and list(elements) != list(frame_elements):
            raise ValueError("batch_xyz lists other atoms than elements/xyz.")
        if propagator not in PROPAGATORS:
            raise ValueError("propagator must be one of %s" % (PROPAGATORS,))
        if str(kick_direction).lower() not in _DIRECTIONS:
            raise ValueError("kick_direction must be 'x', 'y' or 'z'.")

        self.elements = [str(symbol) for symbol in elements]
        self.positions = np.asarray(positions, dtype=float).reshape(-1, 3)
        if str(units).lower() == "angstrom":
            self.positions = self.positions * AA_TO_BOHR
        elif str(units).lower() != "bohr":
            raise ValueError("units must be 'angstrom' or 'bohr'.")
        # a directory is taken as-is; a set name is downloaded once and cached
        self.sk_path = resolve_slko(sk_path)
        self.max_angular_momentum = max_angular_momentum
        self.charge = float(charge)
        self.scc_tolerance = float(scc_tolerance)

        self.ehrenfest = bool(ehrenfest)
        self.propagator = propagator
        self.hybrid_precision = bool(hybrid_precision)
        self.dt_rtdftb_au = None if dt_rtdftb_au is None else float(dt_rtdftb_au)
        self.delta_kick_au = float(delta_kick_au)
        self.kick_direction = _DIRECTIONS[str(kick_direction).lower()]
        self.velocities = None if velocities is None else np.asarray(velocities, float)
        self.init_velocities = bool(init_velocities)
        self.temperature_K = float(temperature_K)
        self.kT = K_TO_AU * self.temperature_K
        self.pre_nvt = bool(pre_nvt)
        self.pre_nvt_duration_ps = float(pre_nvt_duration_ps)
        self.friction_fs = float(friction_fs)
        if self.pre_nvt and (
            self.pre_nvt_duration_ps <= 0.0 or self.friction_fs <= 0.0
        ):
            raise ValueError("pre_nvt_duration_ps and friction_fs must be positive.")
        self.reset_dipole = bool(reset_dipole)
        self.seed = int(seed)
        self._rng = None  # this molecule's random stream, made in initialize()

        # run-time output, opened in initialize() once the molecule ID and the time
        # step are known
        self.property_filename = (
            None if property_filename is None else str(property_filename)
        )
        self.traj_filename = None if traj_filename is None else str(traj_filename)
        self.record_every_steps = int(record_every_steps)
        self.record_max_steps = record_max_steps
        self.record_names = _RECORD_NAMES
        self._recorder = None
        self._trajectory = None
        self._step_index = 0  # steps taken; not part of the snapshot

        # built in initialize(), which is where the SCC ground state is converged
        self.sk_set = None
        self.system = None
        self.ground = None
        self.dynamics = None
        self.n_substeps = 1

        # data returned to MaxwellLink
        self.mu_initial = None  # dipole baseline, set in initialize()
        self._amp = np.zeros(3)  # d(mu)/dt across the step
        self.dipole_vec = np.zeros(3)  # midpoint-averaged mu (mu_half)
        self.dipole_force = np.zeros(3)  # the same value again (mu_force)
        self.energy = 0.0  # midpoint-averaged total energy
        self.energy_kin = 0.0  # nuclear kinetic energy

    # ----------------------- heavy-load initialization ------------------------------
    def initialize(self, dt_new, molecule_id):
        """
        Read the parameter set, converge the SCC ground state and take the bootstrap.

        This is called during the INIT stage of the socket communication, once the
        molecule ID has been assigned. It mirrors ``initializeDynamics``: the ground
        state is converged, the optional delta kick is applied, and one Euler step of
        the electronic time step carries the density into the first leapfrog interval,
        all before any field is received.

        Parameters
        ----------
        dt_new : float
            The new time step in atomic units (a.u.).
        molecule_id : int
            The ID of the molecule assigned by SocketHub.
        """

        self.dt = float(dt_new)
        self.molecule_id = int(molecule_id)
        self.checkpoint_filename = "rtdftb_checkpoint_id_%d.npz" % self.molecule_id
        # one random stream per molecule, for the pre-NVT noise and the velocity draw
        self._rng = np.random.default_rng(self.seed + self.molecule_id)
        # the SCC loop's LAPACK calls are small; a threaded BLAS only slows them down
        limit_blas_threads(1)

        if self.batch_frames is not None:
            if self.molecule_id >= len(self.batch_frames):
                raise ValueError(
                    "[molecule ID %d] batch_xyz holds %d frames, so it has no frame "
                    "for this molecule." % (self.molecule_id, len(self.batch_frames))
                )
            self.positions = self.batch_frames[self.molecule_id].copy()

        species = sorted(set(self.elements))
        self.sk_set = load_sk_set(self.sk_path, species, self.max_angular_momentum)
        self.system = DFTBSystem(
            self.elements, self.positions, self.sk_set, units="bohr"
        )

        # thermalize the geometry on the ground-state surface before the real-time run;
        # the velocities it ends with are the ones an Ehrenfest run continues from
        equilibrated = None
        if self.pre_nvt and not (self.restart and self.checkpoint):
            dt_md = _PRE_NVT_DT_FS * FS_TO_AU
            equilibrated = bomd_equilibrate(
                self.system,
                int(round(self.pre_nvt_duration_ps * 1000.0 * FS_TO_AU / dt_md)),
                dt_md,
                self.kT,
                self.friction_fs * FS_TO_AU,
                self._rng,
                charge=self.charge,
                tolerance=self.scc_tolerance,
            )
            self.positions = self.system.coords.copy()

        h0, overlap = build_h0_overlap(self.system)
        self.ground = scf(
            self.system,
            h0,
            overlap,
            tolerance=self.scc_tolerance,
            charge=self.charge,
            verbose=self.verbose,
        )
        if not self.ground.converged:
            raise RuntimeError(
                "[molecule ID %d] the SCC ground state did not converge."
                % self.molecule_id
            )

        # one RT step per EM step by default, as the external DFTB+ driver does
        self.n_substeps = 1
        if self.dt_rtdftb_au is not None:
            self.n_substeps = max(1, int(round(self.dt / self.dt_rtdftb_au)))
        dt_sub = self.dt / self.n_substeps
        if self.verbose and self.n_substeps > 1:
            print(
                "[molecule ID %d] sub-stepping %d x %.4f a.u. per EM step of %.4f a.u."
                % (self.molecule_id, self.n_substeps, dt_sub, self.dt)
            )

        self.dynamics = RTDynamics(
            self.system,
            self.ground,
            dt_sub,
            propagator=self.propagator,
            ehrenfest=self.ehrenfest,
            velocities=self._initial_velocities(equilibrated),
        )
        self.dynamics.start(None, self.delta_kick_au, self.kick_direction)

        if self.restart and self.checkpoint:
            self._reset_from_checkpoint()

        # Baseline subtracted from the reported dipole, as the external DFTB+ driver's
        # resetDipole does (mxlrtdynamics.F90:43-47): captured once after the whole
        # initialization, so the driver reports how its dipole changes rather than the
        # permanent dipole frozen into the starting geometry.
        if self.mu_initial is None:
            self.mu_initial = (
                self.dynamics.dipole_end.copy() if self.reset_dipole else np.zeros(3)
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
            print(
                f"[RTDFTBModel] Recording {self.record_names} to {self._recorder.path}"
            )
        if self.traj_filename is not None:
            self._trajectory = XYZTrajectoryWriter(
                output_filename(self.traj_filename, self.molecule_id),
                self.elements,
                [self.molecule_id],
                self.dt,
                per_atom=("dq",),
                **output,
            )
            print(f"[RTDFTBModel] Writing the trajectory to {self._trajectory.path}")

    def _initial_velocities(self, equilibrated=None):
        """Nuclear velocities to start from: thermalized, given, sampled, or at rest."""

        if not self.ehrenfest:
            return self.velocities
        if equilibrated is not None:
            return equilibrated
        if not self.init_velocities:
            return self.velocities
        mass = np.array(
            [self.sk_set.mass[sp] for sp in self.system.atom_species], dtype=float
        )
        velocity = np.sqrt(self.kT / mass)[:, None] * self._rng.standard_normal(
            (self.system.n_atom, 3)
        )
        velocity -= (mass[:, None] * velocity).sum(axis=0) / mass.sum()
        return velocity

    # ---------------------------- one EM (FDTD) step --------------------------------
    def propagate(self, effective_efield_vec):
        """
        Advance the density (and the nuclei) by one EM step under the field.

        Parameters
        ----------
        effective_efield_vec : array-like of float, shape (3,)
            Effective electric field vector in the form ``[E_x, E_y, E_z]`` in atomic
            units.
        """

        field = np.asarray(effective_efield_vec, dtype=float)

        # the first sub-step carries the start of the EM step; the last one its end
        self.dynamics.step(field)
        dipole_start = self.dynamics.dipole_start - self.mu_initial
        energy_start = self.dynamics.energy_start
        for _ in range(self.n_substeps - 1):
            self.dynamics.step(field)
        dipole_end = self.dynamics.dipole_end - self.mu_initial
        energy_end = self.dynamics.energy_end

        # The external DFTB+ driver reports midpoint averages and a finite-difference
        # amplitude, and passes the same dipole twice, so mu_half == mu_force here and
        # the EM solver's dipole drift 2 * mu_half - mu_force is a no-op.
        self.dipole_vec = 0.5 * (dipole_start + dipole_end)
        self.dipole_force = self.dipole_vec
        self._amp = (dipole_end - dipole_start) / self.dt
        self.energy = 0.5 * (energy_start + energy_end)
        self.energy_kin = self.dynamics.energy_kinetic
        self.t += self.dt

        # the run-time record: T = 2 K / (3 N k_B) of the nuclei (zero when frozen), the
        # energies and the dipole reported to MaxwellLink; the geometry with the
        # Mulliken charge deviation of every atom
        self._step_index += 1
        if self._recorder is not None:
            temperature = 2.0 * self.energy_kin / (3.0 * self.system.n_atom) / K_TO_AU
            self._recorder.record(
                self._step_index,
                self.t,
                np.concatenate(
                    ([temperature, self.energy, self.energy_kin], self.dipole_vec)
                ),
            )
        if self._trajectory is not None:
            self._trajectory.write(
                self._step_index,
                self.t,
                self.system.coords[None],
                {"dq": self.dynamics.state.dq_atom[None]},
            )

        if self.verbose:
            print(
                f"[molecule ID {self.molecule_id}] t={self.t:.3f} a.u. "
                f"E_tot={self.energy:.8f} a.u. mu={self.dipole_vec}"
            )

    def close(self):
        """Close the output files, if any are being written."""

        for name in ("_recorder", "_trajectory"):
            writer = getattr(self, name)
            if writer is not None:
                writer.close()
                setattr(self, name, None)

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
            A dictionary with ``time_au``, ``energy_au``, ``energy_kin_au`` and the
            dipole components ``mux_au``/``muy_au``/``muz_au`` and their ``_m_au``
            counterparts, which carry the same value for this driver. These are exactly
            the keys the external DFTB+ driver sends (``mxlcommon.F90:120-133``).
        """

        data = {}
        data["time_au"] = self.t
        data["energy_au"] = self.energy
        data["energy_kin_au"] = self.energy_kin
        data["mux_au"] = self.dipole_vec[0]
        data["muy_au"] = self.dipole_vec[1]
        data["muz_au"] = self.dipole_vec[2]
        data["mux_m_au"] = self.dipole_force[0]
        data["muy_m_au"] = self.dipole_force[1]
        data["muz_m_au"] = self.dipole_force[2]
        return data

    def _dump_to_checkpoint(self):
        """
        Dump the internal RT-TDDFTB state to a checkpoint.

        Notes
        -----
        ``self.checkpoint_filename`` includes the molecule ID (set in
        :meth:`initialize`).
        """

        np.savez(self.checkpoint_filename, mu_initial=self.mu_initial, **self._state())

    def _reset_from_checkpoint(self):
        """
        Restore the internal RT-TDDFTB state from a checkpoint.

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
        self._restore({key: data[key] for key in data.files})
        if "mu_initial" in data.files:  # keep the original baseline
            self.mu_initial = data["mu_initial"]

    # ------------------------- stage / commit protocol ------------------------------
    def _state(self):
        """The propagating state: the two density buffers plus the nuclear integrator."""

        dynamics = self.dynamics
        return {
            "time": self.t,
            "rho": dynamics.state.rho,
            "rho_old": dynamics.state.rho_old,
            "coords": self.system.coords,
            "coords_next": dynamics.coords_next,
            "velocity": dynamics.velocity,
            "half_velocity": dynamics.half_velocity,
            "accel": dynamics.accel,
            "force_end": dynamics.force_end,
        }

    def _snapshot(self):
        """
        Return a deep-copied snapshot of the propagating state.

        Used by the stage-commit protocol so a proposed step can be previewed and then
        either committed or discarded.

        Returns
        -------
        dict
            The clock, both density-matrix buffers, and the nuclear integrator state.
        """

        return {
            key: value if np.isscalar(value) else np.array(value, copy=True)
            for key, value in self._state().items()
        }

    def _restore(self, snapshot):
        """
        Restore the internal state from a snapshot.

        Parameters
        ----------
        snapshot : dict
            A snapshot produced by :meth:`_snapshot`.
        """

        dynamics = self.dynamics
        self.t = float(snapshot["time"])
        dynamics.time = self.t
        dynamics.state.rho[:] = snapshot["rho"]
        dynamics.state.rho_old[:] = snapshot["rho_old"]
        self.system.coords[:, :] = snapshot["coords"]
        dynamics.coords_next[:] = snapshot["coords_next"]
        dynamics.velocity = np.array(snapshot["velocity"], copy=True)
        dynamics.half_velocity[:] = snapshot["half_velocity"]
        dynamics.accel = np.array(snapshot["accel"], copy=True)
        dynamics.force_end = np.array(snapshot["force_end"], copy=True)
        dynamics.state.refresh_geometry()
        dynamics.state.update_charges()
        dynamics.state.update_hamiltonian(dynamics.state.field)
