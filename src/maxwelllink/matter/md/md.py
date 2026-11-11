# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
User-facing standalone interface to the built-in classical MD engine.
"""

from dataclasses import dataclass, field

import numpy as np

from maxwelllink.units import FS_TO_AU, K_TO_AU

from ..helpers import field_rows, resolve_backend, to_host
from .md_engine import MDEngine


@dataclass
class MDResult:
    """
    The recorded trajectory of one :meth:`MD.run` call.

    Attributes
    ----------
    dt_au : float
        Integration time step in atomic units.
    record_every : int
        Steps between two records; the sample spacing is ``dt_au * record_every``.
    thermostat : str
        ``'nve'`` or ``'nvt'``.
    time_au : numpy.ndarray, shape (n_records,)
        Time after every recorded step, in atomic units.
    time_fs : numpy.ndarray, shape (n_records,)
        The same record times in femtoseconds.
    dipole_au : numpy.ndarray, shape (n_records, 3)
        Cell dipole moment in atomic units.
    amplitude_au : numpy.ndarray, shape (n_records, 3)
        Dipole velocity ``d(mu)/dt`` in atomic units.
    energy_au : numpy.ndarray, shape (n_records,)
        Total (kinetic + potential) energy in Hartree.
    potential_au : numpy.ndarray, shape (n_records,)
        Potential energy in Hartree.
    temperature_K : numpy.ndarray, shape (n_records,)
        Instantaneous temperature ``2 K / (3 N k_B)`` in Kelvin.
    terms_au : numpy.ndarray, shape (n_records, n_terms)
        The force field's own energy terms, columns ordered as ``reported_term_names``.
    reported_term_names : tuple of str
        Names of those energy terms (e.g. ``('stretch_au', 'bend_au')``).
    snapshots : dict or None
        When ``snapshot_every`` was set: ``time_au`` (m,), ``coords_bohr``
        (m, n_atoms, 3) and ``velocities_au`` (m, n_atoms, 3) arrays.
    details : dict
        Run metadata (force field, backend, target temperature, ...).
    """

    dt_au: float
    record_every: int
    thermostat: str
    time_au: np.ndarray
    time_fs: np.ndarray
    dipole_au: np.ndarray
    amplitude_au: np.ndarray
    energy_au: np.ndarray
    potential_au: np.ndarray
    temperature_K: np.ndarray
    terms_au: np.ndarray
    reported_term_names: tuple
    snapshots: object = None
    details: dict = field(default_factory=dict)

    def ir_spectrum(self, component="total", **kwargs):
        """
        Infrared spectrum of this run's dipole trajectory.

        Parameters
        ----------
        component : {'total', 'x', 'y', 'z'}, default: 'total'
            One Cartesian dipole component, or the sum of the three spectra
            (the isotropic result for a liquid).
        **kwargs
            Extra options of :func:`maxwelllink.tools.ir.ir_spectrum`, e.g.
            ``smooth_window_len`` or ``N`` (zero padding).

        Returns
        -------
        frequency_cm : numpy.ndarray
            Frequency axis in :math:`\\mathrm{cm}^{-1}`.
        intensity : numpy.ndarray
            IR intensity in arbitrary units.
        """

        from maxwelllink.tools.ir import ir_spectrum

        if self.time_au.shape[0] < 8:
            raise ValueError("need at least 8 recorded steps for a spectrum.")
        dtfs = float(self.time_au[1] - self.time_au[0]) / FS_TO_AU
        component = str(component).lower()
        columns = {"x": (0,), "y": (1,), "z": (2,), "total": (0, 1, 2)}
        if component not in columns:
            raise ValueError("component must be 'total', 'x', 'y' or 'z'.")
        intensity = None
        for column in columns[component]:
            signal = self.dipole_au[:, column]
            signal = signal - signal.mean()  # fluctuations only
            frequency_cm, spectrum = ir_spectrum(signal, dtfs, **kwargs)
            intensity = spectrum if intensity is None else intensity + spectrum
        return frequency_cm, intensity

    def save(self, path):
        """Save the recorded arrays (and snapshots, if any) to a ``.npz`` file."""

        payload = dict(
            dt_au=self.dt_au,
            record_every=self.record_every,
            thermostat=self.thermostat,
            time_au=self.time_au,
            dipole_au=self.dipole_au,
            amplitude_au=self.amplitude_au,
            energy_au=self.energy_au,
            potential_au=self.potential_au,
            temperature_K=self.temperature_K,
            terms_au=self.terms_au,
            reported_term_names=np.asarray(self.reported_term_names, dtype=str),
        )
        for key, value in self.details.items():
            payload.setdefault(key, value)
        if self.snapshots is not None:
            payload["snapshot_time_au"] = self.snapshots["time_au"]
            payload["snapshot_coords_bohr"] = self.snapshots["coords_bohr"]
            payload["snapshot_velocities_au"] = self.snapshots["velocities_au"]
        np.savez(path, **payload)


class MD:
    """
    A single standalone classical-MD simulation.

    Example
    -------
    >>> co2 = MD(ff="co2jcp2021", n_molecules=1, dt_fs=0.25)
    >>> co2.equilibrate(duration_ps=2.0, temperature_K=300.0)
    >>> result = co2.run(duration_ps=10.0, thermostat="nve")
    >>> frequency_cm, intensity = result.ir_spectrum()
    """

    def __init__(
        self,
        ff="qtip4pf",
        n_molecules=None,
        xyz=None,
        positions=None,
        box=None,
        replicate=None,
        rcut=None,
        ewald_wrcut=None,
        dt_fs: float = 0.5,
        temperature_K: float = 300.0,
        friction_fs: float = 100.0,
        init_velocities: bool = True,
        backend="cpu",
        force_evaluator: str = "auto",
        seed: int = 0,
        verbose: bool = False,
    ):
        """
        Initialize the MD Engine.

        Parameters
        ----------
        ff : str or DummyForceField, default: 'qtip4pf'
            A registered force-field name (``'qtip4pf'`` flexible water,
            ``'co2jcp2021'`` flexible CO2, ``'cs2compass'`` flexible CS2,
            ``'ch4compass'`` flexible CH4; list them all with
            :func:`maxwelllink.matter.available_force_fields`) or a
            ready-made user-defined force-field instance.
        n_molecules : int, optional
            Number of molecules of a default-built geometry, before any
            ``replicate``. ``None`` keeps each force field's own default (the
            bundled 32-water / 36-CO2 / 36-CS2 / 100-CH4 bulk boxes).
        xyz : str, optional
            XYZ file (Angstrom) with the starting geometry, atoms ordered as the
            force field expects.
        positions : array-like of float, shape (n_atoms, 3), optional
            Starting geometry in Bohr; overridden by ``xyz``.
        box : float or array-like of float, shape (3,), optional
            Periodic box lengths in Bohr; ``None`` treats the system as a cluster.
        replicate : int or array-like of int, shape (3,), optional
            Replicate the periodic cell. The molecules and the box lengths grow together, 
            so the density is unchanged.
        rcut, ewald_wrcut : float, optional
            Real-space cutoffs in Bohr of the periodic non-bonded and Ewald sums.
        dt_fs : float, default: 0.5
            Integration time step in femtoseconds.
        temperature_K : float, default: 300.0
            Default temperature: initial Maxwell-Boltzmann velocities, and the bath
            of :meth:`equilibrate` / NVT runs unless they override it.
        friction_fs : float, default: 100.0
            Default Langevin relaxation time in femtoseconds.
        init_velocities : bool, default: True
            Sample initial velocities at ``temperature_K``; ``False`` starts at rest.
        backend : {'cpu', 'gpu', None}, default: 'cpu'
            ``'cpu'`` runs the compiled (numba) kernels; ``'gpu'`` runs the CUDA
            kernels through CuPy; ``None`` picks the GPU automatically when one is
            reachable. The GPU integrator assigns one CUDA block per system, so a
            *single* cell usually runs fastest on the CPU, while the GPU backend shines
            in the many-cell batch drivers.
        force_evaluator : {'auto', 'numba', 'numpy'}, default: 'auto'
            CPU force evaluator; ``'numpy'`` forces the reference implementation.
        seed : int, default: 0
            Random seed (initial velocities and Langevin noise).
        verbose : bool, default: False
            Print a short note when the cell is initialized.
        """
        self.backend, self.xp = resolve_backend(backend)
        self.dt_fs = float(dt_fs)
        if self.dt_fs <= 0.0:
            raise ValueError("dt_fs must be positive.")
        self.dt_au = self.dt_fs * FS_TO_AU
        self.verbose = bool(verbose)

        # the shared MD engine, in its single-system (num=1) form
        self.engine = MDEngine(
            ff=ff,
            n_molecules=n_molecules,
            positions=positions,
            xyz=xyz,
            box=box,
            replicate=replicate,
            rcut=rcut,
            ewald_wrcut=ewald_wrcut,
            thermostat="nve",  # the thermostat is chosen per run()
            temperature_K=temperature_K,
            friction_fs=friction_fs,
            init_velocities=init_velocities,
            pre_nvt=False,  # thermalization is the explicit equilibrate()
            reset_dipole=True,
            seed=seed,
            force_evaluator=force_evaluator,
            num=1,
            xp=self.xp,
            label="matter.MD",
        )
        self.ff = self.engine.ff  # the force-field object of this cell
        self.n_atoms = self.engine.n_atoms

    # ------------------------------------------------------------------ state
    def get_positions(self):
        """Return the current atomic positions in Bohr, shape ``(n_atoms, 3)``."""

        engine = self.engine
        if engine.x is None:
            return engine.x0.copy()
        return to_host(engine.x)[0]

    def get_velocities(self):
        """Return the current atomic velocities in a.u., shape ``(n_atoms, 3)``."""

        engine = self.engine
        if engine.p is None:
            return np.zeros((engine.n_atoms, 3))
        return to_host(engine.p)[0] / engine._mass_host

    def _ensure_initialized(self):
        """Allocate the engine state on first use (velocities, initial forces)."""

        if self.engine.x is None:
            self.engine.initialize(self.dt_au, [0])
            if self.verbose:
                print(
                    f"[matter.MD] {self.engine.ff_name}: {self.engine.n_atoms} atoms, "
                    f"dt = {self.dt_fs} fs, backend = {self.backend} "
                    f"({self.engine.force_evaluator})."
                )

    # ------------------------------------------------------------------ dynamics
    def equilibrate(self, duration_ps=20.0, temperature_K=None, friction_fs=None):
        """
        Thermalize the cell with a Langevin (NVT) trajectory, in place.

        Nothing is recorded; call :meth:`run` afterwards for production data.

        Parameters
        ----------
        duration_ps : float, default: 20.0
            Equilibration length in picoseconds.
        temperature_K : float, optional
            Bath temperature; the constructor's ``temperature_K`` by default.
        friction_fs : float, optional
            Langevin relaxation time; the constructor's ``friction_fs`` by default.

        Returns
        -------
        MD
            The cell itself, so calls can be chained.
        """

        self._ensure_initialized()
        engine = self.engine
        friction = engine.friction_fs if friction_fs is None else float(friction_fs)
        n_steps = engine.equilibrate(
            float(duration_ps), friction, temperature_K=temperature_K
        )
        if self.verbose:
            print(f"[matter.MD] equilibrated for {n_steps} steps.")
        return self

    def run(
        self,
        duration_ps=None,
        n_steps=None,
        thermostat: str = "nve",
        temperature_K=None,
        friction_fs=None,
        field=None,
        record_every: int = 1,
        snapshot_every=None,
    ):
        """
        Propagate the cell and record its trajectory.

        Successive calls continue from the current state, so a typical session is
        ``equilibrate()`` followed by one or more ``run()`` calls.

        Parameters
        ----------
        duration_ps : float, optional
            Length of the run in picoseconds. Give this or ``n_steps``.
        n_steps : int, optional
            Number of time steps; overrides ``duration_ps``.
        thermostat : {'nve', 'nvt'}, default: 'nve'
            Plain velocity Verlet, or with a Langevin thermostat.
        temperature_K : float, optional
            NVT bath temperature; the constructor's value by default.
        friction_fs : float, optional
            NVT Langevin relaxation time; the constructor's value by default.
        field : optional
            External electric field in atomic units: ``None`` (field-free), a
            constant ``(3,)`` vector, an ``(n_steps, 3)`` table, or a callable
            ``field(t_au) -> (3,)`` evaluated at the start of every step.
        record_every : int, default: 1
            Keep every this-many steps in the returned arrays.
        snapshot_every : int, optional
            Also record the geometry and velocities every this-many steps.

        Returns
        -------
        MDResult
        """

        if (duration_ps is None) == (n_steps is None):
            raise ValueError("give exactly one of duration_ps or n_steps.")
        if n_steps is None:
            n_steps = int(round(float(duration_ps) * 1000.0 * FS_TO_AU / self.dt_au))
        n_steps = int(n_steps)
        if n_steps <= 0:
            raise ValueError("the run must be at least one step long.")
        record_every = max(1, int(record_every))
        if snapshot_every is not None:
            snapshot_every = int(snapshot_every) or None

        self._ensure_initialized()
        engine = self.engine
        engine.set_thermostat(
            thermostat, temperature_K=temperature_K, friction_fs=friction_fs
        )
        field_table = field_rows(field, n_steps, self.dt_au)

        n_records = n_steps // record_every
        time_au = np.zeros(n_records)
        dipole = np.zeros((n_records, 3))
        amplitude = np.zeros((n_records, 3))
        energy = np.zeros(n_records)
        potential = np.zeros(n_records)
        terms = np.zeros((n_records, engine.n_terms))
        snap_time, snap_coords, snap_velocity = [], [], []

        row = np.zeros((1, 3))
        record = 0
        for step in range(n_steps):
            if field_table is not None:
                row[0] = field_table(step)
            out = engine.step(row)
            if (step + 1) % record_every == 0:
                time_au[record] = engine.t
                dipole[record] = out.dipole_half_au[0]
                amplitude[record] = out.amplitude_au[0]
                energy[record] = out.energy_au[0]
                potential[record] = to_host(engine.potential)[0]
                terms[record] = to_host(engine._terms)[0]
                record += 1
            if snapshot_every is not None and (step + 1) % snapshot_every == 0:
                snap_time.append(engine.t)
                snap_coords.append(to_host(engine.x)[0])
                snap_velocity.append(to_host(engine.p)[0] / engine._mass_host)

        snapshots = None
        if snap_time:
            snapshots = dict(
                time_au=np.array(snap_time),
                coords_bohr=np.array(snap_coords),
                velocities_au=np.array(snap_velocity),
            )

        # instantaneous temperature over all 3 N degrees of freedom
        temperature = 2.0 * (energy - potential) / (3.0 * engine.n_atoms) / K_TO_AU
        return MDResult(
            dt_au=self.dt_au,
            record_every=record_every,
            thermostat=str(thermostat).lower(),
            time_au=time_au,
            time_fs=time_au / FS_TO_AU,
            dipole_au=dipole,
            amplitude_au=amplitude,
            energy_au=energy,
            potential_au=potential,
            temperature_K=temperature,
            terms_au=terms,
            reported_term_names=tuple(engine.ff.reported_term_names),
            snapshots=snapshots,
            details=dict(
                ff=engine.ff_name,
                backend=self.backend,
                force_evaluator=engine.force_evaluator,
                dt_fs=self.dt_fs,
                n_atoms=engine.n_atoms,
                target_temperature_K=engine.temperature_K,
            ),
        )

    def close(self):
        """Release the engine state (device buffers included)."""

        self.engine.close()
