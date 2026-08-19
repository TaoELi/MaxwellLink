# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
The RT-TDDFTB time stepper, with the nuclei frozen or moving, and the batch runners.

:class:`RTDynamics` is one DFTB+ ``doTdStep`` held as an object, so the same stepper
serves a whole trajectory (:func:`run_kick`, :func:`run_ehrenfest`) and one socket step
of :class:`~.rtdftb_model.RTDFTBModel`. Ehrenfest dynamics is the same step with the
nuclear block switched on.

The order inside a step follows DFTB+ exactly, because the reference trajectories carry
it: move the nuclei, rebuild ``H`` with this step's field, read the observables at
``t``, propagate the density, adopt the new geometry and rebuild the matrices there,
then recompute the charges, ``H`` and the Ehrenfest force at ``t + dt``.
"""

import numpy as np

try:  # inside the package
    from .ehrenfest import (
        build_coupling,
        ehrenfest_force,
        kinetic_energy,
        overlap_time_derivative,
        velocity_verlet_next,
    )
    from .forces import total_force
    from .h0_overlap import build_h0_overlap
    from .rt import RTState
    from .scc import scf
except (ImportError, ValueError):  # allow running as a stand-alone script
    from ehrenfest import (
        build_coupling,
        ehrenfest_force,
        kinetic_energy,
        overlap_time_derivative,
        velocity_verlet_next,
    )
    from forces import total_force
    from h0_overlap import build_h0_overlap
    from rt import RTState
    from scc import scf

PROPAGATORS = ("leapfrog", "cayley", "cayley-midpoint")


class RTDynamics:
    """
    One system stepped in real time, with the nuclei frozen or moving.

    Parameters
    ----------
    system : dftb_params.DFTBSystem
        Geometry and basis layout; ``system.coords`` is updated in place when the nuclei
        move, so this object owns the current geometry.
    ground : scc.SCCResult
        Converged ground state at the initial geometry.
    dt : float
        Time step in atomic units, shared by the electrons and the nuclei.
    propagator : {"leapfrog", "cayley", "cayley-midpoint"}
        ``"leapfrog"`` (the default) is what DFTB+ runs, so it is the one that reproduces
        an external DFTB+ driver step for step. ``"cayley"`` uses ``H(t)`` and is only
        first order under the SCC charge feedback; prefer ``"cayley-midpoint"``.
    ehrenfest : bool, default: False
        Whether the nuclei move.
    velocities : array-like of float, shape (n_atom, 3), optional
        Initial velocities in atomic units; zero (a 0 K start) by default.

    Attributes
    ----------
    time : float
        Clock at the end of the last step.
    dipole_start, dipole_end : numpy.ndarray of float, shape (3,)
        Dipole moment at the two ends of the last step, in atomic units.
    energy_start, energy_end : float
        Total energy at the two ends of the last step, in Hartree.
    energy_terms : tuple of float
        ``(band, SCC, external, repulsive)`` energy at the start of the last step.
    energy_kinetic : float
        Nuclear kinetic energy of the last step.
    coords_start, velocity, force_start : numpy.ndarray of float, shape (n_atom, 3)
        Geometry, velocity and Ehrenfest force at the start of the last step.
    force_end : numpy.ndarray of float, shape (n_atom, 3)
        Ehrenfest force at the end of the last step, which drives the next one.
    """

    def __init__(
        self,
        system,
        ground,
        dt,
        propagator="leapfrog",
        ehrenfest=False,
        velocities=None,
    ):
        if propagator not in PROPAGATORS:
            raise ValueError("propagator must be one of %s" % (PROPAGATORS,))

        self.system = system
        self.state = RTState(system, ground)
        self.dt = float(dt)
        self.propagator = propagator
        self.midpoint = propagator == "cayley-midpoint"  # the Cayley corrector pass
        self.ehrenfest = bool(ehrenfest)
        self.time = 0.0

        n_atom, n_orb = system.n_atom, self.state.n_orb
        self.mass = np.array(
            [system.sk_set.mass[sp] for sp in system.atom_species], dtype=float
        )
        self.velocity = (
            np.zeros((n_atom, 3))
            if velocities is None
            else np.array(velocities, dtype=float, copy=True)
        )

        # reported at the two ends of every step
        self.dipole_start = np.zeros(3)
        self.dipole_end = np.zeros(3)
        self.energy_start = 0.0
        self.energy_end = 0.0
        self.energy_terms = (0.0, 0.0, 0.0, 0.0)
        self.energy_kinetic = kinetic_energy(self.mass, self.velocity)
        self.coords_start = system.coords.copy()
        self.force_start = np.zeros((n_atom, 3))
        self.force_end = np.zeros((n_atom, 3))

        # nuclear integrator state; the half-step velocity is the Verlet carrier
        self.half_velocity = self.velocity.copy()
        self.coords_next = system.coords.copy()
        self.accel = np.zeros((n_atom, 3))

        self._scratch = {}
        self._overlap_start = np.zeros((n_orb, n_orb))
        self._overlap_mid = np.zeros((n_orb, n_orb))
        self._coupling_mid = np.zeros((n_orb, n_orb))
        self._coords_mid = np.zeros((n_atom, 3))

    # ------------------------------------------------------------------ t = 0 --
    def start(self, field=None, kappa=0.0, pol_dir=2):
        """
        Set up ``t = 0``, apply the delta kick and take the bootstrap half-cycle.

        ``initializeDynamics`` (timeprop.F90:4451): the charges, dipole and energies are
        those of the *unkicked* ground state, the kick perturbs the density once, and a
        single Euler step of ``dt`` carries it to the first leapfrog interval. The first
        nuclear move is likewise a plain ``r(dt) = r(0) + v(0) dt``.
        """

        self.state.update_charges()
        self.state.update_hamiltonian(field)
        if self.ehrenfest:
            self.force_end = ehrenfest_force(self.state, self._scratch)
            self.half_velocity[:] = self.velocity
            self.coords_next[:] = self.system.coords + self.velocity * self.dt
        self._record_start()
        if kappa != 0.0:
            self.state.kick(kappa, pol_dir)
        self._advance(field, bootstrap=True)
        return self

    # ----------------------------------------------------------- one full step --
    def step(self, field=None):
        """
        One ``doTdStep``: move the nuclei, rebuild ``H`` with ``field``, propagate.

        Parameters
        ----------
        field : array-like of float, shape (3,), optional
            External electric field of this step in atomic units; ``None`` is no field.
        """

        if self.ehrenfest:
            self.velocity = velocity_verlet_next(
                self.coords_next, self.half_velocity, self.accel, self.dt
            )
            self.energy_kinetic = kinetic_energy(self.mass, self.velocity)
        self.state.update_hamiltonian(field)
        self._record_start()
        self._advance(field, bootstrap=False)
        return self

    # ------------------------------------------------------------------ pieces --
    def _record_start(self):
        """Snapshot the observables at the time the step begins."""

        self.energy_terms = self.state.energies()
        self.energy_start = sum(self.energy_terms) + self.energy_kinetic
        self.dipole_start[:] = self.state.dipole
        if self.ehrenfest:
            self.coords_start[:] = self.system.coords
            self.force_start[:] = self.force_end

    def _advance(self, field, bootstrap):
        """Propagate the density one step, move the geometry with it, and re-observe."""

        state, dt = self.state, self.dt
        if self.ehrenfest:
            build_coupling(self.system, self.velocity, state.coupling)

        if self.propagator == "leapfrog":
            if bootstrap:  # Euler bootstrap, rho_old keeps rho(0) (timeprop.F90:4815)
                state.rho_old[:] = state.rho
                state.propagate_leapfrog(dt)
            else:
                # the leapfrog interval is [t-dt, t+dt], whose midpoint is t, so S, H
                # and D at t centre the step for free
                state.propagate_leapfrog(2.0 * dt)
            self._adopt_geometry()
        elif bootstrap or not self.ehrenfest:
            state.propagate_cayley(dt, midpoint=self.midpoint, field=field)
            self._adopt_geometry()
        else:
            # Crank-Nicolson runs over [t, t+dt], so every geometry-dependent matrix has
            # to be centred by hand: S by averaging its two ends, D by rebuilding it at
            # the half-way geometry with the half-step velocity. Without this, Tr(S rho)
            # picks up a systematic O(dt^2) bias every step.
            self._overlap_start[:] = state.overlap
            self._coords_mid[:] = 0.5 * (self.system.coords + self.coords_next)
            self.system.coords[:, :] = self._coords_mid
            build_coupling(self.system, self.half_velocity, self._coupling_mid)
            self._adopt_geometry()
            self._overlap_mid[:] = 0.5 * (self._overlap_start + state.overlap)
            state.propagate_cayley(
                dt,
                midpoint=self.midpoint,
                field=field,
                overlap=self._overlap_mid,
                coupling=self._coupling_mid,
            )

        state.update_charges()
        state.update_hamiltonian(field)
        self.energy_end = sum(state.energies()) + self.energy_kinetic
        self.dipole_end[:] = state.dipole
        if self.ehrenfest:
            self.force_end = ehrenfest_force(state, self._scratch)
            self.accel = self.force_end / self.mass[:, None]
        self.time += dt

    def _adopt_geometry(self):
        """Take on ``r(t+dt)`` and rebuild H0, S, S^-1 and gamma there."""

        if self.ehrenfest:
            self.system.coords[:, :] = self.coords_next
            self.state.refresh_geometry()


# ---------------------------------------------------------------------------- #
# batch runners, used by the DFTB+ benchmarks                                  #
# ---------------------------------------------------------------------------- #
def _field_table(n_steps, field, field_table):
    """Expand a constant field, or accept a precomputed ``E(k dt)`` table."""

    if field_table is not None:
        return np.asarray(field_table, dtype=float)
    constant = np.zeros(3) if field is None else np.asarray(field, dtype=float)
    return np.tile(constant, (n_steps + 1, 1))


def run_kick(
    system,
    ground,
    n_steps,
    dt,
    kappa,
    pol_dir,
    propagator="leapfrog",
    field=None,
    field_table=None,
    field_lag=True,
    record_diagnostics=False,
):
    """
    Propagate one system after a delta kick with the nuclei frozen.

    Parameters
    ----------
    system : dftb_params.DFTBSystem
        Geometry and basis layout.
    ground : scc.SCCResult
        Converged ground state at that geometry.
    n_steps : int
        Number of propagation steps; the trajectory has ``n_steps + 1`` rows.
    dt : float
        Time step in atomic units.
    kappa : float
        Kick strength, the parsed field strength in atomic units.
    pol_dir : int
        Kick polarisation, 0, 1 or 2 for x, y, z.
    propagator : {"leapfrog", "cayley", "cayley-midpoint"}
        See :class:`RTDynamics`.
    field : array-like of float, shape (3,), optional
        Static external field in atomic units, applied every step.
    field_table : numpy.ndarray of float, shape (n_steps + 1, 3), optional
        Time-dependent external field, ``field_table[k]`` being ``E(k dt)``; overrides
        ``field``. :func:`rt.laser_field` builds the one DFTB+ uses for a laser run.
    field_lag : bool
        Whether the Hamiltonian that propagates step ``k`` carries ``E((k-1) dt)`` rather
        than ``E(k dt)``. DFTB+'s own laser loop lags by one step, because ``updateH`` at
        the end of ``doTdStep(iStep)`` builds ``H(t+dt)`` but calls
        ``setPresentField(iStep)`` (timeprop.F90:5322). The MaxwellLink socket path does
        *not* lag, which is ``field_lag = False``.
    record_diagnostics : bool
        Also return the electron count and idempotency error at every step.

    Returns
    -------
    dict
        ``times`` (a.u.), ``dipole`` (n_steps+1, 3) in atomic units, ``final_dipole`` and
        ``final_scc_energy`` (the values DFTB+ writes to its autotest tag, one step past
        the last trajectory row), plus diagnostics when asked for.
    """

    table = _field_table(n_steps, field, field_table)
    dynamics = RTDynamics(system, ground, dt, propagator=propagator)
    state = dynamics.state

    times = np.zeros(n_steps + 1)
    dipoles = np.zeros((n_steps + 1, 3))
    energies = np.zeros((n_steps + 1, 4))
    counts = np.zeros(n_steps + 1)
    idempotent = np.zeros(n_steps + 1)

    def observe(i_step):
        """Copy the observables the stepper recorded at the start of its step."""

        times[i_step] = i_step * dt
        dipoles[i_step] = dynamics.dipole_start
        energies[i_step] = dynamics.energy_terms
        if record_diagnostics:
            counts[i_step] = state.electron_count()
            idempotent[i_step] = state.idempotency()

    dynamics.start(table[0], kappa, pol_dir)
    observe(0)
    for i_step in range(1, n_steps + 1):
        dynamics.step(table[i_step - 1 if field_lag else i_step])
        observe(i_step)

    result = {
        "times": times,
        "dipole": dipoles,
        "energies": energies,
        "final_dipole": dynamics.dipole_end.copy(),
        "final_scc_energy": energies[n_steps, 1],
        "state": state,
        "n_orb": state.n_orb,
    }
    if record_diagnostics:
        result["electron_count"] = counts
        result["idempotency"] = idempotent
    return result


def run_ehrenfest(
    system,
    ground,
    n_steps,
    dt,
    kappa,
    pol_dir,
    propagator="leapfrog",
    field=None,
    field_table=None,
    field_lag=True,
    velocities=None,
    record_diagnostics=False,
):
    """
    Full RT-Ehrenfest trajectory: electrons propagated, nuclei moved, both at ``dt``.

    Same arguments as :func:`run_kick`, plus ``velocities`` for the initial nuclear
    velocities. DFTB+'s integrator consumes ``a(t)`` and returns ``r(t+dt)`` together
    with ``v(t)``, so the velocity it reports lags the position it reports by one step;
    that quirk is reproduced here because the reference trajectories carry it.

    Returns
    -------
    dict
        ``times``, ``dipole``, ``coords``, ``velocities``, ``forces`` and ``energies``
        along the trajectory, plus the final geometry, velocity, force and dipole in the
        same convention as the DFTB+ autotest tag.
    """

    table = _field_table(n_steps, field, field_table)
    dynamics = RTDynamics(
        system,
        ground,
        dt,
        propagator=propagator,
        ehrenfest=True,
        velocities=velocities,
    )
    state = dynamics.state
    n_atom, n_orb = system.n_atom, state.n_orb

    times = np.zeros(n_steps + 1)
    dipoles = np.zeros((n_steps + 1, 3))
    trajectory = np.zeros((n_steps + 1, n_atom, 3))
    velocity_log = np.zeros((n_steps + 1, n_atom, 3))
    force_log = np.zeros((n_steps + 1, n_atom, 3))
    energy_log = np.zeros((n_steps + 1, 6))
    counts = np.zeros(n_steps + 1)
    coupling_residual = np.zeros(n_steps + 1)
    sdot = np.zeros((n_orb, n_orb)) if record_diagnostics else None

    def observe(i_step):
        """Copy the observables the stepper recorded at the start of its step."""

        times[i_step] = i_step * dt
        dipoles[i_step] = dynamics.dipole_start
        trajectory[i_step] = dynamics.coords_start
        velocity_log[i_step] = dynamics.velocity
        force_log[i_step] = dynamics.force_start
        energy_log[i_step, :4] = dynamics.energy_terms
        energy_log[i_step, 4] = dynamics.energy_kinetic
        energy_log[i_step, 5] = dynamics.energy_start
        if record_diagnostics:
            counts[i_step] = state.electron_count()
            overlap_time_derivative(system, dynamics.velocity, sdot)
            coupling_residual[i_step] = np.abs(
                sdot - (state.coupling + state.coupling.T)
            ).max()

    dynamics.start(table[0], kappa, pol_dir)
    observe(0)
    for i_step in range(1, n_steps + 1):
        dynamics.step(table[i_step - 1 if field_lag else i_step])
        observe(i_step)

    return {
        "times": times,
        "dipole": dipoles,
        "coords": trajectory,
        "velocities": velocity_log,
        "forces": force_log,
        "energies": energy_log,
        "final_coords": system.coords.copy(),
        "final_velocity": dynamics.velocity.copy(),
        "final_force": dynamics.force_end.copy(),
        "final_dipole": dynamics.dipole_end.copy(),
        "final_scc_energy": energy_log[n_steps, 1],
        "electron_count": counts,
        "coupling_residual": coupling_residual,
        "state": state,
    }


# ---------------------------------------------------------------------------- #
# Born-Oppenheimer pre-equilibration                                           #
# ---------------------------------------------------------------------------- #
def bomd_equilibrate(
    system, n_steps, dt, kT, friction, rng, charge=0.0, tolerance=1.0e-10
):
    """
    Thermalize one system with Langevin Born-Oppenheimer MD, in place.

    OBABO steps of ``dt`` on the SCC ground-state surface: at every geometry the SCC
    charges are reconverged from the previous step's charges and the force is
    :func:`forces.total_force`. Positions are advanced in ``system.coords``; the
    velocities start from a Maxwell-Boltzmann draw and are returned, so a following
    Ehrenfest run can continue the same thermal motion.

    Parameters
    ----------
    system : dftb_params.DFTBSystem
        Geometry and basis layout; ``system.coords`` is updated in place.
    n_steps : int
        Number of MD steps.
    dt : float
        MD time step in atomic units.
    kT : float
        Temperature in Hartree.
    friction : float
        Langevin relaxation time in atomic units.
    rng : numpy.random.Generator
        Source of the initial velocities and the Langevin noise.
    charge : float, default: 0.0
        Net charge of the system in units of ``+e``.
    tolerance : float, default: 1e-10
        SCC convergence threshold on the shell charges.

    Returns
    -------
    numpy.ndarray of float, shape (n_atom, 3)
        Nuclear velocities at the end, in atomic units.

    Raises
    ------
    RuntimeError
        If the SCC loop fails to converge at some geometry.
    """

    n_atom = system.n_atom
    mass = np.array(
        [system.sk_set.mass[sp] for sp in system.atom_species], dtype=float
    )[:, None]
    c1h = float(np.exp(-0.5 * dt / friction))  # Langevin O half-step scaling
    noise = np.sqrt(kT / mass * (1.0 - c1h**2))  # its velocity noise, per atom

    velocity = np.sqrt(kT / mass) * rng.standard_normal((n_atom, 3))
    velocity -= (mass * velocity).sum(axis=0) / mass.sum()  # no centre-of-mass drift

    dq_shell = None

    def acceleration():
        nonlocal dq_shell
        h0, overlap = build_h0_overlap(system)
        ground = scf(
            system,
            h0,
            overlap,
            tolerance=tolerance,
            charge=charge,
            dq_shell_start=dq_shell,
        )
        if not ground.converged:
            raise RuntimeError("the SCC ground state did not converge during pre-NVT.")
        dq_shell = ground.dq_shell.copy()
        return total_force(system, ground) / mass

    accel = acceleration()
    for _ in range(int(n_steps)):
        velocity = c1h * velocity + noise * rng.standard_normal((n_atom, 3))  # O
        velocity += 0.5 * dt * accel  # B
        system.coords += dt * velocity  # A
        accel = acceleration()
        velocity += 0.5 * dt * accel  # B
        velocity = c1h * velocity + noise * rng.standard_normal((n_atom, 3))  # O
    return velocity
