"""
Single-mode cavity electrodynamics for MaxwellLink.

This module defines a lightweight cavity simulator that treats the EM field as
one damped harmonic oscillator coupled to MaxwellLink molecules. The simulation
runs entirely in atomic units and can operate with both socket-connected and
embedded (non-socket) molecular drivers.
"""

from __future__ import annotations

import json
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Union

import numpy as np

from ..molecule import Molecule
from ..sockets import SocketHub
from ..units import FS_TO_AU
from .dummy_em import DummyEMUnits, MoleculeDummyWrapper, DummyEMSimulation


class SingleModeUnits(DummyEMUnits):
    """
    EM units for single-mode cavity simulations (1:1 to atomic units).
    """

    def __init__(self):
        super().__init__()


def _axis_to_index(axis: Union[int, str]) -> int:
    """
    Normalize a coupling axis specification to an integer index.

    Parameters
    ----------
    axis : int or str
        Axis specification. Accepted values: ``0``, ``1``, ``2`` or
        ``"x"``, ``"y"``, ``"z"`` (case-insensitive).

    Returns
    -------
    int
        Axis index in ``{0, 1, 2}``.
    """

    if isinstance(axis, int):
        if axis not in (0, 1, 2):
            raise ValueError("Axis index must be 0 (x), 1 (y), or 2 (z).")
        return axis
    lookup = {"x": 0, "y": 1, "z": 2}
    idx = lookup.get(str(axis).lower())
    if idx is None:
        raise ValueError("Axis must be 0, 1, 2 or one of 'x', 'y', 'z'.")
    return idx


class MoleculeSingleModeWrapper(MoleculeDummyWrapper):
    """
    Wrapper that adapts a ``Molecule`` to SingleModeSimulation, handling units, sources, and IO.
    """

    def __init__(self, molecule: Molecule, dt_au: float, axis: Union[int, str]):
        """
        Initialize the SingleMode molecule wrapper.

        Parameters
        ----------
        molecule : Molecule
            The molecule to wrap.
        dt_au : float
            Time step in atomic units.
        axis : int or str
            Axis of the molecule to be coupled to the cavity mode. Accepted values: ``0``, ``1``, ``2`` or
            ``"x"``, ``"y"``, ``"z"`` (case-insensitive).

        """
        super().__init__(molecule=molecule)
        self.dt_au = float(dt_au)
        if self.dt_au <= 0.0:
            raise ValueError("dt_au must be positive.")
        # direction of the molecule to be coupled to the cavity mode
        self.axis = _axis_to_index(axis)

        # retrieve molecule settings from the wrapped Molecule
        self.mode = self.m.mode
        self.hub: Optional[SocketHub] = getattr(self.m, "hub", None)
        self.molecule_id: int = getattr(self.m, "molecule_id", -1)
        self.init_payload: Dict = dict(self.m.init_payload)
        self.last_amp: np.ndarray = np.zeros(3, dtype=float)
        self.time_units_fs = 1.0 / FS_TO_AU  # so that dt_em == dt_au
        # Refresh time-units and dt to keep init payloads consistent
        self.m._refresh_time_units(self.time_units_fs)
        self.m._refresh_time_step(self.dt_au)
        self.init_payload["dt_au"] = self.dt_au
        self.additional_data_history = self.m.additional_data_history

        if self.mode == "non-socket":
            if self.molecule_id < 0:
                self.molecule_id = 0  # will be overwritten by simulation
            self.m.molecule_id = self.molecule_id

    def append_additional_data(self, time_au: float):
        """
        Store additional molecular data supplied by non-socket drivers.

        Parameters
        ----------
        time_au : float
            Current simulation time in atomic units.
        """

        extra = {}
        if hasattr(self, "d_f"):
            try:
                extra = dict(self.d_f.append_additional_data() or {})
            except Exception:
                extra = {}
        if "time_au" not in extra:
            extra["time_au"] = time_au
        if extra:
            self.additional_data_history.append(extra)


class SingleModeSimulation(DummyEMSimulation):
    r"""
    Damped harmonic oscillator coupled to MaxwellLink molecules.

    The cavity mode (classical harmonic oscillator) obeys

    .. math::

       \dot{q} = p, \qquad
       \dot{p} = -\omega_c^{2}\, q \;+\; g \sum_i \frac{d\mu_i}{dt} \;-\; \gamma_c\, p \;+\; D(t), \qquad

    aka,

    .. math::

        \ddot{q} = -\omega_c^{2}\, q \;+\; g \sum_i \frac{d\mu_i}{dt} \;-\; \gamma_c\, p \;+\; D(t),

    where the effective electric field of this cavity mode is

    .. math::

       E(t) = - g p(t),

    where :math:`g` is ``coupling_strength`` and the sum runs over the selected
    molecular axis of all molecules. All quantities are in atomic units.
    """

    def __init__(
        self,
        dt_au: float,
        frequency_au: float,
        damping_au: float,
        molecules: Optional[Iterable[Molecule]] = None,
        drive: Optional[Union[float, Callable[[float], float]]] = None,
        coupling_strength: float = 1.0,
        coupling_axis: Union[int, str] = 2,
        hub: Optional[SocketHub] = None,
        qc_initial: float = 0.0,
        pc_initial: float = 0.0,
        record_history: bool = True,
    ):
        """
        Parameters
        ----------
        dt_au : float
            Simulation time step in atomic units.
        frequency_au : float
            Cavity angular frequency :math:`\\omega_0` (a.u.).
        damping_au : float
            Damping constant :math:`\\kappa` (a.u.).
        molecules : iterable of Molecule, optional
            Molecules coupled to the cavity.
        drive : float or callable, optional
            Constant drive term or function ``drive(t_au)``.
        coupling_strength : float, default: 1.0
            Prefactor :math:`g` multiplying the summed molecular amplitudes.
        coupling_axis : int or str, default: 2 (``"z"``)
            Component of the molecular amplitude used for coupling.
        hub : SocketHub, optional
            Socket hub shared by all socket-mode molecules.
        qc_initial : float, default: 0.0
            Initial cavity field amplitude :math:`E(0)` (a.u.).
        pc_initial : float, default: 0.0
            Initial time derivative :math:`\\dot{E}(0)` (a.u.).
        record_history : bool, default: True
            Record time, field, velocity, drive, and molecular response histories.
        """

        super().__init__(hub=hub, molecules=molecules)

        self.dt = float(dt_au)
        if self.dt <= 0.0:
            raise ValueError("dt_au must be positive.")
        self.frequency = float(frequency_au)
        self.damping = float(damping_au)
        self.coupling_strength = float(coupling_strength)
        self.axis = _axis_to_index(coupling_axis)
        self.drive = drive if drive is not None else (lambda _: 0.0)
        if isinstance(self.drive, (int, float)):
            const = float(self.drive)
            self.drive = lambda _t, c=const: c

        molecules = list(molecules or [])
        self.wrappers: List[MoleculeSingleModeWrapper] = [
            MoleculeSingleModeWrapper(molecule=m, dt_au=self.dt, axis=self.axis)
            for m in molecules
        ]
        self.socket_wrappers = [w for w in self.wrappers if w.mode == "socket"]
        self.non_socket_wrappers = [w for w in self.wrappers if w.mode == "non-socket"]

        if self.socket_wrappers:
            hubs = {w.hub for w in self.socket_wrappers if w.hub is not None}
            if hub is not None:
                hubs.add(hub)
            if len(hubs) > 1:
                raise ValueError(
                    "All socket-mode molecules must share the same SocketHub."
                )
            self.hub: SocketHub = hub or self.socket_wrappers[0].hub
            if self.hub is None:
                raise ValueError("Socket-mode molecules require a SocketHub instance.")
        else:
            self.hub = None

        # Assign IDs and initialize non-socket drivers
        # By default, SocketHub assigns IDs starting from 0, so we start
        # non-socket IDs after all socket ones.
        next_id = len(self.socket_wrappers)
        for wrapper in self.non_socket_wrappers:
            wrapper.initialize_driver(next_id)
            next_id += 1

        self.time = 0.0
        self.qc = float(qc_initial)
        self.pc = float(pc_initial)
        self.acceleration = self._calc_acceleration(self.time, 0.0, self.qc)

        self.record_history = bool(record_history)
        if self.record_history:
            self.time_history: List[float] = [self.time]
            self.qc_history: List[float] = [self.qc]
            self.pc_history: List[float] = [self.pc]
            self.drive_history: List[float] = [self._evaluate_drive(self.time)]
            self.molecule_response_history: List[float] = [0.0]
        else:
            self.time_history = []
            self.qc_history = []
            self.pc_history = []
            self.drive_history = []
            self.molecule_response_history = []

    # ------------------------------------------------------------------
    # Core helpers
    # ------------------------------------------------------------------
    def _evaluate_drive(self, time_au: float) -> float:
        """
        Evaluate the drive term at the given time.

        Parameters
        ----------
        time_au : float
            Current simulation time in atomic units.

        Returns
        -------
        float
            The evaluated drive term.
        """
        try:
            return float(self.drive(time_au))
        except TypeError:
            return float(self.drive)

    def _ensure_socket_connections(self):
        if not self.socket_wrappers:
            return
        init_payloads = {
            w.molecule_id: {**w.init_payload, "molecule_id": w.molecule_id}
            for w in self.socket_wrappers
        }
        ok = self.hub.wait_until_bound(init_payloads, require_init=True, timeout=None)
        if not ok:
            raise RuntimeError("Timeout waiting for socket molecules to bind.")

    def _collect_socket_responses(self, efield_vec: Sequence[float]) -> Dict[int, dict]:
        """
        Send requests to socket molecules and collect their responses.

        Parameters
        ----------
        efield_vec : array-like of float, shape (3,)
            Effective electric field vector in atomic units.

        Returns
        -------
        dict of int to dict
            Mapping from molecule IDs to their response payloads.
        """
        requests = {
            w.molecule_id: {
                "efield_au": efield_vec,
                "meta": {"time_au": self.time},
                "init": {**w.init_payload, "molecule_id": w.molecule_id},
            }
            for w in self.socket_wrappers
        }

        responses = self.hub.step_barrier(requests)
        while not responses:
            self._ensure_socket_connections()
            responses = self.hub.step_barrier(requests)
        return responses

    def _calc_acceleration(self, time: float, amp_sum: float, qc: float) -> float:
        """
        Calculate the cavity mode acceleration at the given time.

        Parameters
        ----------
        time : float
            Current simulation time in atomic units.
        amp_sum : float
            Sum of molecular source amplitudes along the coupling axis.
        qc : float
            Current cavity field amplitude.

        Returns
        -------
        float
            The calculated acceleration.
        """
        drive_val = self._evaluate_drive(time)
        acceleration = (
            drive_val + self.coupling_strength * amp_sum - (self.frequency**2) * qc
        )
        return acceleration

    def _calc_effective_efield(self, pc: float):
        """
        Calculate the effective electric field vector for the cavity mode.

        Parameters
        ----------
        pc : float
            Current cavity mode velocity.

        Returns
        -------
        numpy.ndarray of float, shape (3,)
            Effective electric field vector in atomic units.
        """
        efield_vec = np.array([0.0, 0.0, 0.0], dtype=float)
        efield_vec[self.axis] = -self.coupling_strength * pc
        return efield_vec

    def _step_molecules(self, efield_vec: Sequence[float], time_au: float):
        """
        Propagate all molecules for one EM step and collect their source amplitudes.

        Parameters
        ----------
        efield_vec : array-like of float, shape (3,)
            Effective electric field vector in atomic units.
        time_au : float
            Current simulation time in atomic units.

        Returns
        -------
        float
            Sum of molecular source amplitudes along the coupling axis.
        """
        # Non-socket molecules
        for wrapper in self.non_socket_wrappers:
            wrapper.propagate(efield_vec)
            amp = wrapper.calc_amp_vector()
            wrapper.last_amp = amp
            wrapper.append_additional_data(time_au=self.time)

        # Socket molecules
        if self.socket_wrappers:
            self._ensure_socket_connections()
            responses = self._collect_socket_responses(efield_vec)
            for wrapper in self.socket_wrappers:
                payload = responses.get(wrapper.molecule_id)
                if not payload:
                    continue
                amp = np.asarray(payload.get("amp", [0.0, 0.0, 0.0]), dtype=float)
                wrapper.last_amp = amp
                extra_blob = payload.get("extra", b"")
                if extra_blob:
                    try:
                        data = json.loads(extra_blob.decode("utf-8"))
                        if isinstance(data, dict):
                            data.setdefault("time_au", self.time)
                            wrapper.additional_data_history.append(data)
                    except Exception:
                        pass

        amp_sum = sum(wrapper.last_amp[self.axis] for wrapper in self.wrappers)
        return amp_sum

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def step(self):
        """
        Advance the simulation by one time step.
        """

        pc_half = self.pc + 0.5 * self.dt * self.acceleration

        efield_vec = self._calc_effective_efield(pc_half)

        amp_sum = self._step_molecules(efield_vec, self.time)

        self.qc += self.dt * pc_half

        acceleration = self._calc_acceleration(self.time + self.dt, amp_sum, self.qc)

        self.pc = pc_half + 0.5 * self.dt * acceleration

        self.pc *= np.exp(-self.damping * self.dt)

        # 5. update acceleration and time
        self.time += self.dt
        self.acceleration = acceleration

        if self.record_history:
            self.time_history.append(self.time)
            self.qc_history.append(self.qc)
            self.pc_history.append(self.pc)
            self.drive_history.append(self._evaluate_drive(self.time))
            self.molecule_response_history.append(float(amp_sum))

    def run(self, until: Optional[float] = None, steps: Optional[int] = None):
        """
        Run the simulation for a specified duration or number of steps.

        Parameters
        ----------
        until : float, optional
            Total simulation time (a.u.). ``steps`` must be ``None``.
        steps : int, optional
            Number of steps to execute. ``until`` must be ``None``.
        """

        if (until is None) == (steps is None):
            raise ValueError("Specify exactly one of 'until' or 'steps'.")

        if until is not None:
            if until < self.time:
                return
            steps = int(np.ceil((until - self.time) / self.dt))

        for _ in range(int(steps)):
            self.step()
