"""
Laser driven dynamics for MaxwellLink.

This module defines a lightweight laser driven simulator that applies a user
defined driven pulse to excited the molecules. The simulation runs entirely 
in atomic units and can operate with both socket-connected and embedded (non-socket) 
molecular drivers.
"""

from __future__ import annotations

import json
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Union
import time

import numpy as np

from ..molecule import Molecule
from ..sockets import SocketHub
from ..units import FS_TO_AU
from .dummy_em import DummyEMUnits, MoleculeDummyWrapper, DummyEMSimulation


class LaserDrivenUnits(DummyEMUnits):
    """
    EM units for laser driven simulations (1:1 to atomic units).
    """

    def __init__(self):
        super().__init__()


class MoleculeLaserDrivenWrapper(MoleculeDummyWrapper):
    """
    Wrapper that adapts a ``Molecule`` to LaserDrivenSimulation, handling units, sources, and IO.
    """

    def __init__(self, molecule: Molecule, dt_au: float):
        """
        Initialize the Laser Driven molecule wrapper.

        Parameters
        ----------
        molecule : Molecule
            The molecule to wrap.
        dt_au : float
            Time step in atomic units.
        """
        super().__init__(molecule=molecule)
        self.dt_au = float(dt_au)
        if self.dt_au <= 0.0:
            raise ValueError("dt_au must be positive.")

        # retrieve molecule settings from the wrapped Molecule
        self.mode = self.m.mode
        self.hub: Optional[SocketHub] = getattr(self.m, "hub", None)
        self.molecule_id: int = getattr(self.m, "molecule_id", -1)
        self.init_payload: Dict = dict(self.m.init_payload)
        self.last_amp: np.ndarray = np.zeros(3, dtype=float)
        self.time_units_fs = 1.0 / FS_TO_AU  # so that dt_em == dt_au
        # to rescale dipoles if needed
        self.rescaling_factor = self.m.rescaling_factor
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


class LaserDrivenSimulation(DummyEMSimulation):
    r"""
    Laser driven dynamics of the MaxwellLink molecules.

    This class employes a user-defined electric field:

    .. math::

       E(t) = f(t)

    applied on x, y, z, or any combinations of the molecular dipole vector

    All quantities are in atomic units.
    """

    def __init__(
        self,
        dt_au: float,
        molecules: Optional[Iterable[Molecule]] = None,
        drive: Optional[Union[float, Callable[[float], float]]] = None,
        coupling_axis: str = "xyz",
        hub: Optional[SocketHub] = None,
        record_history: bool = True,
    ):
        """
        Parameters
        ----------
        dt_au : float
            Simulation time step in atomic units.
        molecules : iterable of Molecule, optional
            Molecules coupled to the cavity.
        drive : float or callable, optional
            Constant drive term or function ``drive(t_au)``.
        coupling_axis : str, default: "xyz"
            Component(s) of the molecular dipole used for coupling.
        hub : SocketHub, optional
            Socket hub shared by all socket-mode molecules.
        record_history : bool, default: True
            Record time, field, velocity, drive, and molecular response histories.
        """

        super().__init__(hub=hub, molecules=molecules)

        self.dt = float(dt_au)
        if self.dt <= 0.0:
            raise ValueError("dt_au must be positive.")
        self.axis = np.array([False, False, False], dtype=bool)

        if "x" in coupling_axis.lower():
            self.axis[0] = True
        if "y" in coupling_axis.lower():
            self.axis[1] = True
        if "z" in coupling_axis.lower():
            self.axis[2] = True

        # we need True in at least one axis
        if not np.any(self.axis):
            raise ValueError(
                "At least one coupling axis (x, y, or z) must be specified."
            )

        self.drive = drive if drive is not None else (lambda _: 0.0)
        if isinstance(self.drive, (int, float)):
            const = float(self.drive)
            self.drive = lambda _t, c=const: c

        molecules = list(molecules or [])
        self.wrappers: List[MoleculeLaserDrivenWrapper] = [
            MoleculeLaserDrivenWrapper(molecule=m, dt_au=self.dt) for m in molecules
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

        self.record_history = bool(record_history)
        if self.record_history:
            self.time_history = []
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

    def _calc_effective_efield(self, time_au: float) -> np.ndarray:
        """
        Calculate the effective electric field vector for the laser driven dynamics.

        Parameters
        ----------
        time_au : float
            Current time in atomic units.

        Returns
        -------
        numpy.ndarray of float, shape (3,)
            Effective electric field vector in atomic units.
        """
        efield_vec = np.ones(3, dtype=float) * self._evaluate_drive(time_au)
        efield_vec *= self.axis
        return efield_vec
    
    def _calc_dipole_vec(self) -> np.ndarray:
        """
        Calculate the total molecular dipole vector.

        Returns
        -------
        numpy.ndarray of float, shape (3,)
            Total molecular dipole vector in atomic units.
        """

        dipole_vec = np.array([0.0, 0.0, 0.0], dtype=float)
        for wrapper in self.wrappers:
            if wrapper.additional_data_history:
                latest_data = wrapper.additional_data_history[-1]
                rescaling_factor = 1.0
                if wrapper.rescaling_factor != None:
                    rescaling_factor = float(wrapper.rescaling_factor)
                dipole_vec[0] += latest_data.get("mux_au") * rescaling_factor
                dipole_vec[1] += latest_data.get("muy_au") * rescaling_factor
                dipole_vec[2] += latest_data.get("muz_au") * rescaling_factor
        return dipole_vec

    def _step_molecules(self, efield_vec: Sequence[float]):
        """
        Propagate all molecules for one EM step.

        Parameters
        ----------
        efield_vec : array-like of float, shape (3,)
            Effective electric field vector in atomic units.

        Returns
        -------
        float
            Sum of molecular dipole moment along the coupling axis.
        """
        # Non-socket molecules
        for wrapper in self.non_socket_wrappers:
            wrapper.propagate(efield_vec)
            amp = wrapper.calc_amp_vector() * wrapper.rescaling_factor
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
                amp = (
                    np.asarray(payload.get("amp", [0.0, 0.0, 0.0]), dtype=float)
                    * wrapper.rescaling_factor
                )
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
        # dmu/dt
        dmudt = np.zeros(3, dtype=float)
        for wrapper in self.wrappers:
            dmudt += wrapper.last_amp
        # for dipole gauge we use mu directly instead of dmu/dt
        dipole = self._calc_dipole_vec()
        # we need to filter only the coupling axis
        dipole = dipole * self.axis
        dmudt = dmudt * self.axis

        # print("In Function, Total Dipole vector:", dipole)
        # print("In Function, Total Dipole velocity (dmu/dt):", dmudt)
        return dipole, dmudt

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def step(self):
        """
        Advance the simulation by one time step.
        """

        efield_vec = self._calc_effective_efield(self.time)

        self.dipole, self.dmudt = self._step_molecules(efield_vec)

        self.time += self.dt

        if self.record_history:
            self.time_history.append(self.time)
            self.drive_history.append(self._evaluate_drive(self.time))
            self.molecule_response_history.append(self.dmudt.copy())

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

        start_time = time.perf_counter()
        previous_time = start_time
        for idx in range(int(steps)):
            self.step()

            if (idx + 1) % 1000 == 0:

                current_time = time.perf_counter()
                avg_time_per_step = (current_time - previous_time) / 1000.0
                previous_time = current_time

                elapsed = current_time - start_time
                remaining = (elapsed / (idx + 1)) * (steps - (idx + 1))
                print(
                    f"[LaserDriven] Completed {idx + 1}/{steps} [{(idx + 1) / steps * 100:.1f}%] steps, time/step: {avg_time_per_step:.2e} seconds, remaining time: {remaining:.2f} seconds."
                )

        # close the hub
        if self.hub is not None:
            self.hub.stop()
