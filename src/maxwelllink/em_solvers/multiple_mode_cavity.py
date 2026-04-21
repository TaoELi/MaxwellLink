"""
Multiple-mode cavity electrodynamics for MaxwellLink.

This module defines a lightweight cavity simulator that treats the EM field as
one damped harmonic oscillator coupled to MaxwellLink molecules. The simulation
runs entirely in atomic units and can operate with both socket-connected and
embedded (non-socket) molecular drivers.
"""

from __future__ import annotations

import json
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Union
import time

import numpy as np

from ..molecule import Molecule
from ..sockets import SocketHub, am_master
from ..units import FS_TO_AU
from .dummy_em import DummyEMUnits, MoleculeDummyWrapper, DummyEMSimulation


class MultipleModeUnits(DummyEMUnits):
    """
    EM units for multiple-mode cavity simulations (1:1 to atomic units).
    """

    def __init__(self):
        super().__init__()


class MoleculeMultipleModeWrapper(MoleculeDummyWrapper):
    """
    Wrapper that adapts a ``Molecule`` to MultipleModeSimulation, handling units, sources, and IO.
    """

    def __init__(self, molecule: Molecule, dt_au: float):
        """
        Initialize the MultipleMode molecule wrapper.

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


class MultipleModeSimulation(DummyEMSimulation):
    r"""
    Damped harmonic oscillator coupled to MaxwellLink molecules.

    Under the dipole gauge, the total light-matter Hamiltonian is

    .. math::

        H = H_{mol} + \frac{1}{2} p_{\rm c}^2 + \frac{1}{2} \sum_{k\lambda}\omega_{k\lambda, \rm c}^2 (q_{k\lambda, \rm c} + \frac{\varepsilon_{k\lambda}}{\omega_{k\lambda,\rm c}^2} \sum_i \mu_i \cdot f_{k\lambda}(r_i))^2

    The cavity mode (classical harmonic oscillator) obeys

    .. math::

        \ddot{q}_{k\lambda, \rm c} = -\omega_{k\lambda, \rm c}^{2}\, q_{k\lambda, \rm c} \; - \; \varepsilon_{k\lambda} \sum_i \mu_i \cdot f_{k\lambda}(r_i) \;-\; \gamma_{k\lambda, \rm c} \, p_{k\lambda, \rm c} \;+\; D_{k\lambda}(t),

    where the effective electric field of this cavity mode is

    .. math::

       E(t) = -\sum_{k\lambda} \varepsilon_{k\lambda} q_{k\lambda, \rm c}(t) - \frac{\varepsilon_{k\lambda}^2}{\omega_{k\lambda, \rm c}^2} \sum_i \mu_i(t) \cdot f_{k\lambda}(r_i),

    where :math:`\varepsilon_{k\lambda}` is ``coupling_strength`` and the sum runs over the selected
    molecular axis of all molecules. The second term in the electric field accounts for the dipole self-energy term if enabled.

    All quantities are in atomic units.
    """

    def __init__(
        self,
        dt_au: float,
        frequency_au: float,
        damping_au: float,
        molecules: Optional[Iterable[Molecule]] = None,
        drive: Optional[Union[float, Callable[[float], float]]] = None,
        coupling_strength: float = 1.0,
        coupling_axis: str = "xy",
        hub: Optional[SocketHub] = None,
        qc_initial: Optional[list] = None,
        pc_initial: Optional[list] = None,
        mu_initial: Optional[list] = None,
        dmudt_initial: Optional[list] = None,
        record_history: bool = True,
        include_dse: bool = True,
        molecule_half_step: bool = False,
        shift_dipole_baseline: bool = False,
        gauge="dipole",
        x_grid_1d: Optional[list] = None,
        y_grid_1d: Optional[list] = None,
        delta_omega_x_au: float = 0.0,
        delta_omega_y_au: float = 0.0,
        n_mode_x: int = 1,
        n_mode_y: int = 1,
        abc_cutoff: float = 0.0,
    ):
        r"""
        Parameters
        ----------
        dt_au : float
            Simulation time step in atomic units.
        frequency_au : float
            Cavity angular frequency :math:`\omega_{\rm c}` (a.u.).
        damping_au : float
            Damping constant :math:`\kappa` (a.u.).
        molecules : iterable of Molecule, optional
            Molecules coupled to the cavity.
        drive : float or callable, optional
            Constant drive term or function ``drive(t_au)``.
        coupling_strength : float, default: 1.0
            Prefactor :math:`\varepsilon`.
        coupling_axis : str, default: "xy"
            Component(s) of the molecular dipole used for coupling.
        hub : :class:`~maxwelllink.sockets.sockets.SocketHub`, optional
            Socket hub shared by all socket-mode molecules.
        qc_initial : list, default: [0.0, 0.0, 0.0]
            Initial cavity field coordinate (a.u.).
        pc_initial : list, default: [0.0, 0.0, 0.0]
            Initial cavity field momentum (a.u.).
        mu_initial : list, default: [0.0, 0.0, 0.0]
            Initial total molecular dipole vector (a.u.).
        dmudt_initial : list, default: [0.0, 0.0, 0.0]
            Initial time derivative of the total molecular dipole vector (a.u.).
        record_history : bool, default: True
            Record time, field, velocity, drive, and molecular response histories.
        include_dse : bool, default: True
            Include dipole self-energy term in the simulation.
        molecule_half_step : bool, default: True
            Whether to further evaluate molecular info for another half time step.
        shift_dipole_baseline : bool, default: False
            Whether to shift all dipole values using the initial dipole value, so initial dipole value is changed to zero.
            Setting this to True can facilitate simulating strong coupling systems with large permanent dipoles.
        gauge : str, default: "dipole"
            Gauge choice for light-matter coupling: "dipole".
        x_grid_1d : list, optional
            1D grid points for molecular bath coordinates along x-axis, in units of cavity length Lx. If None, defaults to [0.5] (single point at the center).
        y_grid_1d : list, optional
            1D grid points for molecular bath coordinates along y-axis, in units of cavity length Ly. If None, defaults to [0.5] (single point at the center).
        delta_omega_x_au : float, default: 0.0
            Frequency spacing along x-axis for cavity modes, in atomic units. The cavity mode frequencies are calculated as :math:`\omega_{k} = \sqrt{\omega_{\rm c}^2 + (l_x \Delta\omega_x)^2 + (l_y \Delta\omega_y)^2}` where :math:`l_x, l_y` are the mode indices determined by ``n_mode_x`` and ``n_mode_y``.
        delta_omega_y_au : float, default: 0.0
            Frequency spacing along y-axis for cavity modes, in atomic units.
        n_mode_x : int, default: 1
            Number of cavity modes along x-axis.
        n_mode_y : int, default: 1
            Number of cavity modes along y-axis.
        abc_cutoff : float, default: 0.0
            Absorbing boundary condition cutoff for the molecular bath grid, in units of cavity length.  The cutoff is applied to both x and y axes. If 0.0, no absorbing boundary condition is applied. If > 0.0, the grid points within the cutoff distance from the boundaries will be smoothly damped to suppress unphysical reflections of the EM field at the boundaries.
        """

        super().__init__(hub=hub, molecules=molecules)

        self.dt = float(dt_au)
        if self.dt <= 0.0:
            raise ValueError("dt_au must be positive.")
        self.frequency = float(frequency_au)
        self.damping = float(damping_au)
        self.coupling_strength = float(coupling_strength)
        self.axis = np.array([False, False, False], dtype=bool)

        self.gauge = gauge.lower()
        if self.gauge not in ["dipole"]:
            raise ValueError("gauge must be 'dipole'.")

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
        self.wrappers: List[MoleculeMultipleModeWrapper] = [
            MoleculeMultipleModeWrapper(molecule=m, dt_au=self.dt) for m in molecules
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

       # generate 2D grid points of molecular bath coords in units of Lx, Ly
        x_grid_2d, y_grid_2d = np.meshgrid(x_grid_1d, y_grid_1d)
        x_grid_2d = np.reshape(x_grid_2d, -1)
        y_grid_2d = np.reshape(y_grid_2d, -1)
        self.n_grid = np.size(x_grid_2d)

        # generate 2D grid points of kx, ky in units of 1/Lx, 1/Ly
        kx_grid_1d = (np.pi * np.array([i + 1.0 for i in range(n_mode_x)]))
        ky_grid_1d = (np.pi * np.array([i + 1.0 for i in range(n_mode_y)]))
        kx_grid_2d, ky_grid_2d = np.meshgrid(kx_grid_1d, ky_grid_1d)
        kx_grid_2d = np.reshape(kx_grid_2d, -1)
        ky_grid_2d = np.reshape(ky_grid_2d, -1)

        # construct cavity mode frequency array for all photon dimensions
        omega_parallel = np.reshape(((kx_grid_2d / np.pi * delta_omega_x_au) ** 2 + (ky_grid_2d / np.pi * delta_omega_y_au) ** 2) ** 0.5, -1)
        print("omega_parallel in cm-1", omega_parallel * 219474.63)
        self.omega_k = (self.frequency**2 + omega_parallel**2) ** 0.5
        print("omega_k in cm-1", self.omega_k * 219474.63)
        
        # construct renormalized cavity mode function for each molecular grid point
        n_mode = n_mode_x * n_mode_y * 1
        ftilde_k = np.zeros((n_mode, self.n_grid, 3), dtype=float)
        for i in range(self.n_grid):
            x, y = x_grid_2d[i], y_grid_2d[i]
            ftilde_k[:, i, 0] = (2.0 * np.cos(kx_grid_2d * x) * np.sin(ky_grid_2d * y))
            ftilde_k[:, i, 1] = (2.0 * np.sin(kx_grid_2d * x) * np.cos(ky_grid_2d * y))

        self.ftilde_k = ftilde_k
        self.varepsilon_k = self.coupling_strength * self.omega_k / np.min(self.omega_k)

        self.x_grid_1d = x_grid_1d
        self.y_grid_1d = y_grid_1d
        abc_x, abc_y = None, None
        self.abc_cutoff = float(abc_cutoff)
        if self.abc_cutoff != 0.00 : 

            r01 = self.abc_cutoff
            r10 = 1 - self.abc_cutoff

            def abc(grid_1d, r01=0.05, r10=0.95):

                if len(grid_1d) < 3 : return np.ones_like(grid_1d)

                grid_1d = np.array(grid_1d)
                r00, r11 = grid_1d[0], grid_1d[-1]

                S = np.zeros_like(grid_1d)
                middle = np.where((grid_1d < r10) & (grid_1d > r01))[0]
                S[middle] = 1
                
                l_side = np.where((grid_1d < r01) & (grid_1d > r00))[0]
                r_side = np.where((grid_1d < r11) & (grid_1d > r10))[0]

                def smooth(x, l, r, p):
                    frac = (l - r) / (l - x) + (r - l) / (x - r)
                    if p : return 1 / (1 + np.exp(-frac))
                    else : return 1 / (1 + np.exp(frac))
                
                l_value = np.array([smooth(i, l=r00, r=r01, p=False) for i in grid_1d[l_side]])
                r_value = np.array([smooth(i, l=r10, r=r11, p=True) for i in grid_1d[r_side]])
                S[l_side] = l_value
                S[r_side] = r_value

                return S
            
            self.smooth_x = abc(self.x_grid_1d, r01=r01, r10=r10)
            self.smooth_y = abc(self.y_grid_1d, r01=r01, r10=r10)
            self.smooth_x_2d, self.smooth_y_2d = np.meshgrid(self.smooth_x, self.smooth_y)
            self.smooth_2d = self.smooth_x_2d * self.smooth_y_2d
            self.smooth_2d = np.diag(np.reshape(self.smooth_2d, -1))
            from scipy.linalg import pinv
            abc_x = self.ftilde_k[:,:,0] @ self.smooth_2d @ pinv(self.ftilde_k[:,:,0])
            abc_y = self.ftilde_k[:,:,1] @ self.smooth_2d @ pinv(self.ftilde_k[:,:,1])
            self.abc_x = abc_x
            self.abc_y = abc_y

        self.if_abc = (abc_x is not None) and (abc_y is not None)
        print(f"Applying Absorbing Boundary Condition : {self.if_abc}, cutoff: {self.abc_cutoff}")
        self.time = 0.0

        if qc_initial is None:
            qc_initial = np.zeros((n_mode, 3), dtype=float)
        if pc_initial is None:
            pc_initial = np.zeros((n_mode, 3), dtype=float)
        if mu_initial is None:
            mu_initial = np.zeros((self.n_grid, 3), dtype=float)
        if dmudt_initial is None:
            dmudt_initial = np.zeros((self.n_grid, 3), dtype=float)

        self.qc = qc_initial * self.axis
        self.pc = pc_initial * self.axis
        self.dipole = mu_initial * self.axis
        self.dipole_prev = self.dipole.copy()
        self.dmudt = dmudt_initial * self.axis
        self.dmudt_prev = self.dmudt.copy()
        self.acceleration = np.zeros((n_mode, 3), dtype=float)

        self.include_dse = bool(include_dse)
        self.molecule_half_step = bool(molecule_half_step)
        self.shift_dipole_baseline = bool(shift_dipole_baseline)

        if self.shift_dipole_baseline:
            # shift all dipole values using the initial dipole value, so initial dipole value is zero
            self.dipole_baseline = self.dipole.copy()
            self.dipole -= self.dipole_baseline
            self.dipole_prev = self.dipole.copy()
            print(
                "[SingleModeCavity] Shifted dipole baseline by:", self.dipole_baseline
            )

        self.record_history = bool(record_history)
        if self.record_history:
            self.time_history = []
            self.qc_history = []
            self.pc_history = []
            self.drive_history = []
            self.molecule_response_history = []
            self.energy_history = []
            self.effective_efield_history = []

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
        efield_vec : array-like of float, shape (n_grid, 3)
            Effective electric field vector in atomic units.

        Returns
        -------
        dict of int to dict
            Mapping from molecule IDs to their response payloads.
        """
        requests = {
            w.molecule_id: {
                "efield_au": efield_vec[w.molecule_id,:],
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

    def _calc_acceleration(self, time: float, mu: np.ndarray, qc: np.ndarray) -> np.ndarray:
        """
        Calculate the cavity mode acceleration at the given time.

        Parameters
        ----------
        time : float
            Current simulation time in atomic units.
        mu : np.ndarray of float, shape (n_grid, 3)
            Sum of individual molecular dipole moment vectors.
        qc : np.ndarray of float, shape (n_mode, 3)
            Current cavity field coordinates.

        Returns
        -------
        float
            The calculated acceleration.
        """
        drive_val = self._evaluate_drive(time)
        mu_dot_f = np.einsum("ijk, jk->ik", self.ftilde_k, mu)
        acceleration = (
            drive_val - np.einsum('i,ik->ik', self.varepsilon_k, mu_dot_f) - np.einsum("i,ik->ik", self.omega_k**2, qc)
        )
        # print("In Function, Cavity acceleration:", acceleration)
        acceleration = np.einsum("ik, k->ik", acceleration, self.axis)
        return acceleration

    def _calc_effective_efield(self, qc: np.ndarray, mu: np.ndarray) -> np.ndarray:
        """
        Calculate the effective electric field vector for the cavity mode.

        Parameters
        ----------
        qc : numpy.ndarray of float, shape (n_mode, 3)
            Current cavity mode coordinate vector with shape (n_mode, 3).
        mu : numpy.ndarray of float, shape (n_grid, 3)
            Current total molecular dipole vector with shape (n_grid, 3).

        Returns
        -------
        numpy.ndarray of float, shape (n_grid, 3)
            Effective electric field vector in atomic units.
        """
        varepsilon_dot_qc = -np.einsum("i,ij->ij", self.varepsilon_k, qc)

        # add dipole self-energy term for the electric field if enabled
        if self.include_dse:
            mu_dot_f = np.einsum("ijk, jk->ik", self.ftilde_k, mu)
            varepsilon_dot_qc -= np.einsum("i,ik,i->ik", self.varepsilon_k**2, mu_dot_f, 1.0 / (self.omega_k**2), optimize=True)
        efield_vec = np.einsum("ijk,ik,k->jk", self.ftilde_k, varepsilon_dot_qc, self.axis, optimize=True)
        assert efield_vec.shape == mu.shape
        return efield_vec

    def _calc_energy(self, pc, qc, mu) -> float:
        """
        Calculate the total energy of the cavity + molecular system.

        Parameters
        ----------
        pc : numpy.ndarray of float, shape (n_mode, 3)
            Current cavity mode momentum with shape (n_mode,3).
        qc : numpy.ndarray of float, shape (n_mode, 3)
            Current cavity mode coordinate with shape (n_mode,3).
        mu : numpy.ndarray of float, shape (n_grid, 3)
            Current total molecular dipole along the coupling axis with shape (n_grid,3).

        Returns
        -------
        float
            Total energy of the cavity + molecular system.
        """
        kinetic_energy = 0.5 * pc**2
        mu_dot_f = np.einsum("ijk, jk->ik", self.ftilde_k, mu)
        potential_energy = 0.5 * np.einsum("i,ij->ij", self.omega_k**2, qc**2) + np.einsum("i,ij,ij->ij", self.varepsilon_k, qc, mu_dot_f)
        if self.include_dse:
            potential_energy += 0.5 * np.einsum("i,ij->ij", self.varepsilon_k**2 / self.omega_k**2, mu_dot_f**2)
    
        e_molecule = sum(
            wrapper.additional_data_history[-1]["energy_au"]
            for wrapper in self.wrappers
        )
        total_energy = (
            np.sum((kinetic_energy + potential_energy) * self.axis) + e_molecule
        )

        return total_energy

    def _calc_dipole_vec(self) -> np.ndarray:
        """
        Calculate the total molecular dipole vector.

        Returns
        -------
        numpy.ndarray of float, shape (n_grid, 3)
            Total molecular dipole vector in atomic units.
        """

        dipole_vec = np.zeros((self.n_grid, 3), dtype=float)
        for wrapper in self.wrappers:
            if wrapper.additional_data_history:
                latest_data = wrapper.additional_data_history[-1]
                rescaling_factor = 1.0
                if wrapper.rescaling_factor != None:
                    rescaling_factor = float(wrapper.rescaling_factor)
                dipole_vec[wrapper.molecule_id, 0] = latest_data.get("mux_au") * rescaling_factor
                dipole_vec[wrapper.molecule_id, 1] = latest_data.get("muy_au") * rescaling_factor
                dipole_vec[wrapper.molecule_id, 2] = latest_data.get("muz_au") * rescaling_factor
        if self.shift_dipole_baseline:
            dipole_vec -= self.dipole_baseline
        return dipole_vec

    def _step_molecules(self, efield_vec: Sequence[float]):
        """
        Propagate all molecules for one EM step and collect their dipole moments.

        Parameters
        ----------
        efield_vec : array-like of float, shape (n_grid, 3)
            Effective electric field vector in atomic units.

        Returns
        -------
        float
            Sum of molecular dipole moment along the coupling axis.
        """
        # Non-socket molecules
        #for wrapper in self.non_socket_wrappers:
        #    wrapper.propagate(efield_vec)
        #    amp = wrapper.calc_amp_vector() * wrapper.rescaling_factor
        #    wrapper.last_amp = amp
        #    wrapper.append_additional_data(time_au=self.time)
        
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
        dmudt = np.zeros((self.n_grid,3), dtype=float)
        for wrapper in self.wrappers:
            dmudt[wrapper.molecule_id,:] = wrapper.last_amp
        # for dipole gauge we use mu directly instead of dmu/dt
        dipole = self._calc_dipole_vec()
        # we need to filter only the coupling axis
        dipole = dipole * self.axis
        dmudt = dmudt * self.axis

        # print("In Function, Total Dipole vector:", dipole)
        # print("In Function, Total Dipole velocity (dmu/dt):", dmudt)
        return dipole, dmudt

    def _step_dipole_gauge(self):
        """
        Advance the simulation by one time step under the dipole gauge.
        """

        # 1. update momentum to half step
        pc_half = self.pc + 0.5 * self.dt * self.acceleration
        # apply absorbing boundary condition to the cavity field if enabled
        if self.if_abc: 
            pc_half[:,0] = self.pc[:,0] @ self.abc_x
            pc_half[:,1] = self.pc[:,1] @ self.abc_y

        # 2. update position to full step
        qc_prev = self.qc.copy()
        self.qc += self.dt * pc_half

        # updating E-field at half step using interpolated dipole
        # this interpolation is not very accurate
        # dipole = self.dipole + 0.5 * self.dt * (1.5 * self.dmudt - 0.5 * self.dmudt_prev)
        # the following expression is accurate to the order of dt^4
        dipole = self.dipole_prev + self.dt * (
            9.0 / 8.0 * self.dmudt + 3.0 / 8.0 * self.dmudt_prev
        )

        efield_vec = self._calc_effective_efield(
            qc_prev + 0.5 * self.dt * pc_half, dipole
        )
        
        # update dipole info
        self.dipole_prev = self.dipole.copy()
        self.dmudt_prev = self.dmudt.copy()

        # the value for n+1/2 time step
        self.dipole, self.dmudt = self._step_molecules(efield_vec)

        # extrapolate to n+1 time step (ONLY NEEDED FOR VELOCITY VERLET MOLECULE PROPAGATION)
        if self.molecule_half_step:
            self.dipole = 2.0 * self.dipole - self.dipole_prev
            self.dmudt = 2.0 * self.dmudt - self.dmudt_prev

        acceleration = self._calc_acceleration(self.time, self.dipole, self.qc)

        # 3. update momentum from half step to full step
        self.pc = pc_half + 0.5 * self.dt * acceleration

        self.pc *= np.exp(-self.damping * self.dt)

        # apply absorbing boundary condition to the cavity field if enabled
        if self.if_abc: 
            self.pc[:,0] = self.pc[:,0] @ self.abc_x
            self.pc[:,1] = self.pc[:,1] @ self.abc_y

        # 4. update acceleration and time
        self.time += self.dt
        self.acceleration = acceleration.copy()

        if self.record_history:
            self.time_history.append(self.time)
            self.qc_history.append(self.qc.copy())
            self.pc_history.append(self.pc.copy())
            self.drive_history.append(self._evaluate_drive(self.time))
            self.molecule_response_history.append(self.dmudt.copy())
            self.energy_history.append(self._calc_energy(self.pc, self.qc, self.dipole))
            self.effective_efield_history.append(self._calc_effective_efield(self.qc, self.dipole))
        
    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def step(self):
        """
        Advance the simulation by one time step.
        """
        if self.gauge == "dipole":
            self._step_dipole_gauge()
        else:
            raise ValueError("gauge must be either 'dipole' or 'velocity'.")

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
                    f"[MultipleModeCavity] Completed {idx + 1}/{steps} [{(idx + 1) / steps * 100:.1f}%] steps, time/step: {avg_time_per_step:.2e} seconds, remaining time: {remaining:.2f} seconds."
                )

        # close the hub
        if self.hub is not None:
            if am_master():
                self.hub.stop()
