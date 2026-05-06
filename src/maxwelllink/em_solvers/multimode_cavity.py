"""
Multimode cavity electrodynamics for MaxwellLink.

This module defines a lightweight cavity simulator that treats the EM field as
multiple damped harmonic oscillators coupled to MaxwellLink molecules. The simulation
runs entirely in atomic units and can operate with both socket-connected and
embedded (non-socket) molecular drivers.
"""

from __future__ import annotations

import json
import os, shutil
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Union
import time

import numpy as np

from ..molecule import Molecule
from ..sockets import SocketHub, am_master
from ..units import AU_TO_FS, FS_TO_AU, AU_TO_CM_INV, AU_TO_K
from .dummy_em import DummyEMUnits, MoleculeDummyWrapper, DummyEMSimulation
from ..tools.harmonic_oscillator_helper import *


class MultiModeUnits(DummyEMUnits):
    """
    EM units for multimode cavity simulations (1:1 to atomic units).
    """

    def __init__(self):
        super().__init__()


class MoleculeMultiModeWrapper(MoleculeDummyWrapper):
    """
    Wrapper that adapts a ``Molecule`` to MultiModeSimulation, handling units, sources, and IO.
    """

    def __init__(self, molecule: Molecule, dt_au: float):
        """
        Initialize the MultiMode molecule wrapper.

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

class FabryPerotCavity():
    r"""
    Damped harmonic oscillators coupled to MaxwellLink molecules.

    Under the dipole gauge, the total light-matter Hamiltonian is

    .. math::

        H = H_{mol} + \frac{1}{2} p_{\rm c}^2 + \frac{1}{2} \sum_{k\lambda}\omega_{k\lambda, \rm c}^2 (q_{k\lambda, \rm c} + \frac{\varepsilon_{k\lambda}}{\omega_{k\lambda,\rm c}^2} \sum_i \mu_i \cdot f_{k\lambda}(r_i))^2

    The cavity mode (classical harmonic oscillator) obeys

    .. math::

        \ddot{q}_{k\lambda, \rm c} = -\omega_{k\lambda, \rm c}^{2}\, q_{k\lambda, \rm c} \; - \; \varepsilon_{k\lambda} \sum_i \mu_i \cdot f_{k\lambda}(r_i) \;-\; \gamma_{k\lambda, \rm c} \, p_{k\lambda, \rm c} \;+\; D_{k\lambda}(t),

    where the effective electric field of this cavity mode is

    .. math::

       E(t) = -\sum_{k\lambda} \varepsilon_{k\lambda} q_{k\lambda, \rm c}(t) - \frac{\varepsilon_{k\lambda}^2}{\omega_{k\lambda, \rm c}^2} \sum_i \mu_i(t) \cdot f_{k\lambda}(r_i),

    where :math:`\varepsilon_{k\lambda}` is effective coupling strength for different photon modes, :math:`f_{k\lambda}(r_i)` is the cavity mode function evaluated at the position of the r_i. The second term in the electric field accounts for the dipole self-energy term if enabled.

    All quantities are in atomic units.
    """

    def __init__(
        self,
        frequency_au: float = None,   
        coupling_strength: float = 1.0,
        coupling_axis: str = "xy",
        x_grid_1d: Optional[list] = None,
        y_grid_1d: Optional[list] = None,
        n_grid_x: int = None,
        n_grid_y: int = None,
        n_repeat_x: int = 1,
        n_repeat_y: int = 1,
        delta_omega_x_au: float = None,
        delta_omega_y_au: float = None,
        n_mode_x: int = 1,
        n_mode_y: int = 1,
        abc_cutoff: float = 0.0,
    ):
        r"""
        Parameters
        ----------
        frequency_au : float
            Cavity angular frequency :math:`\omega_{\rm c}` (a.u.).
        coupling_strength : float, default: 1.0
            Prefactor :math:`\varepsilon`.
        coupling_axis : str, default: "xy"
            Component(s) of the molecular dipole used for coupling.
        x_grid_1d : list, optional
            1D grid points for molecular bath coordinates along x-axis, in units of cavity length Lx. If None, defaults to [0.5] (single point at the center).
        y_grid_1d : list, optional
            1D grid points for molecular bath coordinates along y-axis, in units of cavity length Ly. If None, defaults to [0.5] (single point at the center).
        n_grid_x : int, default: 1
            Number of grid points for molecular bath coordinates along x-axis, in units of cavity length Lx. If provided together with n_grid_y, it will override x_grid_1d and generate a uniform grid along x-axis. If x_grid_1d is provided, n_grid_x will be ignored.
        n_grid_y : int, default: 1
            Number of grid points for molecular bath coordinates along y-axis, in units of cavity length Ly. If provided together with n_grid_x, it will override y_grid_1d and generate a uniform grid along y-axis. If y_grid_1d is provided, n_grid_y will be ignored.
        n_repeat_x : int, default: 1
            Number of times to repeat the grid points along x-axis.
        n_repeat_y : int, default: 1
            Number of times to repeat the grid points along y-axis.
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
        if frequency_au is None:
            raise ValueError("frequency_au must be provided.")
        self.frequency_au = float(frequency_au)

        if delta_omega_x_au is None:
            raise ValueError("delta_omega_x_au must be provided.")
        self.delta_omega_x_au = float(delta_omega_x_au)

        if delta_omega_y_au is None:
            raise ValueError("delta_omega_y_au must be provided.")
        self.delta_omega_y_au = float(delta_omega_y_au)

        self.coupling_strength = float(coupling_strength)

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

        # generate 1D grid points of molecular bath coords in units of Lx, Ly
        if n_grid_x is not None: 
            if n_grid_x <= 0:
                raise ValueError("n_grid_x must be positive if provided.")
            if n_repeat_x <= 0:
                raise ValueError("n_repeat_x must be positive.")
            if x_grid_1d is not None:
                print("Warning: n_grid_x is provided and will override x_grid_1d.")
            x_grid_1d = np.repeat(np.arange(1, n_grid_x + 1) / (n_grid_x + 1), n_repeat_x)
        elif x_grid_1d is not None: 
            x_grid_1d = np.array(x_grid_1d, dtype=float)
        else: 
            raise ValueError("Either n_grid_x or x_grid_1d must be provided.")
        
        if n_grid_y is not None: 
            if n_grid_y <= 0:
                raise ValueError("n_grid_y must be positive if provided.")
            if n_repeat_y <= 0:
                raise ValueError("n_repeat_y must be positive.")
            if y_grid_1d is not None:
                print("Warning: n_grid_y is provided and will override y_grid_1d.")
            y_grid_1d = np.repeat(np.arange(1, n_grid_y + 1) / (n_grid_y + 1), n_repeat_y)
        elif y_grid_1d is not None: 
            y_grid_1d = np.array(y_grid_1d, dtype=float)
        else: 
            raise ValueError("Either n_grid_y or y_grid_1d must be provided.")

        # generate 2D grid points of molecular bath coords in units of Lx, Ly
        x_grid_2d, y_grid_2d = np.meshgrid(x_grid_1d, y_grid_1d)
        x_grid_2d = np.reshape(x_grid_2d, -1)
        y_grid_2d = np.reshape(y_grid_2d, -1)
        self.n_grid = np.size(x_grid_2d)

        # generate 2D grid points of kx, ky in units of 1/Lx, 1/Ly
        kx_grid_1d = np.pi * np.array([i + 1.0 for i in range(n_mode_x)])
        ky_grid_1d = np.pi * np.array([i + 1.0 for i in range(n_mode_y)])
        kx_grid_2d, ky_grid_2d = np.meshgrid(kx_grid_1d, ky_grid_1d)
        kx_grid_2d = np.reshape(kx_grid_2d, -1)
        ky_grid_2d = np.reshape(ky_grid_2d, -1)

        # construct cavity mode frequency array for all photon dimensions
        omega_parallel = np.reshape(((kx_grid_2d / np.pi * self.delta_omega_x_au) ** 2 + (ky_grid_2d / np.pi * self.delta_omega_y_au) ** 2) ** 0.5, -1)
        print("omega_parallel in cm-1", omega_parallel * AU_TO_CM_INV)
        self.omega_k = (self.frequency_au**2 + omega_parallel**2) ** 0.5
        print("omega_k in cm-1", self.omega_k * AU_TO_CM_INV)
        
        # construct renormalized cavity mode function for each molecular grid point
        self.n_mode = n_mode_x * n_mode_y
        ftilde_k = np.zeros((self.n_mode, self.n_grid, 3), dtype=float)
        for i in range(self.n_grid):
            x, y = x_grid_2d[i], y_grid_2d[i]
            ftilde_k[:, i, 0] = 2.0 * np.cos(kx_grid_2d * x) * np.sin(ky_grid_2d * y)
            ftilde_k[:, i, 1] = 2.0 * np.sin(kx_grid_2d * x) * np.cos(ky_grid_2d * y)

        self.ftilde_k = ftilde_k
        self.varepsilon_k = self.coupling_strength * self.omega_k / np.min(self.omega_k)

        self.x_grid_1d = x_grid_1d
        self.y_grid_1d = y_grid_1d
        abc_x, abc_y = None, None
        self.abc_cutoff = float(abc_cutoff)
        if self.abc_cutoff != 0.00:

            r01 = self.abc_cutoff
            r10 = 1 - self.abc_cutoff

            def abc(grid_1d, r01=0.05, r10=0.95):

                if len(grid_1d) < 3:
                    return np.ones_like(grid_1d)

                grid_1d = np.array(grid_1d)
                r00, r11 = grid_1d[0], grid_1d[-1]

                S = np.zeros_like(grid_1d)
                middle = np.where((grid_1d < r10) & (grid_1d > r01))[0]
                S[middle] = 1

                l_side = np.where((grid_1d < r01) & (grid_1d > r00))[0]
                r_side = np.where((grid_1d < r11) & (grid_1d > r10))[0]

                def smooth(x, l, r, p):
                    frac = (l - r) / (l - x) + (r - l) / (x - r)
                    if p:
                        return 1 / (1 + np.exp(-frac))
                    else:
                        return 1 / (1 + np.exp(frac))

                l_value = np.array(
                    [smooth(i, l=r00, r=r01, p=False) for i in grid_1d[l_side]]
                )
                r_value = np.array(
                    [smooth(i, l=r10, r=r11, p=True) for i in grid_1d[r_side]]
                )
                S[l_side] = l_value
                S[r_side] = r_value

                return S

            self.smooth_x = abc(self.x_grid_1d, r01=r01, r10=r10)
            self.smooth_y = abc(self.y_grid_1d, r01=r01, r10=r10)
            self.smooth_x_2d, self.smooth_y_2d = np.meshgrid(
                self.smooth_x, self.smooth_y
            )
            self.smooth_2d = self.smooth_x_2d * self.smooth_y_2d
            self.smooth_2d = np.diag(np.reshape(self.smooth_2d, -1))
            from scipy.linalg import pinv

            abc_x = (
                self.ftilde_k[:, :, 0] @ self.smooth_2d @ pinv(self.ftilde_k[:, :, 0])
            )
            abc_y = (
                self.ftilde_k[:, :, 1] @ self.smooth_2d @ pinv(self.ftilde_k[:, :, 1])
            )
            self.abc_x = abc_x
            self.abc_y = abc_y

        self.if_abc = (abc_x is not None) and (abc_y is not None)
        print(f"Applying Absorbing Boundary Condition : {self.if_abc}, cutoff: {self.abc_cutoff}")


class MultiModeSimulation(DummyEMSimulation):
    r"""
    Mesoscale cavities coupled to MaxwellLink molecules.

    All quantities are in atomic units.
    """

    def __getattr__(self, name):
        if self.cavity_geometry is not None and hasattr(self.cavity_geometry, name):
            return getattr(self.cavity_geometry, name)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def __init__(
        self,
        dt_au: float = None,
        damping_au: float = 0.0,
        molecules: Optional[Iterable[Molecule]] = None,
        hub: Optional[SocketHub] = None,
        qc_initial: Optional[list] = None,
        pc_initial: Optional[list] = None,
        mu_initial: Optional[list] = None,
        dmudt_initial: Optional[list] = None,
        include_dse: bool = True,
        molecule_half_step: bool = False,
        shift_dipole_baseline: bool = False,
        gauge="dipole",
        cavity_geometry: Optional[object] = None,
        excited_mode_list: Optional[list] = [],
        photon_pulse_drive: Optional[Union[float, Callable[[float], float]]] = None,
        photon_pulse_axis: str = "y",
        excited_grid_list: Optional[list] = [],
        molecule_pulse_drive: Optional[Union[float, Callable[[float], float]]] = None,
        molecule_pulse_axis: str = "y",
        initializer: Optional[object] = DummyInitializer(),
        thermostat: Optional[object] = DummyThermostat(),
    ):
        r"""
        Parameters
        ----------
        dt_au : float
            Simulation time step in atomic units.
        damping_au : float
            Damping constant :math:`\kappa` (a.u.).
        molecules : iterable of Molecule, optional
            Molecules coupled to the cavity.
        hub : :class:`~maxwelllink.sockets.sockets.SocketHub`, optional
            Socket hub shared by all socket-mode molecules.
        qc_initial : np.ndarray, default: None
            Initial cavity field coordinate (a.u.).
        pc_initial : np.ndarray, default: None
            Initial cavity field momentum (a.u.).
        mu_initial : np.ndarray, default: None
            Initial total molecular dipole vector (a.u.).
        dmudt_initial : np.ndarray, default: None
            Initial time derivative of the total molecular dipole vector (a.u.).
        include_dse : bool, default: True
            Include dipole self-energy term in the simulation.
        molecule_half_step : bool, default: True
            Whether to further evaluate molecular info for another half time step.
        shift_dipole_baseline : bool, default: False
            Whether to shift all dipole values using the initial dipole value, so initial dipole value is changed to zero.
            Setting this to True can facilitate simulating strong coupling systems with large permanent dipoles.
        gauge : str, default: "dipole"
            Gauge choice for light-matter coupling: "dipole".
        cavity_geometry : object, optional
            Cavity geometry object that defines the cavity mode structure and light-matter coupling. Must be an instance of a class that implements the necessary attributes and methods (e.g., ``omega_k``, ``varepsilon_k``, ``ftilde_k``, etc.) as used in the equations of motion.
        excited_mode_list : list, optional
            List of excited cavity modes.
        photon_pulse_drive : float or callable, optional
            Constant photon pulse drive term or function ``photon_pulse_drive(t_au)``.
        photon_pulse_axis : str, default: "y"
            pulse axis for the photon pulse.
        excited_grid_list : list, optional
            List of grid point indices that are excited by the molecule pulse. The excitation is applied by adding the molecule pulse drive to the effective electric field at these grid points.
        molecule_pulse_drive : float or callable, optional
            Constant molecule pulse drive or function ``molecule_pulse_drive(t_au)`` that determines the strength of the molecule pulse applied to the excited grid points.
        molecule_pulse_axis : str, default: "y"
            pulse axis for the molecule pulse.
        initializer : object, optional
            An object that provides methods to initialize cavity field coordinates and momenta, as well as molecular dipoles if needed. Must have methods ``position_initializer(omega, q)`` and ``momentum_initializer(p)`` that return initialized positions and momenta based on the cavity frequencies and/or other parameters.
            Two example initializers are provided in ``maxwelllink.tools.harmonic_oscillator_helper``: ``DummyInitializer`` (returns zeros) and ``MaxwellBoltzmannInitializer`` (initializes based on Maxwell-Boltzmann distribution at a given temperature).
        thermostat : object, optional
            An object that provides a method to apply a thermostat to the cavity field momenta. Must have a method ``apply_kick(momentum)`` that takes the current momenta and returns the modified momenta after applying the thermostat kick. Two example thermostats are provided in ``maxwelllink.tools.harmonic_oscillator_helper``: ``DummyThermostat`` (does nothing) and a simple ``LangevinThermostat`` that applies a frictional kick based on a specified temperature and damping constant.
        """

        super().__init__(hub=hub, molecules=molecules)

        if dt_au is None:
            raise ValueError("dt_au must be provided.")
        elif dt_au <= 0.0:
            raise ValueError("dt_au must be positive.")
        self.dt = float(dt_au)
        self.damping = float(damping_au)
        self.gauge = gauge.lower()
        if self.gauge not in ["dipole"]:
            raise ValueError("gauge must be 'dipole'.")

        molecules = list(molecules or [])
        self.wrappers: List[MoleculeMultiModeWrapper] = [
            MoleculeMultiModeWrapper(molecule=m, dt_au=self.dt) for m in molecules
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
        self.cavity_geometry = cavity_geometry

        if mu_initial is None: mu_initial = np.zeros((self.n_grid, 3), dtype=float)
        else : mu_initial = np.array(mu_initial, dtype=float).reshape((self.n_grid, 3))
        if dmudt_initial is None: dmudt_initial = np.zeros((self.n_grid, 3), dtype=float)
        else : dmudt_initial = np.array(dmudt_initial, dtype=float).reshape((self.n_grid, 3))
        if qc_initial is None: qc_initial = np.zeros((self.n_mode, 3), dtype=float)
        else : qc_initial = np.array(qc_initial, dtype=float).reshape((self.n_mode, 3))
        if pc_initial is None: pc_initial = np.zeros((self.n_mode, 3), dtype=float)
        else : pc_initial = np.array(pc_initial, dtype=float).reshape((self.n_mode, 3))

        self.initializer = initializer
        self.thermostat = thermostat
        qc_initial = self.initializer.position_initializer(self.omega_k, qc_initial)
        pc_initial = self.initializer.momentum_initializer(pc_initial)

        self.qc = qc_initial * self.axis
        self.pc = pc_initial * self.axis
        self.dipole = mu_initial * self.axis
        self.dipole_prev = self.dipole.copy()
        self.dmudt = dmudt_initial * self.axis
        self.dmudt_prev = self.dmudt.copy()
        self.dipole_lookahead = self.dipole.copy()
        self.has_dipole_lookahead = False
        self.acceleration = np.zeros((self.n_mode, 3), dtype=float)

        if isinstance(excited_mode_list, list):
            self.excited_mode_list = excited_mode_list
        else:
            raise ValueError("excited_mode_list must be a list of integers specifying the indices of the excited cavity modes.")
        
        self.photon_drive = photon_pulse_drive if photon_pulse_drive is not None else (lambda _: 0.0)
        if isinstance(self.photon_drive, (int, float)):
            const = float(self.photon_drive)
            self.photon_drive = lambda _t, c=const: c
            
        if isinstance(excited_grid_list, list):
            self.excited_grid_list = excited_grid_list
        else:            
            raise ValueError("excited_grid_list must be a list of integers specifying the indices of the excited grid points for molecule pulse.")

        self.molecule_pulse = molecule_pulse_drive if molecule_pulse_drive is not None else (lambda _: 0.0)
        if isinstance(self.molecule_pulse, (int, float)):
            const = float(self.molecule_pulse)
            self.molecule_pulse = lambda _t, c=const: c

        self.photon_pulse_axis = np.array([False, False, False], dtype=bool)
        if "x" in photon_pulse_axis.lower():
            self.photon_pulse_axis[0] = True
        if "y" in photon_pulse_axis.lower():
            self.photon_pulse_axis[1] = True
        if "z" in photon_pulse_axis.lower():
            self.photon_pulse_axis[2] = True

        # we need True in at least one axis
        if not np.any(self.photon_pulse_axis):
            raise ValueError(
                "At least one pulse axis (x, y, or z) must be specified."
            )

        self.molecule_pulse_axis = np.array([False, False, False], dtype=bool)
        if "x" in molecule_pulse_axis.lower():
            self.molecule_pulse_axis[0] = True
        if "y" in molecule_pulse_axis.lower():
            self.molecule_pulse_axis[1] = True
        if "z" in molecule_pulse_axis.lower():
            self.molecule_pulse_axis[2] = True

        # we need True in at least one axis
        if not np.any(self.molecule_pulse_axis):
            raise ValueError(
                "At least one pulse axis (x, y, or z) must be specified."
            )

        self.include_dse = bool(include_dse)
        self.molecule_half_step = bool(molecule_half_step)
        self.shift_dipole_baseline = bool(shift_dipole_baseline)

        if self.shift_dipole_baseline:
            # shift all dipole values using the initial dipole value, so initial dipole value is zero
            self.dipole_baseline = self.dipole.copy()
            self.dipole -= self.dipole_baseline
            self.dipole_prev = self.dipole.copy()
            print(
                "[MultiModeCavity] Shifted dipole baseline by:", self.dipole_baseline
            )

    # ------------------------------------------------------------------
    # Core helpers
    # ------------------------------------------------------------------
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
                "efield_au": efield_vec[w.molecule_id, :],
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

    def _calc_acceleration(
        self, time: float, mu: np.ndarray, qc: np.ndarray
    ) -> np.ndarray:
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
        mu_dot_f = np.einsum("ijk, jk->ik", self.ftilde_k, mu)
        acceleration = (
            - np.einsum("i,ik->ik", self.varepsilon_k, mu_dot_f)
            - np.einsum("i,ik->ik", self.omega_k**2, qc)
        )
        if self.excited_mode_list:
            acceleration[self.excited_mode_list, :] += self.photon_drive(time) * self.photon_pulse_axis
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
            varepsilon_dot_qc -= np.einsum(
                "i,ik,i->ik",
                self.varepsilon_k**2,
                mu_dot_f,
                1.0 / (self.omega_k**2),
                optimize=True,
            )
        efield_vec = np.einsum(
            "ijk,ik,k->jk", self.ftilde_k, varepsilon_dot_qc, self.axis, optimize=True
        )
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
        potential_energy = 0.5 * np.einsum(
            "i,ij->ij", self.omega_k**2, qc**2
        ) + np.einsum("i,ij,ij->ij", self.varepsilon_k, qc, mu_dot_f)
        if self.include_dse:
            potential_energy += 0.5 * np.einsum(
                "i,ij->ij", self.varepsilon_k**2 / self.omega_k**2, mu_dot_f**2
            )

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
                dipole_vec[wrapper.molecule_id, 0] = (
                    latest_data.get("mux_au") * rescaling_factor
                )
                dipole_vec[wrapper.molecule_id, 1] = (
                    latest_data.get("muy_au") * rescaling_factor
                )
                dipole_vec[wrapper.molecule_id, 2] = (
                    latest_data.get("muz_au") * rescaling_factor
                )
        if self.shift_dipole_baseline:
            dipole_vec -= self.dipole_baseline
        return dipole_vec

    def _calc_dipole_lookahead_vec(self):
        """
        Reconstruct force-time dipoles one VV drift ahead from returned JSON.

        Drivers may report ``mu`` at both half a time step after force
        evaluation (``mux_au``/``muy_au``/``muz_au``) and at the force
        evaluation time itself (``mux_m_au``/``muy_m_au``/``muz_m_au``). For
        velocity Verlet position dipoles, ``2 * mu_half - mu_force`` is the
        drifted dipole at the next force-evaluation time, before the next
        E-field kick is known.
        """

        half_keys = ("mux_au", "muy_au", "muz_au")
        force_keys = ("mux_m_au", "muy_m_au", "muz_m_au")
        dipole_vec = np.zeros((self.n_grid, 3), dtype=float)
        has_half_step_shift = False

        if not self.wrappers:
            return dipole_vec, False

        for wrapper in self.wrappers:
            if not wrapper.additional_data_history:
                return dipole_vec, False

            latest_data = wrapper.additional_data_history[-1]
            if not all(k in latest_data for k in half_keys + force_keys):
                return dipole_vec, False

            rescaling_factor = 1.0
            if wrapper.rescaling_factor is not None:
                rescaling_factor = float(wrapper.rescaling_factor)

            mu_half = np.array([latest_data[k] for k in half_keys], dtype=float)
            mu_force = np.array([latest_data[k] for k in force_keys], dtype=float)
            has_half_step_shift = has_half_step_shift or not np.array_equal(
                mu_half, mu_force
            )
            dipole_vec[wrapper.molecule_id, :] = (
                2.0 * mu_half - mu_force
            ) * rescaling_factor

        if self.shift_dipole_baseline:
            dipole_vec -= self.dipole_baseline

        return dipole_vec * self.axis, has_half_step_shift

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
        # modify local e-field for molecules under excitation
        if self.excited_grid_list:
            efield_vec[self.excited_grid_list, :] += self.molecule_pulse(self.time) * self.molecule_pulse_axis

        # Non-socket molecules
        for wrapper in self.non_socket_wrappers:
            wrapper.propagate(efield_vec[wrapper.molecule_id,:])
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
        dmudt = np.zeros((self.n_grid, 3), dtype=float)
        for wrapper in self.wrappers:
            dmudt[wrapper.molecule_id, :] = wrapper.last_amp
        # for dipole gauge we use mu directly instead of dmu/dt
        dipole = self._calc_dipole_vec()
        # we need to filter only the coupling axis
        dipole = dipole * self.axis
        dmudt = dmudt * self.axis

        # print("In Function, Total Dipole vector:", dipole)
        # print("In Function, Total Dipole velocity (dmu/dt):", dmudt)
        return dipole, dmudt

    def _step_dipole_gauge(self, savedata: bool = True, step_idx: Optional[int] = None):
        """
        Advance the simulation by one time step under the dipole gauge.
        """

        # 1. update momentum to half step
        pc_half = self.pc + 0.5 * self.dt * self.acceleration

        # 2. update position to full step
        qc_prev = self.qc.copy()
        self.qc += self.dt * pc_half

        # updating E-field at half step using interpolated dipole
        # this interpolation is not very accurate
        # dipole = self.dipole + 0.5 * self.dt * (1.5 * self.dmudt - 0.5 * self.dmudt_prev)
        # the following expression is accurate to the order of dt^4
        if (
            self.include_dse
            and self.has_dipole_lookahead
            and not self.molecule_half_step
        ):
            dipole = self.dipole_lookahead.copy()
        else:
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
        self.dipole_lookahead, self.has_dipole_lookahead = (
            self._calc_dipole_lookahead_vec()
        )

        # extrapolate to n+1 time step (ONLY NEEDED FOR VELOCITY VERLET MOLECULE PROPAGATION)
        if self.molecule_half_step:
            self.dipole = 2.0 * self.dipole - self.dipole_prev
            self.dmudt = 2.0 * self.dmudt - self.dmudt_prev

        acceleration = self._calc_acceleration(self.time, self.dipole, self.qc)

        # 3. update momentum from half step to full step
        # apply absorbing boundary condition to the cavity field if enabled
        self.pc = pc_half + 0.5 * self.dt * acceleration
        if self.if_abc: 
            self.pc[:,0] = self.pc[:,0] @ self.abc_x
            self.pc[:,1] = self.pc[:,1] @ self.abc_y

        self.pc *= np.exp(-self.damping * self.dt)
        self.pc = self.thermostat.apply_kick(self.pc)

        # 4. update acceleration and time
        self.time += self.dt
        self.acceleration = acceleration.copy()

        self._record_history(savedata=savedata, step_idx=step_idx)

    def _record_history(self, savedata: bool = True, step_idx: Optional[int] = None):
        '''
        Record the history of the simulation at the current time step.
        '''

        record_idx = step_idx // self.record_every_steps

        if savedata and (record_idx <= self.record_max_steps) :

            if self.record_to_disk :
                
                if self.file_format == "npz": 

                    if "time" in self.record_list:
                        self.memmaps["time"][record_idx, 0] = self.time
                    if "qc" in self.record_list:
                        self.memmaps["qc"][record_idx,:,:] = self.qc.copy()
                    if "pc" in self.record_list:
                        self.memmaps["pc"][record_idx,:,:] = self.pc.copy()
                    if "photon_drive" in self.record_list:
                        self.memmaps["photon_drive"][record_idx, 0] = self.photon_drive(self.time)
                    if "molecule_pulse" in self.record_list:
                        self.memmaps["molecule_pulse"][record_idx, 0] = self.molecule_pulse(self.time)
                    if "energy" in self.record_list:
                        self.memmaps["energy"][record_idx, 0] = self._calc_energy(self.pc, self.qc, self.dipole)
                    if "effective_efield" in self.record_list:
                        self.memmaps["effective_efield"][record_idx,:,:] = self._calc_effective_efield(self.qc, self.dipole)
                    if "molecule_response" in self.record_list:
                        self.memmaps["molecule_response"][record_idx,:,:] = self.dmudt.copy()
                    if "molecule_dipole" in self.record_list:
                        self.memmaps["molecule_dipole"][record_idx,:,:] = self.dipole.copy()

                    if record_idx % 1000 == 0:
                        for mm in self.memmaps.values():
                            mm.flush()
                        print(f"[MultiModeCavity] Flushed history data to {self.filename} at step {step_idx}.")
                
                else : # h5 format

                    if "time" in self.record_list:
                        self.h5_file["time"][record_idx, 0] = self.time
                    if "qc" in self.record_list:
                        self.h5_file["qc"][record_idx,:,:] = self.qc.copy()
                    if "pc" in self.record_list:
                        self.h5_file["pc"][record_idx,:,:] = self.pc.copy()
                    if "photon_drive" in self.record_list:
                        self.h5_file["photon_drive"][record_idx, 0] = self.photon_drive(self.time)
                    if "molecule_pulse" in self.record_list:
                        self.h5_file["molecule_pulse"][record_idx, 0] = self.molecule_pulse(self.time)
                    if "energy" in self.record_list:
                        self.h5_file["energy"][record_idx, 0] = self._calc_energy(self.pc, self.qc, self.dipole)
                    if "effective_efield" in self.record_list:
                        self.h5_file["effective_efield"][record_idx,:,:] = self._calc_effective_efield(self.qc, self.dipole)
                    if "molecule_response" in self.record_list:
                        self.h5_file["molecule_response"][record_idx,:,:] = self.dmudt.copy()
                    if "molecule_dipole" in self.record_list:
                        self.h5_file["molecule_dipole"][record_idx,:,:] = self.dipole.copy()

                    if record_idx % 1000 == 0:
                        self.h5_file.flush()
                        print(f"[MultiModeCavity] Flushed history data to {self.filename} at step {step_idx}.")

            else :
                if "time" in self.record_list:
                    self.time_history.append(self.time)
                if "qc" in self.record_list:
                    self.qc_history.append(self.qc.copy())
                if "pc" in self.record_list:
                    self.pc_history.append(self.pc.copy())
                if "photon_drive" in self.record_list:
                    self.photon_drive_history.append(self.photon_drive(self.time))
                if "molecule_pulse" in self.record_list:
                    self.molecule_pulse_history.append(self.molecule_pulse(self.time))
                if "energy" in self.record_list:
                    self.energy_history.append(self._calc_energy(self.pc, self.qc, self.dipole))
                if "molecule_response" in self.record_list:
                    self.molecule_response_history.append(self.dmudt.copy())
                if "effective_efield" in self.record_list:
                    self.effective_efield_history.append(self._calc_effective_efield(self.qc, self.dipole))
                if "molecule_dipole" in self.record_list:   
                    self.molecule_dipole_history.append(self.dipole.copy())

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def step(self, savedata: bool = True, step_idx: Optional[int] = None):
        """
        Advance the simulation by one time step.
        """
        if self.gauge == "dipole":
            self._step_dipole_gauge(savedata=savedata, step_idx=step_idx)
        else:
            raise ValueError("gauge must be either 'dipole' or 'velocity'.")

    def storage_initialization(self,
                               steps: Optional[int] = None,
                               record_history: bool = True,
                               record_to_disk: bool = False,
                               disk_folder_address: Optional[str] = None,
                               npz_filename: Optional[str] = None,
                               h5_filename: Optional[str] = None,
                               record_max_steps: Optional[int] = None,
                               record_every_steps: int = 1,
                               record_list: Optional[list] = None,):
        '''
        Initialize storage for recording simulation history.        
        '''

        self.record_history = bool(record_history)
        self.record_to_disk = bool(record_to_disk)
        self.disk_folder_address = disk_folder_address

        if npz_filename is not None and h5_filename is not None:
            raise ValueError("Only one of npz_filename and h5_filename can be provided.")
        elif npz_filename is None and h5_filename is None and record_to_disk:
            raise ValueError("Either npz_filename or h5_filename must be provided when record_to_disk is True.")
        elif npz_filename is not None:
            self.filename = npz_filename
            self.file_format = "npz"
        elif h5_filename is not None:
            self.filename = h5_filename
            self.file_format = "h5"

        if record_every_steps < 1 or isinstance(record_every_steps, int) == False:
            raise ValueError("record_every_steps must be a positive integer.")
        else : self.record_every_steps = int(record_every_steps)

        not_record = (record_list == []) or (record_list is None)
        if not_record :
            self.record_history = False
            self.record_to_disk = False

        if isinstance(record_list, list) == False and (not_record == False) :
            raise ValueError("record_list must be a list or None.")

        if not_record :
            self.record_list = []
        else :
            for item in record_list:
                if item not in ["all", "time", "qc", "pc", "photon_drive", "molecule_pulse", "energy", "effective_efield", "molecule_response", "molecule_dipole"]:
                    raise ValueError(f"Invalid record_list item: {item}. Must be one of 'all', 'time', 'qc', 'pc', 'photon_drive', 'molecule_pulse', 'energy', 'effective_efield', 'molecule_response', 'molecule_dipole'.")
            
            if record_list == ["all"]: 
                self.record_list = ["time", "qc", "pc", "photon_drive", "molecule_pulse", "energy", "effective_efield", "molecule_response", "molecule_dipole"]
            else : 
                self.record_list = record_list

        if record_max_steps is None:
            self.record_max_steps = steps // self.record_every_steps
        else:
            self.record_max_steps = int(record_max_steps)
            if self.record_max_steps < 0:
                raise ValueError("record_max_steps must be non-negative or None.")
        
        if self.record_history:
            
            if self.record_to_disk and disk_folder_address is not None:

                self.dim_dict = {"time": 1,
                    "qc": self.qc.shape, 
                    "pc": self.pc.shape, 
                    "photon_drive": 1, 
                    "molecule_pulse": 1, 
                    "energy": 1, 
                    "effective_efield": self.dmudt.shape, 
                    "molecule_response": self.dmudt.shape, 
                    "molecule_dipole": self.dmudt.shape}

                if self.file_format == "npz": 

                    TEMP_DIR = os.path.join(disk_folder_address, "temp_memmap")
                    self.temp_dir = TEMP_DIR
                    if os.path.exists(TEMP_DIR):
                        print(f"[MultiModeCavity] Temporary directory {TEMP_DIR} already exists. Deleting and recreating it.")
                        shutil.rmtree(TEMP_DIR)
                    os.makedirs(TEMP_DIR, exist_ok=True)
                    self.memmaps = {}
                    self.temp_files = {}

                    for name in self.record_list:
                        dim = self.dim_dict[name]
                        filename = os.path.join(TEMP_DIR, f"temp_{name}.bin")
                        self.temp_files[name] = filename
                        full_shape = (self.record_max_steps,) + (dim if isinstance(dim, tuple) else (dim,))
                        memmap_obj = np.memmap(filename, dtype=np.float64, mode='w+', shape=full_shape)
                        self.memmaps[name] = memmap_obj
                
                else : # h5 format
                    assert self.file_format == "h5"
                    import h5py
                    self.h5_file = h5py.File(os.path.join(disk_folder_address, self.filename), 'w')
                    self.datasets = {}
                    for name in self.record_list:
                        dim = self.dim_dict[name]
                        shape = (self.record_max_steps,) + (dim if isinstance(dim, tuple) else (dim,))
                        self.datasets[name] = self.h5_file.create_dataset(name, shape=shape, dtype=np.float64, maxshape=shape)

            elif self.record_to_disk and self.disk_folder_address is None:
                raise ValueError("disk_folder_address must be provided when record_to_disk is True.")
            
            else:
                
                if "time" in self.record_list:
                    self.time_history = []
                if "qc" in self.record_list:
                    self.qc_history = []
                if "pc" in self.record_list:
                    self.pc_history = []
                if "photon_drive" in self.record_list:
                    self.photon_drive_history = []
                if "molecule_pulse" in self.record_list:
                    self.molecule_pulse_history = []
                if "energy" in self.record_list:
                    self.energy_history = []
                if "effective_efield" in self.record_list:
                    self.effective_efield_history = []
                if "molecule_response" in self.record_list:
                    self.molecule_response_history = []
                if "molecule_dipole" in self.record_list:
                    self.molecule_dipole_history = []
        
        print(f"[MultiModeCavity] Recording history: {self.record_history}, to disk: {self.record_to_disk}, record_every_steps: {self.record_every_steps}, record_max_steps: {self.record_max_steps}")
        print(f"[MultiModeCavity] Recording fields : {self.record_list if self.record_to_disk else 'none'}")
        print(f"[MultiModeCavity] Recording address: {self.disk_folder_address if self.record_to_disk else 'N/A'}, filename: {self.filename if self.record_to_disk else 'N/A'}")
        print(f"[MultiModeCavity] Temporary folder : {self.temp_dir if self.record_to_disk and self.file_format == 'npz' else 'N/A'}")

    def storage_finalization(self):
        '''
        Finalize storage for recording simulation history.        
        '''

        if self.record_history:

            if self.record_to_disk and self.disk_folder_address is not None :
                
                if self.file_format == "npz":

                    for mm in self.memmaps.values():
                        mm.flush()

                    data_for_npz = {}
                    for name in self.record_list:
                        dim = self.dim_dict[name]
                        full_shape = (self.record_max_steps,) + (dim if isinstance(dim, tuple) else (dim,))
                        filename = self.temp_files[name]
                        mmap_ro = np.memmap(filename, dtype=np.float64, mode='r', shape=full_shape)
                        data_for_npz[name] = mmap_ro

                    npz_path = os.path.join(self.disk_folder_address, self.filename)
                    np.savez_compressed(npz_path, **data_for_npz)
                    print(f"[MultiModeCavity] Results saved to {npz_path}")
                    data_for_npz.clear()
                    shutil.rmtree(self.temp_dir)
                    print(f"[MultiModeCavity] Temporary files at {self.temp_dir} deleted.")

                else : # h5 format
                    
                    self.h5_file.close()
                    print(f"[MultiModeCavity] Results saved to {os.path.join(self.disk_folder_address, self.filename)}")

    def run(self, 
            until: Optional[float] = None, 
            steps: Optional[int] = None,
            record_history: bool = True,
            record_to_disk: bool = False,
            disk_folder_address: Optional[str] = None,
            npz_filename: Optional[str] = None,
            h5_filename: Optional[str] = None,
            record_max_steps: Optional[int] = None,
            record_every_steps: int = 1,
            record_list: Optional[list] = None,
            ):
        """
        Run the simulation for a specified duration or number of steps.

        Parameters
        ----------
        until : float, optional
            Total simulation time (a.u.). ``steps`` must be ``None``.
        steps : int, optional
            Number of steps to execute. ``until`` must be ``None``,
        record_history : bool, default: True
            Record time, field, velocity, drive, and molecular response histories.
        record_to_disk : bool, default: False
            Whether to save the history data to disk in HDF5 format. If False, the history data will be stored in memory.
        disk_folder_address : str, optional
            Folder path for saving history data when ``record_to_disk`` is True.
        npz_filename : str, optional
            Name of the .npz file to save the simulation results to when ``record_to_disk`` is True.
        h5_filename : str, optional
            Name of the .h5 file to save the simulation results to when ``record_to_disk`` is True.
        record_max_steps : int, optional
            Upper bound for the on-disk history length when ``record_to_disk`` is
            True. If ``None``, the HDF5 datasets grow dynamically.
        record_every_steps : int, default: 1
            Number of steps to record data every ``record_every_steps`` steps.
        record_list : list of str, optional
            List of specific data fields to record. Possible values include "time", "qc", "pc", "drive", "energy", "effective_efield", "molecule_response",  "molecule_dipole". If "all", all fields will be recorded. If None, none will be recorded.
        """

        if (until is None) == (steps is None):
            raise ValueError("Specify exactly one of 'until' or 'steps'.")

        if until is not None:
            if until < self.time:
                return
            steps = int(np.ceil((until - self.time) / self.dt))

        self.storage_initialization(steps=steps,
                                    record_history=record_history,
                                    record_to_disk=record_to_disk,
                                    disk_folder_address=disk_folder_address,
                                    npz_filename=npz_filename,
                                    h5_filename=h5_filename,
                                    record_max_steps=record_max_steps,
                                    record_every_steps=record_every_steps,
                                    record_list=record_list)

        start_time = time.perf_counter()
        previous_time = start_time
        for idx in range(int(steps)):
            
            if self.record_history :
                if self.record_every_steps >= 2:
                    if idx % self.record_every_steps == 0 : self.step(savedata=True, step_idx=idx)
                    else : self.step(savedata=False, step_idx=idx)
                else : self.step(savedata=True, step_idx=idx)
            else : self.step(savedata=False, step_idx=idx)

            if (idx + 1) % 1000 == 0:

                current_time = time.perf_counter()
                avg_time_per_step = (current_time - previous_time) / 1000.0
                previous_time = current_time

                elapsed = current_time - start_time
                remaining = (elapsed / (idx + 1)) * (steps - (idx + 1))
                print(
                    f"[MultiModeCavity] Completed {idx + 1}/{steps} [{(idx + 1) / steps * 100:.1f}%] steps, time/step: {avg_time_per_step:.2e} seconds, remaining time: {remaining:.2f} seconds."
                )

        self.storage_finalization()
        
        # close the hub
        if self.hub is not None:
            if am_master():
                self.hub.stop()
