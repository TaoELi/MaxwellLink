# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

from typing import Optional

import numpy as np
from pyparsing import Optional
from scipy.linalg import expm
import os
from ....tools.harmonic_oscillator_helper import LangevinThermostat

try:
    from .dummy_model import DummyModel
except:
    from dummy_model import DummyModel


class LorentzBathModel(DummyModel):
    """
    A Lorentzian harmonic oscillator coupled to a bath of classical oscillators.

    This class implements a Lorentz-Bath model for classical molecular dynamics,
    which can be integrated with the MaxwellLink framework. The Lorentz-Bath model 
    represents the overall response of a molecular ensemble as a collective bright
    mode (represented by a single harmonic oscillator, or the Lorentzian oscillator)
    coupled to a set of independent dark modes (represented by a bath of classical oscillators). 

    By tuning the bath density distribution and couplings, this model can be used to 
    simulate the interplay between polaritons and molecular dark modes, which is a key aspect of 
    polaritonic chemistry.

    The Hamiltonian for this Lorentz-Bath model is given by:

    :math:`H = \\frac{1}{2} \\left(p_B - \\sum_{j\\in \\rm{bath}} k_j q_j \\right)^2 +  \\frac{1}{2} \\omega^2  q_B^2 - \\mu_{0} q_B \\cdot E(t) + \\sum_{j\\in \\rm{bath}} \\left( \\frac{1}{2} \\omega_j^2 q_j^2 + \\frac{1}{2} p_j^2\\right)`

    If the anharmonic bath is used, the bath potential becomes
    :math:`V(q_j) = \\frac{1}{2} \\omega_j^2 q_j^2 - \\omega_j^2 \\sqrt{\\frac{\\chi}{2}} q_j^3 + \\frac{7}{12}\\omega_j^2\\chi q_j^4`,
    where :math:`\\chi` is the anharmonicity parameter of the bath.
    
    -----
    This model provides an alternative way to understand the interplay between polaritons and
    molecular dark modes, which should be more straightforward and cheaper to understand than directly
    simulating individual molecules coupled to the EM field (such as LAMMPS+EM simulations).

    TODO: 1) Adding the anharmonicity and thermal effects to the Lorentz-Bath model.
    TODO: 2) Adding simplified settings of the bath forms.
    """

    def __init__(
        self,
        omega: float = 2.4188843e-1,
        mu0: float = 1.870819866e2,
        orientation: int = 2,
        # convenient way to define bath
        num_bath: int=None, 
        bath_width: float=None, 
        bath_form: str=None, 
        bath_dephasing: float=0.0,
        bath_relaxation: float=0.0,
        bath_anharmonicity: float=0.0,
        # direct way to define bath
        omega_bath: list = None,
        k_bath: list = None,
        # initial conditions
        p_initial: float = 0.0,
        q_initial: float = 0.0,
        p_bath_initial: list = None,
        q_bath_initial: list = None,
        # optional thermostats 
        langevin_tau_au: float=None, 
        initializer: str=None,
        temperature_au: float=0.0,
        random_seed: int=114514,
        # checkpoint and restart settings
        checkpoint: bool = False,
        restart: bool = False,
        verbose: bool = False,
    ):
        """
        Initialize the necessary parameters for the SHO classical molecular dynamics model.

        Parameters
        ----------
        omega : float, default: 2.4188843e-1
            Transition frequency in atomic units (a.u.). Default is ``2.4188843e-1``
            a.u. (``1.0`` in MEEP units with ``[T]=0.1 fs``).
        mu0 : float, default: 1.870819866e2
            Dipole-coordinate coupling prefactor in atomic units (a.u.). The
            instantaneous dipole is :math:`\\mu(t) = \\mu_{0}\\, q(t)`. Default
            is ``1.870819866e2`` a.u. (``0.1`` in MEEP units with ``[T]=0.1 fs``).
        orientation : int, default: 2
            Orientation of the dipole moment; can be ``0`` (x), ``1`` (y), or ``2`` (z).
        num_bath : int, optional
            Number of bath oscillators. If not provided, the bath will not be defined via this convenient way.
        bath_width : float, optional
            Frequency width of the bath oscillators in atomic units (a.u.). If not provided, the bath will not be defined via this convenient way.
        bath_form : str, optional
            Form of the bath distribution, which can be "uniform", "gaussian", or "lorentzian". If not provided, the bath will not be defined via this convenient way.
        bath_dephasing : float, optional
            Dephasing rate from the bright state to the dark modes (bath oscillators) in atomic units (a.u.), which can be used to determine the coupling coefficients 
            between the Lorentzian oscillator and the bath oscillators. If not provided, the bath will not be defined via this convenient way.
        bath_relaxation : float, optional
            The direct relaxation rate of the bath oscillators in atomic units (a.u.). If not provided, the bath oscillators will not have relaxation.
        bath_anharmonicity : float, optional
            The anharmonicity of the bath oscillators in atomic units (a.u.). For realistic molecules, this parameter can be set as 1% of the fundamental frequency
        omega_bath : list of float, shape (N_bath,), optional
            Transition frequencies of the bath oscillators in atomic units (a.u.).
        k_bath : list of float, shape (N_bath,), optional
            Coupling coefficients between the Lorentzian oscillator and the bath oscillators in atomic units (a.u.).
        p_initial : float, default: 0.0
            Initial momentum of the oscillator.
        q_initial : float, default: 0.0
            Initial position of the oscillator.
        p_bath_initial : list of float, shape (N_bath,), optional
            Initial momenta of the bath oscillators.
        q_bath_initial : list of float, shape (N_bath,), optional
            Initial positions of the bath oscillators.
        langevin_tau_au : float, optional
            Damping time constant for the Langevin thermostat in atomic units (a.u.). Default is None, not applying the thermostat.
        initializer : str, optional
            Type of initializer to use. Can be "maxwell_boltzmann".
        temperature_au : float, optional
            Temperature in atomic units (a.u.) for the thermostat and initializer. Must be positive.
        random_seed : int, optional
            Random seed for reproducible results in the thermostat and initializer.
        checkpoint : bool, default: False
            Whether to enable checkpointing.
        restart : bool, default: False
            Whether to restart from a checkpoint if available.
        verbose : bool, default: False
            Whether to print verbose output.
        """

        # Initialize the base class (DummyModel)
        super().__init__(verbose, checkpoint, restart)

        # Initialize SHO-specific parameters
        self.omega = omega  # transition frequency in a.u.
        self.dipole_moment = mu0  # dipole-coordinate coupling prefactor mu0 in a.u.
        self.orientation = orientation  # orientation of the dipole moment
        self.orientation_idx = int(orientation)
        if self.orientation_idx < 0 or self.orientation_idx > 2:
            raise ValueError("Orientation must be 0 (x), 1 (y), or 2 (z).")

        self.p = p_initial  # initial momentum of the oscillator
        self.q = q_initial  # initial position of the oscillator
        self.acceleration = 0.0 # acceleration of the oscillator
        self.p_half = 0.0  # half time step momentum of the oscillator

        self.bath_relaxation = float(bath_relaxation)
        self.bath_anharmonicity = float(bath_anharmonicity)
        # Direct way to define the bath, which has higher priority than the convenient way
        if omega_bath is not None and k_bath is not None:
            if len(omega_bath) != len(k_bath):
                raise ValueError("The length of omega_bath and k_bath must be the same.")
            self.omega_bath = np.array(omega_bath)  # transition frequencies of the bath oscillators
            self.k_bath = np.array(k_bath)  # coupling coefficients between the Lorentzian oscillator and the bath oscillators
        # Convenient way to define the bath, which has lower priority than the direct way
        elif num_bath is not None and bath_width is not None and bath_form is not None:
            self.num_bath = int(num_bath)
            if self.num_bath <= 1:
                raise ValueError("num_bath must be greater than 1 to define a bath.")
            self.bath_width = float(bath_width)
            self.bath_form = bath_form.lower()
            self.bath_dephasing = float(bath_dephasing)
            # consider implementing different bath forms
            self.omega_bath = np.linspace(self.omega - self.bath_width*0.5, self.omega + self.bath_width*0.5, self.num_bath)
            omega_bath_relative = self.omega_bath - self.omega
            domega = self.omega_bath[1] - self.omega_bath[0]
            k = np.sqrt(2.0 * domega / np.pi * self.bath_dephasing) 
            if self.bath_form == "uniform":
                self.k_bath = np.ones(self.num_bath) * k
            elif self.bath_form == "lorentzian":
                gamma = self.bath_dephasing 
                self.k_bath = k * ( gamma**2 / (gamma**2 + (omega_bath_relative)**2) )**0.5
            elif self.bath_form == "gaussian":
                gamma = self.bath_dephasing 
                self.k_bath = k * np.exp(-0.25 * (omega_bath_relative / gamma)**2)
            else:
                raise ValueError("Unsupported bath form. Supported forms are: uniform, gaussian, lorentzian.")
        
        # useful parameters for updating equations of motion
        self.omega_bath_squared = self.omega_bath**2
        self.omega_anharm_coeff1 = -3.0 * self.omega_bath_squared * np.sqrt(self.bath_anharmonicity / 2.0)
        self.omega_anharm_coeff2 = 7.0/3.0 * self.omega_bath_squared * self.bath_anharmonicity

        if p_bath_initial is None:
            self.p_bath = np.zeros_like(self.omega_bath)
        else:
            self.p_bath = np.array(p_bath_initial)
        if q_bath_initial is None:
            self.q_bath = np.zeros_like(self.omega_bath)
        else:
            self.q_bath = np.array(q_bath_initial)
        self.acceleration_bath = np.zeros_like(self.omega_bath)  # accelerations of the bath oscillators

        # now let's provide initializer 
        self.random_seed = random_seed
        self.temperature_au = temperature_au
        if initializer is not None:
            initializer = initializer.lower()
            if initializer == "maxwell_boltzmann": 
                self.rng = np.random.default_rng(self.random_seed)  
                self.p = float(self.rng.normal(scale=np.sqrt(self.temperature_au), size=1)[0])   
                self.q = float(self.rng.normal(scale=np.sqrt(self.temperature_au) / self.omega, size=1)[0])
                self.p_bath = self.rng.normal(scale=np.sqrt(self.temperature_au), size=self.omega_bath.shape)
                self.q_bath = self.rng.normal(scale=np.sqrt(self.temperature_au) / self.omega_bath, size=self.omega_bath.shape)          
            else:
                raise ValueError("Unsupported initializer. Supported initializers are: maxwell_boltzmann.")

        self.thermostat = None
        self.langevin_tau_au = langevin_tau_au 
        self.thermostat = None
            
        if self.verbose:
            print(f"[molecule ID {self.molecule_id}] Initialized Lorentzian-bath model with bath oscillators.")

        # optional, checking whether the driver can be paused and resumed properly
        self.restarted = False

        # store dipole moments and energies during rt-tddft propagation
        self.dipole_vec = None
        self.energy = None
        self.energy_lorentz = None
        self.energy_bath = None

    # -------------- heavy-load initialization (at INIT) --------------

    def initialize(self, dt_new, molecule_id):
        """
        Set the time step and molecule ID for this SHO model, and provide
        additional initialization for the SHO.

        Parameters
        ----------
        dt_new : float
            The new time step in atomic units (a.u.).
        molecule_id : int
            The ID of the molecule.
        """

        self.dt = float(dt_new)
        self.molecule_id = int(molecule_id)

        if self.langevin_tau_au is not None and self.temperature_au > 0.0:
            self.thermostat = LangevinThermostat(temperature_au=self.temperature_au, dt_au=self.dt, tau_au=self.langevin_tau_au, random_seed=self.random_seed)

        # Prepare checkpoint filename
        self.checkpoint_filename = "lorentz_bath_checkpoint_id_%d.npz" % self.molecule_id

        # Consider whether to restart from a checkpoint. We do this here because this function
        # is called in the driver during the INIT stage of the socket communication.
        if self.restart and self.checkpoint:
            self._reset_from_checkpoint(self.molecule_id)
            self.restarted = True

    # -------------- one FDTD step under E-field --------------

    def propagate(self, effective_efield_vec):
        """
        Propagate the Lorentzian-bath model dynamics given the effective electric field
        vector.

        Parameters
        ----------
        effective_efield_vec : array-like of float, shape (3,)
            Effective electric field vector in the form ``[E_x, E_y, E_z]``.
        """

        if self.verbose:
            print(
                f"[molecule ID {self.molecule_id}] Time: {self.t:.4f} a.u., receiving effective_efield_vec: {effective_efield_vec}"
            )
        int_ep = effective_efield_vec[self.orientation_idx] * self.dipole_moment

        # update the position and momentum for one time step using velocity verlet 
        # p updated to half time step
        self.p += 0.5 * self.acceleration * self.dt
        self.p_bath += 0.5 * self.acceleration_bath * self.dt

        # bath half-time drift
        self.q_bath += 0.5 * self.p_bath * self.dt

        # couple bath to the Lorentzian oscillator using the congugate momentum
        Phi = self.p - np.dot(self.k_bath, self.q_bath)
        # q updated to full time step
        self.q += Phi * self.dt
        # bath receives the coupling from the Lorentzian oscillator
        self.p_bath += self.k_bath * Phi * self.dt

        # bath full-time drift
        self.q_bath += 0.5 * self.p_bath * self.dt

        # force evaluation time [the same time as the E-field time]
        self.acceleration = -self.omega**2 * self.q + int_ep
        self.acceleration_bath = -self.omega_bath_squared * self.q_bath
        if self.bath_anharmonicity != 0.0:
            self.acceleration_bath += -self.omega_anharm_coeff1 * self.q_bath**2 - self.omega_anharm_coeff2 * self.q_bath**3

        # p also updated to the full time step, the same as the E-field time
        self.p += 0.5 * self.acceleration * self.dt
        self.p_bath += 0.5 * self.acceleration_bath * self.dt

        # thermostat
        if self.thermostat is not None:
            self.p = self.thermostat.apply_kick(self.p)
            self.p_bath = self.thermostat.apply_kick(self.p_bath)

        # enforce direct bath relaxation if bath_relaxation is provided
        if self.bath_relaxation > 0.0:
            self.p_bath *= np.exp(-self.bath_relaxation * self.dt)

        # we expect to return dmu/dt at half a time step after the E-field time
        p_lorentz_half = self.p + 0.5 * self.acceleration * self.dt
        p_bath_half = self.p_bath + 0.5 * self.acceleration_bath * self.dt
        q_bath_half = self.q_bath + 0.5 * p_bath_half * self.dt
        p_shifted_half = np.dot(self.k_bath, q_bath_half)
        self.p_half = p_lorentz_half - p_shifted_half  # coupling to the bath at half time step
        # we also expect to return mu at half a time step after the E-field time
        self.q_half = self.q + 0.5 * self.p_half * self.dt

        # update current time in a.u.
        self.t += self.dt

        # store the information for returning back to the SocketHub
        dipole = self.dipole_moment * self.q_half
        dip_vec = np.zeros(3)
        dip_vec[self.orientation_idx] = dipole

        self.dipole_vec = dip_vec
        # Report the Hamiltonian at the same half step used for the returned dipole response.
        self.energy = (
            0.5 * self.p_half**2
            + 0.5 * self.omega**2 * self.q_half**2
            + np.sum(0.5 * self.omega_bath**2 * q_bath_half**2 + 0.5 * p_bath_half**2)
        )
        self.energy_lorentz = (
            0.5 * p_lorentz_half**2
            + 0.5 * self.omega**2 * self.q_half**2
            + p_lorentz_half * p_shifted_half
            + 0.5 * p_shifted_half**2
        )
        self.energy_bath = np.sum(0.5 * self.omega_bath**2 * q_bath_half**2 + 0.5 * p_bath_half**2)

    def calc_amp_vector(self):
        """
        Update the source amplitude vector after propagating this molecule for one
        time step.

        Returns
        -------
        numpy.ndarray of float, shape (3,)
            Amplitude vector in the form
            :math:`[\\mathrm{d}\\mu_x/\\mathrm{d}t,\\ \\mathrm{d}\\mu_y/\\mathrm{d}t,\\ \\mathrm{d}\\mu_z/\\mathrm{d}t]`.
        """

        # analytical expression for dmu/dt in a SHO
        amp = self.p_half * self.dipole_moment
        amp_vec = np.zeros(3)
        amp_vec[self.orientation_idx] = amp
        if self.verbose:
            print(
                f"[molecule ID {self.molecule_id}] Time: {self.t:.4f} a.u., Dipole: {self.dipole_vec[-1]}, Energy: {self.energy:.6f} a.u., returning Amp: {amp_vec}"
            )
        return amp_vec

    # ------------ optional operation / checkpoint --------------

    def append_additional_data(self):
        """
        Append additional data to be sent back to MaxwellLink.

        The data can be retrieved by the user via the Python interface:
        ``maxwelllink.SocketMolecule.additional_data_history``, where
        ``additional_data_history`` is a list of dictionaries.

        Returns
        -------
        dict
            A dictionary containing additional data.
        """

        data = {}
        data["time_au"] = self.t
        data["energy_au"] = self.energy if self.energy is not None else 0.0
        data["energy_lorentz_au"] = self.energy_lorentz if self.energy_lorentz is not None else 0.0
        data["energy_bath_au"] = self.energy_bath if self.energy_bath is not None else 0.0
        data["mux_au"] = self.dipole_vec[0] if self.dipole_vec is not None else 0.0
        data["muy_au"] = self.dipole_vec[1] if self.dipole_vec is not None else 0.0
        data["muz_au"] = self.dipole_vec[2] if self.dipole_vec is not None else 0.0
        data["mux_m_au"] = 0.0
        data["muy_m_au"] = 0.0
        data["muz_m_au"] = 0.0
        data[["mux_m_au", "muy_m_au", "muz_m_au"][self.orientation_idx]] = (
            self.dipole_moment * self.q
        )
        data["p_au"] = self.p
        data["q_au"] = self.q
        return data

    def _dump_to_checkpoint(self):
        """
        Dump the internal state of the model to a checkpoint.

        Notes
        -----
        ``self.checkpoint_filename`` includes ``molid`` at ``self.initialize()``.
        """

        np.savez(self.checkpoint_filename, time=self.t, p=self.p, q=self.q, 
                 acceleration=self.acceleration, p_bath=self.p_bath, q_bath=self.q_bath,
                 acceleration_bath=self.acceleration_bath)

    def _reset_from_checkpoint(self):
        """
        Reset the internal state of the model from a checkpoint.
        """
        if not os.path.exists(self.checkpoint_filename):
            # No checkpoint file found means this driver has not been paused or terminated abnormally
            # so we just start fresh.
            print(
                "[checkpoint] No checkpoint file found for molecule ID %d, starting fresh."
                % self.molecule_id
            )
        else:
            data = np.load(self.checkpoint_filename)
            self.p = float(data["p"])
            self.q = float(data["q"])
            self.acceleration = float(data["acceleration"])
            self.t = float(data["time"])
            self.p_bath = data["p_bath"]
            self.q_bath = data["q_bath"]
            self.acceleration_bath = data["acceleration_bath"]

    def _snapshot(self):
        """
        Return a snapshot of the internal state for propagation.

        Returns
        -------
        dict
            A dictionary containing the snapshot of the internal state.
        """

        snapshot = {
            "time": self.t,
            "p": self.p,
            "q": self.q,
            "acceleration": self.acceleration,
            "p_bath": self.p_bath,
            "q_bath": self.q_bath,
            "acceleration_bath": self.acceleration_bath,
        }
        return snapshot

    def _restore(self, snapshot):
        """
        Restore the internal state from a snapshot.

        Parameters
        ----------
        snapshot : dict
            A dictionary containing the snapshot of the internal state.
        """

        self.t = snapshot["time"]
        self.p = snapshot["p"]
        self.q = snapshot["q"]
        self.acceleration = snapshot["acceleration"]
        self.p_bath = snapshot["p_bath"]
        self.q_bath = snapshot["q_bath"]
        self.acceleration_bath = snapshot["acceleration_bath"]
