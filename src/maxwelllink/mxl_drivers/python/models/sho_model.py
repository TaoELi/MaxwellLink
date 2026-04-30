# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

import numpy as np
from scipy.linalg import expm
import os

try:
    from .dummy_model import DummyModel
except:
    from dummy_model import DummyModel


class SHOModel(DummyModel):
    """
    A simple harmonic oscillator (SHO) classical molecular dynamics model.

    This class implements a SHO model for classical molecular dynamics,
    which can be integrated with the MaxwellLink framework. The SHO model is
    characterized by its frequency, dipole moment, and orientation of
    the dipole moment. The velocity verlet algorithm is used for integration.

    The Hamiltonian for this SHO is given by:
    :math:`H = \\frac{1}{2} m \\omega^2 q^2 + \\frac{1}{2m} p^2 - \\mu_{0} q \\cdot E`

    Notes
    -----
    Implementing this class is mostly for demonstration purposes. Users who want
    to enjoy advanced classical molecular dynamics simulations should use 
    the **LAMMPS** or **DFTB+** socket driver, or the **ASE** python driver instead.
    """

    def __init__(
        self,
        omega: float = 2.4188843e-1,
        mu0: float = 1.870819866e2,
        orientation: int = 2,
        p_initial: float = 0.0,
        q_initial: float = 0.0,
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
        p_initial : float, default: 0.0
            Initial momentum of the oscillator.
        q_initial : float, default: 0.0
            Initial position of the oscillator.
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

        # optional, checking whether the driver can be paused and resumed properly
        self.restarted = False

        # store dipole moments and energies during rt-tddft propagation
        self.dipole_vec = None
        self.energy = None

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

        # Prepare checkpoint filename
        self.checkpoint_filename = "sho_checkpoint_id_%d.npz" % self.molecule_id

        # Consider whether to restart from a checkpoint. We do this here because this function
        # is called in the driver during the INIT stage of the socket communication.
        if self.restart and self.checkpoint:
            self._reset_from_checkpoint(self.molecule_id)
            self.restarted = True

    # -------------- one FDTD step under E-field --------------

    def propagate(self, effective_efield_vec):
        """
        Propagate the SHO classical molecular dynamics given the effective electric field
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
        # q updated to full time step
        self.q += self.p * self.dt
        # force evaluation time [the same time as the E-field time]
        self.acceleration = -self.omega**2 * self.q + int_ep
        # p also updated to the full time step, the same as the E-field time
        self.p += 0.5 * self.acceleration * self.dt

        # we expect to return dmu/dt at half a time step after the E-field time
        self.p_half = self.p + 0.5 * self.acceleration * self.dt
        # we also expect to return mu at half a time step after the E-field time
        self.q_half = self.q + 0.5 * self.p_half * self.dt

        # update current time in a.u.
        self.t += self.dt

        # store the information for returning back to the SocketHub
        dipole = self.dipole_moment * self.q_half
        dip_vec = np.zeros(3)
        dip_vec[self.orientation_idx] = dipole

        self.dipole_vec = dip_vec
        self.energy = 0.5 * self.omega**2 * self.q_half**2 + 0.5 * self.p_half**2

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

        np.savez(self.checkpoint_filename, time=self.t, p=self.p, q=self.q, acceleration=self.acceleration)

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
            "density_matrix": self.rho.copy(),
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
        self.rho = snapshot["density_matrix"]
