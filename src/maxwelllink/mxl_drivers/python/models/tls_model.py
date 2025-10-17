import numpy as np
from scipy.linalg import expm
import os

try:
    from .dummy_model import DummyModel
except:
    from dummy_model import DummyModel


class TLSModel(DummyModel):
    """
    A two-level system (TLS) quantum dynamics model.

    This class implements a simple two-level system model for quantum dynamics,
    which can be integrated with the MaxwellLink framework. The TLS model is
    characterized by its transition frequency, dipole moment, and orientation of
    the dipole moment.

    Notes
    -----
    Implementing this class is mostly for demonstration purposes. Users who want
    to enjoy model quantum system calculations should use **QuTiPModel** instead,
    which provides a very general and robust implementation of an arbitrary model
    Hamiltonian (with Lindbladian dissipation) based on the **QuTiP** library.
    """

    def __init__(
        self,
        omega: float = 2.4188843e-1,  # 1.0 in MEEP units with [T]=0.1 fs
        mu12: float = 1.870819866e2,  # 0.1 in MEEP units with [T]=0.1 fs
        orientation: int = 2,
        pe_initial: float = 0.0,
        checkpoint: bool = False,
        restart: bool = False,
        verbose: bool = False,
    ):
        """
        Initialize the necessary parameters for the TLS quantum dynamics model.

        Parameters
        ----------
        omega : float, default: 2.4188843e-1
            Transition frequency in atomic units (a.u.). Default is ``2.4188843e-1``
            a.u. (``1.0`` in MEEP units with ``[T]=0.1 fs``).
        mu12 : float, default: 1.870819866e2
            Dipole moment in atomic units (a.u.). Default is ``1.870819866e2`` a.u.
            (``0.1`` in MEEP units with ``[T]=0.1 fs``).
        orientation : int, default: 2
            Orientation of the dipole moment; can be ``0`` (x), ``1`` (y), or ``2`` (z).
        pe_initial : float, default: 0.0
            Initial population in the excited state.
        checkpoint : bool, default: False
            Whether to enable checkpointing.
        restart : bool, default: False
            Whether to restart from a checkpoint if available.
        verbose : bool, default: False
            Whether to print verbose output.
        """

        # Initialize the base class (DummyModel)
        super().__init__(verbose, checkpoint, restart)

        # Initialize TLS-specific parameters
        self.omega = omega  # transition frequency in a.u.
        self.dipole_moment = mu12  # dipole moment in a.u.
        self.orientation = orientation  # orientation of the dipole moment
        self.orientation_idx = int(orientation)
        if self.orientation_idx < 0 or self.orientation_idx > 2:
            raise ValueError("Orientation must be 0 (x), 1 (y), or 2 (z).")

        self.pe_initial = pe_initial  # initial population in the excited state

        # set the Hamiltonian and density matrix for the TLS molecule
        self.Hs = np.array([[0, 0], [0, self.omega]], dtype=np.complex128)
        self.SIGMAX = np.array([[0, 1], [1, 0]], dtype=np.complex128)

        # expHs uses dt, which is 0.0 at construction of the base class (DummyModel)
        # Therefore, we need to update it in the initialize() method.
        self.expHs = expm(-1j * self.dt * self.Hs / 2.0)

        # initial wavefunction and density matrix as ground state
        self.C = np.array([[1], [0]], dtype=np.complex128)
        self.rho = np.dot(self.C, self.C.conj().transpose())
        # reset the initial population
        self._reset_tls_population(excited_population=self.pe_initial)
        # optional, checking whether the driver can be paused and resumed properly
        self.restarted = False

        # store dipole moments and energies during rt-tddft propagation
        self.dipole_vec = None
        self.energy = None

    # -------------- heavy-load initialization (at INIT) --------------

    def initialize(self, dt_new, molecule_id):
        """
        Set the time step and molecule ID for this quantum dynamics model, and provide
        additional initialization for the TLS.

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
        self.checkpoint_filename = "tls_checkpoint_id_%d.npz" % self.molecule_id

        # Rebuild expHs with new dt sent from SocketHub
        self.expHs = expm(-1j * self.dt * self.Hs / 2.0)
        print(
            "init TLSModel with dt = %.6f a.u., molecule ID = %d"
            % (self.dt, self.molecule_id)
        )

        # Consider whether to restart from a checkpoint. We do this here because this function
        # is called in the driver during the INIT stage of the socket communication.
        if self.restart and self.checkpoint:
            self._reset_from_checkpoint(self.molecule_id)
            self.restarted = True

    # ------------ internal functions -------------

    def _reset_tls_population(self, excited_population: float = 0.0):
        """
        Reset the TLS population to a specified excited state population in a pure
        state.

        Notes
        -----
        This function name starts with an underscore to indicate that it is intended
        for internal use.

        Parameters
        ----------
        excited_population : float, default: 0.0
            Initial population in the excited state.
        """

        if excited_population < 0 or excited_population > 1:
            raise ValueError("Excited population must be between 0 and 1.")
        self.C = np.array(
            [[(1 - excited_population) ** 0.5], [excited_population**0.5]],
            dtype=np.complex128,
        )
        self.rho = np.dot(self.C, self.C.conj().transpose())

    # -------------- one FDTD step under E-field --------------

    def propagate(self, effective_efield_vec):
        """
        Propagate the quantum molecular dynamics given the effective electric field
        vector.

        Parameters
        ----------
        effective_efield_vec : array-like of float, shape (3,)
            Effective electric field vector in the form ``[E_x, E_y, E_z]``.
        """

        if self.verbose:
            print(
                f"[molecule ID {self.molecule_id}] Time: {self.t:.4f} a.u., receiving effective_efield_vec: {effective_efield_vec[2]:.6E}"
            )
        int_ep = effective_efield_vec[self.orientation_idx] * self.dipole_moment

        # update the density matrix for one time step
        U = np.dot(
            self.expHs, np.dot(expm((1j * self.dt * int_ep) * self.SIGMAX), self.expHs)
        )
        self.rho = np.dot(np.dot(U, self.rho), U.conj().transpose())

        # update current time in a.u.
        self.t += self.dt

        dipole = self.dipole_moment * 2.0 * np.real(self.rho[0, 1])
        dip_vec = np.zeros(3)
        dip_vec[self.orientation_idx] = dipole

        self.dipole_vec = dip_vec
        self.energy = np.trace(np.dot(self.Hs, self.rho)).real

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

        # analytical expression for dmu/dt in a TLS
        amp = -2.0 * self.omega * np.imag(self.rho[0, 1]) * self.dipole_moment
        amp_vec = np.zeros(3)
        amp_vec[self.orientation_idx] = amp
        if self.verbose:
            print(
                f"[molecule ID {self.molecule_id}] Time: {self.t:.4f} a.u., Dipole: {self.dipole_vec[-1]}, Energy: {self.energy:.6f} a.u., returning Amp: {amp_vec[2]:.6E}"
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
        data["mu_x_au"] = self.dipole_vec[0] if self.dipole_vec is not None else 0.0
        data["mu_y_au"] = self.dipole_vec[1] if self.dipole_vec is not None else 0.0
        data["mu_z_au"] = self.dipole_vec[2] if self.dipole_vec is not None else 0.0
        data["Pe"] = self.rho[1, 1].real
        data["Pg"] = self.rho[0, 0].real
        data["Pge_real"] = np.real(self.rho[0, 1])
        data["Pge_imag"] = np.imag(self.rho[0, 1])
        return data

    def _dump_to_checkpoint(self):
        """
        Dump the internal state of the model to a checkpoint.

        Notes
        -----
        ``self.checkpoint_filename`` includes ``molid`` at ``self.initialize()``.
        """

        np.savez(self.checkpoint_filename, density_matrix=self.rho, time=self.t)

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
            self.rho = np.asarray(data["density_matrix"], dtype=np.complex128)
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
        self.rho = snapshot["density_matrix"]
