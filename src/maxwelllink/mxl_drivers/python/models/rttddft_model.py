import numpy as np
from scipy.linalg import expm
from scipy.linalg import fractional_matrix_power as mat_pow

np.set_printoptions(precision=6, linewidth=200, suppress=True, threshold=2000)

try:
    from .dummy_model import DummyModel
except:
    from dummy_model import DummyModel
import os

# TBD: we need to think carefully the light-matter coupling term when the molecule has permanent dipole moments.


class RTTDDFTModel(DummyModel):
    """
    A real-time time-dependent density functional theory (RT-TDDFT) quantum dynamics model using the Psi4 quantum chemistry package.
    This class implements a RT-TDDFT model for quantum dynamics, which can be integrated with the SocketMEEP framework.

    Example
    -------
    Create from an XYZ file and then set the molecule id with restricted SCF:

    >>> model = RTTDDFTModel(
    ...     engine="psi4",
    ...     molecule_xyz="../tests/data/hcn.xyz",
    ...     functional="SCF",
    ...     basis="sto-3g",
    ...     dt_rttddft_au=0.04,
    ...     delta_kick_au=1.0e-3,
    ...     memory="2GB",
    ...     verbose=False,
    ...     remove_permanent_dipole=False
    ... )
    >>> model.initialize(dt_new=0.12, molecule_id=0)
    Initial SCF energy: -91.6751251525 Eh
    >>> model._get_lr_tddft_spectrum(states=5, tda=False)
    Energy (eV): [ 7.395641  8.558096  8.558096 10.766481 10.76651 ]
    Oscillator strengths: [0.       0.       0.       0.047901 0.047901]
    >>> model._propagate_full_rt_tddft(nsteps=10)
    Step   30 Time 1.200000  Etot = -91.6751251490 Eh  ΔE = 0.0000000036 Eh,  μx = 0.000024 a.u.,  μy = 0.000024 a.u.,  μz = 0.965180 a.u.
    """

    def __init__(
        self,
        engine: str = "psi4",
        molecule_xyz: str = "",
        functional: str = "SCF",
        basis: str = "sto-3g",
        dt_rttddft_au: float = 0.04,
        delta_kick_au: float = 0.0e-3,
        delta_kick_direction: str = "xyz",
        memory: str = "8GB",
        num_threads: int = 1,
        checkpoint: bool = False,
        restart: bool = False,
        verbose: bool = False,
        remove_permanent_dipole: bool = False,
    ):
        """
        Initialize the necessary parameters for the dummy quantum dynamics model.

        + **`engine`** (str): The computational engine to use (e.g., "psi4"). Default is "psi4". Currently, only "psi4" is supported.
        + **`molecule_xyz`** (str): Path to the XYZ file containing the molecular structure. The second line should contain the charge and multiplicity.
        + **`functional`** (str): Any Psi4 functional label, e.g. "PBE", "B3LYP", "SCAN", "PBE0". Default is "SCF" (Hartree-Fock).
        + **`basis`** (str): Any basis set label recognized by Psi4, e.g. "sto-3g", "6-31g", "cc-pVDZ". Default is "sto-3g".
        + **`dt_rttddft_au`** (float): Time step for real-time TDDFT propagation in atomic units (a.u.). Default is 0.04 a.u.
        If the MEEP time step is an integer multiple of this, the driver will sub-step internally. This sub-stepping can avoid propagating EM fields
        too frequently when the molecule requires a small time step.
        + **`delta_kick_au`** (float): Strength of the initial delta-kick perturbation along the x, y, and z direction in atomic units (a.u.).
        Default is 0.0e-3 a.u. If this value is set to a non-zero value, the driver will apply a delta-kick perturbation at t=0 to initiate the dynamics.
        With this delta-kick, and also setting the MEEP coupling to zero, one can compute the conventional RT-TDDFT linear absorption spectrum of the molecule.
        This functinonality is mainly for testing and validation purposes.
        + **`delta_kick_direction`** (str): Direction of the initial delta-kick perturbation. Can be "x", "y", "z", "xy", "xz", "yz", or "xyz". Default is "xyz".
        + **`memory`** (str): Memory allocation for Psi4, e.g. "8GB", "500MB". Default is "8GB".
        + **`num_threads`** (int): Number of CPU threads to use in Psi4. Default is 1.
        + **`checkpoint`** (bool): Whether to dump checkpoint files during propagation to allow restarting from the last checkpoint. Default is False.
        + **`restart`** (bool): Whether to restart the propagation from the last checkpoint. Default is False. Setting this to True requires that checkpoint files exist.
        When restarting, the driver will ignore the initial delta-kick perturbation even if it is set to a non-zero value.
        + **`verbose`** (bool): Whether to print verbose output. Default is False.
        + **`remove_permanent_dipole`** (bool): Whether to remove the effect of permanent dipole moments in the light-matter coupling term. Default is False.
        """
        super().__init__(verbose, checkpoint, restart)

        if engine.lower() != "psi4":
            raise NotImplementedError(
                "Currently, only the 'psi4' engine is supported for the RTTDDFTModel."
            )
        self.engine = engine.lower()
        self.molecule_xyz = molecule_xyz
        if self.molecule_xyz == "":
            raise ValueError(
                "The path to the XYZ file containing the molecular structure must be provided."
            )
        if not os.path.exists(self.molecule_xyz):
            raise FileNotFoundError(
                "The specified XYZ file does not exist: %s" % self.molecule_xyz
            )
        self.functional = functional
        self.basis = basis
        self.dt_rttddft_au = dt_rttddft_au
        self.delta_kick_au = delta_kick_au
        if delta_kick_direction.lower() not in ["x", "y", "z", "xy", "xz", "yz", "xyz"]:
            raise ValueError(
                "Invalid delta_kick_direction. Must be one of 'x', 'y', 'z', 'xy', 'xz', 'yz' or 'xyz'."
            )
        self.delta_kick_direction = delta_kick_direction.lower()
        self.delta_kick_vec = [0.0, 0.0, 0.0]
        if "x" in self.delta_kick_direction:
            self.delta_kick_vec[0] = 1.0
        if "y" in self.delta_kick_direction:
            self.delta_kick_vec[1] = 1.0
        if "z" in self.delta_kick_direction:
            self.delta_kick_vec[2] = 1.0
        if self.delta_kick_au != 0.0:
            print(
                f"Initial delta-kick perturbation will be applied with strength {self.delta_kick_au} a.u. along direction(s) {self.delta_kick_direction}."
            )
        self.memory = memory
        self.num_threads = num_threads
        self.remove_permanent_dipole = remove_permanent_dipole
        self.t = 0.0  # current time in a.u.
        self.count = 0

        # optional, checking whether the driver can be paused and resumed properly
        self.restarted = False

        self.step_started = False
        self.step_completed = False

        # store dipole moments, energies, and times during rt-tddft propagation
        self.dipoles = []
        self.energies = []
        self.times = []

    def initialize(self, dt_new, molecule_id):
        """
        Set the time step and molecule ID for this quantum dynamics model.

        + **`dt_new`** (float): The new time step in atomic units (a.u.).
        + **`molecule_id`** (int): The ID of the molecule.
        """
        self.dt = float(dt_new)
        self.molecule_id = int(molecule_id)
        self.checkpoint_filename = "rttddft_checkpoint_id_%d.npy" % self.molecule_id

        # examine the relation between the MEEP dt and the RT-TDDFT dt
        assert self.dt >= self.dt_rttddft_au, (
            "The MEEP time step (dt=%.4f a.u.) must be greater than or equal to the RT-TDDFT time step (dt_rttddft_au=%.4f a.u.)."
            % (self.dt, self.dt_rttddft_au)
        )
        self.ratio_timestep = int(self.dt / self.dt_rttddft_au)
        if self.verbose:
            print(
                "[molecule %d] The MEEP time step (dt=%.4f a.u.) is (approximately) %d times the RT-TDDFT time step (dt_rttddft_au=%.4f a.u.). The driver will sub-step internally."
                % (self.molecule_id, self.dt, self.ratio_timestep, self.dt_rttddft_au)
            )
        self.dt_rttddft_au = (
            self.dt / self.ratio_timestep
        )  # adjust dt_rttddft_au to be exactly dt / ratio
        if self.verbose:
            print(
                "[molecule %d] To ensure the correct timing, the adjusted RT-TDDFT time step is dt_rttddft_au=%.4f a.u."
                % (self.molecule_id, self.dt_rttddft_au)
            )

        if self.engine == "psi4":
            self._init_psi4()

        # consider whether to restart from a checkpoint. We do this here because this function
        # is called in the driver during the INIT stage of the socket communication.
        if self.restart and self.checkpoint:
            self._reset_from_checkpoint(self.molecule_id)
            self.restarted = True

    def _init_psi4(self):
        """
        Initialize Psi4 and set up the molecule, basis set, and functional.
        This method is called during the first propagation step after receiving the molecule ID.
        """
        import psi4

        # Set memory and output file for Psi4
        psi4.set_memory(self.memory)
        psi4.core.set_num_threads(self.num_threads)
        psi4.core.set_output_file(
            "psi4_rttddft_output_id_%d.dat" % self.molecule_id, False
        )

        # Read the molecular structure from the XYZ file
        with open(self.molecule_xyz, "r") as f:
            lines = f.readlines()

        if len(lines) < 3:
            raise ValueError(
                "The XYZ file must contain at least 3 lines (number of atoms, charge/multiplicity, and atomic coordinates)."
            )

        natoms = int(lines[0].strip())
        try:
            charge, multiplicity = map(int, lines[1].strip().split())
        except Exception as e:
            raise ValueError(
                "The second line of the XYZ file must contain two integers: charge and multiplicity."
            ) from e

        atom_lines = lines[2 : natoms + 2]
        atom_str = "".join(atom_lines)

        # Create the Psi4 molecule object
        geom_str = f"""
        nocom
        noreorient
        {charge} {multiplicity}
        {atom_str} symmetry c1
        """
        if self.verbose:
            print("Initializing molecule with the following geometry:" f"\n{geom_str}")

        # set molecule
        self.mol = psi4.geometry(geom_str)

        opts = {
            "basis": self.basis,
            "reference": "uks" if self.mol.multiplicity() != 1 else "rks",
            "scf_type": "out_of_core",
            "e_convergence": 1e-10,
            "d_convergence": 1e-10,
            "guess": "sad",
            "puream": True,
            "save_jk": True,  # for LR-TDDFT calculations
        }
        psi4.set_options(opts)

        # Initial ground-state KS calculation (to get wfn, grid, V_xc machinery, etc.)
        self.E0, wfn = psi4.energy(f"{self.functional}", return_wfn=True)
        self.wfn = wfn
        print(f"Initial SCF energy: {self.E0:.10f} Eh")

        # Integrals, matrices, helpers
        mints = psi4.core.MintsHelper(wfn.basisset())
        self.S = np.asarray(wfn.S())
        self.H = np.asarray(wfn.H())  # core Hamiltonian
        self.Enuc = self.mol.nuclear_repulsion_energy()

        # Symmetric orthogonalizer
        self.X = mat_pow(self.S, -0.5)
        self.U = mat_pow(self.S, 0.5)

        # Two-electron 4-index integrals (AO). NOTE: OK for small/moderate systems.
        self.I_ao = np.asarray(mints.ao_eri())  # (pq|rs)

        # Pull spin densities and KS matrices
        self.Da = np.asarray(wfn.Da())
        self.is_restricted = (
            hasattr(wfn, "Db")
            and (wfn.Db().shape[0] == wfn.Da().shape[0])
            and (opts["reference"] == "rks")
        )
        self.Db = (
            np.asarray(wfn.Db()) if not self.is_restricted else self.Da.copy()
        )  # in RKS we just mirror Da

        self.Fa = np.asarray(wfn.Fa())
        self.Fb = np.asarray(wfn.Fb()) if not self.is_restricted else self.Fa.copy()

        # AO dipole integrals (x, y, z)
        self.mu_ints = [np.asarray(m) for m in mints.ao_dipole()]
        if self.remove_permanent_dipole:
            # calculate the expectation value of the dipole moment in the ground state
            mu_x0 = np.einsum("pq,pq->", self.mu_ints[0], self.Da + self.Db).real
            mu_y0 = np.einsum("pq,pq->", self.mu_ints[1], self.Da + self.Db).real
            mu_z0 = np.einsum("pq,pq->", self.mu_ints[2], self.Da + self.Db).real
            Identity = np.eye(self.mu_ints[0].shape[0])
            self.mu_ints = [
                self.mu_ints[0] - mu_x0 * Identity / np.trace(self.Da + self.Db),
                self.mu_ints[1] - mu_y0 * Identity / np.trace(self.Da + self.Db),
                self.mu_ints[2] - mu_z0 * Identity / np.trace(self.Da + self.Db),
            ]
            mu_x0_new = np.einsum("pq,pq->", self.mu_ints[0], self.Da + self.Db).real
            mu_y0_new = np.einsum("pq,pq->", self.mu_ints[1], self.Da + self.Db).real
            mu_z0_new = np.einsum("pq,pq->", self.mu_ints[2], self.Da + self.Db).real
            if self.verbose:
                print(
                    f"[molecule {self.molecule_id}] Removed permanent dipole moment: mu_x0 = {mu_x0:.6f} -> {mu_x0_new:.6f}, mu_y0 = {mu_y0:.6f} -> {mu_y0_new:.6f}, mu_z0 = {mu_z0:.6f} -> {mu_z0_new:.6f} (a.u.)"
                )

        # V_xc machinery: VBase from the SCF wavefunction (already configured with functional & grid)
        # We'll reuse this at each step to (re)build V_xc(Da,Db) on the grid efficiently.
        self.Vpot = wfn.V_potential()  # psi4.core.VBase
        # amount of exact exchange in global hybrids (0 for pure DFA)
        self.alpha_hfx = (
            wfn.functional().x_alpha()
        )  # fraction of exact HF exchange in the chosen functional

        if self.alpha_hfx < 1.0:
            self.Vpot.initialize()  # usually already initialized by SCF, harmless if called
            try:
                # prevents recomputing basis collocation on grid in compute_V()
                self.Vpot.build_collocation_cache(self.Vpot.nblocks())
            except Exception:
                pass  # older Psi4 versions may not expose this API

    def _build_KS_psi4(self, Da_np, Db_np, restricted, V_ext=None):
        """Return Fa, Fb given current densities."""
        if restricted:
            # RKS: J from total density (2 * Da), K from Da if hybrid
            Jtot = self._build_J_psi4(2.0 * Da_np)
            if self.alpha_hfx < 1.0:
                Vxc = self._build_Vxc_psi4(Da_np, Db_np, True)
            else:
                Vxc = np.zeros_like(Da_np)
            if self.alpha_hfx > 0.0:
                K = self._build_K_psi4(Da_np)
                Fa = self.H + Jtot - self.alpha_hfx * K + Vxc
            else:
                Fa = self.H + Jtot + Vxc
            Fb = Fa
        else:
            # UKS: J from total density Da+Db; K channel-specific for hybrids
            Dtot = Da_np + Db_np
            Jtot = self._build_J_psi4(Dtot)
            if self.alpha_hfx < 1.0:
                Va, Vb = self._build_Vxc_psi4(Da_np, Db_np, False)
            else:
                Va = np.zeros_like(Da_np)
                Vb = np.zeros_like(Db_np)
            if self.alpha_hfx > 0.0:
                Ka = self._build_K_psi4(Da_np)
                Kb = self._build_K_psi4(Db_np)
                Fa = self.H + Jtot - self.alpha_hfx * Ka + Va
                Fb = self.H + Jtot - self.alpha_hfx * Kb + Vb
            else:
                Fa = self.H + Jtot + Va
                Fb = self.H + Jtot + Vb
        if V_ext is not None:
            Fa += V_ext
            Fb += V_ext
        return Fa, Fb

    def _build_J_psi4(self, P):
        """Coulomb J[P] in AO basis using 4-index (pq|rs)."""
        return np.einsum("pqrs,rs->pq", self.I_ao, P, optimize=True)

    def _build_K_psi4(self, P):
        """Exchange K[P] in AO basis using 4-index (pr|qs). Only used if alpha_hfx > 0."""
        return np.einsum("prqs,rs->pq", self.I_ao, P, optimize=True)

    def _build_Vxc_psi4(self, Da_np, Db_np, restricted):
        import psi4

        """Build V_xc in AO basis from current spin densities via VBase.
        VBase expects *real* densities; pass the real (Hermitian) parts."""
        nbf = Da_np.shape[0]
        if restricted:
            D = psi4.core.Matrix.from_array(np.real((Da_np + Db_np.T) / 2))  # Da==Db
            V = psi4.core.Matrix(nbf, nbf)
            self.Vpot.set_D([D])  # set current density
            self.Vpot.compute_V([V])  # populate V
            return np.asarray(V)
        else:
            Da_m = psi4.core.Matrix.from_array(np.real((Da_np + Da_np.T) / 2))
            Db_m = psi4.core.Matrix.from_array(np.real((Db_np + Db_np.T) / 2))
            Va_m = psi4.core.Matrix(nbf, nbf)
            Vb_m = psi4.core.Matrix(nbf, nbf)
            self.Vpot.set_D([Da_m, Db_m])
            self.Vpot.compute_V([Va_m, Vb_m])
            return np.asarray(Va_m), np.asarray(Vb_m)

    def _energy_dipole_analysis_psi4(self, Da_np, Db_np):
        Dtot = Da_np + Db_np
        Jtot = self._build_J_psi4(Dtot)
        # V_xc energy from VBase quadrature (if available)
        Exc = 0.0
        try:
            Exc = float(self.Vpot.quadrature_values().get("FUNCTIONAL", 0.0))
        except Exception:
            pass
        Ecoul = 0.5 * np.einsum("pq,pq->", Jtot, Dtot).real
        Eone = np.einsum("pq,pq->", self.H, Dtot).real
        ExHF = 0.0
        if self.alpha_hfx > 0.0:
            if self.is_restricted:
                K = self._build_K_psi4(self.Da)
                ExHF = (
                    -self.alpha_hfx * np.einsum("pq,pq->", K, self.Da).real
                )  # factor 1/2 for closed-shell
            else:
                Ka = self._build_K_psi4(self.Da)
                Kb = self._build_K_psi4(self.Db)
                ExHF = (
                    -self.alpha_hfx
                    * 0.5
                    * (
                        np.einsum("pq,pq->", Ka, self.Da)
                        + np.einsum("pq,pq->", Kb, self.Db)
                    ).real
                )
        Etot = Eone + Ecoul + ExHF + Exc + self.Enuc

        mu_x = -np.trace(Dtot @ self.mu_ints[0]).real
        mu_y = -np.trace(Dtot @ self.mu_ints[1]).real
        mu_z = -np.trace(Dtot @ self.mu_ints[2]).real
        dip_vec = np.array([mu_x, mu_y, mu_z])

        return Etot, dip_vec

    def propagate(self, effective_efield_vec):
        """
        Propagate the quantum molecular dynamics given the effective electric field vector.

        + **`effective_efield_vec`**: Effective electric field vector in the form [Ex, Ey, Ez].
        """
        self.step_started = True
        self.step_completed = False

        if self.verbose:
            print(
                f"[molecule ID {self.molecule_id}] Time: {self.t:.4f} a.u., receiving effective_efield_vec: {effective_efield_vec[2]:.6E}"
            )

        if self.engine == "psi4":
            # calculate the effective dipole matrix due to the coupling with the external E-field; minus sign for e = -1
            V_ext = (
                -self.mu_ints[0] * effective_efield_vec[0]
                - self.mu_ints[1] * effective_efield_vec[1]
                - self.mu_ints[2] * effective_efield_vec[2]
            )

            # sub-step the RT-TDDFT propagation if needed
            for step in range(self.ratio_timestep):
                if (
                    self.count == 0
                    and (not self.restarted)
                    and (self.delta_kick_au != 0.0)
                ):
                    # apply the initial delta-kick perturbation at t=0 to initiate the dynamics
                    if self.verbose:
                        print(
                            f"[molecule {self.molecule_id}] Applying initial delta-kick perturbation with strength {self.delta_kick_au} a.u. along direction(s) {self.delta_kick_direction}."
                        )
                    self.Fa += self.delta_kick_au * (
                        self.mu_ints[0] * self.delta_kick_vec[0] + self.mu_ints[1] * self.delta_kick_vec[1] + self.mu_ints[2] * self.delta_kick_vec[2]
                    )
                    self.Fb += self.delta_kick_au * (
                        self.mu_ints[0] * self.delta_kick_vec[0] + self.mu_ints[1] * self.delta_kick_vec[1] + self.mu_ints[2] * self.delta_kick_vec[2]
                    )

                # Orthonormal-basis densities & KS matrices
                DaO = self.U @ self.Da @ self.U.T
                FaO = self.X.T @ self.Fa @ self.X
                if self.is_restricted:
                    # trial propagation, P(t+dt) = U(t) P(t) U†(t), U(t) = exp(-i dt F_O(t))
                    Uprop = expm((-1j * self.dt_rttddft_au) * FaO)
                    DaO_trial = Uprop @ DaO @ Uprop.conj().T
                    Da_trial = self.X @ DaO_trial @ self.X.T
                    Db_trial = Da_trial.copy()
                    # real propagation, P(t+dt) = U'(t) P(t) U'†(t), U'(t) = exp(-i dt/2 F_O(t+dt)) @ exp(-i dt/2 F_O(t))
                    Fa_future, Fb_future = self._build_KS_psi4(
                        Da_trial, Db_trial, self.is_restricted, V_ext=V_ext
                    )
                    FaO_future = self.X.T @ Fa_future @ self.X
                    Uprop = expm((-1j * self.dt_rttddft_au / 2) * FaO_future) @ expm(
                        (-1j * self.dt_rttddft_au / 2) * FaO
                    )
                    DaO = Uprop @ DaO @ Uprop.conj().T
                    self.Da = self.X @ DaO @ self.X.T
                    self.Db = self.Da.copy()
                else:
                    # UKS: propagate α and β separately
                    DbO = self.U @ self.Db @ self.U.T
                    FbO = self.X.T @ self.Fb @ self.X
                    # trial propagation, P(t+dt) = U(t) P(t) U†(t), U(t) = exp(-i dt F_O(t))
                    Uprop_a = expm(-(1j * self.dt_rttddft_au) * FaO)
                    Uprop_b = expm(-(1j * self.dt_rttddft_au) * FbO)
                    DaO_trial = Uprop_a @ DaO @ Uprop_a.conj().T
                    DbO_trial = Uprop_b @ DbO @ Uprop_b.conj().T
                    Da_trial = self.X @ DaO_trial @ self.X.T
                    Db_trial = self.X @ DbO_trial @ self.X.T
                    # real propagation, P(t+dt) = U'(t) P(t) U'†(t), U'(t) = exp(-i dt/2 F_O(t+dt)) @ exp(-i dt/2 F_O(t))
                    Fa_future, Fb_future = self._build_KS_psi4(
                        Da_trial, Db_trial, self.is_restricted, V_ext=V_ext
                    )
                    FaO_future = self.X.T @ Fa_future @ self.X
                    FbO_future = self.X.T @ Fb_future @ self.X
                    Uprop_a = expm((-1j * self.dt_rttddft_au / 2) * FaO_future) @ expm(
                        (-1j * self.dt_rttddft_au / 2) * FaO
                    )
                    Uprop_b = expm((-1j * self.dt_rttddft_au / 2) * FbO_future) @ expm(
                        (-1j * self.dt_rttddft_au / 2) * FbO
                    )
                    DaO = Uprop_a @ DaO @ Uprop_a.conj().T
                    DbO = Uprop_b @ DbO @ Uprop_b.conj().T
                    self.Da = self.X @ DaO @ self.X.T
                    self.Db = self.X @ DbO @ self.X.T

                # Rebuild KS matrices at new time (adiabatic TDDFT)
                self.Fa, self.Fb = self._build_KS_psi4(
                    self.Da, self.Db, self.is_restricted, V_ext=V_ext
                )

                # Energetics and dipole info analysis
                Etot, dip_vec = self._energy_dipole_analysis_psi4(self.Da, self.Db)

                # Dipole (electronic)
                self.dipoles.append(dip_vec)
                self.energies.append(Etot)
                self.times.append(self.t)
                if self.verbose:
                    print(
                        f"Step {self.count:4d} Time {self.dt_rttddft_au*self.count:.6f}  Etot = {Etot:.10f} Eh  ΔE = {Etot-self.E0:.10f} Eh,  μx = {dip_vec[0]:.6f} a.u.,  μy = {dip_vec[1]:.6f} a.u.,  μz = {dip_vec[2]:.6f} a.u."
                    )

                self.count += 1
                self.t += self.dt_rttddft_au

    def _propagate_full_rt_tddft(self, nsteps=100, effective_efield_vec=np.zeros(3)):
        """
        Standalone function to propagate the RT-TDDFT for a given number of steps.
        This function is mainly for testing and validation purposes.
        To use this function, the self.delta_kick_au parameter in self.__init__()
        should be set to a non-zero value to apply an initial delta-kick perturbation at t=0.

        + **`nsteps`** (int): Number of MEEP steps to propagate. Default is 100.
        + **`effective_efield_vec`**: Effective electric field vector received from MEEP in the form [Ex, Ey, Ez]. Default is [0.0, 0.0, 0.0].
        """
        for idx in range(nsteps):
            self.propagate(effective_efield_vec)
        # print out the last step info
        print(
            f"Step {self.count:4d} Time {self.dt_rttddft_au*self.count:.6f}  Etot = {self.energies[-1]:.10f} Eh  ΔE = {self.energies[-1]-self.E0:.10f} Eh,  μx = {self.dipoles[-1][0]:.6f} a.u.,  μy = {self.dipoles[-1][1]:.6f} a.u.,  μz = {self.dipoles[-1][2]:.6f} a.u."
        )

        # ===== Save data to disk =====
        dipoles = np.array(self.dipoles)
        energies = np.array(self.energies)
        times = np.array(self.times)
        np.savetxt(
            "rt_tddft_energy_dipoles_%d.txt" % self.molecule_id,
            np.column_stack((times, energies, dipoles)),
            header="Time(a.u.)  Energy(a.u.)  mu_x(a.u.)  mu_y(a.u.)  mu_z(a.u.)",
            fmt="%.10E %.10E %.10E %.10E %.10E",
        )

    def _get_lr_tddft_spectrum(self, states=20, tda=False):
        """
        Standalone function to compute the linear absorption spectrum of molecules using the linear-response TDDFT (LR-TDDFT) method.
        This function is mainly for testing and validation purposes.
        """
        if self.engine == "psi4":
            from psi4.driver.procrouting.response.scf_response import tdscf_excitations

            res = tdscf_excitations(self.wfn, states=states, tda=tda, triplets="none")
            # Collect poles (Hartree)
            poles = np.array([r["EXCITATION ENERGY"] for r in res])

            tdm_len = np.array(
                [r["ELECTRIC DIPOLE TRANSITION MOMENT (LEN)"] for r in res]
            )  # shape (n,3)
            mu2 = np.sum(tdm_len**2, axis=1)
            # Oscillator strengths (length gauge): f = 2/3 * ω * |μ|^2   (ω in a.u.)
            oscillator_strengths = (2.0 / 3.0) * poles * mu2

            # save to file
            with open("psi4_lrtddft_output_id_%d.txt" % self.molecule_id, "w") as f:
                f.write("# Excitation energies (eV), oscillator strengths\n")
                for p, e in zip(poles, oscillator_strengths):
                    f.write(f"{p*27.211399} {e}\n")

            # print to screen
            print("Energy (eV):", poles * 27.211399)
            print("Oscillator strengths:", oscillator_strengths)

    def calc_amp_vector(self):
        """
        Update the source amplitude vector after propagating this molecule for one time step.
        amp = d/dt[\rho(t) * mu]

        Returns:
        - A numpy array representing the amplitude vector in the form [Ax, Ay, Az].
        """

        # Orthonormal-basis densities & KS matrices
        DaO = self.U @ self.Da @ self.U.T
        FaO = self.X.T @ self.Fa @ self.X
        if self.is_restricted:
            dDaO_dt = -1j * (FaO @ DaO - DaO @ FaO)
            dDa_dt = self.X @ dDaO_dt @ self.X.T
            # the -2.0 prefactor is due to electrons (negative sign) and spin degeneracy (factor 2)
            amp_x = -2.0 * np.trace(dDa_dt @ self.mu_ints[0]).real
            amp_y = -2.0 * np.trace(dDa_dt @ self.mu_ints[1]).real
            amp_z = -2.0 * np.trace(dDa_dt @ self.mu_ints[2]).real
            amp_vec = np.array([amp_x, amp_y, amp_z])
        else:
            DbO = self.U @ self.Db @ self.U.T
            FbO = self.X.T @ self.Fb @ self.X
            dDaO_dt = -1j * (FaO @ DaO - DaO @ FaO)
            dDbO_dt = -1j * (FbO @ DbO - DbO @ FbO)
            dDa_dt = self.X @ dDaO_dt @ self.X.T
            dDb_dt = self.X @ dDbO_dt @ self.X.T
            # the -1.0 prefactor is due to electrons (negative sign), no factor 2 for spin
            amp_x = (
                -1.0 * np.trace(dDa_dt @ self.mu_ints[0]).real
                - 1.0 * np.trace(dDb_dt @ self.mu_ints[0]).real
            )
            amp_y = (
                -1.0 * np.trace(dDa_dt @ self.mu_ints[1]).real
                - 1.0 * np.trace(dDb_dt @ self.mu_ints[1]).real
            )
            amp_z = (
                -1.0 * np.trace(dDa_dt @ self.mu_ints[2]).real
                - 1.0 * np.trace(dDb_dt @ self.mu_ints[2]).real
            )
            amp_vec = np.array([amp_x, amp_y, amp_z])

        if self.verbose:
            print(
                f"[molecule ID {self.molecule_id}] Time: {self.t:.4f} a.u., Energy: {self.energies[-1]:.6f} a.u., returning Amp: {amp_vec[2]:.6E}"
            )

        self.step_completed = True
        self.step_started = False

        return amp_vec

    def append_additional_data(self):
        """
        Append additional data to be sent back to MEEP, which can be retrieved by the user
        via the Python interface of MEEP: mp.SocketMolecule.additional_data_history.

        Returns:
        - A dictionary containing additional data.
        """
        data = {}
        data["time_au"] = self.t
        data["energy_au"] = self.energies[-1] if len(self.energies) > 0 else 0.0
        data["mu_x_au"] = self.dipoles[-1][0] if len(self.dipoles) > 0 else 0.0
        data["mu_y_au"] = self.dipoles[-1][1] if len(self.dipoles) > 0 else 0.0
        data["mu_z_au"] = self.dipoles[-1][2] if len(self.dipoles) > 0 else 0.0
        return data

    def _dump_to_checkpoint(self, molid):
        """
        Dump the internal state of the model to a checkpoint.
        """

        # try to save self.Da, self.Db, self.Fa, self.Fb in a single npy file
        np.save(
            self.checkpoint_filename,
            {
                "Da": self.Da,
                "Db": self.Db,
                "Fa": self.Fa,
                "Fb": self.Fb,
                "time": self.t,
                "count": self.count,
            },
        )

    def _reset_from_checkpoint(self, molid):
        """
        Reset the internal state of the model from a checkpoint.
        """
        if not os.path.exists(self.checkpoint_filename):
            # No checkpoint file found means this driver has not been paused or terminated abnormally
            # so we just keep the current state
            print(
                "[checkpoint] No checkpoint file found for molecule ID %d, starting fresh."
                % molid
            )
        else:
            checkpoint_data = np.load(
                self.checkpoint_filename, allow_pickle=True
            ).item()
            self.Da = np.asarray(checkpoint_data["Da"], dtype=np.complex128)
            self.Db = np.asarray(checkpoint_data["Db"], dtype=np.complex128)
            self.Fa = np.asarray(checkpoint_data["Fa"], dtype=np.complex128)
            self.Fb = np.asarray(checkpoint_data["Fb"], dtype=np.complex128)
            self.t = float(checkpoint_data["time"])
            self.count = int(checkpoint_data["count"])

    def _snapshot(self):
        """
        Return a snapshot of the internal state for propagation.
        """
        snapshot = {
            "time": self.t,
            "count": self.count,
            "Da": self.Da.copy(),
            "Db": self.Db.copy(),
            "Fa": self.Fa.copy(),
            "Fb": self.Fb.copy(),
        }
        return snapshot

    def _restore(self, snapshot):
        """
        Restore the internal state from a snapshot.
        """
        self.t = snapshot["time"]
        self.count = snapshot["count"]
        self.Da = snapshot["Da"]
        self.Db = snapshot["Db"]
        self.Fa = snapshot["Fa"]
        self.Fb = snapshot["Fb"]


if __name__ == "__main__":
    """
    Run the doctests to validate the RTTDDFTModel class.
    >>> python rttddft_model.py -v
    """
    import doctest

    doctest.testmod()
