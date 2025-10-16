import numpy as np
from scipy.linalg import expm
from scipy.linalg import fractional_matrix_power as mat_pow
import os

try:
    from .dummy_model import DummyModel
except:
    from dummy_model import DummyModel

try:
    import psi4
except ImportError:
    raise ImportError("Psi4 is required for the RTEhrenfestModel but is not installed.")


class RTTDDFTModel(DummyModel):
    """
    A real-time time-dependent density functional theory (RT-TDDFT) quantum dynamics model using the Psi4 quantum chemistry package.
    This class implements a RT-TDDFT model for quantum dynamics, which can be integrated with the MaxwellLink framework.

    Example
    -------
    Create from an XYZ file and then set the molecule id with restricted SCF:

    >>> model = RTTDDFTModel(
    ...     engine="psi4",
    ...     molecule_xyz="../../../../../tests/data/hcn.xyz",
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
    Energy (eV): [ 7.39216   8.554808  8.554808 10.736077 10.736077]
    Oscillator strengths: [0.       0.       0.       0.048238 0.048238]
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
        dft_grid_name: str = "SG0",
        dft_radial_points: int = -1,
        dft_spherical_points: int = -1,
        electron_propagation: str = "pc",
    ):
        """
        Initialize the necessary parameters for the RT-TDDFT quantum dynamics model.

        + **`engine`** (str): The computational engine to use (e.g., "psi4"). Default is "psi4". Currently, only "psi4" is supported.
        + **`molecule_xyz`** (str): Path to the XYZ file containing the molecular structure. The second line of the XYZ file may contain the charge and multiplicity.
        + **`functional`** (str): Any Psi4 functional label, e.g. "PBE", "B3LYP", "SCAN", "PBE0". Default is "SCF" (Hartree-Fock).
        + **`basis`** (str): Any basis set label recognized by Psi4, e.g. "sto-3g", "6-31g", "cc-pVDZ". Default is "sto-3g".
        + **`dt_rttddft_au`** (float): Time step for real-time TDDFT propagation in atomic units (a.u.). Default is 0.04 a.u.
        If the FDTD time step is an integer multiple of this, the driver will sub-step internally. This sub-stepping can avoid propagating EM fields
        too frequently when the molecule requires a small time step.
        + **`delta_kick_au`** (float): Strength of the initial delta-kick perturbation along the x, y, and z direction in atomic units (a.u.).
        Default is 0.0e-3 a.u. If this value is set to a non-zero value, the driver will apply a delta-kick perturbation at t=0 to initiate the dynamics.
        With this delta-kick, and also setting the FDTD coupling to zero, one can compute the conventional RT-TDDFT linear absorption spectrum of the molecule.
        + **`delta_kick_direction`** (str): Direction of the initial delta-kick perturbation. Can be "x", "y", "z", "xy", "xz", "yz", or "xyz". Default is "xyz".
        + **`memory`** (str): Memory allocation for Psi4, e.g. "8GB", "500MB". Default is "8GB".
        + **`num_threads`** (int): Number of CPU threads to use in Psi4. Default is 1.
        + **`checkpoint`** (bool): Whether to dump checkpoint files during propagation to allow restarting from the last checkpoint. Default is False.
        + **`restart`** (bool): Whether to restart the propagation from the last checkpoint. Default is False. Setting this to True requires that checkpoint files exist.
        When restarting, the driver will ignore the initial delta-kick perturbation even if it is set to a non-zero value.
        + **`verbose`** (bool): Whether to print verbose output. Default is False.
        + **`remove_permanent_dipole`** (bool): Whether to remove the effect of permanent dipole moments in the light-matter coupling term. Default is False.
        + **`dft_grid_name`** (str): Name of the DFT grid to use in Psi4, e.g. "SG0", "SG1". Default is "" (Psi4 default). Using "SG0" can speed up DFT calculations significantly.
        + **`dft_radial_points`** (int): Number of radial points in the DFT grid. Default is -1 (Psi4 default).
        + **`dft_spherical_points`** (int): Number of spherical points in the DFT grid. Default is -1 (Psi4 default).
        + **`electron_propagation`** (str): The electron propagation scheme to use. Options are "etrs" (Enforced Time-Reversal Symmetry) or "pc" (Predictor-Corrector). Default is "pc".
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
        if self.delta_kick_au != 0.0 and self.verbose:
            print(
                f"Initial delta-kick perturbation will be applied with strength {self.delta_kick_au} a.u. along direction(s) {self.delta_kick_direction}."
            )
        self.memory = memory
        self.num_threads = num_threads
        self.remove_permanent_dipole = remove_permanent_dipole
        # current time in a.u.
        self.t = 0.0
        self.count = 0

        # optional, checking whether the driver can be paused and resumed properly
        self.restarted = False

        self.step_started = False
        self.step_completed = False

        # store dipole moments, energies, and times during rt-tddft propagation
        self.dipoles = []
        self.energies = []
        self.times = []

        # extra DFT grid settings
        self.dft_grid_name = str(dft_grid_name)
        self.dft_radial_points = int(dft_radial_points)
        self.dft_spherical_points = int(dft_spherical_points)

        self.electron_propagation = electron_propagation.lower()
        if self.electron_propagation not in ["etrs", "pc"]:
            raise ValueError(
                "Invalid electron_propagation. Must be one of 'etrs' (Enforced Time-Reversal Symmetry) or 'pc' (Predictor-Corrector)."
            )

    # -------------- heavy-load initialization (at INIT) --------------

    def initialize(self, dt_new, molecule_id):
        """
        Set the time step and molecule ID for this quantum dynamics model.

        + **`dt_new`** (float): The new time step in atomic units (a.u.).
        + **`molecule_id`** (int): The ID of the molecule.
        """
        self.dt = float(dt_new)
        self.molecule_id = int(molecule_id)
        self.checkpoint_filename = "rttddft_checkpoint_id_%d.npy" % self.molecule_id

        # examine the relation between the FDTD dt and the RT-TDDFT dt
        assert self.dt >= self.dt_rttddft_au, (
            "The FDTD time step (dt=%.4f a.u.) must be greater than or equal to the RT-TDDFT time step (dt_rttddft_au=%.4f a.u.)."
            % (self.dt, self.dt_rttddft_au)
        )
        self.ratio_timestep = int(self.dt / self.dt_rttddft_au)
        if self.verbose:
            print(
                "[molecule %d] The FDTD time step (dt=%.4f a.u.) is (approximately) %d times the RT-TDDFT time step (dt_rttddft_au=%.4f a.u.). The driver will sub-step internally."
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

    # ------------ internal functions -------------

    def _init_psi4(self):
        """
        Initialize Psi4 and set up the molecule, basis set, and functional.
        This method is called during the first propagation step after receiving the molecule ID.
        """
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

        self.opts = {
            "basis": self.basis,
            "reference": "uks" if self.mol.multiplicity() != 1 else "rks",
            "scf_type": "out_of_core",
            "e_convergence": 1e-10,
            "d_convergence": 1e-10,
            "guess": "sad",
            "puream": True,
            "save_jk": True,  # for LR-TDDFT calculations
        }
        # ---- optional DFT functional and grid settings ----
        if self.dft_grid_name != "":
            self.opts["dft_grid_name"] = self.dft_grid_name
        if self.dft_radial_points > 0:
            self.opts["dft_radial_points"] = self.dft_radial_points
        if self.dft_spherical_points > 0:
            self.opts["dft_spherical_points"] = self.dft_spherical_points
        psi4.set_options(self.opts)

        # Initial ground-state KS calculation (to get wfn, grid, V_xc machinery, etc.)
        self.E0, wfn = psi4.energy(f"{self.functional}", return_wfn=True)
        self.wfn = wfn
        print(f"Initial SCF energy: {self.E0:.10f} Eh")

        # Integrals, matrices, helpers
        mints = psi4.core.MintsHelper(wfn.basisset())
        self.S = np.asarray(wfn.S())
        self.H = np.asarray(wfn.H())  # core Hamiltonian
        self.Enuc = self.mol.nuclear_repulsion_energy()
        # kinetic energy of classical nuclei, which will be assigned and updated in the Ehrenfest model
        self.kinEnuc = 0.0

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
            and (self.opts["reference"] == "rks")
        )
        self.Db = (
            np.asarray(wfn.Db()) if not self.is_restricted else self.Da.copy()
        )  # in RKS we just mirror Da

        self.Fa = np.asarray(wfn.Fa())
        self.Fb = np.asarray(wfn.Fb()) if not self.is_restricted else self.Fa.copy()

        # for predictor-corrector scheme
        self.Fa_halfprev = self.Fa.copy()
        self.Fb_halfprev = self.Fb.copy()

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
        self.Vpot = wfn.V_potential()
        # amount of exact HF exchange in global hybrids (0 for pure DFA)
        self.alpha_hfx = wfn.functional().x_alpha()

        if self.alpha_hfx < 1.0:
            self.Vpot.initialize()
            try:
                # prevents recomputing basis collocation on grid in compute_V()
                self.Vpot.build_collocation_cache(self.Vpot.nblocks())
            except Exception:
                pass

    def _build_KS_psi4(self, Da_np, Db_np, restricted, V_ext=None):
        """
        Return Fa, Fb given current densities.

        + **`Da_np`** (ndarray): Alpha density matrix in AO basis.
        + **`Db_np`** (ndarray): Beta density matrix in AO basis.
        + **`restricted`** (bool): Whether the calculation is restricted (RKS) or unrestricted (UKS).
        + **`V_ext`** (ndarray or None): External potential in AO basis to be added to both Fa and Fb. Default is None.
        """
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
        """
        Coulomb J[P] in AO basis using 4-index (pq|rs).

        + **`P`** (ndarray): Density matrix in AO basis.
        """
        return np.einsum("pqrs,rs->pq", self.I_ao, P, optimize=True)

    def _build_K_psi4(self, P):
        """
        Exchange K[P] in AO basis using 4-index (pr|qs). Only used if alpha_hfx > 0.

        + **`P`** (ndarray): Density matrix in AO basis.
        """
        return np.einsum("prqs,rs->pq", self.I_ao, P, optimize=True)

    def _build_Vxc_psi4(self, Da_np, Db_np, restricted):
        """
        Build V_xc in AO basis from current spin densities via VBase.
        VBase expects *real* densities; pass the real (Hermitian) parts.

        + **`Da_np`** (ndarray): Alpha density matrix in AO basis.
        + **`Db_np`** (ndarray): Beta density matrix in AO basis.
        + **`restricted`** (bool): Whether the calculation is restricted (RKS) or unrestricted (UKS).
        """
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

    def _molecule_positions_bohr(self):
        """
        Return current Cartesian positions (nat,3) in Bohr from psi4.Molecule.

        Returns
        -------
        R : (nat,3) ndarray in Bohr
        """
        nat = self.mol.natom()
        R = np.zeros((nat, 3), dtype=float)
        for a in range(nat):
            R[a, 0] = self.mol.x(a)
            R[a, 1] = self.mol.y(a)
            R[a, 2] = self.mol.z(a)
        return R

    def _energy_dipole_analysis_psi4(self, Da_np, Db_np):
        """
        Compute total energy and dipole moment from current densities.
        This is mainly for analysis and output during the propagation.

        + **`Da_np`** (ndarray): Alpha density matrix in AO basis.
        + **`Db_np`** (ndarray): Beta density matrix in AO basis.
        """
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
                ExHF = -self.alpha_hfx * np.einsum("pq,pq->", K, self.Da).real
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
        Etot = Eone + Ecoul + ExHF + Exc + self.Enuc + self.kinEnuc

        # electronic contribution to dipole moment
        mu_x = -np.trace(Dtot @ self.mu_ints[0]).real
        mu_y = -np.trace(Dtot @ self.mu_ints[1]).real
        mu_z = -np.trace(Dtot @ self.mu_ints[2]).real

        # nuclear contribution to dipole moment
        R = self._molecule_positions_bohr()
        Z = np.array([self.mol.Z(a) for a in range(self.mol.natom())], dtype=float)
        dip_n = (Z[:, None] * R).sum(axis=0)

        mu_x += dip_n[0]
        mu_y += dip_n[1]
        mu_z += dip_n[2]

        dip_vec = np.array([mu_x, mu_y, mu_z])

        return Etot, dip_vec

    def _propagate_etrs(self, effective_efield_vec, reset_substep_num=None):
        """
        Propagate the quantum molecular dynamics given the effective electric field vector using
        the Enforced Time-Reversal Symmetry (ETRS) scheme (https://octopus-code.org/documentation/14/manual/calculations/time-dependent/).

        + **`effective_efield_vec`**: Effective electric field vector in the form [Ex, Ey, Ez].
        """
        self.step_started = True
        self.step_completed = False

        if self.verbose:
            print(
                f"[molecule ID {self.molecule_id}] Time: {self.t:.4f} a.u., receiving e-field vector: [{effective_efield_vec[0]:.4E}, {effective_efield_vec[1]:.4E}, {effective_efield_vec[2]:.4E}]"
            )

        if self.engine == "psi4":
            # calculate the effective dipole matrix due to the coupling with the external E-field; minus sign for e = -1
            V_ext = (
                -self.mu_ints[0] * effective_efield_vec[0]
                - self.mu_ints[1] * effective_efield_vec[1]
                - self.mu_ints[2] * effective_efield_vec[2]
            )

            # sub-step the RT-TDDFT propagation if needed
            if reset_substep_num is not None:
                substep_num = int(reset_substep_num)
            else:
                substep_num = self.ratio_timestep
            # main for loop
            for step in range(substep_num):
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
                    V_ext += self.delta_kick_au * (
                        self.mu_ints[0] * self.delta_kick_vec[0]
                        + self.mu_ints[1] * self.delta_kick_vec[1]
                        + self.mu_ints[2] * self.delta_kick_vec[2]
                    )
                    self.Fa += V_ext
                    self.Fb += V_ext

                # Orthonormal-basis densities & KS matrices
                DaO = self.U @ self.Da @ self.U.T
                FaO = self.X.T @ self.Fa @ self.X
                if self.is_restricted:
                    # trial propagation, P(t+dt) = U(t) P(t) U^{\dagger}(t), U(t) = exp(-i dt F_O(t))
                    Uprop = expm((-1j * self.dt_rttddft_au) * FaO)
                    DaO_trial = Uprop @ DaO @ Uprop.conj().T
                    Da_trial = self.X @ DaO_trial @ self.X.T
                    Db_trial = Da_trial.copy()
                    # real propagation, P(t+dt) = U'(t) P(t) U'^{\dagger}(t), U'(t) = exp(-i dt/2 F_O(t+dt)) @ exp(-i dt/2 F_O(t))
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
                    # UKS: propagate alpha and beta separately
                    DbO = self.U @ self.Db @ self.U.T
                    FbO = self.X.T @ self.Fb @ self.X
                    # trial propagation, P(t+dt) = U(t) P(t) U^{\dagger}(t), U(t) = exp(-i dt F_O(t))
                    Uprop_a = expm(-(1j * self.dt_rttddft_au) * FaO)
                    Uprop_b = expm(-(1j * self.dt_rttddft_au) * FbO)
                    DaO_trial = Uprop_a @ DaO @ Uprop_a.conj().T
                    DbO_trial = Uprop_b @ DbO @ Uprop_b.conj().T
                    Da_trial = self.X @ DaO_trial @ self.X.T
                    Db_trial = self.X @ DbO_trial @ self.X.T
                    # real propagation, P(t+dt) = U'(t) P(t) U'^{\dagger}(t), U'(t) = exp(-i dt/2 F_O(t+dt)) @ exp(-i dt/2 F_O(t))
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

    def _propagate_pc(self, effective_efield_vec, reset_substep_num=None):
        """
        Propagate the quantum molecular dynamics given the effective electric field vector using
        the Predictor-Corrector (PC) scheme (https://pubs.acs.org/doi/10.1021/acs.jctc.0c00053).

        + **`effective_efield_vec`**: Effective electric field vector in the form [Ex, Ey, Ez].
        """
        self.step_started = True
        self.step_completed = False

        if self.verbose:
            print(
                f"[molecule ID {self.molecule_id}] Time: {self.t:.4f} a.u., receiving e-field vector: [{effective_efield_vec[0]:.4E}, {effective_efield_vec[1]:.4E}, {effective_efield_vec[2]:.4E}]"
            )

        if self.engine == "psi4":
            # calculate the effective dipole matrix due to the coupling with the external E-field; minus sign for e = -1
            V_ext = (
                -self.mu_ints[0] * effective_efield_vec[0]
                - self.mu_ints[1] * effective_efield_vec[1]
                - self.mu_ints[2] * effective_efield_vec[2]
            )

            # sub-step the RT-TDDFT propagation if needed
            if reset_substep_num is not None:
                substep_num = int(reset_substep_num)
            else:
                substep_num = self.ratio_timestep
            # main for loop
            for step in range(substep_num):
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
                    V_ext += self.delta_kick_au * (
                        self.mu_ints[0] * self.delta_kick_vec[0]
                        + self.mu_ints[1] * self.delta_kick_vec[1]
                        + self.mu_ints[2] * self.delta_kick_vec[2]
                    )
                    self.Fa += V_ext
                    self.Fb += V_ext

                if self.count == 0 or self.restarted:
                    self.Fa_halfprev = self.Fa.copy()
                    self.Fb_halfprev = self.Fb.copy()

                # Orthonormal-basis densities & KS matrices
                DaO = self.U @ self.Da @ self.U.T
                FaO = self.X.T @ self.Fa @ self.X
                FaO_halfprev = self.X.T @ self.Fa_halfprev @ self.X
                if self.is_restricted:
                    # RKS: propagate alpha only, beta = alpha
                    FaO_halffuture = 2.0 * FaO - FaO_halfprev
                    FaO_test = FaO_halffuture.copy()
                    counter = 1
                    while True:
                        Uprop = expm((-1j * self.dt_rttddft_au) * FaO_halffuture)
                        DaO_future = Uprop @ DaO @ Uprop.conj().T
                        Da_future = self.X @ DaO_future @ self.X.T
                        Db_future = Da_future.copy()
                        Fa_future, Fb_future = self._build_KS_psi4(
                            Da_future, Db_future, self.is_restricted, V_ext=V_ext
                        )
                        FaO_future = self.X.T @ Fa_future @ self.X
                        FaO_halffuture = 0.5 * (FaO + FaO_future)
                        if counter > 1:
                            FaO_diff = FaO_future - FaO_test
                            if self.verbose:
                                print(
                                    f"[molecule {self.molecule_id}]  Predictor-Corrector iteration {counter}, max|ΔF| = {np.max(np.abs(FaO_diff)):.8E}"
                                )
                            if np.linalg.norm(FaO_diff) < 1e-6:
                                break
                        FaO_test = FaO_future.copy()
                        counter += 1
                    # refresh for next step
                    self.Da = self.X @ DaO_future @ self.X.T
                    self.Db = self.Da.copy()
                    self.Fa = Fa_future.copy()
                    self.Fb = Fb_future.copy()
                    self.Fa_halfprev = self.U @ FaO_halffuture @ self.U.T
                    self.Fb_halfprev = self.Fa_halfprev.copy()

                else:
                    # UKS: propagate alpha and beta separately
                    DbO = self.U @ self.Db @ self.U.T
                    FbO = self.X.T @ self.Fb @ self.X
                    FbO_halfprev = self.X.T @ self.Fb_halfprev @ self.X

                    FaO_halffuture = 2.0 * FaO - FaO_halfprev
                    FbO_halffuture = 2.0 * FbO - FbO_halfprev
                    FaO_test = FaO_halffuture.copy()
                    FbO_test = FbO_halffuture.copy()
                    counter = 1
                    while True:
                        Uprop_alpha = expm((-1j * self.dt_rttddft_au) * FaO_halffuture)
                        DaO_future = Uprop_alpha @ DaO @ Uprop_alpha.conj().T
                        Da_future = self.X @ DaO_future @ self.X.T

                        Uprop_beta = expm((-1j * self.dt_rttddft_au) * FbO_halffuture)
                        DbO_future = Uprop_beta @ DbO @ Uprop_beta.conj().T
                        Db_future = self.X @ DbO_future @ self.X.T

                        Fa_future, Fb_future = self._build_KS_psi4(
                            Da_future, Db_future, self.is_restricted, V_ext=V_ext
                        )

                        FaO_future = self.X.T @ Fa_future @ self.X
                        FaO_halffuture = 0.5 * (FaO + FaO_future)

                        FbO_future = self.X.T @ Fb_future @ self.X
                        FbO_halffuture = 0.5 * (FbO + FbO_future)

                        if counter > 1:
                            FaO_diff = FaO_future - FaO_test
                            FbO_diff = FbO_future - FbO_test
                            if self.verbose:
                                print(
                                    f"[molecule {self.molecule_id}]  Predictor-Corrector iteration {counter}, max|ΔF| = {np.max(np.abs(FaO_diff)):.8E}"
                                )
                            if (
                                np.linalg.norm(FaO_diff) < 1e-6
                                and np.linalg.norm(FbO_diff) < 1e-6
                            ):
                                break
                        FaO_test = FaO_future.copy()
                        FbO_test = FbO_future.copy()

                        counter += 1
                    # refresh for next step
                    self.Da = self.X @ DaO_future @ self.X.T
                    self.Db = self.X @ DbO_future @ self.X.T
                    self.Fa = Fa_future.copy()
                    self.Fb = Fb_future.copy()
                    self.Fa_halfprev = self.U @ FaO_halffuture @ self.U.T
                    self.Fb_halfprev = self.U @ FbO_halffuture @ self.U.T

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

    # -------------- one FDTD step under E-field --------------

    def propagate(self, effective_efield_vec, reset_substep_num=None):
        """
        Propagate the quantum molecular dynamics given the effective electric field vector.

        + **`effective_efield_vec`**: Effective electric field vector in the form [Ex, Ey, Ez].
        + **`reset_substep_num`** (int or None): If provided, reset the number of sub-steps to this value for the current propagation step.
        """
        if self.electron_propagation == "etrs":
            self._propagate_etrs(
                effective_efield_vec, reset_substep_num=reset_substep_num
            )
        elif self.electron_propagation == "pc":
            self._propagate_pc(
                effective_efield_vec, reset_substep_num=reset_substep_num
            )
        else:
            raise ValueError(
                f"Unknown electron_propagation method: {self.electron_propagation}"
            )

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
                f"[molecule ID {self.molecule_id}] Time: {self.t:.4f} a.u., Energy: {self.energies[-1]:.6f} a.u., returning dmu_e/dt: {amp_vec[2]:.6E}"
            )

        self.step_completed = True
        self.step_started = False

        return amp_vec

    # ------------ optional operation / checkpoint --------------

    def append_additional_data(self):
        """
        Append additional data to be sent back to MaxwellLink, which can be retrieved by the user
        via: maxwelllink.SocketMolecule.additional_data_history.

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

    def _dump_to_checkpoint(self):
        """
        Dump the internal state of the model to a checkpoint.

        This function saves the current density matrices (Da, Db), Fock matrices (Fa, Fb),
        current time (t), and step count (count) to a NumPy .npy file.
        The checkpoint file is named "rttddft_checkpoint_id_<molid>.npy".
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

    def _reset_from_checkpoint(self):
        """
        Reset the internal state of the model from a checkpoint.
        """
        if not os.path.exists(self.checkpoint_filename):
            # No checkpoint file found means this driver has not been paused or terminated abnormally
            # so we just keep the current state
            print(
                "[checkpoint] No checkpoint file found for molecule ID %d, starting fresh."
                % self.molecule_id
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
        Return a snapshot of the internal state for propagation. Deep copy the arrays to avoid mutation issues.
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

    # ------------ standalone functions for debugging and testing --------------

    def _propagate_full_rt_tddft(self, nsteps=100, effective_efield_vec=np.zeros(3)):
        """
        Standalone function to propagate the RT-TDDFT for a given number of steps.
        This function is mainly for testing and validation purposes.
        To use this function, the self.delta_kick_au parameter in self.__init__()
        should be set to a non-zero value to apply an initial delta-kick perturbation at t=0.

        + **`nsteps`** (int): Number of FDTD steps to propagate. Default is 100.
        + **`effective_efield_vec`**: Effective electric field vector received from FDTD in the form [Ex, Ey, Ez]. Default is [0.0, 0.0, 0.0].
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

        + **`states`** (int): Number of excited states to compute. Default is 20.
        + **`tda`** (bool): Whether to use the Tamm-Dancoff approximation (TDA). Default is False (full TDDFT).
        0 = full TDDFT, 1 = TDA.
        """
        if self.engine == "psi4":
            from psi4.driver.procrouting.response.scf_response import tdscf_excitations

            res = tdscf_excitations(self.wfn, states=states, tda=tda, triplets="none")
            # Collect poles (Hartree)
            poles = np.array([r["EXCITATION ENERGY"] for r in res])

            tdm_len = np.array(
                [r["ELECTRIC DIPOLE TRANSITION MOMENT (LEN)"] for r in res]
            )
            mu2 = np.sum(tdm_len**2, axis=1)
            # Oscillator strengths (length gauge): f = 2/3 * \omega * |\mu|^2   (\omega in a.u.)
            oscillator_strengths = (2.0 / 3.0) * poles * mu2

            # save to file
            with open("psi4_lrtddft_output_id_%d.txt" % self.molecule_id, "w") as f:
                f.write("# Excitation energies (eV), oscillator strengths\n")
                for p, e in zip(poles, oscillator_strengths):
                    f.write(f"{p*27.211399} {e}\n")

            # print to screen
            # set numpy print precision
            np.set_printoptions(precision=6, suppress=True)
            print("Energy (eV):", poles * 27.211399)
            print("Oscillator strengths:", oscillator_strengths)


if __name__ == "__main__":
    """
    Run the doctests to validate the RTTDDFTModel class.
    >>> python rttddft_model.py -v
    """
    import doctest

    doctest.testmod()
