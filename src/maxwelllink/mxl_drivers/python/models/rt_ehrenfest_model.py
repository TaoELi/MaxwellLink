import numpy as np

# from scipy.linalg import expm
from scipy.linalg import fractional_matrix_power as mat_pow
import time
import os

try:
    from .rttddft_model import RTTDDFTModel
except:
    from rttddft_model import RTTDDFTModel

try:
    import psi4
except ImportError:
    raise ImportError("Psi4 is required for the RTEhrenfestModel but is not installed.")

np.set_printoptions(precision=12, suppress=True)


class RTEhrenfestModel(RTTDDFTModel):
    """
    A real-time time-dependent density functional theory Ehrenfest (RT-TDDFT-Ehrenfest)
    quantum dynamics model using the Psi4 quantum chemistry package.

    This class implements an RT-TDDFT-Ehrenfest model for quantum dynamics,
    which can be integrated with the MaxwellLink framework.

    Examples
    --------
    Create from an XYZ file and then set the molecule ID with restricted SCF:

    >>> model = RTEhrenfestModel(
    ...     engine="psi4",
    ...     molecule_xyz="../../../../../tests/data/hcn.xyz",
    ...     functional="scf",
    ...     basis="sto-3g",
    ...     dt_rttddft_au=0.04,
    ...     delta_kick_au=1.0e-3,
    ...     memory="2GB",
    ...     verbose=True,
    ...     remove_permanent_dipole=False
    ... )
    >>> model.initialize(dt_new=0.12, molecule_id=0)
    Initial SCF energy: -91.6751251525 Eh
    >>> model._propagate_rttddft_ehrenfest(n_nuc_steps=2)
    """

    def __init__(
        self,
        # ---- general RT-TDDFT parameters ----
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
        threshold_pc: float = 1e-6,
        # ---- control of Ehrenfest dynamics ----
        force_type: str = "ehrenfest",
        n_fock_per_nuc: int = 10,
        n_elec_per_fock: int = 10,
        mass_amu=None,
        friction_gamma_au: float = 0.0,
        temperature_K: float = None,
        rng_seed: int = 1234,
        homo_to_lumo: bool = False,
        partial_charges: list = None,
        fix_nuclei_indices: list = None,
        save_xyz: str = None,
    ):
        """
        Initialize the necessary parameters for the RT-TDDFT-Ehrenfest quantum dynamics model.

        Parameters
        ----------
        engine : str, default: "psi4"
            The computational engine to use (e.g., ``"psi4"``). Currently, only ``"psi4"`` is supported.
        molecule_xyz : str
            Path to the XYZ file containing the molecular structure. The second line of the XYZ file
            may contain the charge and multiplicity.
        functional : str, default: "SCF"
            Psi4 functional label, e.g., ``"PBE"``, ``"B3LYP"``, ``"SCAN"``, ``"PBE0"``.
        basis : str, default: "sto-3g"
            Basis set label recognized by Psi4, e.g., ``"sto-3g"``, ``"6-31g"``, ``"cc-pVDZ"``.
        dt_rttddft_au : float, default: 0.04
            Time step for real-time TDDFT propagation in atomic units (a.u.).
        delta_kick_au : float, default: 0.0e-3
            Strength of the initial delta-kick perturbation in atomic units.
        delta_kick_direction : str, default: "xyz"
            Direction of the delta-kick perturbation (e.g., ``"x"``, ``"xy"``, or ``"xyz"``).
        memory : str, default: "8GB"
            Memory allocation for Psi4.
        num_threads : int, default: 1
            Number of CPU threads used by Psi4.
        checkpoint : bool, default: False
            Whether to dump checkpoint files during propagation.
        restart : bool, default: False
            Whether to restart propagation from a checkpoint.
        verbose : bool, default: False
            Whether to print verbose output.
        remove_permanent_dipole : bool, default: False
            Remove the permanent dipole contribution from the light–matter coupling term.
        dft_grid_name : str, default: "SG0"
            Name of the DFT grid (e.g., ``"SG0"`` or ``"SG1"``).
        dft_radial_points : int, default: -1
            Number of radial grid points (Psi4 default if negative).
        dft_spherical_points : int, default: -1
            Number of angular grid points (Psi4 default if negative).
        electron_propagation : str, default: "pc"
            Electron propagation scheme. Options: ``"etrs"`` or ``"pc"``.
        threshold_pc : float, default: 1e-6
            Convergence threshold for predictor–corrector propagation.
        force_type : str, default: "ehrenfest"
            Type of forces used for nuclei: ``"bo"`` (Born–Oppenheimer) or ``"ehrenfest"``.
        n_fock_per_nuc : int, default: 10
            Number of Fock builds per nuclear update.
        n_elec_per_fock : int, default: 10
            Number of electronic RT-TDDFT steps per Fock build.
        mass_amu : array-like, optional
            Atomic masses (amu) for each atom; if ``None``, Psi4 defaults are used.
        friction_gamma_au : float, default: 0.0
            Friction coefficient for Langevin dynamics (a.u.).
        temperature_K : float, optional
            Temperature for Langevin thermostat (K).
        rng_seed : int, default: 1234
            Random seed for Langevin dynamics.
        homo_to_lumo : bool, default: False
            Promote one alpha electron from HOMO→LUMO at initialization.
        partial_charges : list, optional
            Per-atom partial charges for additional external-field forces.
        fix_nuclei_indices : list, optional
            Indices of fixed nuclei during propagation.
        save_xyz : str, optional
            Path to save XYZ trajectory during propagation.
        """

        super().__init__(
            engine=engine,
            molecule_xyz=molecule_xyz,
            functional=functional,
            basis=basis,
            dt_rttddft_au=dt_rttddft_au,
            delta_kick_au=delta_kick_au,
            delta_kick_direction=delta_kick_direction,
            memory=memory,
            num_threads=num_threads,
            checkpoint=checkpoint,
            restart=restart,
            verbose=verbose,
            remove_permanent_dipole=remove_permanent_dipole,
            dft_grid_name=dft_grid_name,
            dft_radial_points=dft_radial_points,
            dft_spherical_points=dft_spherical_points,
            electron_propagation=electron_propagation,
            threshold_pc=threshold_pc,
        )

        # --- Ehrenfest-specific parameters ---
        self.n_fock_per_nuc = int(n_fock_per_nuc)
        self.n_elec_per_fock = int(n_elec_per_fock)
        self.em_coupling_regime = None

        self.force_type = force_type.lower()
        self.mass_amu = mass_amu
        self.friction_gamma_au = friction_gamma_au
        self.temperature_K = temperature_K
        self.rng_seed = int(rng_seed)

        self.homo_to_lumo = homo_to_lumo
        self.partial_charges = partial_charges
        self.fix_nuclei_indices = fix_nuclei_indices
        self.save_xyz = save_xyz

        # dipole and dmudt info in previous time steps
        self.dipole_prev = None
        self.dmudt_prev = None
        self.dipole_middlepoint = None
        self.dmudt_middlepoint = None
        self.dipole_projected = None
        self.dmudt_projected = None

    # -------------- heavy-load initialization (at INIT) --------------

    def initialize(self, dt_new, molecule_id):
        """
        Set the time step and molecule ID for this quantum dynamics model.

        Parameters
        ----------
        dt_new : float
            The new time step in atomic units (a.u.).
        molecule_id : int
            The ID of the molecule.
        """
        super().initialize(dt_new, molecule_id)
        # now self.dt_rttddft_au is set as an integer divisor of dt_new
        # we need to determine whether dt_new matches the fock step or the nuc step, otherwise we warn
        # self.ratio_timestep = int(dt_new / self.dt_rttddft_au) defined in super().initialize()
        if self.verbose:
            print("# RT-TDDFT time step (a.u.):", self.dt_rttddft_au)
            print("# FDTD time step (a.u.):", dt_new)
            print(
                "# Ehrenfest Fock update (n_elec_per_fock) every",
                self.n_elec_per_fock,
                "RT-TDDFT steps",
            )
            print(
                "# Ehrenfest Nuclear update (n_fock_per_nuc * n_elec_per_fock) every",
                self.n_fock_per_nuc * self.n_elec_per_fock,
                "RT-TDDFT steps",
            )
            print("# Ratio of FDTD to RT-TDDFT step:", self.ratio_timestep)
            print(
                "To make our life easier, we need this ratio (>=1) to satisfy (i) n_elec_per_fock MOD ratio = 0 , or (ii) n_fock_per_nuc * n_elec_per_fock."
            )
            print(
                "When this FDTD/RT-TDDFT step ratio satisfies *n_elec_per_fock MOD ratio = 0*, we are in the 'electronic' coupling regime."
            )
            print(
                "When this ratio matches *n_fock_per_nuc * n_elec_per_fock*, we are in the 'nuclear' coupling regime."
            )
        if self.n_elec_per_fock % self.ratio_timestep == 0 and self.ratio_timestep >= 1:
            self.em_coupling_regime = "electronic"
        elif self.ratio_timestep == self.n_fock_per_nuc * self.n_elec_per_fock:
            self.em_coupling_regime = "nuclear"
        else:
            raise RuntimeError(
                f"FDTD step / RT-TDDFT step = {self.ratio_timestep} does not equal to n_elec_per_fock = {self.n_elec_per_fock} or n_fock_per_nuc * n_elec_per_fock = {self.n_fock_per_nuc * self.n_elec_per_fock}. Please adjust dt_rttddft_au or the n_elec_per_fock / n_fock_per_nuc parameters."
            )

        if self.verbose:
            print("EM coupling regime:", self.em_coupling_regime)

        # additional initialization before Ehrenfest dynamics
        self.rng = np.random.default_rng(int(self.rng_seed))

        # --- sizes and masses ---
        nat = self.mol.natom()
        if self.mass_amu is None:
            self.mass_amu = np.array(
                [self.mol.mass(a) for a in range(nat)], dtype=float
            )
        self.mass_au = self.mass_amu * 1822.888486209

        # --- time partitioning ---
        self.dtN = self.dt_rttddft_au * self.n_elec_per_fock * self.n_fock_per_nuc
        self.dtNe = self.dt_rttddft_au * self.n_elec_per_fock
        self.dte = self.dt_rttddft_au

        # --- choose forces ---
        if self.force_type not in ("bo", "ehrenfest"):
            raise ValueError("force_type must be 'bo' or 'ehrenfest'")
        self.compute_forces = (
            self._compute_ehrenfest_forces_bohr
            if self.force_type == "ehrenfest"
            else self._compute_bo_forces_bohr
        )

        # --- thermostat constants (optional) ---
        self.sigma_noise = None
        if self.friction_gamma_au > 0.0 and (self.temperature_K is not None):
            kB_au_per_K = 3.166811563e-6
            self.sigma_noise = np.sqrt(
                2.0 * self.friction_gamma_au * kB_au_per_K * self.temperature_K
            )

        # --- initial state ---
        self.Rk = self._molecule_positions_bohr()
        self.Vk = np.zeros_like(self.Rk)

        if self.homo_to_lumo:
            self._prepare_alpha_homo_to_lumo_excited_state()

        self.Fk = self.compute_forces(efield_vec=None)

        if self.partial_charges is not None:
            self.partial_charges = np.array(self.partial_charges, dtype=float)
        else:
            self.partial_charges = np.zeros(self.mol.natom(), dtype=float)

        self._step_in_cycle = 0
        self._V_inst = self.Vk.copy()

        self.traj_R = [self.Rk.copy()]
        self.traj_V = [self.Vk.copy()]
        self.traj_F = [self.Fk.copy()]

        self.energies_eh = []
        self.dipoles_eh = []

        self._count_append_xyz_to_file = 0

        if self.fix_nuclei_indices is None:
            self.fix_nuclei_indices = []

        # consider whether to restart from a checkpoint. We do this here because this function
        # is called in the driver during the INIT stage of the socket communication.
        if self.restart and self.checkpoint:
            self._reset_from_checkpoint(self.molecule_id)
            self.restarted = True

        if self.save_xyz is not None:
            self._append_xyz_to_file(filename=self.save_xyz)

    # ------------ internal functions -------------

    def _set_molecule_positions_bohr(self, R):
        """
        Set Psi4 molecule geometry from new Cartesian positions in Bohr and update Psi4.

        Parameters
        ----------
        R : numpy.ndarray
            New Cartesian positions with shape ``(nat, 3)`` in Bohr.
        """

        geom = psi4.core.Matrix.from_array(R)
        self.mol.set_geometry(geom)
        self.mol.update_geometry()

    def _density_to_orth(self, Da, Db):
        """
        Return spin densities in the orthonormal AO basis.

        Transformation:
        :math:`P_O = S^{1/2} P S^{1/2}`

        Returns
        -------
        DaO : numpy.ndarray
            Alpha density in the orthonormal AO basis.
        DbO : numpy.ndarray
            Beta density in the orthonormal AO basis.
        """

        DaO = self.U @ Da @ self.U.T
        if self.is_restricted:
            DbO = DaO.copy()
        else:
            DbO = self.U @ Db @ self.U.T
        return DaO, DbO

    def _fock_to_orth(self, Fa, Fb):
        """
        Return spin Fock matrices in the orthonormal AO basis.

        Transformation:
        :math:`F_O = S^{-1/2} F S^{-1/2}`

        Parameters
        ----------
        Fa : numpy.ndarray
            Alpha Fock matrix in AO basis.
        Fb : numpy.ndarray
            Beta Fock matrix in AO basis.

        Returns
        -------
        FaO : numpy.ndarray
            Alpha Fock matrix in orthonormal AO basis.
        FbO : numpy.ndarray
            Beta Fock matrix in orthonormal AO basis.
        """

        FaO = self.X.T @ Fa @ self.X
        if self.is_restricted:
            FbO = FaO.copy()
        else:
            FbO = self.X.T @ Fb @ self.X
        return FaO, FbO

    def _density_from_orth(self, DaO, DbO):
        """
        Reconstruct AO densities from orthonormal densities using
        :math:`X = S^{-1/2}`.

        Parameters
        ----------
        DaO : numpy.ndarray
            Alpha density in the orthonormal AO basis.
        DbO : numpy.ndarray
            Beta density in the orthonormal AO basis.

        Returns
        -------
        Da : numpy.ndarray
            Alpha density in AO basis.
        Db : numpy.ndarray
            Beta density in AO basis.
        """

        Da = self.X @ DaO @ self.X.T
        if self.is_restricted:
            Db = Da.copy()
        else:
            Db = self.X @ DbO @ self.X.T
        return Da, Db

    def _fock_from_orth(self, FaO, FbO):
        """
        Reconstruct AO Fock matrices from orthonormal Fock matrices using
        :math:`U = S^{1/2}`.

        Parameters
        ----------
        FaO : numpy.ndarray
            Alpha Fock matrix in the orthonormal AO basis.
        FbO : numpy.ndarray
            Beta Fock matrix in the orthonormal AO basis.

        Returns
        -------
        Fa : numpy.ndarray
            Alpha Fock matrix in AO basis.
        Fb : numpy.ndarray
            Beta Fock matrix in AO basis.
        """

        Fa = self.U @ FaO @ self.U.T
        if self.is_restricted:
            Fb = Fa.copy()
        else:
            Fb = self.U @ FbO @ self.U.T
        return Fa, Fb

    def _rebuild_at_geometry_preserving_PO(self, R_new, effective_efield_vec=None):
        """
        Move the molecule to new positions, refresh Psi4 internals,
        and preserve orthonormal density :math:`P_O` across the basis change.

        Parameters
        ----------
        R_new : numpy.ndarray
            New Cartesian positions ``(nat, 3)`` in Bohr.
        effective_efield_vec : numpy.ndarray, optional
            Electric field vector ``[E_x, E_y, E_z]`` in a.u.
        """

        timer_start = time.time()

        # 1. capture P_O in the old basis
        DaO, DbO = self._density_to_orth(self.Da, self.Db)
        FaO, FbO = self._fock_to_orth(self.Fa, self.Fb)
        FaO_halfprev, FbO_halfprev = self._fock_to_orth(
            self.Fa_halfprev, self.Fb_halfprev
        )

        # 2. move nuclei and refresh integrals/grid machinery
        self._set_molecule_positions_bohr(R_new)
        self._refresh_psi4_internals_after_geom_change()  # updates S, X, U, H, ERI, Vpot,...

        # 3. map P_O -> new AO densities with the new X = S^{-1/2}
        self.Da, self.Db = self._density_from_orth(DaO, DbO)
        self.Fa, self.Fb = self._fock_from_orth(FaO, FbO)
        self.Fa_halfprev, self.Fb_halfprev = self._fock_from_orth(
            FaO_halfprev, FbO_halfprev
        )

        # 4. rebuild KS/Fock at the new geometry with the mapped densities
        V_ext = None
        if effective_efield_vec is not None:
            V_ext = (
                +self.mu_ints[0] * effective_efield_vec[0]
                + self.mu_ints[1] * effective_efield_vec[1]
                + self.mu_ints[2] * effective_efield_vec[2]
            )
        self.Fa, self.Fb = self._build_KS_psi4(
            self.Da, self.Db, self.is_restricted, V_ext=V_ext
        )

        timer_end = time.time()
        elapsed_time = timer_end - timer_start
        if self.verbose:
            print(f"Geometry change and Psi4 refresh time: {elapsed_time:.6f} seconds")

    def _refresh_psi4_internals_after_geom_change(self):
        """
        Refresh Psi4 internal matrices and integrals after a geometry change.

        Notes
        -----
        Performs a short SCF (3 iterations, loose convergence) to refresh the
        wavefunction and DFT grid without recomputing a full SCF cycle.
        """

        if not hasattr(self, "wfn") or self.wfn is None:
            raise RuntimeError(
                "Wavefunction container (self.wfn) is not set. Call initialize() first."
            )
        _refresh_opts = self.opts.copy()
        _refresh_opts["e_convergence"] = 1e-1
        _refresh_opts["d_convergence"] = 1e-1
        _refresh_opts["maxiter"] = 3
        _refresh_opts["fail_on_maxiter"] = False
        # if a homo->lumo occurs, we need the correct ref to refresh XC
        ref = "rks" if self.is_restricted else "uks"
        _refresh_opts["reference"] = ref
        psi4.set_options(_refresh_opts)

        # we use a cheap SCF call to refresh the Wavefunction object
        _, wfn = psi4.energy(f"{self.functional}", return_wfn=True)
        self.wfn = wfn

        # Integrals, matrices, helpers
        mints = psi4.core.MintsHelper(wfn.basisset())
        self.S = np.asarray(wfn.S())
        self.H = np.asarray(wfn.H())  # core Hamiltonian

        # AO integrals at new geometry
        I_ao = np.asarray(mints.ao_eri())
        mu_ints = [np.asarray(m) for m in mints.ao_dipole()]

        if self.remove_permanent_dipole:
            mu_x0 = self.mu_x0
            mu_y0 = self.mu_y0
            mu_z0 = self.mu_z0
            Iden = np.eye(mu_ints[0].shape[0])
            trD = np.trace(self.Da + self.Db)
            mu_ints = [
                mu_ints[0] - mu_x0 * Iden / trD,
                mu_ints[1] - mu_y0 * Iden / trD,
                mu_ints[2] - mu_z0 * Iden / trD,
            ]

        # Update metric transforms
        self.I_ao = I_ao
        self.mu_ints = mu_ints
        self.X = mat_pow(self.S, -0.5)
        self.U = mat_pow(self.S, 0.5)

        # Fresh XC quadrature/grid without SCF (critical for B3LYP)
        if self.alpha_hfx < 1.0:
            self.Vpot = self.wfn.V_potential()
            try:
                self.Vpot.initialize()
                self.Vpot.build_collocation_cache(self.Vpot.nblocks())
            except Exception:
                print(
                    "[Warning] Failed to initialize Psi4 DFT V_potential grid, previous V_potential will be used."
                )

        # Update nuclear repulsion (reporting / forces)
        self.Enuc = self.mol.nuclear_repulsion_energy()

    def _compute_bo_forces_bohr(self, efield_vec=None):
        """
        Compute Born–Oppenheimer forces
        :math:`F_A = -\\partial E / \\partial R_A` (Hartree/Bohr)
        at the current geometry using Psi4’s analytic SCF gradient.

        Parameters
        ----------
        efield_vec : numpy.ndarray, optional
            External electric field vector ``(3,)`` in a.u.

        Returns
        -------
        numpy.ndarray
            Force array ``(nat, 3)`` in Hartree/Bohr.
        """

        timer_start = time.time()

        # to override the psi4 scf options, which might be changed by the geometry refresh function
        psi4.set_options(self.opts)

        G = np.asarray(psi4.gradient(f"{self.functional}", molecule=self.mol))

        # ---- Direct gradient from partial charge (-Z_A * E(t)) ----
        if efield_vec is None:
            efield_vec = np.zeros(3, dtype=float)
        else:
            efield_vec = np.asarray(efield_vec, dtype=float)
        nat = self.mol.natom()
        if self.partial_charges is None:
            self.partial_charges = np.zeros(nat, dtype=float)
        g_field_nuc = -np.outer(self.partial_charges, efield_vec).reshape(nat, 3)

        G += g_field_nuc

        F = -G

        if self.fix_nuclei_indices is not None:
            for idx in self.fix_nuclei_indices:
                F[idx, :] = 0.0

        timer_end = time.time()
        elapsed_time = timer_end - timer_start
        if self.verbose:
            print("BO forces (Eh/Bohr):\n", F)
            print(f"BO force computation time: {elapsed_time:.6f} seconds")
        return F

    def _compute_ehrenfest_forces_bohr(
        self, include_xc_grad: bool = True, efield_vec=None
    ):
        """
        Compute Ehrenfest forces
        :math:`F_A = -\\partial E / \\partial R_A`
        from current RT-TDDFT densities and Fock matrices.

        Parameters
        ----------
        include_xc_grad : bool, default: True
            Include exchange–correlation gradient contributions.
        efield_vec : numpy.ndarray, optional
            External electric field vector ``(3,)`` in a.u.

        Returns
        -------
        numpy.ndarray
            Force array ``(nat, 3)`` in Hartree/Bohr.
        """

        timer_start = time.time()

        nat = self.mol.natom()
        nbf = self.S.shape[0]

        Da = self.Da.copy()
        Db = self.Db.copy()
        D = Da + Db

        # AO Focks from your RT step (already built against current geometry)
        Fa = np.asarray(self.Fa, dtype=complex)
        Fb = np.asarray(self.Fb, dtype=complex)

        # Overlap factors
        S = np.asarray(self.S)
        X = np.asarray(self.X)
        # U = np.asarray(self.U)

        # Use MintsHelper convenience wrappers for derivatives.
        mints = psi4.core.MintsHelper(self.wfn.basisset())

        #  Allocate energy gradient accumulator (nat,3)
        g = np.zeros((nat, 3), dtype=float)

        # --- 1. One-electron derivatives: dT + dV_nuc (Hellmann-Feynman) ---
        g_1e_T = np.zeros((nat, 3), dtype=float)
        g_1e_V = np.zeros((nat, 3), dtype=float)
        g_1e = np.zeros((nat, 3), dtype=float)
        for A in range(nat):
            dT = [
                np.asarray(M) for M in mints.ao_oei_deriv1(oei_type="KINETIC", atom=A)
            ]
            dV = [
                np.asarray(M) for M in mints.ao_oei_deriv1(oei_type="POTENTIAL", atom=A)
            ]
            for c in range(3):
                g_1e_T[A, c] += np.einsum("pq,pq->", D, dT[c], optimize=True).real
                g_1e_V[A, c] += np.einsum("pq,pq->", D, dV[c], optimize=True).real
                g_1e[A, c] += np.einsum("pq,pq->", D, dT[c] + dV[c], optimize=True).real

        # --- 2. Two-electron derivatives: Coulomb & HF exchange ---
        g_2e_coul = np.zeros((nat, 3), dtype=float)
        g_2e_exch_alpha = np.zeros((nat, 3), dtype=float)
        g_2e_exch_beta = np.zeros((nat, 3), dtype=float)
        g_2e_exch_alpha_real = np.zeros((nat, 3), dtype=float)
        g_2e_exch_beta_real = np.zeros((nat, 3), dtype=float)
        g_2e_exch_alpha_imag = np.zeros((nat, 3), dtype=float)
        g_2e_exch_beta_imag = np.zeros((nat, 3), dtype=float)
        g_2e_exch = np.zeros((nat, 3), dtype=float)
        use_hf_exchange = self.alpha_hfx > 0.0
        for A in range(nat):
            dERI_xyz = mints.ao_tei_deriv1(A)
            # Convert to NumPy (nbf,nbf,nbf,nbf)
            dERI = [np.asarray(T) for T in dERI_xyz]
            for c in range(3):
                g_2e_coul[A, c] += (
                    0.5 * np.einsum("pq,rs,pqrs->", D, D, dERI[c], optimize=True).real
                )
                if use_hf_exchange:
                    g_2e_exch_alpha[A, c] -= (
                        0.5
                        * self.alpha_hfx
                        * np.einsum("pr,qs,pqrs->", Da, Da, dERI[c], optimize=True).real
                    )
                    g_2e_exch_beta[A, c] -= (
                        0.5
                        * self.alpha_hfx
                        * np.einsum("pr,qs,pqrs->", Db, Db, dERI[c], optimize=True).real
                    )
                    g_2e_exch_alpha_real[A, c] -= (
                        0.5
                        * self.alpha_hfx
                        * np.einsum(
                            "pr,qs,pqrs->",
                            np.real(Da),
                            np.real(Da),
                            dERI[c],
                            optimize=True,
                        )
                    )
                    g_2e_exch_beta_real[A, c] -= (
                        0.5
                        * self.alpha_hfx
                        * np.einsum(
                            "pr,qs,pqrs->",
                            np.real(Db),
                            np.real(Db),
                            dERI[c],
                            optimize=True,
                        )
                    )
                    g_2e_exch_alpha_imag[A, c] -= (
                        0.5
                        * self.alpha_hfx
                        * np.einsum(
                            "pr,qs,pqrs->",
                            np.imag(Da),
                            np.imag(Da),
                            dERI[c],
                            optimize=True,
                        )
                    )
                    g_2e_exch_beta_imag[A, c] -= (
                        0.5
                        * self.alpha_hfx
                        * np.einsum(
                            "pr,qs,pqrs->",
                            np.imag(Db),
                            np.imag(Db),
                            dERI[c],
                            optimize=True,
                        )
                    )
        g_2e_exch = g_2e_exch_alpha + g_2e_exch_beta

        # --- 3. Metric (non-variational) term from dS^{1/2} ---
        # Helpers to build dS^{1/2} from dS (Zhao et al. JCP 153, 224111 (2020))
        sigma, s_vec = np.linalg.eigh(S)
        sqrt_sigma = np.sqrt(sigma)

        def dS_half_from_dS(dS):
            dS_half = 0
            for i in range(nbf):
                s_vec_i = s_vec[:, i][:, None]  # (nbf,1)
                for j in range(nbf):
                    s_vec_j = s_vec[:, j][:, None]  # (nbf,1)
                    inv_sigma_i_sigma_j = 1.0 / (sqrt_sigma[i] + sqrt_sigma[j] + 1e-20)
                    sij = s_vec_i.T @ dS @ s_vec_j
                    dS_half += (s_vec_i * (inv_sigma_i_sigma_j * sij)) @ s_vec_j.T
            return dS_half

        g_s = np.zeros((nat, 3), dtype=float)
        for A in range(nat):
            dS = [
                np.asarray(M) for M in mints.ao_oei_deriv1(oei_type="OVERLAP", atom=A)
            ]
            for c in range(3):
                dS_half = dS_half_from_dS(dS[c])
                g_s[A, c] -= (
                    np.trace(Fa @ X @ dS_half @ Da)
                    + np.trace(Da @ dS_half @ X @ Fa)
                    + np.trace(Fb @ X @ dS_half @ Db)
                    + np.trace(Db @ dS_half @ X @ Fb)
                ).real

        # --- 4. Exchange–correlation gradient ---
        g_xc = np.zeros((nat, 3), dtype=float)
        if include_xc_grad and (self.alpha_hfx < 1.0):
            # Ensure the V_potential at the current geometry is initialized.
            V = self.Vpot
            try:
                V.initialize()
                V.build_collocation_cache(V.nblocks())
            except Exception:
                print(
                    "[Warning] Failed to initialize Psi4 DFT V_potential grid for gradients, previous V_potential will be used."
                )

            if self.is_restricted:
                Dm = psi4.core.Matrix.from_array(np.real(Da))
                V.set_D([Dm])
            else:
                Da_m = psi4.core.Matrix.from_array(np.real(Da))
                Db_m = psi4.core.Matrix.from_array(np.real(Db))
                V.set_D([Da_m, Db_m])

            Gxc_mat = V.compute_gradient()
            g_xc = np.asarray(Gxc_mat)

        # --- 5. Nuclear–nuclear repulsion gradient by hand ---
        g_nuc = np.zeros((nat, 3), dtype=float)
        Z = np.array([self.mol.Z(a) for a in range(nat)], dtype=float)
        R = np.array(
            [[self.mol.x(a), self.mol.y(a), self.mol.z(a)] for a in range(nat)],
            dtype=float,
        )
        for A in range(nat):
            for B in range(nat):
                if A == B:
                    continue
                dR = R[A] - R[B]
                r3 = (dR @ dR) ** 1.5 + 1e-20
                g_nuc[A] += -Z[A] * Z[B] * dR / r3

        # ---- 6. Direct nuclear field gradient (-Z_A * E(t)) ----
        if efield_vec is None:
            efield_vec = np.zeros(3, dtype=float)
        else:
            efield_vec = np.asarray(efield_vec, dtype=float)
        g_field_nuc = -np.outer(Z, efield_vec).reshape(nat, 3)

        # Convert energy gradients to forces
        g = g_1e + g_2e_coul + g_2e_exch + g_s + g_nuc + g_xc + g_field_nuc
        forces = -g

        if self.fix_nuclei_indices is not None:
            for idx in self.fix_nuclei_indices:
                forces[idx, :] = 0.0

        timer_end = time.time()
        elapsed_time = timer_end - timer_start
        if self.verbose:
            """
            print("Force components (Eh/Bohr):")
            print("begin components")
            print("  1e kinetic:\n", g_1e_T)
            print("  1e nuclear attraction:\n", g_1e_V)
            print("  1e Hellmann–Feynman:\n", g_1e)
            print("  2e Coulomb:\n", g_2e_coul)
            print("  2e HF exchange (alpha):\n", g_2e_exch_alpha)
            print("  2e HF exchange (beta):\n", g_2e_exch_beta)
            print("  2e HF exchange (alpha) real:\n", g_2e_exch_alpha_real)
            print("  2e HF exchange (beta) real:\n", g_2e_exch_beta_real)
            print("  2e HF exchange (alpha) imag:\n", g_2e_exch_alpha_imag)
            print("  2e HF exchange (beta) imag:\n", g_2e_exch_beta_imag)
            print("  2e total:\n", g_2e_coul + g_2e_exch)
            print("  S term:\n", g_s)
            print("  nuclear repulsion:\n", g_nuc)
            if include_xc_grad and (self.alpha_hfx < 1.0):
                print("  XC:\n", g_xc)
            print("  nuclear-field:\n", g_field_nuc)
            print("end of components\n")
            """
            print("Total Ehrenfest forces (Eh/Bohr):\n", forces)
            print(f"Ehrenfest force computation time: {elapsed_time:.6f} seconds")

        return forces

    def _prepare_alpha_homo_to_lumo_excited_state(self):
        """
        Promote one alpha electron from HOMO→LUMO (beta unchanged) and switch
        to unrestricted propagation. Rebuild Fock matrices at ``t = 0``.
        """

        wfn = self.wfn

        # --- sanity: closed-shell start ---
        nalpha = wfn.nalpha()
        nbeta = wfn.nbeta()
        if nalpha != nbeta:
            raise RuntimeError("This helper expects a closed-shell SCF (nα == nβ).")

        # --- MO coefficients (AO->MO) and sizes ---
        C = np.asarray(wfn.Ca())  # (nbf, nmo) real-valued for RHF/RKS
        nbf, nmo = C.shape
        nocc = nalpha  # closed-shell: nocc spatial orbitals
        homo = nocc - 1
        lumo = nocc
        if lumo >= nmo:
            raise RuntimeError("No LUMO available (nmo == nocc). Use a larger basis.")

        # --- build spin occupations ---
        # start from ground-state occupations (1 per spin in the first nocc orbitals)
        occ_a = np.zeros(nmo)
        occ_a[:nocc] = 1.0
        occ_b = np.zeros(nmo)
        occ_b[:nocc] = 1.0

        # promote ONE alpha electron: HOMO -> LUMO
        occ_a[homo] -= 1.0
        occ_a[lumo] += 1.0

        if self.verbose:
            print("[init] Preparing alpha (HOMO->LUMO) excited determinant")
            print("[init] occ_a:", occ_a)
            print("[init] occ_b:", occ_b)
            print("[init] Switching to unrestricted KS propagation.")

        # --- spin densities in AO basis: Dσ = C * diag(occσ) * C^T ---
        Da = C @ np.diag(occ_a) @ C.T
        Db = C @ np.diag(occ_b) @ C.T

        # --- install densities and switch to UKS mode for propagation ---
        self.Da = Da
        self.Db = Db
        self.is_restricted = False  # force UKS propagation branch

        # Ensure the XC machinery can accept two-spin densities.
        # Most builds of Psi4's VBase handle set_D([Da, Db]) fine; if not, fall back by
        # rebuilding a UKS V_potential once (one-off SCF cost).
        if self.alpha_hfx < 1.0:
            try:
                _ = self._build_Vxc_psi4(self.Da, self.Db, restricted=False)
            except Exception:
                # Rebuild a UKS wavefunction solely to get a UKS-configured V_potential.
                # This is a one-time cost and does not affect your manually set densities.
                psi4.set_options({"reference": "uks"})
                _, wfn_uks = psi4.energy(f"{self.functional}", return_wfn=True)
                self.wfn = wfn_uks
                self.Vpot = wfn_uks.V_potential()
                self.alpha_hfx = self.wfn.functional().x_alpha()
                self.Vpot.initialize()
                try:
                    self.Vpot.build_collocation_cache(self.Vpot.nblocks())
                except Exception:
                    pass

        # Build initial Fa/Fb at t = 0 (no external field)
        self.Fa, self.Fb = self._build_KS_psi4(
            self.Da, self.Db, restricted=False, V_ext=None
        )

        Etot, __ = self._energy_dipole_analysis_psi4(self.Da, self.Db)
        self.E0 = Etot
        if self.verbose:
            print(f"[init] After HOMO->LUMO swap: E0 = {self.E0:.6f} Eh")

    def _propagate_rttddft_ehrenfest(
        self,
        n_nuc_steps: int,
        efield_vec=np.zeros(3),
        nuc_dt_au: float = 0.4,
        n_fock_per_nuc: int = 2,
        elec_substeps_per_fock: int = None,
        mass_amu=None,
        friction_gamma_au: float = 0.0,
        temperature_K: float = None,
        rng_seed: int = 1234,
        save_trajectory: bool = True,
        force_type: str = "ehrenfest",
    ):
        """
        Three-time-scale Ehrenfest integrator
        (Li–Tully–Schlegel–Frisch scheme).

        Performs velocity Verlet for nuclei (Δt_N),
        midpoint-coupled Fock updates (Δt_Ne = Δt_N / n),
        and MMUT-like real-time electronic propagation (Δt_e = Δt_Ne / m).

        Parameters
        ----------
        n_nuc_steps : int
            Number of nuclear steps.
        efield_vec : numpy.ndarray, default: zeros(3)
            Constant electric field in a.u.
        nuc_dt_au : float, default: 0.4
            Nuclear time step (a.u.).
        n_fock_per_nuc : int, default: 2
            Fock builds per nuclear step.
        elec_substeps_per_fock : int, optional
            RT-TDDFT substeps per Fock update.
        mass_amu : numpy.ndarray, optional
            Atomic masses in amu.
        friction_gamma_au : float, default: 0.0
            Friction coefficient for Langevin dynamics.
        temperature_K : float, optional
            Bath temperature in Kelvin for Langevin dynamics.
        rng_seed : int, default: 1234
            Random seed for the thermostat.
        save_trajectory : bool, default: True
            Whether to save trajectory data to disk.
        force_type : str, default: "ehrenfest"
            Type of force used: ``"bo"`` or ``"ehrenfest"``.
        """

        rng = np.random.default_rng(int(rng_seed))

        # --- sizes and masses ---
        nat = self.mol.natom()
        if mass_amu is None:
            mass_amu = np.array([self.mol.mass(a) for a in range(nat)], dtype=float)
        mass_au = mass_amu * 1822.888486209

        # --- time partitioning ---
        assert n_fock_per_nuc >= 1
        dtN = float(nuc_dt_au)
        dtNe = dtN / int(n_fock_per_nuc)
        if elec_substeps_per_fock is None:
            elec_substeps_per_fock = max(1, int(round(dtNe / self.dt_rttddft_au)))
        dte = self.dt_rttddft_au
        assert (
            abs(elec_substeps_per_fock * dte - dtNe) / dtNe < 1e-12
        ), "Choose dt_N and n_fock_per_nuc so that dt_Ne is an integer multiple of dt_e."

        print(
            f"[RT-Ehrenfest] dt_N = {dtN:.4f} au, n_fock_per_nuc = {n_fock_per_nuc}, dt_Ne = {dtNe:.4f} au, elec_substeps_per_fock = {elec_substeps_per_fock}, dt_e = {dte:.4f} au"
        )

        # --- choose forces ---
        ftype = force_type.lower()
        if ftype not in ("bo", "ehrenfest"):
            raise ValueError("force_type must be 'bo' or 'ehrenfest'")
        compute_forces = (
            self._compute_ehrenfest_forces_bohr
            if ftype == "ehrenfest"
            else self._compute_bo_forces_bohr
        )

        # --- thermostat constants (optional) ---
        sigma_noise = None
        if friction_gamma_au > 0.0 and (temperature_K is not None):
            kB_au_per_K = 3.166811563e-6
            sigma_noise = np.sqrt(2.0 * friction_gamma_au * kB_au_per_K * temperature_K)

        # --- initial state ---
        Rk = self._molecule_positions_bohr()
        Vk = np.zeros_like(Rk)
        Fk = compute_forces(efield_vec=efield_vec)

        traj_R, traj_V, traj_t = [], [], []

        for k in range(int(n_nuc_steps)):
            # ---- (1) first half-kick of velocity Verlet (p_{k+1/2})
            Vk_half = Vk + 0.5 * (Fk / mass_au[:, None]) * dtN

            # ---- (2) electronic propagation over n Fock windows
            # Geometry used for integrals in the j-th window:
            # R_mid(j) = R_k + (j + 1/2) * dt_Ne * Vk_half   (Li et al. midpoint rule)
            for j in range(int(n_fock_per_nuc)):
                tau = (j + 0.5) * dtNe
                R_mid = Rk + Vk_half * tau

                # Move integral geometry only; keep P_O invariant across this change
                self._rebuild_at_geometry_preserving_PO(
                    R_mid, effective_efield_vec=efield_vec
                )

                # Now propagate electrons m times with fixed integrals (at R_mid)
                for _ in range(int(elec_substeps_per_fock)):
                    super().propagate(np.asarray(efield_vec, dtype=float))

            # ---- (3) drift nuclei to end of step using p_{k+1/2}
            Rk1 = Rk + Vk_half * dtN

            # Optional Langevin kicks (applied once per nuclear step)
            if sigma_noise is not None and friction_gamma_au > 0.0:
                xi = rng.standard_normal(size=Rk.shape)
                Vk_half = (1.0 - friction_gamma_au * dtN) * Vk_half + (
                    sigma_noise * np.sqrt(dtN) / mass_au[:, None]
                ) * xi

            # Before evaluating forces at R_{k+1}, move the actual geometry
            # and keep P_O from the last electronic substep.
            self._rebuild_at_geometry_preserving_PO(
                Rk1, effective_efield_vec=efield_vec
            )

            # ---- (4) compute forces at end geometry and second half-kick (p_{k+1})
            Fk1 = compute_forces(efield_vec=efield_vec)

            Vk1 = Vk_half + 0.5 * (Fk1 / mass_au[:, None]) * dtN

            # ---- (5) book-keeping / update loop variables
            Rk, Vk, Fk = Rk1, Vk1, Fk1
            if save_trajectory:
                traj_R.append(Rk.copy())
                traj_V.append(Vk.copy())
                traj_t.append(self.t)

            # store positions, velocities, forces for analysis
            self.Rk = Rk
            self.Vk = Vk
            self.Fk = Fk

        if save_trajectory:
            np.savez(
                f"rt_ehrenfest_traj_id_{self.molecule_id}.npz",
                R=np.asarray(traj_R),
                V=np.asarray(traj_V),
                t=np.asarray(traj_t),
                dipoles=np.asarray(self.dipoles),
                energies=np.asarray(self.energies),
                times=np.asarray(self.times),
            )

    def _propagate_electronic_regime(self, effective_efield_vec):
        """
        One full electronic step in the Ehrenfest integrator.

        Parameters
        ----------
        effective_efield_vec : numpy.ndarray
            Constant electric field vector ``(3,)`` in a.u.
        """

        Vk = self.Vk
        Rk = self.Rk
        Fk = self.Fk
        mass_au = self.mass_au
        dtN = self.dtN
        dtNe = self.dtNe
        n_fock_per_nuc = self.n_fock_per_nuc
        n_elec_per_fock = self.n_elec_per_fock
        n_elec_per_nuc = n_fock_per_nuc * n_elec_per_fock
        efield_vec = np.asarray(effective_efield_vec, dtype=float)
        sigma_noise = self.sigma_noise
        friction_gamma_au = self.friction_gamma_au
        rng = self.rng

        i = self._step_in_cycle

        if i == 0:
            # ---- (1) first half-kick of velocity Verlet (p_{k+1/2})
            self.Vk_half = Vk + 0.5 * (Fk / mass_au[:, None]) * dtN
            self._V_inst = self.Vk_half
            self.kinEnuc = 0.5 * np.sum(self.mass_au[:, None] * (self._V_inst**2))

            # ---- (2) electronic propagation over n Fock windows
            # Geometry used for integrals in the j-th window:
            # R_mid(j) = R_k + (j + 1/2) * dt_Ne * Vk_half   (Li et al. midpoint rule)
        if i % n_elec_per_fock == 0:
            j = int((self.count // n_elec_per_fock) % n_fock_per_nuc)
            tau = (j + 0.5) * dtNe
            R_mid = Rk + self.Vk_half * tau

            # Move integral geometry only; keep P_O invariant across this change
            self._rebuild_at_geometry_preserving_PO(
                R_mid, effective_efield_vec=efield_vec
            )

        # Now propagate electrons for one time (at R_mid)
        # FDTD will be called after this function returns, so we need to run multiple RT-TDDFT steps here
        for _ in range(self.ratio_timestep):
            super().propagate(efield_vec, reset_substep_num=1)
            i += 1

        if i == n_elec_per_nuc:
            # ---- (3) drift nuclei to end of step using p_{k+1/2}
            Rk1 = Rk + self.Vk_half * dtN

            # Optional Langevin kicks (applied once per nuclear step)
            if sigma_noise is not None and friction_gamma_au > 0.0:
                xi = rng.standard_normal(size=Rk.shape)
                self.Vk_half = (1.0 - friction_gamma_au * dtN) * self.Vk_half + (
                    sigma_noise * np.sqrt(dtN) / mass_au[:, None]
                ) * xi

            # Before evaluating forces at R_{k+1}, move the actual geometry
            # and keep P_O from the last electronic substep.
            self._rebuild_at_geometry_preserving_PO(
                Rk1, effective_efield_vec=efield_vec
            )

            # ---- (4) compute forces at end geometry and second half-kick (p_{k+1})
            Fk1 = self.compute_forces(efield_vec=efield_vec)

            Vk1 = self.Vk_half + 0.5 * (Fk1 / mass_au[:, None]) * dtN

            # ---- (5) book-keeping / update loop variables
            Rk, Vk, Fk = Rk1, Vk1, Fk1
            self.Rk = Rk
            self.Vk = Vk
            self.Fk = Fk
            # clear step counter
            self._step_in_cycle = 0
            self._V_inst = self.Vk
            self.kinEnuc = 0.5 * np.sum(self.mass_au[:, None] * (self._V_inst**2))

            if self.verbose:
                print("[RT-Ehrenfest] one nuclear step done.")
                print("[RT-Ehrenfest] updated molecular geometry:")
                print(self.Rk)

            # store positions
            self.traj_R.append(self.Rk.copy())

            if self.save_xyz is not None:
                self._append_xyz_to_file(filename=self.save_xyz)
        else:
            self._step_in_cycle = i

    def _propagate_nuclear_regime(self, effective_efield_vec):
        """
        One full nuclear step in the Ehrenfest integrator.

        Parameters
        ----------
        effective_efield_vec : numpy.ndarray
            Constant electric field vector ``(3,)`` in a.u.
        """

        Vk = self.Vk
        Rk = self.Rk
        Fk = self.Fk
        mass_au = self.mass_au
        dtN = self.dtN
        dtNe = self.dtNe
        n_fock_per_nuc = self.n_fock_per_nuc
        n_elec_per_fock = self.n_elec_per_fock
        efield_vec = np.asarray(effective_efield_vec, dtype=float)
        sigma_noise = self.sigma_noise
        friction_gamma_au = self.friction_gamma_au
        rng = self.rng

        for k in range(int(1)):
            # ---- (1) first half-kick of velocity Verlet (p_{k+1/2})
            Vk_half = Vk + 0.5 * (Fk / mass_au[:, None]) * dtN
            self.kinEnuc = 0.5 * np.sum(self.mass_au[:, None] * (Vk_half**2))

            # ---- (2) electronic propagation over n Fock windows
            # Geometry used for integrals in the j-th window:
            # R_mid(j) = R_k + (j + 1/2) * dt_Ne * Vk_half   (Li et al. midpoint rule)
            for j in range(int(n_fock_per_nuc)):
                tau = (j + 0.5) * dtNe
                R_mid = Rk + Vk_half * tau

                # Move integral geometry only; keep P_O invariant across this change
                self._rebuild_at_geometry_preserving_PO(
                    R_mid, effective_efield_vec=efield_vec
                )

                # Now propagate electrons m times with fixed integrals (at R_mid)
                for _ in range(int(n_elec_per_fock)):
                    super().propagate(efield_vec, reset_substep_num=1)

            # ---- (3) drift nuclei to end of step using p_{k+1/2}
            Rk1 = Rk + Vk_half * dtN

            # Optional Langevin kicks (applied once per nuclear step)
            if sigma_noise is not None and friction_gamma_au > 0.0:
                xi = rng.standard_normal(size=Rk.shape)
                Vk_half = (1.0 - friction_gamma_au * dtN) * Vk_half + (
                    sigma_noise * np.sqrt(dtN) / mass_au[:, None]
                ) * xi

            # Before evaluating forces at R_{k+1}, move the actual geometry
            # and keep P_O from the last electronic substep.
            self._rebuild_at_geometry_preserving_PO(
                Rk1, effective_efield_vec=efield_vec
            )

            # ---- (4) compute forces at end geometry and second half-kick (p_{k+1})
            Fk1 = self.compute_forces(efield_vec=efield_vec)

            Vk1 = Vk_half + 0.5 * (Fk1 / mass_au[:, None]) * dtN

            # ---- (5) book-keeping / update loop variables
            Rk, Vk, Fk = Rk1, Vk1, Fk1
            self.Rk = Rk
            self.Vk = Vk
            self.Fk = Fk
            self._V_inst = self.Vk
            self.kinEnuc = 0.5 * np.sum(self.mass_au[:, None] * (self.Vk**2))

            if self.verbose:
                print("[RT-Ehrenfest] one nuclear step done.")
                print("[RT-Ehrenfest] updated molecular geometry:")
                print(self.Rk)

            self.traj_R.append(self.Rk.copy())

            if self.save_xyz is not None:
                self._append_xyz_to_file(filename=self.save_xyz)

    def _append_xyz_to_file(self, filename="rt_ehrenfest_traj.xyz"):
        """
        Append the current molecular geometry to an XYZ trajectory file.

        Parameters
        ----------
        filename : str, default: "rt_ehrenfest_traj.xyz"
            Path to the output XYZ file.
        """

        # remove existing file if this is the first time appending
        if self._count_append_xyz_to_file == 0 and os.path.exists(filename):
            os.remove(filename)

        nat = self.mol.natom()
        with open(filename, "a") as f:
            f.write(f"{nat}\n")
            f.write(f"t = {self.t * 2.4188843265857e-2:.6f} fs\n")
            for a in range(nat):
                sym = self.mol.symbol(a)
                x, y, z = self.Rk[a] * 0.52917721092  # convert Bohr to Angstrom
                f.write(f"{sym:2} {x:15.8f} {y:15.8f} {z:15.8f}\n")
        self._count_append_xyz_to_file += 1

    # -------------- one FDTD step under E-field --------------

    def propagate(self, effective_efield_vec):
        """
        Propagate coupled electronic–nuclear Ehrenfest dynamics under an external field.

        The integrator involves four nested time scales:

        1. Velocity Verlet for nuclei (Δt_N)
        2. Midpoint Fock updates (Δt_Ne = Δt_N / n)
        3. MMUT-like electronic propagation (Δt_e = Δt_Ne / m)
        4. External FDTD driver coupling (Δt_FDTD)

        Parameters
        ----------
        effective_efield_vec : numpy.ndarray
            Effective electric field vector ``[E_x, E_y, E_z]`` in a.u.
        """

        # enforce RT-TDDFT is run for once per super().propagate() call
        if self.em_coupling_regime == "electronic":
            self._propagate_electronic_regime(effective_efield_vec)
        elif self.em_coupling_regime == "nuclear":
            self._propagate_nuclear_regime(effective_efield_vec)

        # compute total energy (kinetic + electronic + nuclear repulsion)
        E_tot = self.energies[-1] if len(self.energies) > 0 else 0.0
        self.energies_eh.append(E_tot)

        dip = self.dipoles[-1] if len(self.dipoles) > 0 else np.zeros(3)
        self.dipoles_eh.append(dip)

    def calc_amp_vector(self):
        r"""
        Update the total amplitude vector after one full propagation step.

        The total derivative is
        :math:`\frac{d}{dt}[\rho(t)\mu_e] + \frac{d}{dt}\Big[\sum_A Z_A R_A(t)\Big]`.

        Returns
        -------
        numpy.ndarray
            Amplitude vector ``[A_x, A_y, A_z]`` in a.u.
        """

        # nuclear contribution to dipole moment time derivative
        nat = self.mol.natom()
        Z = np.array([self.mol.Z(a) for a in range(nat)], dtype=float)
        amp_n = (Z[:, None] * self._V_inst).sum(axis=0)

        # electronic contribution to dipole moment time derivative
        amp_e = super().calc_amp_vector()
        amp_total = amp_e + amp_n

        # now here is the issue: if we are at the electronic regime, amp_e is important, which is evaluated at
        # time step t + dt_e/2, where t is the time for the E-field amplitude. This scheme agrees with FDTD time stepping.
        # However, if we are at the nuclear regime, amp_n is important, which is evaluated at time step t (E-field time), and we
        # would expect amp_e/amp_n to be evaluated at time step t + dt_N/2. This inconsistency may lead to some issues when coupling with FDTD.

        # so we need to provide a better amp_total evaluation scheme when at nuclear regime
        if self.em_coupling_regime == "nuclear":
            self.dmudt_prev = (
                self.dmudt_projected.copy()
                if self.dmudt_projected is not None
                else amp_total.copy()
            )
            self.dmudt_middlepoint = amp_total.copy()
            self.dmudt_projected = 2.0 * self.dmudt_middlepoint - self.dmudt_prev
            amp_total = self.dmudt_projected

        return amp_total

    # ------------ optional operation / checkpoint --------------

    def append_additional_data(self):
        """
        Append additional data to be returned to MaxwellLink.

        Returns
        -------
        dict
            Contains current time, total energy, and dipole components.
        """

        data = {}
        data["time_au"] = self.t
        data["energy_au"] = self.energies_eh[-1] if len(self.energies_eh) > 0 else 0.0
        data["energy_kin_au"] = self.kinEnuc
        data["mux_au"] = self.dipoles_eh[-1][0] if len(self.dipoles_eh) > 0 else 0.0
        data["muy_au"] = self.dipoles_eh[-1][1] if len(self.dipoles_eh) > 0 else 0.0
        data["muz_au"] = self.dipoles_eh[-1][2] if len(self.dipoles_eh) > 0 else 0.0
        data["mux_m_au"] = data["mux_au"]
        data["muy_m_au"] = data["muy_au"]
        data["muz_m_au"] = data["muz_au"]

        # now here is the issue: if we are at the electronic regime, dipole is evaluated at
        # time step t + dt_e/2, where t is the time for the E-field amplitude. This scheme agrees with FDTD time stepping.
        # However, if we are at the nuclear regime, dipole is evaluated at time step t (E-field time), and we
        # would expect dipole to be evaluated at time step t + dt_N/2. This inconsistency may lead to some issues when coupling with FDTD.
        if self.em_coupling_regime == "nuclear":
            self.dipole_prev = (
                self.dipole_projected.copy()
                if self.dipole_projected is not None
                else self.dipoles_eh[-1].copy()
            )
            self.dipole_middlepoint = self.dipoles_eh[-1].copy()
            self.dipole_projected = 2.0 * self.dipole_middlepoint - self.dipole_prev
            # augment data with projected dipole
            data["mux_au"] = float(self.dipole_projected[0])
            data["muy_au"] = float(self.dipole_projected[1])
            data["muz_au"] = float(self.dipole_projected[2])
            data["mux_m_au"] = float(self.dipole_middlepoint[0])
            data["muy_m_au"] = float(self.dipole_middlepoint[1])
            data["muz_m_au"] = float(self.dipole_middlepoint[2])

        return data

    def _dump_to_checkpoint(self):
        """
        Dump the full Ehrenfest model state to a checkpoint file.

        Saves density matrices, Fock matrices, nuclear positions, velocities,
        and auxiliary variables in ``rttddft_checkpoint_id_<molid>.npy``.
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
                "Vk": self.Vk,
                "Rk": self.Rk,
                "Fk": self.Fk,
                "_V_inst": self._V_inst,
                "_step_in_cycle": self._step_in_cycle,
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
            self.Vk = np.asarray(checkpoint_data["Vk"], dtype=float)
            self.Rk = np.asarray(checkpoint_data["Rk"], dtype=float)
            self.Fk = np.asarray(checkpoint_data["Fk"], dtype=float)
            self._V_inst = np.asarray(checkpoint_data["_V_inst"], dtype=float)
            self._step_in_cycle = int(checkpoint_data["_step_in_cycle"])

    def _snapshot(self):
        """
        Return a deep-copied snapshot of the current model state.

        Returns
        -------
        dict
            Dictionary containing densities, Focks, velocities, positions,
            and auxiliary integration variables.
        """

        snapshot = {
            "time": self.t,
            "count": self.count,
            "Da": self.Da.copy(),
            "Db": self.Db.copy(),
            "Fa": self.Fa.copy(),
            "Fb": self.Fb.copy(),
            "Vk": self.Vk.copy(),
            "Rk": self.Rk.copy(),
            "Fk": self.Fk.copy(),
            "_V_inst": self._V_inst.copy(),
            "_step_in_cycle": self._step_in_cycle,
        }
        return snapshot

    def _restore(self, snapshot):
        """
        Restore the model state from a snapshot dictionary.

        Parameters
        ----------
        snapshot : dict
            Snapshot dictionary returned by ``_snapshot()``.
        """

        self.t = snapshot["time"]
        self.count = snapshot["count"]
        self.Da = snapshot["Da"]
        self.Db = snapshot["Db"]
        self.Fa = snapshot["Fa"]
        self.Fb = snapshot["Fb"]
        self.Vk = snapshot["Vk"]
        self.Rk = snapshot["Rk"]
        self.Fk = snapshot["Fk"]
        self._V_inst = snapshot["_V_inst"]
        self._step_in_cycle = snapshot["_step_in_cycle"]


if __name__ == "__main__":
    """
    Run the doctests to validate the RTTDDFTModel class.

    >>> python rt_ehrenfest_model.py -v
    """
    # import doctest
    # doctest.testmod()

    model = RTEhrenfestModel(
        engine="psi4",
        molecule_xyz="../../../../../tests/data/hcn.xyz",
        functional="hf",
        basis="sto-3g",
        dt_rttddft_au=0.04,
        delta_kick_au=0.0e-3,
        memory="2GB",
        verbose=True,
        remove_permanent_dipole=False,
        n_fock_per_nuc=10,
        n_elec_per_fock=10,
        homo_to_lumo=True,
    )
    model.initialize(dt_new=4.0, molecule_id=0)
    # model._prepare_alpha_homo_to_lumo_excited_state()
    # model._propagate_rttddft_ehrenfest(n_nuc_steps=2, efield_vec=np.array([0.0, 0.0, 1e-2]), force_type="ehrenfest")
    for i in range(2):
        model.propagate(effective_efield_vec=np.array([0.0, 0.0, 0.0]))
        model._append_xyz_to_file(filename="rt_ehrenfest_traj.xyz")
    # save molecular geometries
    # np.savez(f"ohba_geom_{i}.npz", geometry=model.traj_R)
