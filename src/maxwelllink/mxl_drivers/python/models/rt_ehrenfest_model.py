import numpy as np

# from scipy.linalg import expm
from scipy.linalg import fractional_matrix_power as mat_pow
import time

try:
    from .rttddft_model import RTTDDFTModel
except:
    from rttddft_model import RTTDDFTModel

try:
    import psi4
except ImportError:
    raise ImportError("Psi4 is required for the RTEhrenfestModel but is not installed.")

np.set_printoptions(precision=6, suppress=True)


class RTEhrenfestModel(RTTDDFTModel):
    """
    A real-time time-dependent density functional theory Ehrenfest (RT-TDDFT-Ehrenfest) quantum dynamics model using the Psi4 quantum chemistry package.
    This class implements a RT-TDDFT-Ehrenfest model for quantum dynamics, which can be integrated with the MaxwellLink framework.

    Example
    -------
    Create from an XYZ file and then set the molecule id with restricted SCF:

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
        # ---- control of Ehrenfest dynamics ----
        force_type: str = "ehrenfest",
        n_fock_per_nuc: int = 10,
        n_elec_per_fock: int = 10,
        mass_amu=None,
        friction_gamma_au: float = 0.0,
        temperature_K: float = None,
        rng_seed: int = 1234,
    ):
        """
        Initialize the necessary parameters for the RT-TDDFT quantum dynamics model.

        + **`engine`** (str): The computational engine to use (e.g., "psi4"). Default is "psi4". Currently, only "psi4" is supported.
        + **`molecule_xyz`** (str): Path to the XYZ file containing the molecular structure. The second line of the XYZ file may contain the charge and multiplicity.
        + **`functional`** (str): Any Psi4 functional label, e.g. "PBE", "B3LYP", "SCAN", "PBE0". Default is "SCF" (Hartree-Fock).
        + **`basis`** (str): Any basis set label recognized by Psi4, e.g. "sto-3g", "6-31g", "cc-pVDZ". Default is "sto-3g".
        + **`dt_rttddft_au`** (float): Time step for real-time TDDFT propagation in atomic units (a.u.). Default is 0.04 a.u.
        If the MEEP time step is an integer multiple of this, the driver will sub-step internally. This sub-stepping can avoid propagating EM fields
        too frequently when the molecule requires a small time step.
        + **`delta_kick_au`** (float): Strength of the initial delta-kick perturbation along the x, y, and z direction in atomic units (a.u.).
        Default is 0.0e-3 a.u. If this value is set to a non-zero value, the driver will apply a delta-kick perturbation at t=0 to initiate the dynamics.
        With this delta-kick, and also setting the MEEP coupling to zero, one can compute the conventional RT-TDDFT linear absorption spectrum of the molecule.
        + **`delta_kick_direction`** (str): Direction of the initial delta-kick perturbation. Can be "x", "y", "z", "xy", "xz", "yz", or "xyz". Default is "xyz".
        + **`memory`** (str): Memory allocation for Psi4, e.g. "8GB", "500MB". Default is "8GB".
        + **`num_threads`** (int): Number of CPU threads to use in Psi4. Default is 1.
        + **`checkpoint`** (bool): Whether to dump checkpoint files during propagation to allow restarting from the last checkpoint. Default is False.
        + **`restart`** (bool): Whether to restart the propagation from the last checkpoint. Default is False. Setting this to True requires that checkpoint files exist.
        When restarting, the driver will ignore the initial delta-kick perturbation even if it is set to a non-zero value.
        + **`verbose`** (bool): Whether to print verbose output. Default is False.
        + **`remove_permanent_dipole`** (bool): Whether to remove the effect of permanent dipole moments in the light-matter coupling term. Default is False.
        + **`dft_grid_name`** (str): Name of the DFT grid to use in Psi4, e.g. "SG0", "SG1", "SG2", "SG3". Default is "" (Psi4 default). Using "SG0" can speed up calculations.
        + **`dft_radial_points`** (int): Number of radial points in the DFT grid. Default is -1 (Psi4 default).
        + **`dft_spherical_points`** (int): Number of spherical points in the DFT grid. Default is -1 (Psi4 default).
        + **`density_fitting`** (bool): Whether to use density fitting (DF) to accelerate the DFT calculations. Default is False.
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

    def initialize(self, dt_new, molecule_id):
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
        self.Fk = self.compute_forces(efield_vec=None)

        self._step_in_cycle = 0
        self._V_inst = self.Vk.copy()

        self.traj_R = [self.Rk.copy()]
        self.traj_V = [self.Vk.copy()]
        self.traj_F = [self.Fk.copy()]

    # ------------ standalone functions for debugging and testing --------------
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

    def _set_molecule_positions_bohr(self, R):
        """
        Set psi4.Molecule geometry from (nat,3) Bohr array and update Psi4.

        + **`R`** (ndarray): New Cartesian positions (nat, 3) in Bohr.
        """
        geom = psi4.core.Matrix.from_array(R)
        self.mol.set_geometry(geom)
        self.mol.update_geometry()

    def _density_to_orth(self):
        """
        Return spin densities in the orthonormal AO basis: P_O = S^{1/2} P' S^{1/2}.

        Returns
        -------
        DaO : (nbf,nbf) ndarray
            Alpha density in orthonormal AO basis.
        DbO : (nbf,nbf) ndarray
            Beta density in orthonormal AO basis.
        """
        DaO = self.U @ self.Da @ self.U.T
        if self.is_restricted:
            DbO = DaO.copy()
        else:
            DbO = self.U @ self.Db @ self.U.T
        return DaO, DbO

    def _density_from_orth(self, DaO, DbO):
        """
        Set AO densities from orthonormal densities using current X = S^{-1/2}.

        + **`DaO`** (ndarray): Alpha density in orthonormal AO basis.
        + **`DbO`** (ndarray): Beta density in orthonormal AO basis.
        """
        self.Da = self.X @ DaO @ self.X.T
        if self.is_restricted:
            self.Db = self.Da.copy()
        else:
            self.Db = self.X @ DbO @ self.X.T

    def _rebuild_at_geometry_preserving_PO(self, R_new, effective_efield_vec=None):
        """
        Move the molecule to R_new, refresh all Psi4 objects (S, H, ERI, Vpot, ...),
        and keep the orthonormal density P_O invariant across the basis change.

        + **`R_new`** (ndarray): New Cartesian positions (nat, 3) in Bohr.
        """
        # 1. capture P_O in the old basis
        DaO, DbO = self._density_to_orth()

        # 2. move nuclei and refresh integrals/grid machinery
        self._set_molecule_positions_bohr(R_new)
        self._refresh_psi4_internals_after_geom_change()  # updates S, X, U, H, ERI, Vpot,...

        # 3. map P_O -> new AO densities with the new X = S^{-1/2}
        print("[debugging] to do _density_from_orth")
        self._density_from_orth(DaO, DbO)
        print("[debugging] _density_from_orth done")

        # 4. rebuild KS/Fock at the new geometry with the mapped densities
        print("[debugging] to do _build_KS_psi4")
        V_ext = None
        if effective_efield_vec is not None:
            V_ext = (
                -self.mu_ints[0] * effective_efield_vec[0]
                - self.mu_ints[1] * effective_efield_vec[1]
                - self.mu_ints[2] * effective_efield_vec[2]
            )
        self.Fa, self.Fb = self._build_KS_psi4(
            self.Da, self.Db, self.is_restricted, V_ext=V_ext
        )
        print("[debugging] _build_KS_psi4 done")

    def _refresh_psi4_internals_after_geom_change_old(self):
        """
        After moving nuclei, rebuild integrals, and XC machinery. We use a quick SCF
        to get a valid Wavefunction / basis / grid, but we will not use this SCF density.
        """
        # Recompute a ground-state SCF just to refresh wfn, basis, grids, V_potential, etc.
        _Eref, wfn = psi4.energy(
            f"{self.functional}", molecule=self.mol, return_wfn=True
        )
        self.wfn = wfn
        mints = psi4.core.MintsHelper(wfn.basisset())

        # One- and two-electron AO integrals (new geometry)
        self.S = np.asarray(wfn.S())
        self.H = np.asarray(wfn.H())
        self.I_ao = np.asarray(mints.ao_eri())  # (pq|rs)

        # Dipole integrals (AO)
        self.mu_ints = [np.asarray(m) for m in mints.ao_dipole()]
        if self.remove_permanent_dipole:
            # Re-apply the permanent-dipole removal in the *new* geometry
            mu_x0 = np.einsum("pq,pq->", self.mu_ints[0], self.Da + self.Db).real
            mu_y0 = np.einsum("pq,pq->", self.mu_ints[1], self.Da + self.Db).real
            mu_z0 = np.einsum("pq,pq->", self.mu_ints[2], self.Da + self.Db).real
            Iden = np.eye(self.mu_ints[0].shape[0])
            trD = np.trace(self.Da + self.Db)
            self.mu_ints = [
                self.mu_ints[0] - mu_x0 * Iden / trD,
                self.mu_ints[1] - mu_y0 * Iden / trD,
                self.mu_ints[2] - mu_z0 * Iden / trD,
            ]

        # Metric transforms for your orthogonalization pathway
        self.X = mat_pow(self.S, -0.5)
        self.U = mat_pow(self.S, 0.5)

        # XC potential machinery (reuse Psi4 V_potential)
        self.Vpot = wfn.V_potential()
        self.alpha_hfx = wfn.functional().x_alpha()
        if self.alpha_hfx < 1.0:
            self.Vpot.initialize()
            try:
                self.Vpot.build_collocation_cache(self.Vpot.nblocks())
            except Exception:
                pass

        # Nuclear repulsion for reporting/analysis
        self.Enuc = self.mol.nuclear_repulsion_energy()

    def _rebuild_vpot_fast(self):
        """
        Build a fresh V_potential (VBase) tied to the *current* geometry and basis,
        without running SCF. Also refresh self.alpha_hfx for hybrids.
        """
        basis = self.wfn.basisset()  # basis object follows mol's updated positions
        # 1) Get or build the SuperFunctional that matches self.functional
        sf = None
        try:
            # If the current wfn has a functional, reuse its definition (safer for custom options)
            sf = self.wfn.functional()
            print(
                f"[debugging] _rebuild_vpot_fast: Reusing existing functional from wfn: {sf.name()}"
            )
        except Exception:
            # Fall back to building from the functional string
            try:
                # Newer Psi4:
                from psi4.driver.dft import build_superfunctional

                sf = build_superfunctional(self.functional, True)
                print(
                    f"[debugging] _rebuild_vpot_fast: Built functional from string: {sf.name()}"
                )
            except Exception:
                # Older Psi4 fallback:
                sf = psi4.core.SuperFunctional.build_from_string(self.functional, True)
                print(
                    f"[debugging] _rebuild_vpot_fast: Built functional from string (old Psi4): {sf.name()}"
                )

        # We need potential (and often gradient) values; request up to first derivative
        try:
            sf.set_max_deriv(1)
            print("[debugging] _rebuild_vpot_fast: Set functional max_deriv=1")
        except Exception:
            pass
        try:
            sf.initialize()
            print("[debugging] _rebuild_vpot_fast: Initialized functional")
        except Exception:
            pass

        # 2) Build a *new* VBase tied to current basis & functional
        # Tag: "RV" for RKS, "UV" for UKS (Psi4 convention)
        ks_tag = "RV" if self.is_restricted else "UV"
        self.Vpot = psi4.core.VBase.build(basis, sf, ks_tag)
        self.Vpot.initialize()
        try:
            # This primes collocation caches for all blocks and avoids per-call overheads
            self.Vpot.build_collocation_cache(self.Vpot.nblocks())
            print("[debugging] _rebuild_vpot_fast: Built collocation cache for Vpot")
        except Exception:
            pass

    def _refresh_psi4_internals_after_geom_change(self):
        """
        Cheap, SCF-free refresh after a geometry change.

        Recomputes one- and two-electron AO integrals and dipoles with libmints
        on the current molecule geometry, updates metric transforms (X,U),
        and reinitializes the DFT V_potential grid freshly (no SCF).
        """
        import psi4

        if not hasattr(self, "wfn") or self.wfn is None:
            raise RuntimeError(
                "Wavefunction container (self.wfn) is not set. Call initialize() first."
            )

        # basis object follows mol updated positions
        basis = self.wfn.basisset()
        mints = psi4.core.MintsHelper(basis)

        # (2) AO integrals at new geometry
        S = np.asarray(mints.ao_overlap())
        T = np.asarray(mints.ao_kinetic())
        V = np.asarray(mints.ao_potential())
        H = T + V
        I_ao = np.asarray(mints.ao_eri())
        mu_ints = [np.asarray(m) for m in mints.ao_dipole()]

        if self.remove_permanent_dipole:
            mu_x0 = np.einsum("pq,pq->", mu_ints[0], self.Da + self.Db).real
            mu_y0 = np.einsum("pq,pq->", mu_ints[1], self.Da + self.Db).real
            mu_z0 = np.einsum("pq,pq->", mu_ints[2], self.Da + self.Db).real
            Iden = np.eye(mu_ints[0].shape[0])
            trD = np.trace(self.Da + self.Db)
            mu_ints = [
                mu_ints[0] - mu_x0 * Iden / trD,
                mu_ints[1] - mu_y0 * Iden / trD,
                mu_ints[2] - mu_z0 * Iden / trD,
            ]

        # (4) Update metric transforms
        self.S = S
        self.H = H
        self.I_ao = I_ao
        self.mu_ints = mu_ints
        self.X = mat_pow(self.S, -0.5)
        self.U = mat_pow(self.S, 0.5)

        # (5) Fresh XC quadrature/grid *without* SCF (critical for B3LYP etc.)
        if self.alpha_hfx < 1.0:
            try:
                self._rebuild_vpot_fast()
            except Exception as e:
                if self.verbose:
                    print(
                        f"[warning] V_potential fast rebuild failed: {e!r}. Falling back to a micro-SCF reinit."
                    )
                # --- Safe fallback: a *very cheap* one-iteration SCF just to re-seed the Wavefunction ---
                #   This creates a brand-new Wavefunction and V_potential consistent with the new geometry.
                self.opts["e_convergence"] = 1e-1
                self.opts["d_convergence"] = 1e-1
                self.opts["maxiter"] = 3
                psi4.set_options(self.opts)
                _Eref, wfn = psi4.energy(
                    f"{self.functional}", molecule=self.mol, return_wfn=True
                )
                self.wfn = wfn
                self.Vpot = self.wfn.V_potential()
                try:
                    self.Vpot.initialize()
                    self.Vpot.build_collocation_cache(self.Vpot.nblocks())
                except Exception:
                    pass
                try:
                    self.alpha_hfx = self.wfn.functional().x_alpha()
                except Exception:
                    if not hasattr(self, "alpha_hfx"):
                        self.alpha_hfx = 0.0

        # (6) Update nuclear repulsion (reporting / forces)
        self.Enuc = self.mol.nuclear_repulsion_energy()

    def _compute_bo_forces_bohr(self, efield_vec=None):
        """
        Compute Born–Oppenheimer forces F_A = - dE/dR_A (Hartree/Bohr) at the current geometry
        using Psi4's analytic gradient for SCF ground state
        (not Ehrenfest forces from the RT density). In this implementation, efield_vec does not
        affect the forces, and this function is provided for debugging only.

        Returns
        -------
        forces : (nat, 3) ndarray in Hartree/Bohr
        """
        G = np.asarray(psi4.gradient(f"{self.functional}", molecule=self.mol))
        F = -G
        if self.verbose:
            print("BO forces (Eh/Bohr):\n", F)
        return F

    def _compute_ehrenfest_forces_bohr(
        self, include_xc_grad: bool = True, efield_vec=None
    ):
        """
        Ehrenfest nuclear forces F_A = - dE/dR_A (Hartree/Bohr) at the current geometry,
        using the instantaneous AO densities self.Da, self.Db and AO Focks self.Fa, self.Fb.

        + **`include_xc_grad`** (bool): Whether to include the exchange-correlation gradient
          contribution using the real-component of the RT density. True by default.

        Returns
        -------
        forces : (nat, 3) ndarray in Hartree/Bohr
        """
        nat = self.mol.natom()
        nbf = self.S.shape[0]

        # (not necessary) refresh all Psi4 integrals and machinery at the current geometry for safety
        # self._refresh_psi4_internals_after_geom_change()

        # symmetrize real AO densities
        Da = np.real((self.Da + self.Da.T) / 2.0)
        Db = np.real((self.Db + self.Db.T) / 2.0)
        D = Da + Db

        # AO Focks from your RT step (already built against current geometry)
        Fa = np.asarray(self.Fa)
        Fb = np.asarray(self.Fb)

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
                g_1e_T[A, c] += np.einsum("pq,pq->", D, dT[c], optimize=True)
                g_1e_V[A, c] += np.einsum("pq,pq->", D, dV[c], optimize=True)
                g_1e[A, c] += np.einsum("pq,pq->", D, dT[c] + dV[c], optimize=True)

        # --- 2. Two-electron derivatives: Coulomb & HF exchange ---
        g_2e_coul = np.zeros((nat, 3), dtype=float)
        g_2e_exch = np.zeros((nat, 3), dtype=float)
        use_hf_exchange = self.alpha_hfx > 0.0
        for A in range(nat):
            dERI_xyz = mints.ao_tei_deriv1(A)
            # Convert to NumPy (nbf,nbf,nbf,nbf)
            dERI = [np.asarray(T) for T in dERI_xyz]
            for c in range(3):
                g_2e_coul[A, c] += 0.5 * np.einsum(
                    "pq,rs,pqrs->", D, D, dERI[c], optimize=True
                )
                if use_hf_exchange:
                    g_2e_exch[A, c] -= (
                        0.5
                        * self.alpha_hfx
                        * (
                            np.einsum("pr,qs,pqrs->", Da, Da, dERI[c], optimize=True)
                            + np.einsum("pr,qs,pqrs->", Db, Db, dERI[c], optimize=True)
                        )
                    )

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
                    dS_half += (s_vec_i * inv_sigma_i_sigma_j * sij) @ s_vec_j.T
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
                try:
                    V.build_collocation_cache(V.nblocks())
                except Exception:
                    pass
            except Exception:
                pass

            if self.is_restricted:
                Dm = psi4.core.Matrix.from_array(np.real((Da + Db) * 0.5))
                V.set_D([Dm])
            else:
                Da_m = psi4.core.Matrix.from_array(np.real((Da + Da.T) * 0.5))
                Db_m = psi4.core.Matrix.from_array(np.real((Db + Db.T) * 0.5))
                V.set_D([Da_m, Db_m])

            Gxc_mat = V.compute_gradient()
            g_xc = np.asarray(Gxc_mat).copy()

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
        if self.verbose:
            print("Force components (Eh/Bohr):")
            print("begin components")
            print("  1e kinetic:\n", g_1e_T)
            print("  1e nuclear attraction:\n", g_1e_V)
            print("  1e Hellmann–Feynman:\n", g_1e)
            print("  2e total:\n", g_2e_coul + g_2e_exch)
            print("  S term:\n", g_s)
            print("  nuclear repulsion:\n", g_nuc)
            if include_xc_grad and (self.alpha_hfx < 1.0):
                print("  XC:\n", g_xc)
            print("  nuclear-field:\n", g_field_nuc)
            print("end of components\n")
            print("Total Ehrenfest forces (Eh/Bohr):\n", forces)
        return forces

    def _prepare_alpha_homo_to_lumo_excited_state(self):
        """
        Promote one alpha electron from HOMO to LUMO (beta unchanged) and
        switch the propagator to unrestricted (UKS). Rebuild Fa/Fb at t=0.

        Use:
            self._init_psi4()   # already done inside initialize()
            self.prepare_alpha_homo_to_lumo_excited_state()
            # then call propagate(...)

        Notes:
        - Assumes a closed-shell ground-state SCF was just performed (nα == nβ).
        - Leaves self.E0 as the SCF ground-state energy; Etot - E0 will then be positive.
        - You may want to set self.delta_kick_au = 0.0 for a pure promoted-state start.
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

        # --- spin densities in AO basis: Dσ = C * diag(occσ) * C^T ---
        # (multiply columns of C by occ, then right-multiply by C^T)
        Da = (C * occ_a) @ C.T
        Db = (C * occ_b) @ C.T

        # enforce Hermiticity/realness
        Da = 0.5 * (Da + Da.T.conj()).real
        Db = 0.5 * (Db + Db.T.conj()).real

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

        if self.verbose:
            print(
                "[init-excited] Prepared alpha (HOMO->LUMO) excited determinant; "
                "switched to UKS propagation and rebuilt Fa/Fb."
            )

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
        Three-time-scale Ehrenfest integrator (Li–Tully–Schlegel–Frisch):
          - velocity Verlet for nuclei with dt_N
          - nuclear-position–coupled midpoint Fock with dt_Ne = dt_N / n
          - MMUT-like RT propagation for electrons with dt_e = dt_Ne / m

        + **`n_nuc_steps`** (int): Number of nuclear steps to propagate.
        + **`efield_vec`** (ndarray): Constant external electric field vector (3,) in atomic units (default: zero field).
        + **`nuc_dt_au`** (float): Nuclear time step Δt_N in atomic units (default: 0.4 au).
        + **`n_fock_per_nuc`** (int): Number of Fock steps per nuclear step (default: 2).
        + **`elec_substeps_per_fock`** (int): Number of electronic RT substeps per Fock step (default: None, which chooses
          the largest integer such that dt_e <= self.dt_rttddft_au).
        + **`mass_amu`** (ndarray): Atomic masses in amu (nat,) (default: None, which uses Psi4 masses).
        + **`friction_gamma_au`** (float): Langevin friction coefficient gamma in atomic units (default: 0.0, no friction).
        + **`temperature_K`** (float): Langevin bath temperature in Kelvin (default: None, no thermostat).
        + **`rng_seed`** (int): Random number generator seed for Langevin thermostat (default: 1234).
        + **`save_trajectory`** (bool): Whether to save the trajectory to a file (default: True).
        + **`force_type`** (str): Type of forces to use: "bo" for Born–Oppenheimer forces from SCF gradient,
          "ehrenfest" for Ehrenfest forces from the RT density (default: "ehrenfest").
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
                start_time = time.perf_counter()
                self._rebuild_at_geometry_preserving_PO(
                    R_mid, effective_efield_vec=efield_vec
                )
                end_time = time.perf_counter()
                elapsed_time = end_time - start_time
                if self.verbose:
                    print(
                        f"[debugging] _rebuild_at_geometry_preserving_PO took {elapsed_time:.6f} seconds"
                    )

                # Now propagate electrons m times with fixed integrals (at R_mid)
                start_time = time.perf_counter()
                for _ in range(int(elec_substeps_per_fock)):
                    super().propagate(np.asarray(efield_vec, dtype=float))
                end_time = time.perf_counter()
                elapsed_time = end_time - start_time
                if self.verbose:
                    print(
                        f"[debugging] {elec_substeps_per_fock} electronic RT steps took {elapsed_time:.6f} seconds"
                    )

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
            start_time = time.perf_counter()
            self._rebuild_at_geometry_preserving_PO(
                Rk1, effective_efield_vec=efield_vec
            )
            end_time = time.perf_counter()
            elapsed_time = end_time - start_time
            if self.verbose:
                print(
                    f"[debugging] _rebuild_at_geometry_preserving_PO took {elapsed_time:.6f} seconds"
                )

            # ---- (4) compute forces at end geometry and second half-kick (p_{k+1})
            start_time = time.perf_counter()
            Fk1 = compute_forces(efield_vec=efield_vec)
            end_time = time.perf_counter()
            elapsed_time = end_time - start_time
            if self.verbose:
                print(f"[debugging] compute_forces took {elapsed_time:.6f} seconds")

            Vk1 = Vk_half + 0.5 * (Fk1 / mass_au[:, None]) * dtN

            # ---- (5) book-keeping / update loop variables
            Rk, Vk, Fk = Rk1, Vk1, Fk1
            if save_trajectory:
                traj_R.append(Rk.copy())
                traj_V.append(Vk.copy())
                traj_t.append(self.t)

            if self.verbose:
                vrms = np.sqrt((Vk**2).mean())
                print(
                    f"[RT-Ehrenfest] k={k:5d}/{n_nuc_steps}  t_e={self.t:.6f} a.u.  |V|_rms={vrms:.3e} a.u."
                )

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

    def calc_amp_vector(self):
        """
        Update the source amplitude vector after propagating this molecule for one time step.
        amp = d/dt[\rho(t) * mu_e] + d/dt[sum_A Z_A * R_A(t)]

        Returns:
        - A numpy array representing the amplitude vector in the form [Ax, Ay, Az].
        """
        # nuclear contribution to dipole moment time derivative
        nat = self.mol.natom()
        Z = np.array([self.mol.Z(a) for a in range(nat)], dtype=float)
        amp_n = (Z[:, None] * self._V_inst).sum(axis=0)

        # electronic contribution to dipole moment time derivative
        amp_e = super().calc_amp_vector()
        amp_total = amp_e + amp_n
        return amp_total

    def _propagate_electronic_regime(self, effective_efield_vec):
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

            # ---- (2) electronic propagation over n Fock windows
            # Geometry used for integrals in the j-th window:
            # R_mid(j) = R_k + (j + 1/2) * dt_Ne * Vk_half   (Li et al. midpoint rule)
        if i % n_elec_per_fock == 0:
            j = int((self.count // n_elec_per_fock) % n_fock_per_nuc)
            tau = (j + 0.5) * dtNe
            R_mid = Rk + self.Vk_half * tau

            # Move integral geometry only; keep P_O invariant across this change
            start_time = time.perf_counter()
            self._rebuild_at_geometry_preserving_PO(
                R_mid, effective_efield_vec=efield_vec
            )
            end_time = time.perf_counter()
            elapsed_time = end_time - start_time
            if self.verbose:
                print(
                    f"[debugging] _rebuild_at_geometry_preserving_PO took {elapsed_time:.6f} seconds"
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
            start_time = time.perf_counter()
            self._rebuild_at_geometry_preserving_PO(
                Rk1, effective_efield_vec=efield_vec
            )
            end_time = time.perf_counter()
            elapsed_time = end_time - start_time
            if self.verbose:
                print(
                    f"[debugging] _rebuild_at_geometry_preserving_PO took {elapsed_time:.6f} seconds"
                )

            # ---- (4) compute forces at end geometry and second half-kick (p_{k+1})
            start_time = time.perf_counter()
            Fk1 = self.compute_forces(efield_vec=efield_vec)
            end_time = time.perf_counter()
            elapsed_time = end_time - start_time
            if self.verbose:
                print(f"[debugging] compute_forces took {elapsed_time:.6f} seconds")

            Vk1 = self.Vk_half + 0.5 * (Fk1 / mass_au[:, None]) * dtN

            # ---- (5) book-keeping / update loop variables
            Rk, Vk, Fk = Rk1, Vk1, Fk1
            self.Rk = Rk
            self.Vk = Vk
            self.Fk = Fk
            # clear step counter
            self._step_in_cycle = 0
            self._V_inst = self.Vk

            if self.verbose:
                print("[RT-Ehrenfest] one nuclear step done.")
                print("[RT-Ehrenfest] updated molecular geometry:")
                print(self.Rk)

            # store positions
            self.traj_R.append(self.Rk.copy())
        else:
            self._step_in_cycle = i

    def _propagate_nuclear_regime(self, effective_efield_vec):
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

            # ---- (2) electronic propagation over n Fock windows
            # Geometry used for integrals in the j-th window:
            # R_mid(j) = R_k + (j + 1/2) * dt_Ne * Vk_half   (Li et al. midpoint rule)
            for j in range(int(n_fock_per_nuc)):
                tau = (j + 0.5) * dtNe
                R_mid = Rk + Vk_half * tau

                # Move integral geometry only; keep P_O invariant across this change
                start_time = time.perf_counter()
                self._rebuild_at_geometry_preserving_PO(
                    R_mid, effective_efield_vec=efield_vec
                )
                end_time = time.perf_counter()
                elapsed_time = end_time - start_time
                if self.verbose:
                    print(
                        f"[debugging] _rebuild_at_geometry_preserving_PO took {elapsed_time:.6f} seconds"
                    )

                # Now propagate electrons m times with fixed integrals (at R_mid)
                start_time = time.perf_counter()
                for _ in range(int(n_elec_per_fock)):
                    super().propagate(efield_vec, reset_substep_num=1)
                end_time = time.perf_counter()
                elapsed_time = end_time - start_time
                if self.verbose:
                    print(
                        f"[debugging] {n_elec_per_fock} electronic RT steps took {elapsed_time:.6f} seconds"
                    )

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
            start_time = time.perf_counter()
            self._rebuild_at_geometry_preserving_PO(
                Rk1, effective_efield_vec=efield_vec
            )
            end_time = time.perf_counter()
            elapsed_time = end_time - start_time
            if self.verbose:
                print(
                    f"[debugging] _rebuild_at_geometry_preserving_PO took {elapsed_time:.6f} seconds"
                )

            # ---- (4) compute forces at end geometry and second half-kick (p_{k+1})
            start_time = time.perf_counter()
            Fk1 = self.compute_forces(efield_vec=efield_vec)
            end_time = time.perf_counter()
            elapsed_time = end_time - start_time
            if self.verbose:
                print(f"[debugging] compute_forces took {elapsed_time:.6f} seconds")

            Vk1 = Vk_half + 0.5 * (Fk1 / mass_au[:, None]) * dtN

            # ---- (5) book-keeping / update loop variables
            Rk, Vk, Fk = Rk1, Vk1, Fk1
            self.Rk = Rk
            self.Vk = Vk
            self.Fk = Fk
            self._V_inst = self.Vk

            if self.verbose:
                print("[RT-Ehrenfest] one nuclear step done.")
                print("[RT-Ehrenfest] updated molecular geometry:")
                print(self.Rk)
            
            self.traj_R.append(self.Rk.copy())

    def propagate(self, effective_efield_vec):
        """
        Propagate the quantum molecular dynamics given the effective electric field vector. This
        propagation involves the coupled evolution of electronic and nuclear degrees of freedom.

        Four time steps are involved in this propagation:
        - Velocity Verlet for nuclei with dt_N
        - Nuclear-position–coupled midpoint Fock with dt_Ne = dt_N / n
        - MMUT-like RT propagation for electrons with dt_e = dt_Ne / m
        - FDTD time step, i.e., the time step for calling this function

        It is better that this function can be used to handle IR- and UV-vis light at the same time.

        To make our life easier, we assume that the FDTD time step is the same as either
          1. the nuclear time step dt_N.
          2. or the Nuclear-position–coupled midpoint Fock time step dt_Ne.
        The first case is easy to implement (all we need to do is to use self._propagate_rttddft_ehrenfest for one nuclear step).
        The second case is a bit tricky, as we need to count how many FDTD calls we have done since the last nuclear step, and
        then determine if we need to do a nuclear step or not.

        Additionally, some operations in self._propagate_rttddft_ehrenfest may need to be
        moved to the self.initialize function, such as the initialization of the random number generator.

        + **`effective_efield_vec`**: Effective electric field vector in the form [Ex, Ey, Ez].
        """
        # enforce RT-TDDFT is run for once per super().propagate() call
        if self.em_coupling_regime == "electronic":
            self._propagate_electronic_regime(effective_efield_vec)
        elif self.em_coupling_regime == "nuclear":
            self._propagate_nuclear_regime(effective_efield_vec)


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
        functional="b3lyp",
        basis="sto-3g",
        dt_rttddft_au=0.04,
        delta_kick_au=1.0e-3,
        memory="2GB",
        verbose=True,
        remove_permanent_dipole=False,
        n_fock_per_nuc=2,
        n_elec_per_fock=5,
    )
    model.initialize(dt_new=0.2, molecule_id=0)
    model._prepare_alpha_homo_to_lumo_excited_state()
    # model._propagate_rttddft_ehrenfest(n_nuc_steps=2, efield_vec=np.array([0.0, 0.0, 1e-2]), force_type="ehrenfest")
    for i in range(20):
        model.propagate(effective_efield_vec=np.array([0.0, 0.0, 1e-2]))
    # save molecular geometries
    np.savez(f"ohba_geom_{i}.npz", geometry=model.traj_R)
