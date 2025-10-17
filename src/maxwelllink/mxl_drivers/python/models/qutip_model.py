import numpy as np
import os, importlib.util, ast
from typing import Dict, Optional

try:
    from .dummy_model import DummyModel
except:
    from dummy_model import DummyModel

try:
    import qutip as qt
except ImportError as e:
    raise ImportError(
        "QuTiPModel requires qutip. Install with `conda install conda-forge::qutip`."
    ) from e


def _load_spec_module(path: str):
    """
    Import a user-supplied Python file that defines ``build_model(**kwargs)``.

    Parameters
    ----------
    path : str
        Path to the Python file.

    Returns
    -------
    module
        The imported Python module containing ``build_model(**kwargs)``.

    Raises
    ------
    ImportError
        If the module cannot be loaded from the given path.
    AttributeError
        If the loaded module does not define ``build_model(**kwargs)``.
    """

    path = os.path.abspath(path)
    spec = importlib.util.spec_from_file_location("mxl_qutip_user_spec", path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module from {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    if not hasattr(mod, "build_model"):
        raise AttributeError(
            f"{path} must define a function `build_model(**kwargs)` "
            "that returns a dict with keys: H0, mu_ops (dict), c_ops (list), rho0."
        )
    return mod


def _parse_kwargs_string(s: str) -> Dict:
    """
    Parse a compact ``'k1=v1,k2=v2'`` string into a dictionary with numbers/bools
    auto-cast. Used for preset parameters and for passing kwargs to a user spec.

    Parameters
    ----------
    s : str
        Input string.

    Returns
    -------
    dict
        Parsed key-value pairs with best-effort type casting.
    """

    if not s:
        return {}

    def _strip_quotes(s):
        if not isinstance(s, str):
            return s
        s = s.strip()
        if (len(s) >= 2) and ((s[0] == s[-1]) and s[0] in ("'", '"')):
            return s[1:-1]
        return s

    s = _strip_quotes(s)
    out = {}
    for token in s.split(","):
        if not token.strip():
            continue
        if "=" not in token:
            # allow bare flags as True
            out[token.strip()] = True
            continue
        k, v = token.split("=", 1)
        k = k.strip()
        v = v.strip()
        # try literal (int/float/bool/list/tuple) -> else string
        try:
            out[k] = ast.literal_eval(v)
        except Exception:
            # also allow "true"/"false"
            lv = v.lower()
            if lv in ("true", "false"):
                out[k] = lv == "true"
            else:
                out[k] = v
    return out


def _calc_mu_vector_expectation(rho, mu_ops):
    """
    Return :math:`\\langle\\mu_x\\rangle`, :math:`\\langle\\mu_y\\rangle`,
    :math:`\\langle\\mu_z\\rangle` as a length-3 NumPy array.

    Parameters
    ----------
    rho : qutip.Qobj
        Density matrix of the system.
    mu_ops : dict
        Dictionary with keys ``'x'``, ``'y'``, ``'z'`` and values as ``qutip.Qobj`` or
        ``None``, storing dipole operators.

    Returns
    -------
    numpy.ndarray of float, shape (3,)
        The expectation values of the dipole components.
    """

    vec = np.zeros(3, dtype=float)
    for i, key in enumerate(("x", "y", "z")):
        op = mu_ops.get(key, None)
        if op is not None:
            vec[i] = float(qt.expect(op, rho))
    return vec


def _build_model_tls(
    omega=0.242,
    mu12=187,
    orientation=2,
    pe_initial=0.0,
    gamma_relax=0.0,
    gamma_dephase=0.0,
):
    r"""
    Simple 2-level model preset like ``TLSModel``, but using QuTiP objects.

    Equations
    ---------
    - :math:`H_0 = \\lvert e \\rangle \\langle e \\rvert \\, \\omega`
    - :math:`\\boldsymbol{\\mu} = \\mu_{12}\\big(\\lvert g \\rangle \\langle e \\rvert + \\lvert e \\rangle \\langle g \\rvert\\big)` along the chosen axis
    - Lindblad: relaxation (:math:`\\sigma_-`) at rate ``gamma_relax``, pure dephasing at ``gamma_dephase``.

    This function provides a reference implementation for the ``build_model(**kwargs)`` function.

    Parameters
    ----------
    omega : float, default: 0.242
        Transition frequency in atomic units (a.u.).
    mu12 : float, default: 187
        Dipole moment in atomic units (a.u.).
    orientation : int, default: 2
        Orientation of the dipole moment; can be ``0`` (x), ``1`` (y), or ``2`` (z).
    pe_initial : float, default: 0.0
        Initial population in the excited state.
    gamma_relax : float, default: 0.0
        Relaxation rate (a.u.).
    gamma_dephase : float, default: 0.0
        Pure dephasing rate (a.u.).

    Returns
    -------
    dict
        A dictionary with keys ``H0``, ``mu_ops``, ``c_ops``, and ``rho0``.
    """

    # basis
    g = qt.basis(2, 0)
    e = qt.basis(2, 1)
    H0 = omega * e * e.dag()

    # dipole operator along chosen axis; others set to None
    sigmax = g * e.dag() + e * g.dag()
    mux = muy = muz = None
    dip = mu12 * sigmax
    if orientation == 0:
        mux = dip
    elif orientation == 1:
        muy = dip
    else:
        muz = dip
    mu_ops = {"x": mux, "y": muy, "z": muz}

    # collapse operators
    c_ops = []
    if gamma_relax > 0.0:
        c_ops.append(np.sqrt(gamma_relax) * (g * e.dag()))
    if gamma_dephase > 0.0:
        c_ops.append(np.sqrt(gamma_dephase) * (e * e.dag() - g * g.dag()))

    # initial state (density matrix form of a coherent state)
    pe = pe_initial
    rho0 = (
        (1.0 - pe) * (g * g.dag())
        + pe * (e * e.dag())
        + np.sqrt(pe * (1.0 - pe)) * (g * e.dag() + e * g.dag())
    )

    return dict(H0=H0, mu_ops=mu_ops, c_ops=c_ops, rho0=rho0)


class QuTiPModel(DummyModel):
    """
    General N-level quantum model driven by an external E-field using QuTiP.

    The time-dependent Hamiltonian is
    :math:`H(t) = H_0 - E_x(t)\\mu_x - E_y(t)\\mu_y - E_z(t)\\mu_z`.

    Two options for constructing the N-level quantum model are provided:

    - **Preset TLS** via a simple CLI parameter, e.g.:
      ``--param "preset=tls,preset_kwargs=omega=0.242,mu12=187,orientation=2,pe_initial=1e-4"``
    - **Fully custom model** via:
      ``--param "module=/path/spec.py,kwargs=..."``

      The module must define a callable ``build_model(**kwargs)`` that returns a
      dictionary with the following keys:

      .. code-block:: python

          def build_model(**kwargs):
              return {
                  "H0": qutip.Qobj,                         # (N x N)
                  "mu_ops": {"x": Qobj|None, "y": Qobj|None, "z": Qobj|None},
                  "c_ops": [Qobj, ...],
                  "rho0":  Qobj,                             # ket or density matrix
              }

      Optional fields may be omitted; defaults: no ``c_ops``, and ``rho0`` is the
      ground state if not provided.
    """

    def __init__(
        self,
        # --- Preset controls ---
        preset: str = "tls",
        preset_kwargs: str = "",
        # --- Custom module controls ---
        module: Optional[str] = None,
        kwargs: str = "",
        # --- finite-difference or analytical dmu/dt ---
        fd_dmudt: bool = True,
        # --- Common controls ---
        verbose: bool = False,
        checkpoint: bool = False,
        restart: bool = False,
        **extra,
    ):
        """
        Initialize the necessary parameters for the QuTiP quantum dynamics model.

        Parameters
        ----------
        preset : str, default: 'tls'
            Preset model name, e.g. ``'tls'``. Default is ``'tls'``.
        preset_kwargs : str
            Comma-separated ``key=value`` pairs for the preset, such as
            ``preset_kwargs=omega=0.242,mu12=187,orientation=2,pe=1e-4``.
            All key value pairs not recognized will be treated as preset parameters.
        module : str or None, default: None
            Path to a Python file defining ``build_model(**kwargs)``.
        kwargs : str
            Comma-separated ``key=value`` pairs for the user module, such as
            ``kwargs=omega=0.242,mu12=187,orientation=2,pe=1e-4``.
            All key value pairs not recognized will be treated as user module
            parameters if ``module`` is not ``None``.
        fd_dmudt : bool, default: True
            Whether to use finite-difference :math:`\\mathrm{d}\\mu/\\mathrm{d}t` for current
            density computation. Default is ``True``. If ``False``, an analytical
            derivative will be used if available.
        verbose : bool, default: False
            Whether to print verbose output. Default is ``False``.
        checkpoint : bool, default: False
            Whether to enable checkpointing. Default is ``False``.
        restart : bool, default: False
            Whether to restart from a checkpoint if available. Default is ``False``.
        **extra
            Additional keyword arguments for future extensions.
        """
        super().__init__(verbose=verbose, checkpoint=checkpoint, restart=restart)

        # First deal with preset parsing and merging with top-level tokens.
        # Given --param `preset_kwargs=omega=0.242,mu12=187,orientation=2,pe=1e-4`, the mxl_driver will
        # parse it as `preset_kwargs=omega=0.242; mu12=187; orientation=2; pe=1e-4` (splitted by the comma).
        # By default, `mu12=187; orientation=2; pe=1e-4` will be merged into extra dict. We need to collect
        # them back to preset_kwargs for the preset model to consume. This brings some inconvenience.

        # 1. given known subkeys for the TLS preset
        _tls_keys = {
            "omega",
            "mu12",
            "orientation",
            "pe_initial",
            "gamma_relax",
            "gamma_dephase",
        }

        # 2. recover the first hit of 'preset_kwargs'
        self.preset = preset.lower().strip()
        self.preset_kwargs = _parse_kwargs_string(preset_kwargs)

        # 3. merge available top-level tokens in **extra** to TLS preset
        for k in list(extra.keys()):
            if k in _tls_keys:
                # here we do not attempt to pop extra[k] to avoid removing user module params
                self.preset_kwargs[k] = extra[k]

        # 4. deal with module path and module kwargs in a similar way
        self.module_path = module
        # here only the first hit of 'kwargs' is recovered
        self.module_kwargs = _parse_kwargs_string(kwargs)

        # if user chose custom module, *assume* any remaining extra items
        # are intended for build_model(**kwargs) unless they are our own config flags.
        _own_keys = {
            "preset",
            "preset_kwargs",
            "module",
            "kwargs",
            "fd_dmudt",
            "verbose",
            "checkpoint",
            "restart",
        }
        if self.preset == "custom" and self.module_path is not None:
            for k in list(extra.keys()):
                if k not in _own_keys:
                    self.module_kwargs[k] = extra.pop(k)

        if self.preset != "custom" and self.module_path is None:
            print(
                f"[QuTiPModel] Using preset '{self.preset}' with params: {self.preset_kwargs}"
            )
        else:
            print(
                f"[QuTiPModel] Using custom module '{self.module_path}' with params: {self.module_kwargs}"
            )

        self.fd_dmudt = bool(fd_dmudt)
        print(
            f"[QuTiPModel] Using {'finite-difference' if self.fd_dmudt else 'analytical'} dmu/dt for current density."
        )

        # internal state
        self.H0 = None
        self.mu_ops = {"x": None, "y": None, "z": None}
        self.c_ops = []
        self.rho = None
        self._mu_prev = np.zeros(3, float)
        self._mu_curr = np.zeros(3, float)
        self._dim = None
        self._eye = None
        # operators for evaluating analytical dmu/dt
        self._dmudt_op = {}

        # whether restarted from checkpoint
        self.restarted = False

    # -------------- heavy-load initialization (at INIT) --------------

    def initialize(self, dt_new, molecule_id):
        """
        Initialize the model with the new time step and molecule ID.

        Parameters
        ----------
        dt_new : float
            The new time step in atomic units (a.u.).
        molecule_id : int
            The ID of the molecule.
        """

        self.dt = float(dt_new)
        self.molecule_id = int(molecule_id)

        self.checkpoint_filename = {}
        self.checkpoint_filename["rho"] = f"qutip_rho_{self.molecule_id}.qu"
        self.checkpoint_filename["meta"] = f"qutip_meta_{self.molecule_id}.npz"

        # Build model either from preset or user module
        if self.preset == "tls" and self.module_path is None:
            cfg = _build_model_tls(**self.preset_kwargs)
        elif self.preset == "custom" and self.module_path is not None:
            mod = _load_spec_module(self.module_path)
            cfg = mod.build_model(**self.module_kwargs)
        else:
            raise ValueError(
                f"Invalid quantum model: preset={self.preset}, module={self.module_path}"
            )

        self.H0 = cfg["H0"]
        self.mu_ops = {"x": None, "y": None, "z": None, **cfg.get("mu_ops", {})}
        self.c_ops = list(cfg.get("c_ops", []))

        if self.verbose:
            print(
                f"[QuTiPModel {self.molecule_id}] initialized with dt={self.dt}, "
                f"H0 shape={self.H0.shape}, mu_ops keys={list(self.mu_ops.keys())}, "
                f"c_ops count={len(self.c_ops)}"
            )

        # Initialize density matrix rho (accept ket or density matrix)
        rho0 = cfg.get("rho0", None)
        if rho0 is None:
            print(
                "[QuTiPModel {self.molecule_id}] Warning: No initial state rho0 provided, using ground state."
            )
            # try ground state projector of H0
            evals, evecs = self.H0.eigenstates()
            rho0 = evecs[0] * evecs[0].dag()
        elif isinstance(rho0, qt.Qobj) and rho0.isket:
            rho0 = rho0 * rho0.dag()
        self.rho = rho0
        if self.verbose:
            print(
                f"[QuTiPModel {self.molecule_id}] Initial state rho0 (dim={self.rho.shape[0]}):\n{self.rho}"
            )

        # temporary storage
        self._dim = self.H0.shape[0]
        self._eye = qt.qeye(self._dim)

        # optional restart
        if self.restart and self.checkpoint:
            self._reset_from_checkpoint()
            self.restarted = True

        # calculate initial dipole
        self._mu_curr = _calc_mu_vector_expectation(self.rho, self.mu_ops)

        # construct operators for analytical dmu/dt
        # dmu_k/dt = i*[H0, mu_k] - i sum_j E_j*[mu_j, mu_k] + sum_j [c_j^dag * mu_k * c_j * rho - 0.5 * {c_j^dag * c_j, mu_k}]
        # d<mu_k>/dt = Tr[rho * dmu_k/dt]
        if not self.fd_dmudt:
            axes = ("x", "y", "z")
            for ax in axes:
                mu_k = self.mu_ops.get(ax, None)
                if mu_k is None:
                    continue
                # K0 = i * [H0, mu_k]
                K0 = 1j * (self.H0 * mu_k - mu_k * self.H0)
                # Kj = -i * [mu_j, mu_k]
                Kj = {}
                for aj in axes:
                    mu_j = self.mu_ops.get(aj, None)
                    if mu_j is not None:
                        Kj[aj] = -1j * (mu_j * mu_k - mu_k * mu_j)
                # L = sum_j [c_j^dag * mu_k * c_j - 0.5 * {c_j^dag * c_j, mu_k}]
                L = 0
                for c in self.c_ops:
                    L += c.dag() * mu_k * c - 0.5 * (
                        c.dag() * c * mu_k + mu_k * c.dag() * c
                    )
                # combine E-field independent parts in a single operator
                H_indep = K0 + L
                self._dmudt_op[ax] = {"H_indep": H_indep, "Kj": Kj}
                if self.verbose:
                    print(
                        f"[QuTiPModel {self.molecule_id}] Analytical dmu/dt operator for axis '{ax}' constructed."
                    )
                    print(f"  H_indep:\n{H_indep}")
                    for aj, K_j in Kj.items():
                        print(f"  K_{aj}:\n{K_j}")

    # ------------ internal functions -------------

    def _effective_unitary_step(self, E_vec):
        r"""
        Fast path for closed-system evolution without collapse operators.

        This uses the effective Hamiltonian
        :math:`H_{\\mathrm{eff}} = H_0 - \\mathbf{E} \\cdot \\boldsymbol{\\mu}`.

        Parameters
        ----------
        E_vec : array-like of float, shape (3,)
            Electric field vector ``[E_x, E_y, E_z]`` at the current time step.
        """

        Heff = self.H0
        if self.mu_ops["x"] is not None:
            Heff = Heff - float(E_vec[0]) * self.mu_ops["x"]
        if self.mu_ops["y"] is not None:
            Heff = Heff - float(E_vec[1]) * self.mu_ops["y"]
        if self.mu_ops["z"] is not None:
            Heff = Heff - float(E_vec[2]) * self.mu_ops["z"]

        U = (-1j * Heff * self.dt).expm()
        self.rho = U * self.rho * U.dag()

    def _lindblad_step(self, E_vec):
        """
        Slow path for open-system evolution with collapse operators using QuTiP
        ``mesolve``.

        Parameters
        ----------
        E_vec : array-like of float, shape (3,)
            Electric field vector ``[E_x, E_y, E_z]`` at the current time step.
        """

        H = self.H0
        if self.mu_ops["x"] is not None:
            H = H - float(E_vec[0]) * self.mu_ops["x"]
        if self.mu_ops["y"] is not None:
            H = H - float(E_vec[1]) * self.mu_ops["y"]
        if self.mu_ops["z"] is not None:
            H = H - float(E_vec[2]) * self.mu_ops["z"]

        # Single "macro" step: piecewise-constant E over [0, dt]
        res = qt.mesolve(H, self.rho, tlist=[0.0, self.dt], c_ops=self.c_ops, e_ops=[])
        self.rho = res.states[-1]

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

        self.E_vec = np.asarray(effective_efield_vec, dtype=float).reshape(3)

        if self.verbose:
            print(
                f"[QuTiPModel {self.molecule_id}] t={self.t:.6f} a.u., E={self.E_vec}"
            )

        # Store starting dipole
        self._mu_prev = self._mu_curr

        # Choose fast path if no collapse operators are present
        if len(self.c_ops) == 0:
            self._effective_unitary_step(self.E_vec)
        else:
            self._lindblad_step(self.E_vec)

        self.t += self.dt
        self._mu_curr = _calc_mu_vector_expectation(self.rho, self.mu_ops)

    def calc_amp_vector(self):
        """
        Calculate the amplitude vector :math:`\\mathrm{d}\\langle\\mu\\rangle/\\mathrm{d}t`
        for the current time step.

        If ``fd_dmudt`` is ``True``, use finite differences (cheaper); otherwise, use
        the analytical derivative if available.

        Returns
        -------
        numpy.ndarray of float, shape (3,)
            The amplitude vector
            :math:`[\\mathrm{d}\\langle\\mu_x\\rangle/\\mathrm{d}t,\\ \\mathrm{d}\\langle\\mu_y\\rangle/\\mathrm{d}t,\\ \\mathrm{d}\\langle\\mu_z\\rangle/\\mathrm{d}t]`.
        """

        amp = np.zeros(3, float)
        if self.fd_dmudt:
            amp = (self._mu_curr - self._mu_prev) / self.dt
        else:
            # Analytical derivative
            for i, ax in enumerate(("x", "y", "z")):
                op_info = self._dmudt_op.get(ax, None)
                if op_info is None:
                    continue
                # construct the overall operator to do trace with rho
                H_indep = op_info["H_indep"]
                Kj = op_info["Kj"]
                op = H_indep
                for j, Ej in zip(("x", "y", "z"), self.E_vec):
                    K_j = Kj.get(j, None)
                    if K_j is not None:
                        op = op + Ej * K_j
                amp[i] = float(qt.expect(op, self.rho))

        if self.verbose:
            print(f"[QuTiPModel {self.molecule_id}] d<mu>/dt = {amp}")
        return amp

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

        data = {
            "time_au": self.t,
            "mu_x_au": float(self._mu_curr[0]),
            "mu_y_au": float(self._mu_curr[1]),
            "mu_z_au": float(self._mu_curr[2]),
            "rho_diag": np.real(np.diag(self.rho.full())).tolist(),
        }
        if self._dim == 2:
            data["Pg"] = float(self.rho[0, 0].real)
            data["Pe"] = float(self.rho[1, 1].real)
            data["Pge_real"] = float(self.rho[0, 1].real) if self._dim == 2 else None
            data["Pge_imag"] = float(self.rho[0, 1].imag) if self._dim == 2 else None

        return data

    def _dump_to_checkpoint(self):
        """
        Dump the internal state of the model to a checkpoint.
        """
        qt.qsave(self.rho, self.checkpoint_filename["rho"])
        np.savez(
            self.checkpoint_filename["meta"],
            t=self.t,
            mu_prev=self._mu_prev,
            mu_curr=self._mu_curr,
        )

    def _reset_from_checkpoint(self):
        """
        Reset the internal state of the model from a checkpoint.
        """
        if not os.path.exists(self.checkpoint_filename["rho"]) or not os.path.exists(
            self.checkpoint_filename["meta"]
        ):
            # No checkpoint file found means this driver has not been paused or terminated abnormally
            # so we just start fresh.
            if self.verbose:
                print(
                    f"[QuTiPModel] No checkpoint file found for id={self.molecule_id}, starting fresh."
                )
        else:
            self.rho = qt.qload(self.checkpoint_filename["rho"])
            meta = np.load(self.checkpoint_filename["meta"])
            self.t = float(meta["t"])
            self._mu_prev = np.asarray(meta["mu_prev"], float)
            self._mu_curr = np.asarray(meta["mu_curr"], float)
            if self.verbose:
                print(
                    f"[QuTiPModel] Restarted from checkpoint for id={self.molecule_id}"
                )

    def _snapshot(self):
        """
        Return a snapshot of the internal state for propagation.

        Notes
        -----
        Deep copy the arrays to avoid mutation issues.

        Returns
        -------
        dict
            A dictionary containing the snapshot of the internal state.
        """

        return {
            "time": self.t,
            "rho": self.rho.full().copy(),
            "mu_prev": self._mu_prev.copy(),
            "mu_curr": self._mu_curr.copy(),
        }

    def _restore(self, snapshot):
        """
        Restore the internal state from a snapshot.

        Parameters
        ----------
        snapshot : dict
            A dictionary containing the snapshot of the internal state.
        """

        self.t = snapshot["time"]
        self.rho = qt.Qobj(snapshot["rho"])
        self._mu_prev = snapshot["mu_prev"].copy()
        self._mu_curr = snapshot["mu_curr"].copy()
