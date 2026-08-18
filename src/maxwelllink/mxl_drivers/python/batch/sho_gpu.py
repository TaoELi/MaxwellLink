# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
GPU-batched simple-harmonic-oscillator (SHO) model built on the scalar ``SHOModel``.

This driver runs one execution path on each backend:

- **CPU reference** (``xp=numpy``): a vectorized velocity-Verlet step.
- **GPU kernel** (``xp=cupy``): a *fused* ``numba.cuda.jit`` kernel that gives
  **one GPU thread to each oscillator**.
"""

import numpy as np

from ..models.sho_model import SHOModel
from .dummy_gpu import BatchStepResult, DummyBatchModel

# The following molecular driver flags are unsupported in the GPU-batched backend
_UNSUPPORTED_GPU_FLAGS = ("checkpoint", "restart", "verbose")

# Threads per block for the CUDA launch.  A multiple of the warp size (32).
_THREADS_PER_BLOCK = 128

# The fused kernel is compiled once per process and cached here
_STEP_KERNEL = None

# The ``numba.cuda`` module is imported lazily and cached here.  It is only
# required when ``xp=cupy`` is used, so hosts without CUDA can run the CPU
# reference without installing numba.
cuda = None


def _load_cuda():
    """Import ``numba.cuda`` lazily, raising a clear error if numba is absent.

    Returns
    -------
    module
        The ``numba.cuda`` module (also cached in the module global ``cuda``).

    Raises
    ------
    ImportError
        If numba (or its CUDA target) cannot be imported.
    """

    global cuda
    if cuda is None:
        try:
            from numba import cuda as numba_cuda
        except Exception as exc:  # ImportError, or a numba/CUDA runtime failure
            raise ImportError(
                "The GPU backend requires numba for its CUDA kernel. Install "
                "it alongside CuPy, e.g., 'pip install maxwelllink[gpu-cuda12]'. On "
                "hosts without CUDA, inject xp=numpy to run the CPU reference."
            ) from exc
        cuda = numba_cuda
    return cuda


def _build_step_kernel():
    """Compile (once) and return the fused velocity-Verlet CUDA kernel.

    The kernel launches one GPU thread per oscillator.  The per-oscillator
    physics lives in a small ``device`` function so that heavier drivers can
    reuse the same structure.

    Returns
    -------
    numba.cuda.dispatcher.CUDADispatcher
        The compiled ``step_kernel`` ready to launch with ``kernel[blocks, tpb]``.
    """

    global _STEP_KERNEL
    if _STEP_KERNEL is not None:
        return _STEP_KERNEL

    _load_cuda()  # populates the module-global ``cuda`` the kernel closes over

    @cuda.jit(device=True)
    def advance_one_oscillator(q, p, acc, drive, omega_sq, dt):
        """Advance ONE oscillator by a single velocity-Verlet step.

        Mirrors :meth:`SHOModel.propagate`.
        """

        p = p + 0.5 * acc * dt  # momentum to the half step
        q = q + p * dt  # position to the full step
        acc = -omega_sq * q + drive  # force at the field (force) time
        p = p + 0.5 * acc * dt  # momentum to the full step
        p_half = p + 0.5 * acc * dt  # momentum half a step later (dmu/dt)
        q_half = q + 0.5 * p_half * dt  # position half a step later (dipole)
        return q, p, acc, p_half, q_half

    @cuda.jit
    def step_kernel(
        q,
        p,
        acc,
        field,
        omega_sq,
        mu0,
        orientation,
        dt,
        amp,
        mu_half,
        mu_force,
        energy,
    ):
        """
        One thread per oscillator computation for GPU kernel.  The thread index is the oscillator index.
        """

        i = cuda.grid(1)  # global thread index == oscillator index
        if i < q.shape[0]:
            drive = mu0 * field[i, orientation]
            q_new, p_new, acc_new, p_half, q_half = advance_one_oscillator(
                q[i], p[i], acc[i], drive, omega_sq, dt
            )
            q[i] = q_new
            p[i] = p_new
            acc[i] = acc_new
            amp[i, orientation] = mu0 * p_half  # dmu/dt   (source amplitude)
            mu_half[i, orientation] = mu0 * q_half  # dipole half a step later
            mu_force[i, orientation] = mu0 * q_new  # dipole at the force time
            energy[i] = 0.5 * omega_sq * q_half * q_half + 0.5 * p_half * p_half

    # Cache the compiled kernel so that multiple batch models do not recompile it.
    _STEP_KERNEL = step_kernel
    return _STEP_KERNEL


class SHOGPUBatchModel(DummyBatchModel):
    """
    Vectorized simple-harmonic-oscillator batch model.
    """

    def __init__(
        self, *, num, driver_kwargs, xp, driver_args=None, store_additional_data=False
    ):
        """
        Initialize a GPU-batched SHO model with a single template oscillator.

        Parameters
        ----------
        num : int
            Number of oscillators in the batch; must be positive.
        driver_kwargs : mapping
            Keyword arguments for the template :class:`SHOModel`.
        xp : module
            Array module exposing the NumPy API (``numpy`` or ``cupy``).
        driver_args : sequence, optional
            Positional arguments for the template :class:`SHOModel`.
        store_additional_data : bool, default: False
            Reserved for the future columnar fast path.

        Raises
        ------
        ValueError
            If ``num`` is not positive, or ``checkpoint``/``restart``/``verbose``
            is enabled (unsupported for the batched GPU backend).
        """

        self.xp = xp
        self.num = int(num)
        if self.num <= 0:
            raise ValueError("num must be a positive integer.")
        self.store_additional_data = bool(store_additional_data)

        # One template validates args/kwargs and supplies canonical scalar
        # params; no per-oscillator Python objects are created.
        template = SHOModel(*tuple(driver_args or ()), **dict(driver_kwargs or {}))
        for flag in _UNSUPPORTED_GPU_FLAGS:
            if getattr(template, flag, False):
                raise ValueError(
                    f"GPU SHO backend does not support {flag}=True; batching "
                    f"per-oscillator checkpoint/log streams defeats the purpose."
                )
        self.omega = float(template.omega)
        self.mu0 = float(template.dipole_moment)
        self.orientation = int(template.orientation_idx)  # 0 (x), 1 (y), 2 (z)
        self.q0 = float(template.q)
        self.p0 = float(template.p)

        self.dt = 0.0  # shared time step in a.u.
        self.t = 0.0  # current time in a.u.
        self.molecule_ids = ()  # molecule IDs, set in initialize()
        self.q = self.p = self.acc = None  # oscillator state, set in initialize()
        self._on_gpu = False  # True when xp is CuPy (use the CUDA kernel)
        self._kernel = None  # compiled numba kernel, built in initialize()

    # ----------------------- heavy-load initialization ------------------------------

    def initialize(self, dt_au, molecule_ids):
        """
        Allocate the contiguous oscillator state and output buffers.

        On a CuPy backend this also compiles the fused CUDA kernel once.

        Parameters
        ----------
        dt_au : float
            The shared time step in atomic units (a.u.).
        molecule_ids : array-like of int
            Molecule IDs assigned by the hub; must number exactly ``num``.

        Raises
        ------
        ValueError
            If the number of molecule IDs does not match ``num``.
        """

        xp, n = self.xp, self.num
        self.molecule_ids = tuple(int(mid) for mid in molecule_ids)
        if len(self.molecule_ids) != n:
            raise ValueError(
                f"SHOGPUBatchModel expected {n} molecule ids, "
                f"got {len(self.molecule_ids)}."
            )
        self.dt = float(dt_au)
        self.t = 0.0

        # Backend choice: CuPy means a real CUDA device (use the fused kernel);
        # NumPy means a CUDA-less host (use the vectorized CPU reference).
        self._on_gpu = getattr(xp, "__name__", "") == "cupy"

        # oscillator state (num,); acceleration starts at 0, matching SHOModel
        self.q = xp.full(n, self.q0, dtype=xp.float64)
        self.p = xp.full(n, self.p0, dtype=xp.float64)
        self.acc = xp.zeros(n, dtype=xp.float64)

        # preallocated output columns reused every step; off-axis columns stay 0
        self._amp = xp.zeros((n, 3), dtype=xp.float64)
        self._mu_half = xp.zeros((n, 3), dtype=xp.float64)
        self._mu_force = xp.zeros((n, 3), dtype=xp.float64)
        self._energy = xp.zeros(n, dtype=xp.float64)

        # Compile the CUDA kernel once, only when actually on a GPU (this also
        # imports numba and populates the module-global ``cuda``).
        self._kernel = _build_step_kernel() if self._on_gpu else None

    # ----------------------- internal helper methods --------------------------

    def _to_host(self, array):
        """
        Return a fresh host NumPy snapshot of an array-module array.

        Parameters
        ----------
        array : numpy.ndarray or cupy.ndarray
            Array from ``self.xp`` to copy to host memory.

        Returns
        -------
        numpy.ndarray
            A host copy: ``cupy.asnumpy`` for a device array, otherwise a fresh
            NumPy copy so reused output buffers cannot mutate returned data.
        """

        asnumpy = getattr(self.xp, "asnumpy", None)
        if asnumpy is not None:  # cupy: device -> host copy
            return asnumpy(array)
        return np.array(array)  # numpy: force a copy so column reuse cannot mutate it

    # ----------------------- per-step methods ------------------------------

    def step(self, efield_au):
        """
        Advance every oscillator by one velocity-Verlet step.

        Parameters
        ----------
        efield_au : numpy.ndarray of float, shape (num, 3)
            Contiguous host array of effective electric field vectors in a.u.

        Returns
        -------
        BatchStepResult
            Columnar amplitude (``dmu/dt``), half-step dipole, force-time dipole,
            and energy for every oscillator.

        Raises
        ------
        RuntimeError
            If called before :meth:`initialize`.
        ValueError
            If ``efield_au`` does not have shape ``(num, 3)``.
        """

        if self.q is None:
            raise RuntimeError("SHOGPUBatchModel.step() called before initialize().")

        xp = self.xp
        field = xp.asarray(efield_au, dtype=xp.float64)  # host -> device (one copy)
        if field.shape != (self.num, 3):
            raise ValueError(
                f"efield_au must have shape ({self.num}, 3); got {tuple(field.shape)}."
            )

        if self._on_gpu:
            self._step_on_gpu(field)
        else:
            self._step_on_cpu(field)
        self.t += self.dt

        h = self._to_host
        return BatchStepResult(
            amplitude_au=h(self._amp),
            dipole_half_au=h(self._mu_half),
            dipole_force_au=h(self._mu_force),
            energy_au=h(self._energy),
        )

    def _step_on_cpu(self, field):
        """
        Vectorized velocity-Verlet reference (NumPy for CPU).

        Parameters
        ----------
        field : numpy.ndarray of float, shape (num, 3)
            Effective electric field vectors in a.u. (already an ``xp`` array).
        """

        o, dt = self.orientation, self.dt
        w2, mu0 = self.omega**2, self.mu0
        drive = mu0 * field[:, o]

        # EXACT scalar velocity-Verlet order (see SHOModel.propagate).
        self.p += 0.5 * self.acc * dt
        self.q += self.p * dt
        self.acc = -w2 * self.q + drive
        self.p += 0.5 * self.acc * dt
        p_half = self.p + 0.5 * self.acc * dt
        q_half = self.q + 0.5 * p_half * dt

        self._amp[:, o] = mu0 * p_half  # dmu/dt   (calc_amp_vector)
        self._mu_half[:, o] = mu0 * q_half  # mux_au   (half step)
        self._mu_force[:, o] = mu0 * self.q  # mux_m_au (force time)
        self._energy[:] = 0.5 * w2 * q_half**2 + 0.5 * p_half**2

    def _step_on_gpu(self, field):
        """
        CUDA step: one ``numba.cuda`` thread advances one oscillator.

        The whole velocity-Verlet update and all diagnostics run in a single
        kernel launch, so each oscillator's state stays in registers for the
        entire step.  This is the template to extend for heavier batched
        drivers.

        Parameters
        ----------
        field : cupy.ndarray of float, shape (num, 3)
            Effective electric field vectors in a.u. on the device.  CuPy arrays
            are passed straight to numba via the CUDA array interface (no copy).
        """

        blocks = (self.num + _THREADS_PER_BLOCK - 1) // _THREADS_PER_BLOCK
        self._kernel[blocks, _THREADS_PER_BLOCK](
            self.q,
            self.p,
            self.acc,
            field,
            self.omega**2,
            self.mu0,
            self.orientation,
            self.dt,
            self._amp,
            self._mu_half,
            self._mu_force,
            self._energy,
        )
        # numba and CuPy may schedule on different CUDA streams; synchronize once
        # here so the subsequent device->host copies see the finished results.
        cuda.synchronize()

    # ------------ optional operation --------------

    def append_additional_data(self):
        """
        Return per-oscillator dicts matching ``SHOModel.append_additional_data``.

        Returns
        -------
        list of dict
            One dictionary per oscillator with ``time_au``, ``energy_au``, the
            half-step dipole (``mux_au``/``muy_au``/``muz_au``), the force-time
            dipole (``mux_m_au``/``muy_m_au``/``muz_m_au``), and ``p_au``/``q_au``.

        Raises
        ------
        RuntimeError
            If called before the first :meth:`step`.
        """

        if self.q is None:
            raise RuntimeError(
                "append_additional_data() called before the first step()."
            )
        q = self._to_host(self.q)
        p = self._to_host(self.p)
        mu_half = self._to_host(self._mu_half)
        mu_force = self._to_host(self._mu_force)
        energy = self._to_host(self._energy)
        t = self.t
        return [
            {
                "time_au": t,
                "energy_au": float(energy[i]),
                "mux_au": float(mu_half[i, 0]),
                "muy_au": float(mu_half[i, 1]),
                "muz_au": float(mu_half[i, 2]),
                "mux_m_au": float(mu_force[i, 0]),
                "muy_m_au": float(mu_force[i, 1]),
                "muz_m_au": float(mu_force[i, 2]),
                "p_au": float(p[i]),
                "q_au": float(q[i]),
            }
            for i in range(self.num)
        ]

    def close(self):
        """
        Drop references to the oscillator state and output buffers.

        Notes
        -----
        CuPy frees the underlying device memory once these references are
        released.
        """

        self.q = self.p = self.acc = None
        self._amp = self._mu_half = self._mu_force = self._energy = None
        self._kernel = None
