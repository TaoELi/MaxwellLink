# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
GPU-batched simple-harmonic-oscillator (SHO) model built on the scalar ``SHOModel``
"""

import numpy as np

from ..models.sho_model import SHOModel
from .base import BatchStepResult, DummyBatchModel

# Flags whose per-oscillator side effects (checkpoint files, log streams) defeat
# batching; rejected for the GPU backend in this first implementation.
_UNSUPPORTED_GPU_FLAGS = ("checkpoint", "restart", "verbose")


class SHOGPUBatchModel(DummyBatchModel):
    """
    Vectorized simple-harmonic-oscillator batch model.

    Parameters
    ----------
    num : int
        Number of oscillators in the batch.
    driver_kwargs : mapping
        Keyword arguments for the template :class:`SHOModel`.
    xp : module
        Array module exposing the NumPy API (``numpy`` or ``cupy``).
    driver_args : sequence, optional
        Positional arguments for the template :class:`SHOModel`, forwarded
        exactly as ``mxl_driver``/``mxl_bridge`` forward bare ``--param`` tokens.
    store_additional_data : bool, default: False
        Reserved for the future columnar fast path; unused on the correctness
        path, which always reports per-oscillator data.
    """

    def __init__(
        self, *, num, driver_kwargs, xp, driver_args=None, store_additional_data=False
    ):
        """
        Validate the SHO parameters and cache the canonical scalar values.

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
        self.q = self.p = self.acc = None  # oscillator state, allocated in initialize()

    def initialize(self, dt_au, molecule_ids):
        """
        Allocate the contiguous oscillator state and output buffers.

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

        # oscillator state (num,); acceleration starts at 0, matching SHOModel
        self.q = xp.full(n, self.q0, dtype=xp.float64)
        self.p = xp.full(n, self.p0, dtype=xp.float64)
        self.acc = xp.zeros(n, dtype=xp.float64)

        # preallocated output columns reused every step; off-axis columns stay 0
        self._amp = xp.zeros((n, 3), dtype=xp.float64)
        self._mu_half = xp.zeros((n, 3), dtype=xp.float64)
        self._mu_force = xp.zeros((n, 3), dtype=xp.float64)
        self._energy = xp.zeros(n, dtype=xp.float64)

    def step(self, efield_au):
        """
        Advance every oscillator by one velocity-Verlet step.

        Reproduces :meth:`SHOModel.propagate` verbatim, but vectorized across
        all oscillators, so results match the scalar driver to float64
        tolerance.

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
        xp, o, dt = self.xp, self.orientation, self.dt
        w2, mu0 = self.omega**2, self.mu0

        field = xp.asarray(efield_au, dtype=xp.float64)  # host -> device (one copy)
        if field.shape != (self.num, 3):
            raise ValueError(
                f"efield_au must have shape ({self.num}, 3); got {tuple(field.shape)}."
            )
        drive = mu0 * field[:, o]

        # EXACT scalar velocity-Verlet order (see SHOModel.propagate).
        self.p += 0.5 * self.acc * dt
        self.q += self.p * dt
        self.acc = -w2 * self.q + drive
        self.p += 0.5 * self.acc * dt
        p_half = self.p + 0.5 * self.acc * dt
        q_half = self.q + 0.5 * p_half * dt
        self.t += dt

        self._amp[:, o] = mu0 * p_half  # dmu/dt   (calc_amp_vector)
        self._mu_half[:, o] = mu0 * q_half  # mux_au   (half step)
        self._mu_force[:, o] = mu0 * self.q  # mux_m_au (force time)
        self._energy[:] = 0.5 * w2 * q_half**2 + 0.5 * p_half**2

        h = self._to_host
        return BatchStepResult(
            amplitude_au=h(self._amp),
            dipole_half_au=h(self._mu_half),
            dipole_force_au=h(self._mu_force),
            energy_au=h(self._energy),
        )

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
        released (and on the next memory-pool collection).
        """

        self.q = self.p = self.acc = None
        self._amp = self._mu_half = self._mu_force = self._energy = None

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
