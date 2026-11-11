# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Small helpers shared by the standalone matter interfaces (MD, ...).
"""

import numpy as np

__all__ = ["THREADS_PER_BLOCK", "resolve_backend", "to_host", "field_rows"]

# Threads per block shared by every CUDA kernel launch. A multiple of the warp size (32)
THREADS_PER_BLOCK = 128


def _quiet_cuda_warnings():
    """Small systems launch tiny CUDA grids by design; silence Numba's occupancy warning."""

    try:
        import numba

        numba.config.CUDA_LOW_OCCUPANCY_WARNINGS = 0
    except Exception:
        pass


def resolve_backend(backend):
    """
    Resolve the requested backend to ``(name, array_module)``.

    ``None`` (auto) picks the GPU when CuPy can reach a CUDA device and falls
    back to the CPU otherwise; ``'cpu'`` and ``'gpu'`` force the choice.
    """

    choice = None if backend is None else str(backend).strip().lower()
    if choice in (None, "auto"):
        try:
            import cupy

            cupy.zeros(1)  # a CuPy build without a device raises here
            _quiet_cuda_warnings()
            return "gpu", cupy
        except Exception:
            return "cpu", np
    if choice == "cpu":
        return "cpu", np
    if choice == "gpu":
        try:
            import cupy
        except Exception as exc:  # ImportError, or a CuPy CUDA-runtime failure
            raise ImportError(
                "The GPU backend requires CuPy. Install a CUDA build, e.g. "
                "'pip install maxwelllink[gpu-cuda12]'. On hosts without CUDA, "
                "use backend='cpu' instead."
            ) from exc
        _quiet_cuda_warnings()
        return "gpu", cupy
    raise ValueError("backend must be None (auto), 'cpu' or 'gpu'.")


def to_host(array):
    """A NumPy copy of ``array``, whether it lives on the host or the device."""

    if array is None:
        return None
    if isinstance(array, np.ndarray):
        return np.array(array, copy=True)
    import cupy

    return cupy.asnumpy(array)


def field_rows(field, n_steps, dt_au):
    """Normalize ``field`` to ``None`` or a per-step ``(3,)`` getter."""

    if field is None:
        return None
    if callable(field):
        return lambda step: np.asarray(field(step * dt_au), dtype=float)
    table = np.asarray(field, dtype=float)
    if table.shape == (3,):
        constant = table.copy()
        return lambda step: constant
    if table.shape == (n_steps, 3):
        return lambda step: table[step]
    raise ValueError(
        "field must be None, a (3,) vector, an (n_steps, 3) table, or a "
        "callable t_au -> (3,); got shape %s." % (table.shape,)
    )
