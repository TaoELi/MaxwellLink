# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Vectorized batch-model backends and their lazy registry.
"""

from __future__ import annotations

from .dummy_gpu import BatchStepResult, DummyBatchModel

# Batch drivers available on the GPU backend
_GPU_BATCH_DRIVERS = ("sho", "md")

__all__ = [
    "DummyBatchModel",
    "BatchStepResult",
    "get_batch_model",
    "supported_batch_drivers",
    "import_gpu_array_module",
]


def supported_batch_drivers(backend: str) -> tuple[str, ...]:
    """Return the driver names supported for ``backend`` (CUDA-free lookup)."""

    if str(backend).strip().lower() == "gpu":
        return tuple(_GPU_BATCH_DRIVERS)
    return ()


def get_batch_model(backend: str, driver: str):
    """Return the :class:`DummyBatchModel` subclass for ``(backend, driver)``.

    The concrete model is imported lazily and imports no GPU library at module
    load, so this stays CUDA-free until a model is actually stepped.

    Parameters
    ----------
    backend : str
        Batch backend name (currently only ``"gpu"``).
    driver : str
        Batch driver name (currently only ``"sho"``).

    Raises
    ------
    ValueError
        If ``(backend, driver)`` has no registered batch model.
    """

    normalized_backend = str(backend).strip().lower()
    normalized_driver = str(driver).strip().lower()
    if normalized_backend == "gpu" and normalized_driver in _GPU_BATCH_DRIVERS:
        if normalized_driver == "sho":
            from .sho_gpu import SHOGPUBatchModel

            return SHOGPUBatchModel
        from .md_gpu import MDGPUBatchModel

        return MDGPUBatchModel
    supported = supported_batch_drivers(normalized_backend)
    raise ValueError(
        f"No batch model for backend={backend!r}, driver={driver!r}. "
        f"Supported {normalized_backend!r} drivers: {supported or '(none)'}."
    )


def import_gpu_array_module():
    """Import and return CuPy, raising a clear error on a CUDA-less host.

    NumPy hosts (e.g. macOS) can still exercise every batch model by injecting
    ``xp=numpy`` directly; only the live GPU bridge calls this function.
    """

    try:
        import cupy
    except Exception as exc:  # ImportError, or a CuPy CUDA-runtime failure
        raise ImportError(
            "The GPU batch backend requires CuPy. Install a CUDA build, e.g. "
            "'pip install maxwelllink[gpu-cuda12]'. On hosts without CUDA, "
            "inject xp=numpy to run the vectorized model on the CPU."
        ) from exc
    return cupy
