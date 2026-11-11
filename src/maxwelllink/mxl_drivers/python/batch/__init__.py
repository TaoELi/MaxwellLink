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
        Batch driver name; see :func:`supported_batch_drivers`.

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
