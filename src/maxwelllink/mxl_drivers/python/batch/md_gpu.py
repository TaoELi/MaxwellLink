# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Placeholder for the future in-house GPU molecular-dynamics batch model.
"""

from .dummy_gpu import DummyBatchModel

_NOT_IMPLEMENTED = (
    "The GPU molecular-dynamics batch model is not implemented yet. See "
    "SHOGPUBatchModel for the reference DummyBatchModel implementation."
)


class MDGPUBatchModel(DummyBatchModel):
    """
    Not-yet-implemented GPU molecular-dynamics batch model (interface stub).
    """

    def __init__(
        self, *, num, driver_kwargs, xp, driver_args=None, store_additional_data=False
    ):
        """
        Reject construction: the GPU MD batch model is not implemented yet.

        Parameters
        ----------
        num : int
            Number of sub-systems (accepted for signature parity).
        driver_kwargs : mapping
            Keyword arguments for the eventual MD model.
        xp : module
            Array module (``numpy`` or ``cupy``).
        driver_args : sequence, optional
            Positional arguments for the eventual MD model.
        store_additional_data : bool, default: False
            Reserved for the future columnar fast path.

        Raises
        ------
        NotImplementedError
        """

        raise NotImplementedError(_NOT_IMPLEMENTED)
