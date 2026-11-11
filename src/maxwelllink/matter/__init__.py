# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Standalone matter calculations with MaxwellLink's built-in drivers.
"""

from .spectra import dipole_spectrum, SpectrumResult
from .md import MD, available_force_fields

__all__ = [
    "dipole_spectrum",
    "SpectrumResult",
    "MD",
    "available_force_fields",
]
