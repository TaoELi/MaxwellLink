# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Direct classical molecular-dynamics (MD) models without calling LAMMPS.
"""

from .force_field import DummyForceField
from .qtip4pf import QTIP4PFForceField
from .md_model import MDModel

__all__ = ["MDModel", "DummyForceField", "QTIP4PFForceField"]
