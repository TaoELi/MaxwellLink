# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Built-in classical force fields of the MaxwellLink MD engine
"""

from .force_field import DummyForceField, replicate_cell
from .qtip4pf import QTIP4PFForceField
from .co2jcp2021 import CO2JCP2021ForceField
from .cs2compass import CS2COMPASSForceField
from .ch4compass import CH4COMPASSForceField

__all__ = [
    "DummyForceField",
    "QTIP4PFForceField",
    "CO2JCP2021ForceField",
    "CS2COMPASSForceField",
    "CH4COMPASSForceField",
    "available_force_fields",
    "replicate_cell",
]

_FORCE_FIELDS = {
    QTIP4PFForceField.name: QTIP4PFForceField,
    CO2JCP2021ForceField.name: CO2JCP2021ForceField,
    CS2COMPASSForceField.name: CS2COMPASSForceField,
    CH4COMPASSForceField.name: CH4COMPASSForceField,
}


def available_force_fields():
    """Return the registered force-field names."""

    return tuple(sorted(_FORCE_FIELDS))
