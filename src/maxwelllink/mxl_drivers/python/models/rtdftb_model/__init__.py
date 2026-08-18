# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Direct real-time TDDFTB-Ehrenfest models.
"""

from .dftb_params import (
    AU_TO_FS,
    BOHR_TO_AA,
    V_PER_AA_TO_AU,
    V_PER_M_TO_AU,
    DFTBSystem,
    SlaterKosterSet,
    load_sk_set,
)
from .dynamics import RTDynamics, run_ehrenfest, run_kick
from .ehrenfest import ehrenfest_force, ehrenfest_gradient
from .forces import total_force, total_gradient
from .h0_overlap import build_h0_overlap, rotate_block, sk_interpolate
from .rt import RTState, laser_field
from .rtdftb_model import RTDFTBModel
from .scc import SCCResult, ShellLayout, build_gamma, dipole_moment, scf
from .skfiles import SkfData, read_skf, repulsive_energy

__all__ = [
    "RTDFTBModel",
    "SkfData",
    "read_skf",
    "repulsive_energy",
    "SlaterKosterSet",
    "DFTBSystem",
    "load_sk_set",
    "BOHR_TO_AA",
    "AU_TO_FS",
    "V_PER_M_TO_AU",
    "V_PER_AA_TO_AU",
    "build_h0_overlap",
    "rotate_block",
    "sk_interpolate",
    "ShellLayout",
    "SCCResult",
    "scf",
    "dipole_moment",
    "build_gamma",
    "total_force",
    "total_gradient",
    "RTState",
    "laser_field",
    "ehrenfest_force",
    "ehrenfest_gradient",
    "RTDynamics",
    "run_kick",
    "run_ehrenfest",
]
