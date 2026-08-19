# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Direct real-time TDDFTB-Ehrenfest models.

The package is layered bottom-up: :mod:`skfiles` parses the Slater-Koster files,
:mod:`dftb_params` holds the parameter set and the system, :mod:`h0_overlap` and
:mod:`sk_deriv` build the two-centre matrices and their derivatives, :mod:`scc` converges
the ground state, :mod:`forces` differentiates the energy, :mod:`rt` and :mod:`dynamics`
propagate the density and the nuclei, and :mod:`rtdftb_model` is the MaxwellLink driver
on top. Every compiled body is registered in :mod:`jit`, which compiles it once for the
CPU and once as a CUDA device function; :mod:`kernels_gpu` composes the device functions
into the CUDA kernels of the batched step that the GPU batch driver launches.
"""

from .dftb_params import (
    AU_TO_FS,
    BOHR_TO_AA,
    V_PER_AA_TO_AU,
    V_PER_M_TO_AU,
    DFTBSystem,
    ShellLayout,
    SlaterKosterSet,
    load_sk_set,
)
from .dynamics import RTDynamics, bomd_equilibrate, run_ehrenfest, run_kick
from .forces import energy_gradient, total_force, total_gradient
from .h0_overlap import build_h0_overlap, rotate_block, sk_interpolate
from .rt import RTState, laser_field
from .rtdftb_model import RTDFTBModel
from .scc import SCCResult, build_gamma, dipole_moment, scc_loop, scf
from .skfiles import SkfData, read_skf, repulsive_energy

__all__ = [
    "RTDFTBModel",
    "SkfData",
    "read_skf",
    "repulsive_energy",
    "SlaterKosterSet",
    "DFTBSystem",
    "ShellLayout",
    "load_sk_set",
    "BOHR_TO_AA",
    "AU_TO_FS",
    "V_PER_M_TO_AU",
    "V_PER_AA_TO_AU",
    "build_h0_overlap",
    "rotate_block",
    "sk_interpolate",
    "SCCResult",
    "scf",
    "scc_loop",
    "dipole_moment",
    "build_gamma",
    "energy_gradient",
    "total_force",
    "total_gradient",
    "RTState",
    "laser_field",
    "RTDynamics",
    "run_kick",
    "run_ehrenfest",
    "bomd_equilibrate",
]
