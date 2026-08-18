# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

from .ir import ir_spectrum
from .tddft_spectrum import rt_tddft_spectrum, lr_tddft_spectrum
from .pulses import (
    gaussian_pulse,
    gaussian_enveloped_cosine,
    cosine_drive,
    k_parallel_pulse,
    k_parallel_pulse_with_seed,
)
from .transverse_components import (
    calc_transverse_components_3d,
    project_transverse_field_3d,
)
from .fast_json import json_loads
from .slko import (
    available_sets,
    fetch_slko,
    resolve_slko,
)

__all__ = [
    "json_loads",
    "available_sets",
    "fetch_slko",
    "resolve_slko",
    "ir_spectrum",
    "rt_tddft_spectrum",
    "lr_tddft_spectrum",
    "gaussian_pulse",
    "gaussian_enveloped_cosine",
    "cosine_drive",
    "k_parallel_pulse",
    "k_parallel_pulse_with_seed",
    "calc_transverse_components_3d",
    "project_transverse_field_3d",
]
