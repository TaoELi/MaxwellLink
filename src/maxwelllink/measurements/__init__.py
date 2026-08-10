# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
Light-induced measurements for the MaxwellLink EM solvers.

- ``dummy_measurement.DummyMeasurement`` -- the solver-agnostic template
  (reference / signal_run / postprocess, chained by ``run()``);
- ``meep_linear`` -- Meep cavity spectroscopy: the shared two-run machinery
  (``MeepCavityMeasurement``) and its subclasses
  ``MeepTransmissionSpectroscopy``, ``MeepReflectionSpectroscopy``,
  ``MeepScatteringSpectroscopy``, and ``MeepPurcellSpectroscopy``.
"""

from .dummy_measurement import DummyMeasurement
from .meep_linear import (
    MeepCavityMeasurement,
    MeepPurcellSpectroscopy,
    MeepReflectionSpectroscopy,
    MeepScatteringSpectroscopy,
    MeepTransmissionSpectroscopy,
)

__all__ = [
    "DummyMeasurement",
    "MeepCavityMeasurement",
    "MeepTransmissionSpectroscopy",
    "MeepReflectionSpectroscopy",
    "MeepScatteringSpectroscopy",
    "MeepPurcellSpectroscopy",
]
