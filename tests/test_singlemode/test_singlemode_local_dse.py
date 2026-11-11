# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""The single-mode solver's local dipole self-energy coefficient: eps^2 / omega^2 on the coupled axes."""

import numpy as np
import pytest

import maxwelllink as mxl


@pytest.mark.core
def test_single_mode_local_coefficient():
    sim = mxl.SingleModeSimulation(
        dt_au=0.5,
        frequency_au=0.1286,
        damping_au=0.0,
        coupling_strength=3.354e-3,
        coupling_axis="y",
        molecules=[],
        record_history=False,
    )
    expected = np.array([0.0, 3.354e-3**2 / 0.1286**2, 0.0])
    assert np.allclose(sim.local_dipole_self_energy(), expected, rtol=1e-12, atol=0.0)
