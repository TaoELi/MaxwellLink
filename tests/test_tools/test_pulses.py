# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""The ramped continuous-wave drive: the sin^2 turn-on, the plateau, the window and the mode attribute."""

import math

import numpy as np
import pytest

from maxwelllink.tools import cosine_drive, ramped_cosine_drive


@pytest.mark.core
def test_ramped_cosine_drive_matches_the_closed_form():
    amplitude, omega, t0, ramp = (
        0.035,
        0.1057,
        4134.0,
        4134.0,
    )  # the 2.876 eV pump of the anthracene runs
    drive = ramped_cosine_drive(amplitude, omega, t_start_au=t0, ramp_au=ramp, mode=143)
    assert drive.excited_mode_list == [143]
    assert drive(0.0) == 0.0 and drive(t0 - 1e-9) == 0.0
    for t in np.linspace(t0, t0 + 3 * ramp, 17):
        s = (t - t0) / ramp
        envelope = 1.0 if s >= 1.0 else math.sin(0.5 * math.pi * s) ** 2
        assert drive(t) == pytest.approx(
            amplitude * envelope * math.cos(omega * (t - t0)), abs=1e-15
        )
    # on the plateau the drive is the plain cosine of the same phase
    plain = cosine_drive(amplitude, omega, t_start_au=t0)
    for t in (t0 + 2 * ramp, t0 + 5 * ramp):
        assert drive(t) == pytest.approx(
            amplitude * math.cos(omega * (t - t0)), abs=1e-15
        )
        assert plain(t) == pytest.approx(amplitude * math.cos(omega * t), abs=1e-15)


@pytest.mark.core
def test_ramped_cosine_drive_window_and_zero_ramp():
    drive = ramped_cosine_drive(1.0, 0.2, t_start_au=10.0, ramp_au=0.0, t_end_au=20.0)
    assert drive(10.0) == pytest.approx(1.0)  # no ramp: on at once
    assert drive(20.0 + 1e-9) == 0.0
    assert not hasattr(drive, "excited_mode_list")
