# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

from pathlib import Path

import h5py
import numpy as np
import pytest

mxl = pytest.importorskip("maxwelllink", reason="maxwelllink is required for this test")


@pytest.mark.core
def test_multimode_record_to_disk_appends_history(tmp_path: Path):
    """
    Multi-mode disk recording should append one frame per propagated step.
    """

    history_path = tmp_path / "multimode_history.h5"

    sim = mxl.MultiModeSimulation(
        dt_au=0.5,
        frequency_au=0.242,
        damping_au=0.0,
        molecules=[],
        drive=0.0,
        x_grid_1d=[0.5],
        y_grid_1d=[0.5],
        record_history=True,
        record_to_disk=True,
        disk_address=str(history_path),
    )

    sim.run(steps=3)
    sim.h5file.close()

    expected_time = np.array([0.5, 1.0, 1.5])

    with h5py.File(history_path, "r") as h5file:
        np.testing.assert_allclose(h5file["time"][:], expected_time)
        assert h5file["qc"].shape == (3, 1, 3)
        assert h5file["pc"].shape == (3, 1, 3)
        assert h5file["drive"].shape == (3,)
        assert h5file["energy"].shape == (3,)
        assert h5file["effective_efield"].shape == (3, 1, 3)
