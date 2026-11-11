# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""Two stress tests for the q-TIP4P/F force field and NVE integrator in MDModel.

The reference in ``tests/data/qtip4pf_reference.npz`` was produced by the i-pi
Fortran q-TIP4P/F driver (``drivers/f90/pes/qtip4pf.f90``) on the bundled 216-molecule
bulk-water geometry (``matter/md/force_fields/data/water_216.xyz``, atomic units) velocity Verlet from
zero initial velocity. It stores:

* ``positions``       : the initial 216-water geometry (Bohr),
* ``forces``          : the force on every atom at that geometry,
* ``positions_final`` : the geometry after a 20 fs (40-step) field-free NVE run,
* ``dt_au`` / ``n_steps`` : the time step and step count of that run.
"""

import os

import numpy as np
import pytest

md_mod = pytest.importorskip(
    "maxwelllink.mxl_drivers.python.models.md_model",
    reason="maxwelllink is required for this test",
)
qtip_mod = pytest.importorskip(
    "maxwelllink.matter.md.force_fields.qtip4pf",
    reason="maxwelllink is required for this test",
)
MDModel = md_mod.MDModel
QTIP4PFForceField = qtip_mod.QTIP4PFForceField

_REFERENCE = os.path.join(
    os.path.dirname(__file__), os.pardir, "data", "qtip4pf_reference.npz"
)


@pytest.mark.core
def test_forces_match_ipi_reference():
    """q-TIP4P/F forces on the explicit 216-water box match the reference calculations."""
    ref = np.load(_REFERENCE)
    ff = QTIP4PFForceField(n_molecules=216)
    assert np.array_equal(ff.positions, ref["positions"])  # original reference geometry
    forces, _ = ff.compute(ff.positions, np.zeros(3))
    assert np.max(np.abs(forces - ref["forces"])) < 1e-10


@pytest.mark.core
def test_nve_trajectory_matches_ipi_reference():
    """A 20 fs field-free NVE run (v0 = 0) reproduces the reference final geometry."""
    ref = np.load(_REFERENCE)
    m = MDModel(ff="qtip4pf", n_molecules=216, thermostat="nve", init_velocities=False)
    m.initialize(float(ref["dt_au"]), 0)
    assert np.array_equal(m.x, ref["positions"])  # started from the reference geometry

    for _ in range(int(ref["n_steps"])):
        m.propagate(np.zeros(3))  # no external field
    assert np.max(np.abs(m.x - ref["positions_final"])) < 1.2e-8


if __name__ == "__main__":
    test_forces_match_ipi_reference()
    test_nve_trajectory_matches_ipi_reference()
    print("q-TIP4P/F force + NVE stress tests match i-pi")


@pytest.mark.core
def test_default_32_water_geometry():
    """The small bundled box has whole water molecules and finite balanced forces."""
    ff = QTIP4PFForceField()
    assert ff.n_molecules == 32
    assert ff.positions.shape == (96, 3)
    assert np.array_equal(ff.box, np.full(3, 18.63))
    waters = ff.positions.reshape(32, 3, 3)
    bonds = np.linalg.norm(waters[:, 1:] - waters[:, :1], axis=2)
    assert np.all((bonds > 1.3) & (bonds < 2.3))
    forces, potential = ff.compute(ff.positions, np.zeros(3))
    assert np.isfinite(forces).all() and np.isfinite(potential)
    assert np.max(np.abs(forces.sum(axis=0))) < 1e-10
    # Loader returns copies, not mutable shared initial states.
    ff.positions[:] = 0
    assert np.any(QTIP4PFForceField().positions != 0)
    custom = QTIP4PFForceField(positions=waters[:1].copy().reshape(3, 3))
    assert custom.n_molecules == 1 and custom.box is None
