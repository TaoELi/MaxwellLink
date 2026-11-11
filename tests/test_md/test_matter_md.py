# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""Standalone ``maxwelllink.matter.MD`` interface built on the shared MD engine.

The wrapper must be pure glue: a ``matter.MD`` trajectory has to reproduce the
scalar socket driver (``MDModel``) step for step, because both are the ``num=1``
case of the same :class:`~maxwelllink.matter.md.md_engine.MDEngine`.
"""

import numpy as np
import pytest

matter_md = pytest.importorskip(
    "maxwelllink.matter.md", reason="maxwelllink is required for this test"
)
units_mod = pytest.importorskip("maxwelllink.units")
MD = matter_md.MD
DummyForceField = matter_md.force_fields.DummyForceField
FS_TO_AU = units_mod.FS_TO_AU

_DT_FS = 0.5
_DT_AU = _DT_FS * FS_TO_AU


@pytest.mark.core
def test_md_nve_run_matches_the_scalar_socket_driver():
    """A field-free matter.MD run reproduces MDModel (molecule ID 0) step for step,
    and the NVE energy is conserved."""
    from maxwelllink.mxl_drivers.python.models.md_model import MDModel

    n_steps = 40
    cell = MD(ff="co2jcp2021", n_molecules=1, dt_fs=_DT_FS, seed=3)
    result = cell.run(n_steps=n_steps, thermostat="nve", record_every=1)

    scalar = MDModel(ff="co2jcp2021", n_molecules=1, thermostat="nve", seed=3)
    scalar.initialize(_DT_AU, 0)
    for step in range(n_steps):
        scalar.propagate(np.zeros(3))
        data = scalar.append_additional_data()
        np.testing.assert_allclose(
            result.dipole_au[step],
            [data["mux_au"], data["muy_au"], data["muz_au"]],
            rtol=0.0,
            atol=1e-14,
        )
        assert abs(result.energy_au[step] - data["energy_au"]) < 1e-14
    # a 20 fs NVE stretch of one CO2 conserves the total energy tightly
    assert np.max(np.abs(result.energy_au - result.energy_au[0])) < 1e-5


@pytest.mark.core
def test_md_equilibrate_then_nvt_holds_the_target_temperature():
    """Langevin equilibration plus an NVT run keep the bulk CO2 box near 300 K."""
    cell = MD(ff="co2jcp2021", dt_fs=_DT_FS, temperature_K=300.0, seed=5)
    cell.equilibrate(duration_ps=0.05, friction_fs=20.0)  # a short, hard thermostat
    result = cell.run(n_steps=200, thermostat="nvt", friction_fs=20.0, record_every=5)
    assert result.time_au.shape == (40,)
    assert 200.0 < result.temperature_K.mean() < 400.0
    # successive runs continue the same trajectory (time keeps increasing)
    again = cell.run(n_steps=10, thermostat="nve")
    assert again.time_au[0] > result.time_au[-1]


@pytest.mark.core
def test_ir_spectrum_and_save_roundtrip(tmp_path):
    """The IR helper returns a finite spectrum and save() writes the arrays."""
    cell = MD(ff="co2jcp2021", n_molecules=1, dt_fs=0.25, seed=1)
    result = cell.run(duration_ps=0.2, thermostat="nve", snapshot_every=100)
    frequency_cm, intensity = result.ir_spectrum(component="total")
    assert frequency_cm.shape == intensity.shape
    assert np.all(np.isfinite(intensity)) and frequency_cm[0] == 0.0
    single = result.ir_spectrum(component="z", smooth_window_len=None)
    assert single[0].shape == single[1].shape

    result.save(tmp_path / "co2.npz")
    with np.load(tmp_path / "co2.npz") as data:
        np.testing.assert_allclose(data["dipole_au"], result.dipole_au)
        assert data["snapshot_coords_bohr"].shape[1:] == (cell.n_atoms, 3)

    with pytest.raises(ValueError):  # exactly one run length must be given
        cell.run()
    with pytest.raises(ValueError):
        cell.run(duration_ps=0.1, n_steps=5)


@pytest.mark.core
def test_a_user_force_field_instance_plugs_into_md():
    """A DummyForceField subclass (NumPy path, no compiled kernels) runs end to end:
    the documented hook for user-defined force fields."""

    class HarmonicDimer(DummyForceField):
        """Two proton-mass particles with charges +/-0.5 on a harmonic spring."""

        name = "harmonic_dimer"
        r0, k, q = 2.0, 0.5, 0.5

        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.masses = np.full((self.n_atoms, 1), 1836.0)  # nuclear, not electron

        def _build_default_geometry(self, n_molecules, box):
            return np.array([[0.0, 0.0, -0.5 * self.r0], [0.0, 0.0, 0.7 * self.r0]])

        def compute(self, x, efield, terms=None):
            bond = x[1] - x[0]
            r = float(np.linalg.norm(bond))
            unit = bond / r
            forces = np.empty((2, 3))
            forces[0] = self.k * (r - self.r0) * unit + self.q * efield
            forces[1] = -self.k * (r - self.r0) * unit - self.q * efield
            return forces, 0.5 * self.k * (r - self.r0) ** 2

        def dipole(self, x):
            return self.q * x[0] - self.q * x[1]

        def dipole_velocity(self, v):
            return self.q * v[0] - self.q * v[1]

    cell = MD(ff=HarmonicDimer(), dt_fs=0.25, init_velocities=False, seed=0)
    assert cell.engine.force_evaluator == "numpy"  # no compiled kernels -> reference
    result = cell.run(n_steps=400, thermostat="nve")
    # the stretched bond oscillates; the energy shows only the bounded
    # velocity-Verlet oscillation (~(omega dt)^2), no drift or blow-up
    assert np.ptp(result.dipole_au[:, 2]) > 0.01
    assert np.max(np.abs(result.energy_au - result.energy_au[0])) < 1e-3
    with pytest.raises(ValueError):  # an instance carries its own geometry
        MD(ff=HarmonicDimer(), n_molecules=2)


@pytest.mark.core
def test_replicate_reaches_every_front_end(tmp_path):
    """``replicate`` is one keyword shared by matter.MD, MDModel and the engine: the
    force field tiles the cell, XYZ inputs describe the cell before replication, and
    a ready-made force-field instance still carries its own geometry."""
    from maxwelllink.matter.md.force_fields import CO2JCP2021ForceField, replicate_cell
    from maxwelllink.mxl_drivers.python.models.md_model import MDModel
    from maxwelllink.units import BOHR_PER_ANG

    cell = MD(ff="co2jcp2021", replicate=8, dt_fs=_DT_FS, seed=2)  # 2 x 2 x 2 images
    assert cell.n_atoms == 864 and cell.engine.n_molecules == 288
    assert len(cell.engine.symbols) == 864
    assert cell.engine.symbols[:3] == ["C", "O", "O"]
    np.testing.assert_allclose(cell.ff.box, 2 * 25.263)
    result = cell.run(n_steps=10, thermostat="nve")
    assert np.all(np.isfinite(result.energy_au)) and result.details["n_atoms"] == 864
    with pytest.raises(ValueError):  # an instance carries its own geometry
        MD(ff=CO2JCP2021ForceField(), replicate=8)

    # xyz / batch_xyz hold the cell before replication and are tiled the same way
    unit = CO2JCP2021ForceField()
    path = tmp_path / "co2_frames.xyz"
    with open(path, "w") as handle:
        for scale in (1.0, 1.001):
            handle.write(f"{unit.n_atoms}\n\n")
            rows = unit.positions * scale / BOHR_PER_ANG
            for symbol, row in zip(unit.molecule_symbols * 36, rows):
                handle.write(f"{symbol} {row[0]:.10f} {row[1]:.10f} {row[2]:.10f}\n")
    model = MDModel(ff="co2jcp2021", xyz=str(path), box=unit.box, replicate=8)
    np.testing.assert_allclose(
        model.engine.x0, CO2JCP2021ForceField(replicate=8).positions, atol=1e-8
    )
    model = MDModel(ff="co2jcp2021", batch_xyz=str(path), box=unit.box, replicate=8)
    assert model.engine.batch_frames.shape == (2, 864, 3)
    model.initialize(_DT_AU, 1)  # molecule 1 starts from frame 1, tiled
    tiled, box, _ = replicate_cell(unit.positions * 1.001, unit.box, 8)
    np.testing.assert_allclose(model.x, tiled, atol=1e-8)
    np.testing.assert_allclose(box, model.ff.box)


@pytest.mark.core
def test_public_import_paths_resolve():
    """The user namespace stays small (MD + available_force_fields), and every
    front door imports from its canonical home: the physics from
    ``maxwelllink.matter.md``, the socket drivers from ``mxl_drivers``."""
    import maxwelllink.matter as matter
    from maxwelllink.matter import MD as MD2, available_force_fields  # noqa: F401
    from maxwelllink.matter.md.force_fields import DummyForceField  # noqa: F401
    from maxwelllink.mxl_drivers.python.models.md_model import MDModel  # noqa: F401
    from maxwelllink.mxl_drivers.python.batch import get_batch_model

    assert set(available_force_fields()) >= {"co2jcp2021", "qtip4pf"}
    # `from maxwelllink.matter import *` exposes only the small user surface
    assert set(matter.__all__) & {"MD", "available_force_fields"} == {
        "MD",
        "available_force_fields",
    }
    assert "MDEngine" not in matter.__all__
    assert get_batch_model("gpu", "md").__name__ == "MDGPUBatchModel"


if __name__ == "__main__":
    import tempfile
    from pathlib import Path

    test_md_nve_run_matches_the_scalar_socket_driver()
    test_md_equilibrate_then_nvt_holds_the_target_temperature()
    with tempfile.TemporaryDirectory() as folder:
        test_ir_spectrum_and_save_roundtrip(Path(folder))
    test_a_user_force_field_instance_plugs_into_md()
    with tempfile.TemporaryDirectory() as folder:
        test_replicate_reaches_every_front_end(Path(folder))
    test_public_import_paths_resolve()
    print("matter.MD standalone interface tests passed")
