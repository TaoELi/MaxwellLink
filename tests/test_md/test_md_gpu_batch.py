# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""Force and MD tests for the GPU-batched MD driver, against the same references
used for the scalar MDModel.

The batched driver runs each force field's own compiled kernels
(``md_model/kernels_co2.py``, ``md_model/kernels_qtip4pf.py``), which evaluate the
non-bonded sums in single precision on the GPU. The references are:

* ``test_co2jcp2021.LAMMPS_FORCES``  : LAMMPS forces on the bundled 36-CO2 box,
* ``tests/data/qtip4pf_reference.npz`` : i-pi forces and a 20 fs NVE trajectory on the
  bundled 216-water box.

Tests that need a CUDA device are skipped when none is available.
"""

import os

import numpy as np
import pytest

md_mod = pytest.importorskip(
    "maxwelllink.mxl_drivers.python.models.md_model.md_model",
    reason="maxwelllink is required for this test",
)
batch_mod = pytest.importorskip(
    "maxwelllink.mxl_drivers.python.batch",
    reason="maxwelllink is required for this test",
)
units_mod = pytest.importorskip("maxwelllink.units")
MDModel = md_mod.MDModel
MDBatch = batch_mod.get_batch_model("gpu", "md")
FS_TO_AU = units_mod.FS_TO_AU
K_TO_AU = units_mod.K_TO_AU

from test_co2jcp2021 import LAMMPS_FORCES  # noqa: E402

_REFERENCE = os.path.join(
    os.path.dirname(__file__), os.pardir, "data", "qtip4pf_reference.npz"
)
_DT_AU = 0.5 * FS_TO_AU


def _cupy_or_skip():
    """Return CuPy, or skip the test when no usable CUDA device is present."""
    cp = pytest.importorskip("cupy", reason="cupy not installed")
    pytest.importorskip("numba", reason="numba not installed")
    try:
        if cp.cuda.runtime.getDeviceCount() < 1:
            pytest.skip("No CUDA device available for the GPU batch model.")
    except Exception:  # pragma: no cover - cupy present but no usable driver
        pytest.skip("No usable CUDA runtime for the GPU batch model.")
    return cp


def _batch_at_rest(xp, ff, num=2):
    """Build and initialize a batch of ``num`` systems at the reference geometry."""
    model = MDBatch(
        num=num,
        driver_kwargs=dict(
            ff=ff, thermostat="nve", init_velocities=False, pre_nvt=False
        ),
        xp=xp,
    )
    model.initialize(_DT_AU, list(range(num)))
    return model


@pytest.mark.core
def test_gpu_forces_match_lammps_reference():
    """GPU forces on the default 36-CO2 box match the LAMMPS reference."""
    cp = _cupy_or_skip()
    model = _batch_at_rest(cp, "co2jcp2021")  # initialize() evaluates the forces
    forces = cp.asnumpy(model.F)[0]
    model.close()
    assert np.max(np.abs(forces - LAMMPS_FORCES)) < 5e-5  # tolerance of the CPU test


@pytest.mark.core
def test_gpu_forces_match_ipi_reference():
    """GPU forces on the default 216-water box match the i-pi reference."""
    cp = _cupy_or_skip()
    ref = np.load(_REFERENCE)
    model = _batch_at_rest(cp, "qtip4pf")
    forces = cp.asnumpy(model.F)[0]
    model.close()
    assert np.max(np.abs(forces - ref["forces"])) < 1e-6  # single-precision non-bonded


@pytest.mark.core
def test_gpu_nve_trajectory_matches_ipi_reference():
    """A 20 fs field-free NVE run (v0 = 0) reproduces the reference final geometry."""
    cp = _cupy_or_skip()
    ref = np.load(_REFERENCE)
    model = _batch_at_rest(cp, "qtip4pf")
    assert np.array_equal(cp.asnumpy(model.x)[0], ref["positions"])

    zero_field = np.zeros((model.num, 3))
    for _ in range(int(ref["n_steps"])):
        model.step(zero_field)  # no external field
    positions = cp.asnumpy(model.x)[0]
    model.close()
    assert np.max(np.abs(positions - ref["positions_final"])) < 1e-5


@pytest.mark.core
def test_batch_reproduces_the_scalar_mdmodel():
    """A batch member and the scalar MDModel with the same molecule ID agree."""
    molecule_ids = [0, 1, 2]
    kwargs = dict(ff="co2jcp2021", thermostat="nve", pre_nvt=False, seed=7)
    rng = np.random.default_rng(11)
    efields = rng.normal(0.0, 2e-4, (4, len(molecule_ids), 3))

    scalars = []
    for molecule_id in molecule_ids:
        m = MDModel(**kwargs)
        m.initialize(_DT_AU, molecule_id)
        scalars.append(m)

    batch = MDBatch(num=len(molecule_ids), driver_kwargs=kwargs, xp=np)
    batch.initialize(_DT_AU, molecule_ids)

    for step in range(efields.shape[0]):
        result = batch.step(efields[step])
        for i, m in enumerate(scalars):
            m.propagate(efields[step, i])
            data = m.append_additional_data()
            mu_half = [data["mux_au"], data["muy_au"], data["muz_au"]]
            assert np.max(np.abs(result.amplitude_au[i] - m.calc_amp_vector())) < 1e-14
            assert np.max(np.abs(result.dipole_half_au[i] - mu_half)) < 1e-11
            assert abs(result.energy_au[i] - data["energy_au"]) < 1e-12


@pytest.mark.core
def test_nvt_reaches_the_target_temperature():
    """A Langevin NVT run brings every system in the batch to 300 K."""
    cp = _cupy_or_skip()
    num, n_atoms = 16, 108
    model = MDBatch(
        num=num,
        driver_kwargs=dict(
            ff="co2jcp2021",
            thermostat="nvt",
            temperature_K=300.0,
            friction_fs=100.0,
            pre_nvt=False,
            seed=5,
        ),
        xp=cp,
    )
    model.initialize(_DT_AU, list(range(num)))

    zero_field = np.zeros((num, 3))
    for _ in range(2000):  # 1 ps, several Langevin relaxation times
        model.step(zero_field)
    momenta = cp.asnumpy(model.p)
    masses = model.force_kernels.masses[None, :, :]  # (1, na, 1), per atom
    model.close()

    kinetic = 0.5 * np.sum(momenta**2 / masses, axis=(1, 2))
    temperature = 2.0 * kinetic / (3.0 * n_atoms) / K_TO_AU
    assert 250.0 < temperature.mean() < 350.0


if __name__ == "__main__":
    test_gpu_forces_match_lammps_reference()
    test_gpu_forces_match_ipi_reference()
    test_gpu_nve_trajectory_matches_ipi_reference()
    test_batch_reproduces_the_scalar_mdmodel()
    test_nvt_reaches_the_target_temperature()
    print("GPU-batched MD force + NVE + NVT tests match the references")
