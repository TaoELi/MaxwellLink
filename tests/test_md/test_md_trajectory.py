# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Run-time trajectory output of the MD drivers.
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

_DT_AU = 0.5 * FS_TO_AU
_NAMES = ("temperature_K", "energy_au", "stretch_au", "bend_au")


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


def _read(path):
    """Return the recorded datasets of an HDF5 or NPZ trajectory file as arrays."""
    if path.endswith(".h5"):
        import h5py

        with h5py.File(path, "r") as handle:
            return {key: handle[key][...] for key in handle}
    with np.load(path) as data:
        return {key: data[key] for key in data}


def _scalar_run(tmp_path, filename, n_steps, every, **overrides):
    """Drive one scalar MDModel while it records, and return its recorded values."""
    kwargs = dict(
        ff="co2jcp2021",
        thermostat="nvt",
        pre_nvt=False,
        seed=3,
        record_filename=os.path.join(str(tmp_path), filename),
        record_every_steps=every,
    )
    kwargs.update(overrides)
    model = MDModel(**kwargs)
    model.initialize(_DT_AU, 7)
    field = np.array([0.0, 0.0, 2e-4])
    energies = []
    for _ in range(n_steps):
        model.propagate(field)
        energies.append(model.append_additional_data()["energy_au"])
    model.close()
    return model, np.array(energies)


@pytest.mark.core
@pytest.mark.parametrize("filename", ["traj.h5", "traj.npz"])
def test_scalar_driver_records_temperature_energy_and_bonded_terms(tmp_path, filename):
    """Every ``record_every_steps`` steps the file gains one row of the four scalars,
    consistent with what the driver reports and with the force field's own terms."""
    n_steps, every = (
        6,
        2,
    )  # a multiple of the stride: the last record is the final state
    model, energies = _scalar_run(tmp_path, filename, n_steps, every)
    path = os.path.join(str(tmp_path), filename.replace(".", "_id_7."))
    assert os.path.exists(path), "the file carries the molecule ID"
    data = _read(path)

    n_records = n_steps // every
    assert data["time_au"].shape == (n_records,)
    np.testing.assert_allclose(
        data["time_au"], _DT_AU * every * np.arange(1, n_records + 1)
    )
    assert data["molecule_ids"].tolist() == [7]
    for name in _NAMES:
        assert data[name].shape == (n_records, 1)
    # the total energy is the one reported to MaxwellLink at those steps
    np.testing.assert_allclose(data["energy_au"][:, 0], energies[every - 1 :: every])
    # the bonded terms are the force field's, at the final geometry
    terms = np.zeros(2)
    model.ff.compute(model.x, np.zeros(3), terms)
    stretch, bend = terms
    assert abs(data["stretch_au"][-1, 0] - stretch) < 1e-12
    assert abs(data["bend_au"][-1, 0] - bend) < 1e-12
    # temperature: 2 K / (3 N k_B) with K = E - U at the final step
    kinetic = energies[-1] - model.potential
    assert (
        abs(data["temperature_K"][-1, 0] - 2 * kinetic / (3 * model.na) / K_TO_AU)
        < 1e-9
    )
    assert 150.0 < data["temperature_K"][-1, 0] < 450.0


@pytest.mark.core
@pytest.mark.parametrize("ff", ["co2jcp2021", "qtip4pf"])
def test_numpy_and_compiled_force_paths_report_the_same_terms(tmp_path, ff):
    """The NumPy reference and the compiled kernels split the potential the same way."""
    _scalar_run(tmp_path, "numba.h5", 4, 1, ff=ff, force_backend="numba")
    _scalar_run(tmp_path, "numpy.h5", 4, 1, ff=ff, force_backend="numpy")
    a = _read(os.path.join(str(tmp_path), "numba_id_7.h5"))
    b = _read(os.path.join(str(tmp_path), "numpy_id_7.h5"))
    for name in _NAMES:
        np.testing.assert_allclose(a[name], b[name], rtol=0, atol=1e-10)


@pytest.mark.core
@pytest.mark.parametrize("ff", ["co2jcp2021", "qtip4pf"])
@pytest.mark.parametrize("backend", ["numpy", "cupy"])
def test_batch_records_what_the_scalar_drivers_record(tmp_path, backend, ff):
    """One file for the batch, one column per molecule, matching the scalar drivers."""
    xp = np if backend == "numpy" else _cupy_or_skip()
    molecule_ids = [4, 5, 6]
    n_steps, every = 6, 3
    # NVE: the GPU draws its Langevin noise from another generator than the scalar
    # driver, so only a thermostat-free trajectory can be compared step for step
    kwargs = dict(
        ff=ff, thermostat="nve", pre_nvt=False, seed=3, record_every_steps=every
    )
    field = np.tile([0.0, 0.0, 2e-4], (len(molecule_ids), 1))

    batch = MDBatch(
        num=len(molecule_ids),
        driver_kwargs=dict(
            record_filename=os.path.join(str(tmp_path), "batch.h5"), **kwargs
        ),
        xp=xp,
    )
    batch.initialize(_DT_AU, molecule_ids)
    for _ in range(n_steps):
        batch.step(field)
    batch.close()
    recorded = _read(os.path.join(str(tmp_path), "batch_id_4.h5"))
    assert recorded["molecule_ids"].tolist() == molecule_ids
    assert recorded["energy_au"].shape == (n_steps // every, len(molecule_ids))

    for column, molecule_id in enumerate(molecule_ids):
        scalar = MDModel(
            record_filename=os.path.join(str(tmp_path), "scalar.h5"), **kwargs
        )
        scalar.initialize(_DT_AU, molecule_id)
        for _ in range(n_steps):
            scalar.propagate(field[0])
        scalar.close()
        reference = _read(os.path.join(str(tmp_path), f"scalar_id_{molecule_id}.h5"))
        # the CPU batch is the scalar driver bit for bit; the GPU evaluates the
        # non-bonded forces in single precision, so its trajectory drifts slightly
        tolerance = 1e-12 if backend == "numpy" else 5e-6
        for name in _NAMES:
            np.testing.assert_allclose(
                recorded[name][:, column],
                reference[name][:, 0],
                rtol=tolerance,
                atol=tolerance,
                err_msg=f"{name} of molecule {molecule_id} on {backend}",
            )


@pytest.mark.core
def test_recording_is_off_by_default_and_the_stride_is_checked(tmp_path):
    """No file name means no file; a non-positive stride is refused when recording."""
    model = MDModel(ff="co2jcp2021")
    model.initialize(_DT_AU, 0)
    model.close()  # harmless without a recorder
    assert not list(tmp_path.iterdir())
    bad = MDModel(
        ff="co2jcp2021", record_filename=str(tmp_path / "a.h5"), record_every_steps=0
    )
    with pytest.raises(ValueError):
        bad.initialize(_DT_AU, 0)


@pytest.mark.core
def test_h5_falls_back_to_npz_without_h5py(tmp_path, monkeypatch):
    """Without h5py an ``.h5`` name is written as ``.npz`` rather than failing."""
    import sys

    monkeypatch.setitem(sys.modules, "h5py", None)  # makes `import h5py` fail
    _scalar_run(tmp_path, "fallback.h5", 2, 1)
    assert not (tmp_path / "fallback_id_7.h5").exists()
    data = _read(str(tmp_path / "fallback_id_7.npz"))
    assert data["energy_au"].shape == (2, 1)


if __name__ == "__main__":
    import tempfile

    with tempfile.TemporaryDirectory() as folder:
        for filename in ("traj.h5", "traj.npz"):
            test_scalar_driver_records_temperature_energy_and_bonded_terms(
                folder, filename
            )
        for ff in ("co2jcp2021", "qtip4pf"):
            test_numpy_and_compiled_force_paths_report_the_same_terms(folder, ff)
            for backend in ("numpy", "cupy"):
                test_batch_records_what_the_scalar_drivers_record(folder, backend, ff)
    print("MD trajectory recording tests passed")
