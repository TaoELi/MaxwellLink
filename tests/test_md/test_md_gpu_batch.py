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
BOHR_PER_ANG = units_mod.BOHR_PER_ANG

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
@pytest.mark.parametrize(
    "thermostat, pre_nvt",
    [("nve", False), ("nvt", False), ("nvt", True)],
    ids=["nve", "nvt", "nvt+pre_nvt"],
)
def test_batch_reproduces_the_scalar_mdmodel(thermostat, pre_nvt):
    """A batch member and the scalar MDModel with the same molecule ID agree.

    That includes the thermostatted cases: both draw a system's initial momenta and
    its Langevin noise from the same ``seed + molecule_id`` generator, in the same
    order, so the CPU batch reproduces the scalar driver bit for bit.
    """
    molecule_ids = [0, 1, 2]
    kwargs = dict(
        ff="co2jcp2021",
        thermostat=thermostat,
        pre_nvt=pre_nvt,
        pre_nvt_duration_ps=0.005,  # ten steps
        seed=7,
    )
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
@pytest.mark.parametrize("backend", ["numpy", "cupy"])
def test_noise_streams_are_keyed_per_molecule(backend):
    """A molecule's Langevin noise is set by its ID alone, never by its batch mates.

    Two batches share molecule 7 at different rows, so it must follow one trajectory
    in both. Their row-0 members are different molecules that both start with zero
    centre-of-mass momentum; p_com is then driven by the noise alone, so it must not
    agree either -- with streams keyed on the row it would, to round-off.
    """
    xp = np if backend == "numpy" else _cupy_or_skip()
    kwargs = dict(ff="co2jcp2021", thermostat="nvt", pre_nvt=False, seed=3)
    zero_field = np.zeros((2, 3))

    def momenta_after(molecule_ids, n_steps=50):
        batch = MDBatch(num=len(molecule_ids), driver_kwargs=kwargs, xp=xp)
        batch.initialize(_DT_AU, molecule_ids)
        for _ in range(n_steps):
            batch.step(zero_field)
        momenta = batch._to_host(batch.p)
        batch.close()
        return momenta

    p_a = momenta_after([0, 7])
    p_b = momenta_after([7, 10])
    scale = np.max(np.abs(p_a))
    # molecule 7 in either batch
    assert np.max(np.abs(p_a[1] - p_b[0])) <= 1e-12 * scale
    # molecules 0 and 10, both in row 0
    p_com_a = p_a[0].sum(axis=0)
    p_com_b = p_b[0].sum(axis=0)
    assert np.max(np.abs(p_com_a - p_com_b)) > 1e-3 * scale


def _write_co2_frames(path, positions_bohr, scales, swap=False):
    """Write the CO2 box at a few uniform scalings as a multi-frame XYZ, in Angstrom."""
    symbols = ["C", "O", "O"] * (len(positions_bohr) // 3)
    if swap:
        symbols = ["O", "C", "O"] * (len(positions_bohr) // 3)
    with open(path, "w") as handle:
        for scale in scales:
            handle.write(f"{len(symbols)}\nscale {scale}\n")
            for symbol, (x, y, z) in zip(
                symbols, positions_bohr * scale / BOHR_PER_ANG
            ):
                handle.write(f"{symbol} {x:.10f} {y:.10f} {z:.10f}\n")
    return str(path)


@pytest.mark.core
@pytest.mark.parametrize("backend", ["numpy", "cupy"])
def test_xyz_and_batch_xyz_set_the_starting_geometries(tmp_path, backend):
    """``xyz`` starts every system from one file geometry, ``batch_xyz`` starts molecule
    ``m`` from frame ``m`` -- for the scalar driver and the batch alike."""
    xp = np if backend == "numpy" else _cupy_or_skip()
    reference = MDModel(ff="co2jcp2021").ff  # the bundled periodic box
    frames_bohr = [reference.positions * scale for scale in (1.0, 1.001, 1.002)]
    frames = _write_co2_frames(
        tmp_path / "frames.xyz", reference.positions, (1.0, 1.001, 1.002)
    )
    one = _write_co2_frames(tmp_path / "one.xyz", reference.positions, (1.001,))
    kwargs = dict(ff="co2jcp2021", box=reference.box, thermostat="nve", pre_nvt=False)

    scalar = MDModel(batch_xyz=frames, **kwargs)
    scalar.initialize(_DT_AU, 2)
    np.testing.assert_allclose(scalar.x, frames_bohr[2], atol=1e-9)
    scalar = MDModel(xyz=one, **kwargs)
    scalar.initialize(_DT_AU, 2)
    np.testing.assert_allclose(scalar.x, frames_bohr[1], atol=1e-9)

    batch = MDBatch(num=3, driver_kwargs=dict(batch_xyz=frames, **kwargs), xp=xp)
    batch.initialize(_DT_AU, [0, 1, 2])
    for row in range(3):
        np.testing.assert_allclose(
            batch._to_host(batch.x)[row], frames_bohr[row], atol=1e-9
        )
    batch.close()
    batch = MDBatch(num=3, driver_kwargs=dict(xyz=one, **kwargs), xp=xp)
    batch.initialize(_DT_AU, [0, 1, 2])
    x = batch._to_host(batch.x)
    assert np.abs(x - x[0]).max() == 0.0 and np.allclose(
        x[0], frames_bohr[1], atol=1e-9
    )
    batch.close()

    with pytest.raises(ValueError):  # atoms in the wrong order for the force field
        MDModel(
            xyz=_write_co2_frames(
                tmp_path / "bad.xyz", reference.positions, (1.0,), swap=True
            ),
            **kwargs,
        )
    with pytest.raises(ValueError):  # frame 5 does not exist
        MDModel(batch_xyz=frames, **kwargs).initialize(_DT_AU, 5)


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


@pytest.mark.core
def test_reset_dipole_subtracts_the_dipole_at_time_zero():
    """The reported dipole is shifted by mu(t=0), and d(mu)/dt is left alone."""
    molecule_ids = [0, 1, 2]
    kwargs = dict(ff="co2jcp2021", thermostat="nve", pre_nvt=False, seed=4)
    field = np.zeros((len(molecule_ids), 3))

    on = MDBatch(num=len(molecule_ids), driver_kwargs=kwargs, xp=np)
    on.initialize(_DT_AU, molecule_ids)
    off = MDBatch(
        num=len(molecule_ids), driver_kwargs=dict(reset_dipole=False, **kwargs), xp=np
    )
    off.initialize(_DT_AU, molecule_ids)

    # the baseline is the permanent dipole of the starting geometry, and it is
    # large enough here that leaving it in would swamp the dipole fluctuations
    baseline = np.asarray(on.mu_initial)
    assert np.linalg.norm(baseline, axis=1).min() > 0.1
    assert np.max(np.abs(off.mu_initial)) == 0.0

    for _ in range(5):
        shifted, absolute = on.step(field), off.step(field)
        assert (
            np.max(np.abs(absolute.dipole_half_au - shifted.dipole_half_au - baseline))
            < 1e-12
        )
        assert (
            np.max(
                np.abs(absolute.dipole_force_au - shifted.dipole_force_au - baseline)
            )
            < 1e-12
        )
        assert np.max(np.abs(absolute.amplitude_au - shifted.amplitude_au)) == 0.0

    # the scalar MDModel applies the same shift, so the two paths still agree
    scalar = MDModel(**kwargs)
    scalar.initialize(_DT_AU, molecule_ids[0])
    assert np.max(np.abs(scalar.mu_initial - baseline[0])) < 1e-11
    on.close()
    off.close()


if __name__ == "__main__":
    test_gpu_forces_match_lammps_reference()
    test_gpu_forces_match_ipi_reference()
    test_gpu_nve_trajectory_matches_ipi_reference()
    for thermostat, pre_nvt in (("nve", False), ("nvt", False), ("nvt", True)):
        test_batch_reproduces_the_scalar_mdmodel(thermostat, pre_nvt)
    for backend in ("numpy", "cupy"):
        test_noise_streams_are_keyed_per_molecule(backend)
    test_nvt_reaches_the_target_temperature()
    test_reset_dipole_subtracts_the_dipole_at_time_zero()
    print("GPU-batched MD force + NVE + NVT tests match the references")
