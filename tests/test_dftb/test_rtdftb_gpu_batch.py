# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Tests for the GPU-batched RT-TDDFTB model.
"""

import inspect
import re

import numpy as np
import pytest

from slko_helpers import sk_path

batch_mod = pytest.importorskip(
    "maxwelllink.mxl_drivers.python.batch",
    reason="maxwelllink is required for this test",
)
dftb_mod = pytest.importorskip(
    "maxwelllink.mxl_drivers.python.models.rtdftb_model",
    reason="maxwelllink is required for this test",
)
RTDFTBModel = dftb_mod.RTDFTBModel

_DT = 0.2  # atomic units
_NUM = 6
_N_STEPS = 30

_VELOCITIES = np.array(
    [[1.0e-4, 0.0, 2.0e-4], [0.0, -3.0e-4, 1.0e-4], [2.0e-4, 1.0e-4, 0.0]]
)

_ELEMENTS = ["O", "H", "H"]
_POSITIONS = np.array(
    [[0.0, 0.0, 0.1173], [0.0, 0.7572, -0.4692], [0.0, -0.7572, -0.4692]]
)


def _kwargs(**overrides):
    """Scalar-driver keyword arguments shared by the batch and the reference."""

    kwargs = dict(
        sk_path=sk_path(skip=pytest.skip),
        elements=_ELEMENTS,
        positions=_POSITIONS,
        # the CUDA backend defaults gpu_init on; the reference suite pins the CPU
        # initialization so batch-vs-scalar stays bitwise-comparable
        gpu_init=False,
    )
    kwargs.update(overrides)
    return kwargs


def _array_module(name):
    """Import the requested array module, or skip when it is unavailable."""

    if name == "numpy":
        return np
    cupy = pytest.importorskip("cupy", reason="CuPy is required for the GPU path.")
    try:
        cupy.zeros(1)
    except Exception as exc:  # a CuPy build without a working CUDA runtime
        pytest.skip(f"CuPy cannot reach a CUDA device: {exc}")
    return cupy


def _fields(n_steps=_N_STEPS, num=_NUM):
    """A different field for every system and step, so no two can be confused."""

    return np.random.default_rng(0).normal(scale=2.0e-3, size=(n_steps, num, 3))


def _scalar_reference(fields, **overrides):
    """Drive one independent scalar driver per batch member."""

    n_steps, num = fields.shape[0], fields.shape[1]
    drivers = []
    for molecule_id in range(num):
        model = RTDFTBModel(**_kwargs(**overrides))
        model.initialize(_DT, molecule_id)
        drivers.append(model)

    dipole = np.zeros((n_steps, num, 3))
    amplitude = np.zeros_like(dipole)
    energy = np.zeros((n_steps, num))
    for step in range(n_steps):
        for d, model in enumerate(drivers):
            model.propagate(fields[step, d])
            data = model.append_additional_data()
            dipole[step, d] = [data["mux_au"], data["muy_au"], data["muz_au"]]
            amplitude[step, d] = model.calc_amp_vector()
            energy[step, d] = data["energy_au"]
    return dipole, amplitude, energy


def _run_batch(xp, fields, blocks_per_system=None, **overrides):
    """Drive the batch model over the same fields and collect the same quantities."""

    n_steps, num = fields.shape[0], fields.shape[1]
    model = batch_mod.get_batch_model("gpu", "rtdftb")(
        num=num,
        driver_kwargs=_kwargs(**overrides),
        xp=xp,
        blocks_per_system=blocks_per_system,
    )
    model.initialize(_DT, list(range(num)))
    dipole = np.zeros((n_steps, num, 3))
    amplitude = np.zeros_like(dipole)
    energy = np.zeros((n_steps, num))
    for step in range(n_steps):
        out = model.step(fields[step])
        dipole[step] = out.dipole_half_au
        amplitude[step] = out.amplitude_au
        energy[step] = out.energy_au
        # this driver reports the same dipole twice, as DFTB+ does
        assert np.array_equal(out.dipole_half_au, out.dipole_force_au)
    return dipole, amplitude, energy, model


@pytest.mark.core
@pytest.mark.parametrize("backend", ["numpy", "cupy"])
def test_batch_reproduces_independent_scalar_drivers(backend):
    """Every batch member follows its own scalar driver, step for step."""

    xp = _array_module(backend)
    fields = _fields()
    reference = _scalar_reference(fields)
    dipole, amplitude, energy, model = _run_batch(xp, fields)
    model.close()

    assert np.abs(dipole - reference[0]).max() < 1e-13
    assert np.abs(amplitude - reference[1]).max() < 1e-12
    assert np.abs(energy - reference[2]).max() < 1e-12
    # and the trajectory is a real signal, not a constant the tolerances would pass
    assert np.abs(dipole - dipole[0]).max() > 1e-6


@pytest.mark.core
def test_the_two_backends_agree():
    """The scalar drivers (the CPU backend) and the CUDA kernels are the same physics."""

    cupy = _array_module("cupy")
    fields = _fields()
    on_cpu = _run_batch(np, fields)
    on_gpu = _run_batch(cupy, fields)
    on_cpu[3].close()
    on_gpu[3].close()

    assert np.abs(on_cpu[0] - on_gpu[0]).max() < 1e-13
    assert np.abs(on_cpu[1] - on_gpu[1]).max() < 1e-12
    assert np.abs(on_cpu[2] - on_gpu[2]).max() < 1e-12


@pytest.mark.core
@pytest.mark.parametrize("backend", ["numpy", "cupy"])
def test_sub_stepping_follows_the_scalar_drivers(backend):
    """``dt_rtdftb_au`` sub-steps the batch exactly as it sub-steps the scalar driver.

    Two electronic steps per EM step: the reported dipole and energy are the midpoint
    averages over the whole EM step and the amplitude its finite difference, as the
    scalar driver builds them, so the batch must follow it to round-off.
    """

    xp = _array_module(backend)
    fields = _fields(n_steps=12)
    overrides = dict(dt_rtdftb_au=0.5 * _DT, ehrenfest=True, velocities=_VELOCITIES)
    reference = _scalar_reference(fields, **overrides)
    dipole, amplitude, energy, model = _run_batch(xp, fields, **overrides)
    assert model.n_substeps == 2
    model.close()

    assert np.abs(dipole - reference[0]).max() < 1e-13
    assert np.abs(amplitude - reference[1]).max() < 1e-12
    assert np.abs(energy - reference[2]).max() < 1e-12
    # and sub-stepping changed the trajectory against the single-step batch
    single = _run_batch(xp, fields, ehrenfest=True, velocities=_VELOCITIES)
    single[3].close()
    assert np.abs(dipole - single[0]).max() > 1e-8


@pytest.mark.core
@pytest.mark.parametrize("backend", ["numpy", "cupy"])
def test_systems_do_not_leak_into_each_other(backend):
    """Driving one member must leave the rest exactly where they were.

    A batch kernel that indexed a shared buffer instead of a per-system one would still
    reproduce the scalar driver when every member sees the same field, so the members
    are driven differently here: one field on, the rest at zero. The undriven members are
    compared against an all-quiet batch rather than against their own first value,
    because an unperturbed ground state still drifts by ~1e-12 through the leapfrog's
    round-off, which would otherwise set the sensitivity floor of this test.
    """

    xp = _array_module(backend)
    mixed = np.zeros((_N_STEPS, _NUM, 3))
    mixed[:, 0, 2] = 5.0e-3  # only system 0 is driven
    quiet = np.zeros((_N_STEPS, _NUM, 3))

    driven_run, _, _, model = _run_batch(xp, mixed)
    model.close()
    quiet_run, _, _, model = _run_batch(xp, quiet)
    model.close()

    response = np.abs(driven_run[:, 0] - quiet_run[:, 0]).max()
    leak = np.abs(driven_run[:, 1:] - quiet_run[:, 1:]).max()
    assert response > 1e-5, "the driven system did not respond"
    assert leak == 0.0, f"an undriven system saw its neighbour, by {leak:.3e}"


def _ehrenfest_reference(fields):
    """Scalar Ehrenfest drivers, plus the trajectories the batch must reproduce."""

    n_steps, num = fields.shape[0], fields.shape[1]
    kwargs = _kwargs(ehrenfest=True, velocities=_VELOCITIES)
    drivers = []
    for molecule_id in range(num):
        model = RTDFTBModel(**kwargs)
        model.initialize(_DT, molecule_id)
        drivers.append(model)

    dipole = np.zeros((n_steps, num, 3))
    kinetic = np.zeros((n_steps, num))
    for step in range(n_steps):
        for d, model in enumerate(drivers):
            model.propagate(fields[step, d])
            data = model.append_additional_data()
            dipole[step, d] = [data["mux_au"], data["muy_au"], data["muz_au"]]
            kinetic[step, d] = data["energy_kin_au"]
    coords = np.array([m.system.coords for m in drivers])
    velocity = np.array([m.dynamics.velocity for m in drivers])
    return dipole, kinetic, coords, velocity


@pytest.mark.core
@pytest.mark.parametrize("backend", ["numpy", "cupy"])
def test_ehrenfest_batch_reproduces_scalar_ehrenfest_drivers(backend):
    """With the nuclei moving, every member still follows its own scalar driver.

    The geometry and the velocities are compared as well as the dipole: an Ehrenfest
    batch that got the force right but the integrator wrong would still report a
    plausible dipole for a while.
    """

    xp = _array_module(backend)
    fields = _fields()
    dipole, kinetic, coords, velocity = _ehrenfest_reference(fields)

    mine, _, _, model = _run_batch(xp, fields, ehrenfest=True, velocities=_VELOCITIES)
    mine_kinetic = np.array(
        [row["energy_kin_au"] for row in model.append_additional_data()]
    )

    assert np.abs(mine - dipole).max() < 1e-13
    assert np.abs(model.coordinates() - coords).max() < 1e-13
    assert np.abs(model.velocities() - velocity).max() < 1e-13
    assert np.abs(mine_kinetic - kinetic[-1]).max() < 1e-15
    # the nuclei must have actually moved, or none of the above means anything
    start = np.asarray(_POSITIONS) * 1.8897261254535  # Angstrom -> Bohr, roughly
    assert np.abs(model.coordinates()[0] - start).max() > 1e-5
    assert model.kinetic_energies().min() > 0.0
    model.close()


@pytest.mark.core
@pytest.mark.parametrize("backend", ["numpy", "cupy"])
def test_ehrenfest_systems_do_not_share_a_geometry(backend):
    """Each moving system owns its own coordinates, matrices and force scratch.

    With the nuclei frozen the geometry-dependent matrices are deliberately shared, so
    this is the case where a shared buffer would actually be wrong; driving one system
    differently must move only that one.
    """

    xp = _array_module(backend)
    fields = np.zeros((_N_STEPS, _NUM, 3))
    fields[:, 0, 2] = 2.0e-2  # only system 0 is driven, and hard

    _, _, _, model = _run_batch(xp, fields, ehrenfest=True, velocities=_VELOCITIES)
    coords = model.coordinates()
    model.close()

    spread = np.abs(coords[1:] - coords[1]).max()
    assert spread == 0.0, "undriven systems drifted apart from each other"
    assert np.abs(coords[0] - coords[1]).max() > 1e-9, "the driven system did not move"


@pytest.mark.core
@pytest.mark.parametrize("ehrenfest", [False, True])
def test_a_batch_of_one_runs_on_the_cpu_backend(ehrenfest):
    """A batch of one system runs on the CPU backend like any other batch.

    The CPU backend is the scalar drivers themselves, one per system; a batch of one
    must step, report and close exactly as a larger one does.
    """
    fields = _fields(n_steps=3, num=1)
    dipole, amplitude, energy, model = _run_batch(np, fields, ehrenfest=ehrenfest)
    model.close()
    reference = _scalar_reference(fields, ehrenfest=ehrenfest)
    assert np.abs(dipole - reference[0]).max() < 1e-13
    assert np.abs(energy - reference[2]).max() < 1e-12


def _write_frames(path, num):
    """A multi-frame XYZ of ``num`` waters, each stretched and bent a little differently."""

    with open(path, "w") as handle:
        for k in range(num):
            positions = _POSITIONS * (1.0 + 0.02 * k)
            positions[1, 1] += 0.01 * k  # and a slightly different bend angle
            handle.write(f"3\nframe {k}\n")
            for symbol, (x, y, z) in zip(_ELEMENTS, positions):
                handle.write(f"{symbol} {x:.8f} {y:.8f} {z:.8f}\n")
    return str(path)


@pytest.mark.core
@pytest.mark.parametrize("ehrenfest", [False, True])
@pytest.mark.parametrize("backend", ["numpy", "cupy"])
def test_batch_xyz_gives_every_system_its_own_geometry(tmp_path, backend, ehrenfest):
    """With ``batch_xyz`` molecule ``m`` starts from frame ``m``: the batch member is the
    scalar driver of that molecule ID, and the members start from different geometries.
    """
    xp = _array_module(backend)
    frames = _write_frames(tmp_path / "frames.xyz", _NUM)
    fields = _fields(n_steps=10)
    reference = _scalar_reference(fields, batch_xyz=frames, ehrenfest=ehrenfest)
    dipole, amplitude, energy, model = _run_batch(
        xp, fields, batch_xyz=frames, ehrenfest=ehrenfest
    )
    assert np.abs(dipole - reference[0]).max() < 1e-13
    assert np.abs(amplitude - reference[1]).max() < 1e-12
    assert np.abs(energy - reference[2]).max() < 1e-12
    coords = model.coordinates()
    assert np.abs(coords[1] - coords[0]).max() > 1e-3, "rows share a geometry"
    model.close()


@pytest.mark.core
@pytest.mark.parametrize("backend", ["numpy", "cupy"])
def test_pre_nvt_thermalizes_every_system_on_its_own(backend):
    """A short Born-Oppenheimer pre-equilibration leaves every system at its own
    geometry with its own velocities, and each is exactly the scalar driver's."""
    xp = _array_module(backend)
    fields = _fields(n_steps=10)
    settings = dict(ehrenfest=True, pre_nvt=True, pre_nvt_duration_ps=0.005)
    reference = _scalar_reference(fields, **settings)
    dipole, amplitude, energy, model = _run_batch(xp, fields, **settings)
    assert np.abs(dipole - reference[0]).max() < 1e-13
    assert np.abs(energy - reference[2]).max() < 1e-12
    coords, velocity = model.coordinates(), model.velocities()
    assert np.abs(coords[1] - coords[0]).max() > 1e-4, "pre-NVT left the rows alike"
    assert (
        np.abs(velocity).max() > 0.0 and np.abs(velocity[1] - velocity[0]).max() > 1e-6
    )
    model.close()


@pytest.mark.core
@pytest.mark.parametrize("backend", ["numpy", "cupy"])
def test_sampled_velocities_are_drawn_per_system(backend):
    """``init_velocities`` gives every molecule ID its own draw, as the scalar driver."""
    xp = _array_module(backend)
    fields = _fields(n_steps=5)
    settings = dict(ehrenfest=True, init_velocities=True, temperature_K=300.0)
    reference = _scalar_reference(fields, **settings)
    dipole, amplitude, energy, model = _run_batch(xp, fields, **settings)
    assert np.abs(dipole - reference[0]).max() < 1e-13
    velocity = model.velocities()
    assert np.abs(velocity[1] - velocity[0]).max() > 1e-6, "one draw was copied around"
    model.close()


@pytest.mark.core
@pytest.mark.parametrize("backend", ["numpy", "cupy"])
def test_one_geometry_starts_every_system_alike(backend):
    """Without ``batch_xyz`` or ``pre_nvt`` every system starts from the given geometry."""
    xp = _array_module(backend)
    dipole, amplitude, energy, model = _run_batch(
        xp, _fields(n_steps=1), ehrenfest=True
    )
    coords = model.coordinates()
    assert np.abs(coords - coords[0]).max() == 0.0
    model.close()


@pytest.mark.core
@pytest.mark.parametrize("ehrenfest", [False, True])
@pytest.mark.parametrize("backend", ["numpy", "cupy"])
def test_property_and_trajectory_files_match_the_scalar_drivers(
    tmp_path, backend, ehrenfest
):
    """The batch writes one property file and one XYZ trajectory for all its molecules,
    holding what the scalar drivers of those molecule IDs write for themselves."""
    import h5py

    from maxwelllink.tools import read_xyz_trajectory

    xp = _array_module(backend)
    n_steps, every = 6, 3
    fields = _fields(n_steps=n_steps)
    settings = dict(
        ehrenfest=ehrenfest,
        property_filename=str(tmp_path / "prop.h5"),
        traj_filename=str(tmp_path / "traj.xyz"),
        record_every_steps=every,
    )
    _, _, _, model = _run_batch(xp, fields, **settings)
    coords = model.coordinates()
    model.close()
    with h5py.File(tmp_path / "prop_id_0.h5", "r") as handle:
        batch_props = {key: handle[key][...] for key in handle}
    assert batch_props["molecule_ids"].tolist() == list(range(_NUM))
    assert batch_props["energy_au"].shape == (n_steps // every, _NUM)
    _, frames = read_xyz_trajectory(str(tmp_path / "traj_id_0.xyz"), _NUM)
    assert frames.shape == (n_steps // every, _NUM, len(_ELEMENTS), 3)
    np.testing.assert_allclose(frames[-1] / dftb_mod.BOHR_TO_AA, coords, atol=1e-8)

    # the scalar drivers of molecules 0 and 4, each writing its own files
    for molecule_id in (0, 4):
        model = RTDFTBModel(**_kwargs(**settings))
        model.initialize(_DT, molecule_id)
        for step in range(n_steps):
            model.propagate(fields[step, molecule_id])
        model.close()
        with h5py.File(tmp_path / f"prop_id_{molecule_id}.h5", "r") as handle:
            for name in ("temperature_K", "energy_au", "energy_kin_au", "muz_au"):
                assert (
                    np.abs(handle[name][:, 0] - batch_props[name][:, molecule_id]).max()
                    < 1e-12
                ), name
        _, own = read_xyz_trajectory(str(tmp_path / f"traj_id_{molecule_id}.xyz"), 1)
        np.testing.assert_allclose(own[:, 0], frames[:, molecule_id], atol=1e-8)
        if ehrenfest:  # the nuclei moved, and the frames say so
            assert np.abs(own[-1, 0] - own[0, 0]).max() > 1e-7


@pytest.mark.core
def test_reset_dipole_matches_the_scalar_driver():
    """The baseline is captured once and subtracted, exactly as the scalar does."""

    fields = _fields()
    for reset in (True, False):
        reference = _scalar_reference(fields, reset_dipole=reset)
        dipole, _, _, model = _run_batch(np, fields, reset_dipole=reset)
        model.close()
        assert np.abs(dipole - reference[0]).max() < 1e-13

    on = _run_batch(np, fields, reset_dipole=True)
    off = _run_batch(np, fields, reset_dipole=False)
    on[3].close()
    off[3].close()
    baseline = off[0][0] - on[0][0]
    assert np.abs(baseline).max() > 1e-3, "water has a permanent dipole to subtract"
    assert np.abs((off[0] - on[0]) - baseline).max() < 1e-14
    assert np.abs(off[1] - on[1]).max() < 1e-14  # dmu/dt is unaffected


@pytest.mark.core
def test_columnar_block_matches_the_dictionary_reply():
    """The fast columnar path and the per-system dictionaries carry the same numbers."""

    keys = (
        "mux_au",
        "muy_au",
        "muz_au",
        "mux_m_au",
        "muy_m_au",
        "muz_m_au",
        "energy_au",
    )
    fields = _fields(n_steps=3)
    _, _, _, model = _run_batch(np, fields)

    block = model.additional_data_columns(keys)
    rows = model.append_additional_data()
    assert block.shape == (_NUM, len(keys))
    assert block.dtype == np.float64
    assert block.flags["C_CONTIGUOUS"]
    for i, row in enumerate(rows):
        for k, key in enumerate(keys):
            assert block[i, k] == row[key]
    # the requested order is honoured, and an unknown key is an error
    reversed_block = model.additional_data_columns(tuple(reversed(keys)))
    assert np.array_equal(reversed_block, block[:, ::-1])
    with pytest.raises(KeyError):
        model.additional_data_columns(("not_a_key",))
    model.close()


@pytest.mark.core
def test_guards_and_registration():
    """The registry, the argument checks and the before-initialize guards."""

    assert "rtdftb" in batch_mod.supported_batch_drivers("gpu")
    Model = batch_mod.get_batch_model("gpu", "rtdftb")
    assert Model.__name__ == "RTDFTBGPUBatchModel"
    # the registry must not fall through to another driver
    assert batch_mod.get_batch_model("gpu", "md") is not Model
    assert batch_mod.get_batch_model("gpu", "sho") is not Model

    with pytest.raises(ValueError, match="positive"):
        Model(num=0, driver_kwargs=_kwargs(), xp=np)
    with pytest.raises(ValueError, match="leapfrog"):
        Model(num=2, driver_kwargs=_kwargs(propagator="cayley-midpoint"), xp=np)
    with pytest.raises(ValueError, match="verbose"):
        Model(num=2, driver_kwargs=_kwargs(verbose=True), xp=np)

    model = Model(num=2, driver_kwargs=_kwargs(), xp=np)
    with pytest.raises(RuntimeError, match="before initialize"):
        model.step(np.zeros((2, 3)))
    with pytest.raises(ValueError, match="molecule IDs"):
        model.initialize(_DT, [0, 1, 2])

    model.initialize(_DT, [0, 1])
    with pytest.raises(RuntimeError, match="before the first step"):
        model.append_additional_data()
    with pytest.raises(ValueError, match="shape"):
        model.step(np.zeros((3, 3)))
    model.close()
    model.close()  # close is called on every teardown path, so it must be idempotent


@pytest.mark.core
def test_every_kernel_avoids_cuda_hostile_constructs():
    """Guard the constructs that compile under njit and fail under cuda.jit.

    The two-argument ``min``/``max`` builtins are the dangerous case: numba accepts them
    on the CPU and rejects them on the GPU, so a regression would pass every other test
    in this directory and only surface at the first CUDA launch. It has already happened
    once, in ``sk_interpolate``.
    """

    kernels = dftb_mod.jit.KERNELS
    assert len(kernels) > 50, "the kernel registry looks empty"

    forbidden = {
        "two-argument min()/max()": r"\b(?:min|max)\(\s*[^)\n]*,",
        "allocation inside a kernel": r"\bnp\.(?:zeros|empty|array|ones)\s*\(",
        "linear algebra call": r"\bnp\.linalg\.",
        "module-attribute kernel call": (
            r"\b(?:jit|skfiles|dftb_params|h0_overlap|sk_deriv|scc|forces|rt"
            r"|dynamics|kernels_gpu)\.\w+\("
        ),
    }
    offenders = []
    for name, body in sorted(kernels.items()):
        source = inspect.getsource(body)
        for label, pattern in forbidden.items():
            if re.search(pattern, source):
                offenders.append(f"{name}: {label}")
    assert not offenders, "\n".join(offenders)


@pytest.mark.core
def test_every_kernel_builds_for_cuda():
    """Every registered body must also compile as a CUDA device function."""

    _array_module("cupy")  # skips when there is no device
    device = dftb_mod.jit.device_kernels()
    assert set(device) == set(dftb_mod.jit.KERNELS)


@pytest.mark.core
@pytest.mark.parametrize("ehrenfest", [False, True])
def test_hybrid_precision_tracks_fp64_and_is_ignored_on_cpu(ehrenfest):
    """``hybrid_precision=True``: FP32 dense algebra on the CUDA backend.

    On the GPU the hybrid run must stay within FP32-round-off reach of the FP64 run
    over 30 steps while being bit-identical in shape and reporting; on the numpy
    backend the flag is ignored, so the results are bit-identical to FP64.
    """

    fields = _fields()
    overrides = dict(ehrenfest=ehrenfest)
    if ehrenfest:
        overrides["velocities"] = _VELOCITIES

    on_cpu = _run_batch(np, fields, **overrides)
    on_cpu_hybrid = _run_batch(np, fields, hybrid_precision=True, **overrides)
    on_cpu[3].close(), on_cpu_hybrid[3].close()
    for reference, hybrid in zip(on_cpu[:3], on_cpu_hybrid[:3]):
        assert np.array_equal(reference, hybrid)  # FP64 only on the CPU backend

    cupy = _array_module("cupy")
    on_gpu = _run_batch(cupy, fields, **overrides)
    on_gpu_hybrid = _run_batch(cupy, fields, hybrid_precision=True, **overrides)
    assert on_gpu_hybrid[3]._hybrid and not on_gpu[3]._hybrid
    on_gpu[3].close(), on_gpu_hybrid[3].close()
    # single-precision round-off enters only the per-step increment, so 30 steps stay
    # within ~n_steps * 1e-7 of the FP64 trajectory; a wrong product would be O(1)
    assert np.abs(on_gpu_hybrid[0] - on_gpu[0]).max() < 5e-6  # dipole
    assert np.abs(on_gpu_hybrid[2] - on_gpu[2]).max() < 5e-6  # energy
    assert np.abs(on_gpu_hybrid[0] - on_gpu[0]).max() > 0.0  # FP32 really ran


@pytest.mark.core
@pytest.mark.parametrize("ehrenfest", [False, True])
def test_wide_launch_matches_the_narrow_kernels(ehrenfest):
    """``blocks_per_system > 1`` spreads one system over many blocks, same physics.

    The auto rule keeps these tiny systems on the narrow block-per-system path, so the
    wide phase kernels are forced explicitly; only reduction order may differ, so the
    agreement bound is round-off, not exactness.
    """

    cupy = _array_module("cupy")
    fields = _fields()
    overrides = dict(ehrenfest=ehrenfest)
    if ehrenfest:
        overrides["velocities"] = _VELOCITIES

    narrow = _run_batch(cupy, fields, **overrides)
    wide = _run_batch(cupy, fields, blocks_per_system=4, **overrides)
    assert narrow[3]._wide_bps == 1 and wide[3]._wide_bps == 4
    if ehrenfest:
        coords_gap = np.abs(wide[3].coordinates() - narrow[3].coordinates()).max()
        assert coords_gap < 1e-12
    narrow[3].close(), wide[3].close()

    assert np.abs(wide[0] - narrow[0]).max() < 1e-12  # dipole
    assert (
        np.abs(wide[1] - narrow[1]).max() < 1e-11
    )  # amplitude (dipole difference / dt)
    assert np.abs(wide[2] - narrow[2]).max() < 1e-11  # energy


@pytest.mark.core
@pytest.mark.parametrize("ehrenfest", [False, True])
def test_full_gpu_path_reproduces_the_cpu_path_after_a_kick(ehrenfest):
    """End to end: GPU ground state + GPU bootstrap + GPU dynamics == pure CPU.

    With ``gpu_init=True`` no dense matrix is ever built or multiplied on the CPU --
    the SCC ground state, the strong delta kick and the Euler bootstrap all run on
    the device -- yet the trajectory must match independent scalar CPU drivers (CPU
    SCF + CPU real-time propagation) to round-off: the SCC fixed point is backend
    independent, so only summation-order noise may remain.
    """

    cupy = _array_module("cupy")
    fields = _fields()
    overrides = dict(ehrenfest=ehrenfest, delta_kick_au=0.2, kick_direction="z")
    if ehrenfest:
        overrides["velocities"] = _VELOCITIES

    reference = _scalar_reference(fields, **overrides)  # pure CPU, scalar drivers
    mine = _run_batch(cupy, fields, gpu_init=True, **overrides)

    # the kick must actually have rung the system, or the comparison is empty
    assert np.abs(reference[0]).max() > 1e-2
    assert np.abs(mine[0] - reference[0]).max() < 5e-12  # dipole
    assert np.abs(mine[1] - reference[1]).max() < 5e-11  # amplitude (finite diff / dt)
    assert np.abs(mine[2] - reference[2]).max() < 1e-11  # energy
    if ehrenfest:
        # the nuclei moved, and moved identically
        start = np.asarray(_POSITIONS) * 1.8897261254535
        assert np.abs(mine[3].coordinates()[0] - start).max() > 1e-6
    mine[3].close()

    # on the numpy backend the flag is ignored and the CPU initialization runs
    plain = _run_batch(np, fields, **overrides)
    ignored = _run_batch(np, fields, gpu_init=True, **overrides)
    plain[3].close(), ignored[3].close()
    for a, b in zip(plain[:3], ignored[:3]):
        assert np.array_equal(a, b)


@pytest.mark.core
def test_gpu_init_defaults_on_for_cuda_and_off_for_numpy():
    """With ``gpu_init`` unset the CUDA backend initializes on the device."""

    cupy = _array_module("cupy")
    fields = _fields(n_steps=2)
    auto = _run_batch(cupy, fields, gpu_init=None)
    assert auto[3]._gpu_initialized  # the GPU initialization ran
    auto[3].close()
    plain = _run_batch(np, fields, gpu_init=None)
    assert not plain[3]._gpu_initialized  # the numpy backend never uses it
    plain[3].close()


@pytest.mark.core
def test_pre_nvt_runs_on_the_gpu_and_matches_the_cpu_thermalization():
    """GPU pre-NVT: the same OBABO draws, charges and forces as the CPU BOMD.

    The random streams are identical by construction, so over a short
    thermalization the two initializations may differ only by the SCF/force
    round-off between the eigensolver backends; every system must also end up
    somewhere of its own.
    """

    cupy = _array_module("cupy")
    fields = _fields(n_steps=3)
    overrides = dict(ehrenfest=True, pre_nvt=True, pre_nvt_duration_ps=0.005, seed=7)

    on_cpu = _run_batch(cupy, fields, gpu_init=False, **overrides)
    on_gpu = _run_batch(cupy, fields, gpu_init=True, **overrides)

    coords_cpu, coords_gpu = on_cpu[3].coordinates(), on_gpu[3].coordinates()
    velocity_cpu, velocity_gpu = on_cpu[3].velocities(), on_gpu[3].velocities()
    on_cpu[3].close(), on_gpu[3].close()

    assert np.abs(coords_gpu - coords_cpu).max() < 1e-6
    assert np.abs(velocity_gpu - velocity_cpu).max() < 1e-6
    assert np.abs(on_gpu[0] - on_cpu[0]).max() < 1e-6  # dipole after 3 steps
    # the thermalization really happened, and differently per system
    start = np.asarray(_POSITIONS) * 1.8897261254535
    assert np.abs(coords_gpu[0] - start).max() > 1e-4
    assert np.abs(coords_gpu[0] - coords_gpu[1]).max() > 1e-4


@pytest.mark.core
def test_two_phase_scf_converges_the_same_ground_state():
    """``hybrid_precision`` + ``gpu_init``: FP32 SCF bulk, FP64 tail, same physics.

    Both runs must reach the same converged charges within ``scc_tolerance``, so
    after a strong kick the trajectories may differ only at the tolerance level
    (plus the hybrid stepping's own FP32 round-off); the FP32 phase must actually
    have run, and the FP64 tail must have finished the job.
    """

    cupy = _array_module("cupy")
    fields = _fields(n_steps=3)
    base = dict(
        ehrenfest=True, delta_kick_au=0.2, velocities=_VELOCITIES, gpu_init=True
    )

    fp64 = _run_batch(cupy, fields, **base)
    hybrid = _run_batch(cupy, fields, hybrid_precision=True, **base)
    n_fp32, n_fp64 = hybrid[3]._scf_iterations
    assert n_fp32 > 0 and n_fp64 > 0
    fp64[3].close(), hybrid[3].close()

    assert np.abs(hybrid[0] - fp64[0]).max() < 1e-6  # dipole, tolerance-limited
    assert np.abs(hybrid[2] - fp64[2]).max() < 1e-6  # energy


if __name__ == "__main__":
    test_batch_reproduces_independent_scalar_drivers("numpy")
    test_systems_do_not_leak_into_each_other("numpy")
    test_every_kernel_avoids_cuda_hostile_constructs()
    print("the batched RT-TDDFTB driver reproduces the scalar one")
