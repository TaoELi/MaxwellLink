# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""Correctness tests for the GPU-batched SHO model and its aggregate bridge.

Everything runs on the CPU by injecting ``xp=numpy``; the identical code path
uses CuPy on a CUDA device in production.  The device-only checks (no per-step
allocation, fused kernel) live in a separate ``importorskip("cupy")`` suite.
"""

from __future__ import annotations

import json

import numpy as np
import pytest

pytest.importorskip("maxwelllink")

from maxwelllink.mxl_drivers.python.batch import (  # noqa: E402
    DummyBatchModel,
    get_batch_model,
    supported_batch_drivers,
)
from maxwelllink.mxl_drivers.python.batch.md_gpu import MDGPUBatchModel  # noqa: E402
from maxwelllink.mxl_drivers.python.batch.sho_gpu import (  # noqa: E402
    SHOGPUBatchModel,
)
from maxwelllink.mxl_drivers.python.models.sho_model import SHOModel  # noqa: E402
from maxwelllink.sockets.aggregated import (  # noqa: E402
    CPUBatchBridge,
    GPUBatchBridge,
)

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _scalar_reference(driver_kwargs, dt_au, mids, field_seq):
    """Run one scalar SHOModel per id over ``field_seq``; record amp + extra."""

    models = {}
    for mid in mids:
        model = SHOModel(**driver_kwargs)
        model.initialize(dt_au, mid)
        models[mid] = model

    records = []  # per step: {mid: (amp(3,), extra_dict)}
    for fields in field_seq:
        step = {}
        for mid in mids:
            model = models[mid]
            model.stage_step(np.asarray(fields[mid], dtype=float))
            amp = np.asarray(model.commit_step(), dtype=float)
            step[mid] = (amp, model.append_additional_data())
        records.append(step)
    return records


def _field_sequence(mids, orientation, n_steps):
    """Distinct per-oscillator fields, mixing zero and nonzero steps.

    Off-axis components are deliberately nonzero to confirm both the scalar and
    batch models use only the driving (orientation) component.
    """

    field_seq = []
    for s in range(n_steps):
        fields = {}
        for j, mid in enumerate(mids):
            vec = np.zeros(3)
            if s % 7 != 0:
                vec[orientation] = 1e-3 * (j + 1) * np.sin(0.05 * s)
                vec[(orientation + 1) % 3] = 1e-2  # ignored by both models
            fields[mid] = vec
        field_seq.append(fields)
    return field_seq


# --------------------------------------------------------------------------- #
# Model parity vs the scalar SHO driver
# --------------------------------------------------------------------------- #


@pytest.mark.core
@pytest.mark.parametrize("orientation", [0, 1, 2])
@pytest.mark.parametrize("q0,p0", [(0.0, 0.0), (0.3, -0.15)])
@pytest.mark.parametrize("n_osc", [1, 5])
def test_gpu_sho_matches_scalar(orientation, q0, p0, n_osc):
    driver_kwargs = {
        "omega": 0.21,
        "mu0": 187.0,
        "orientation": orientation,
        "q_initial": q0,
        "p_initial": p0,
    }
    dt_au = 0.1
    mids = list(range(n_osc))
    field_seq = _field_sequence(mids, orientation, n_steps=500)

    reference = _scalar_reference(driver_kwargs, dt_au, mids, field_seq)

    model = SHOGPUBatchModel(num=len(mids), driver_kwargs=driver_kwargs, xp=np)
    model.initialize(dt_au, mids)

    for s, fields in enumerate(field_seq):
        block = np.stack([fields[mid] for mid in mids])
        result = model.step(block)
        extras = model.append_additional_data()
        for i, mid in enumerate(mids):
            amp_ref, extra_ref = reference[s][mid]
            # Source amplitude dmu/dt.
            np.testing.assert_allclose(
                result.amplitude_au[i], amp_ref, rtol=0.0, atol=1e-12
            )
            # Columnar dipoles/energy agree with the reconstructed JSON.
            np.testing.assert_allclose(
                result.dipole_half_au[i],
                [extra_ref["mux_au"], extra_ref["muy_au"], extra_ref["muz_au"]],
                rtol=0.0,
                atol=1e-12,
            )
            np.testing.assert_allclose(
                result.dipole_force_au[i],
                [extra_ref["mux_m_au"], extra_ref["muy_m_au"], extra_ref["muz_m_au"]],
                rtol=0.0,
                atol=1e-12,
            )
            np.testing.assert_allclose(
                result.energy_au[i], extra_ref["energy_au"], rtol=0.0, atol=1e-12
            )
            # Full additional-data dict (time/energy/dipoles/p/q) matches.
            assert extras[i] == pytest.approx(extra_ref, rel=0.0, abs=1e-10)


# --------------------------------------------------------------------------- #
# Registry, backend validation, and the MD placeholder
# --------------------------------------------------------------------------- #


@pytest.mark.core
def test_batch_registry_and_md_stub():
    assert get_batch_model("gpu", "sho") is SHOGPUBatchModel
    assert supported_batch_drivers("gpu") == ("sho",)
    assert supported_batch_drivers("cpu") == ()

    with pytest.raises(ValueError, match="No batch model"):
        get_batch_model("gpu", "tls")
    with pytest.raises(ValueError, match="No batch model"):
        get_batch_model("tpu", "sho")

    # MD placeholder: importable and a DummyBatchModel, but not usable yet.
    assert issubclass(MDGPUBatchModel, DummyBatchModel)
    with pytest.raises(NotImplementedError):
        MDGPUBatchModel(num=1, driver_kwargs={}, xp=np)


@pytest.mark.core
def test_positional_param_is_consistent_with_mxl_driver():
    # `mxl_bridge --param "0.2,2.0,1"` must build SHOModel(0.2, 2.0, 1) exactly
    # like `mxl_driver` -- not silently fall back to SHOModel() defaults.
    from maxwelllink.mxl_drivers.python.mxl_driver import _read_args_kwargs

    args, kwargs = _read_args_kwargs("0.2,2.0,1")
    assert args == [0.2, 2.0, 1]
    assert kwargs == {}

    positional = SHOGPUBatchModel(num=2, driver_args=args, driver_kwargs=kwargs, xp=np)
    keyword = SHOGPUBatchModel(
        num=2,
        driver_kwargs={"omega": 0.2, "mu0": 2.0, "orientation": 1},
        xp=np,
    )
    assert (positional.omega, positional.mu0, positional.orientation) == (
        keyword.omega,
        keyword.mu0,
        keyword.orientation,
    )


# --------------------------------------------------------------------------- #
# Bridge milestone-1 repackaging (no sockets): amp + exact scalar JSON
# --------------------------------------------------------------------------- #


@pytest.mark.core
def test_gpu_bridge_run_step_matches_scalar_json():
    driver_kwargs = {"omega": 0.2, "mu0": 187.0, "orientation": 2, "p_initial": 0.01}
    dt_au = 0.1
    mids = [0, 1]

    bridge = GPUBatchBridge(
        group_id="sho-grid",
        upstream_host="127.0.0.1",
        upstream_port=1,  # never dialed; we drive the hooks directly
        driver="sho",
        driver_kwargs=driver_kwargs,
        xp=np,
    )
    payload = {
        "group_id": "sho-grid",
        "init_payloads": {mid: {"molecule_id": mid, "dt_au": dt_au} for mid in mids},
    }
    bridge._handle_group_init(payload)

    fields = {0: np.array([0.0, 0.0, 0.5]), 1: np.array([0.0, 0.0, -0.25])}
    responses = bridge._run_step(fields)
    assert set(responses) == set(mids)

    for mid in mids:
        model = SHOModel(**driver_kwargs)
        model.initialize(dt_au, mid)
        model.stage_step(fields[mid])
        amp_ref = np.asarray(model.commit_step(), dtype=float)
        extra_ref = model.append_additional_data()

        np.testing.assert_allclose(responses[mid]["amp"], amp_ref, rtol=0.0, atol=1e-12)
        decoded = json.loads(responses[mid]["extra"].decode("utf-8"))
        assert decoded == pytest.approx(extra_ref, rel=0.0, abs=1e-10)

    bridge._teardown()


@pytest.mark.core
@pytest.mark.parametrize("backend", ["cpu", "gpu"])
def test_batch_bridge_amps_only_omits_extra_json(backend):
    # When the hub signals result_format="amps_only" (e.g. the Meep
    # susceptibility hub), the batch bridge returns empty ``extra`` -- skipping
    # per-molecule JSON -- while amplitudes stay exact vs the scalar SHO driver.
    driver_kwargs = {"omega": 0.2, "mu0": 187.0, "orientation": 2, "p_initial": 0.01}
    dt_au = 0.1
    mids = [0, 1]
    common = dict(
        group_id="sho-grid",
        upstream_host="127.0.0.1",
        upstream_port=1,
        driver="sho",
        driver_kwargs=driver_kwargs,
    )
    bridge = (
        GPUBatchBridge(xp=np, **common)
        if backend == "gpu"
        else CPUBatchBridge(**common)
    )
    payload = {
        "group_id": "sho-grid",
        "result_format": "amps_only",
        "init_payloads": {mid: {"molecule_id": mid, "dt_au": dt_au} for mid in mids},
    }
    bridge._handle_group_init(payload)

    fields = {0: np.array([0.0, 0.0, 0.5]), 1: np.array([0.0, 0.0, -0.25])}
    responses = bridge._run_step(fields)
    assert set(responses) == set(mids)
    for mid in mids:
        assert responses[mid]["extra"] == b""  # no JSON built
        model = SHOModel(**driver_kwargs)
        model.initialize(dt_au, mid)
        model.stage_step(fields[mid])
        amp_ref = np.asarray(model.commit_step(), dtype=float)
        np.testing.assert_allclose(responses[mid]["amp"], amp_ref, rtol=0.0, atol=1e-12)
    bridge._teardown()


# --------------------------------------------------------------------------- #
# Device path: the numba.cuda kernel matches the CPU reference (GPU-only)
# --------------------------------------------------------------------------- #


@pytest.mark.optional
def test_gpu_sho_kernel_matches_cpu_reference():
    """
    The fused numba.cuda kernel (xp=cupy) reproduces the NumPy reference.
    """

    cp = pytest.importorskip("cupy")
    pytest.importorskip("numba")
    try:
        if cp.cuda.runtime.getDeviceCount() < 1:
            pytest.skip("No CUDA device available for the GPU SHO kernel.")
    except Exception:  # pragma: no cover - CuPy present but no usable driver
        pytest.skip("No usable CUDA runtime for the GPU SHO kernel.")

    driver_kwargs = {
        "omega": 0.21,
        "mu0": 187.0,
        "orientation": 2,
        "q_initial": 0.3,
        "p_initial": -0.15,
    }
    dt_au = 0.1
    n_osc = 1024  # spans many CUDA blocks (128 threads each)
    mids = list(range(n_osc))
    field_seq = _field_sequence(mids, driver_kwargs["orientation"], n_steps=200)

    cpu = SHOGPUBatchModel(num=n_osc, driver_kwargs=driver_kwargs, xp=np)
    gpu = SHOGPUBatchModel(num=n_osc, driver_kwargs=driver_kwargs, xp=cp)
    cpu.initialize(dt_au, mids)
    gpu.initialize(dt_au, mids)

    for fields in field_seq:
        block = np.stack([fields[mid] for mid in mids])
        ref = cpu.step(block)
        got = gpu.step(block)
        for column_ref, column_got in (
            (ref.amplitude_au, got.amplitude_au),
            (ref.dipole_half_au, got.dipole_half_au),
            (ref.dipole_force_au, got.dipole_force_au),
            (ref.energy_au, got.energy_au),
        ):
            np.testing.assert_allclose(column_got, column_ref, rtol=0.0, atol=1e-10)
