# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""Integrated test of the GPU-batched MD driver behind the aggregate socket hub.

This wires the whole production path together -- hub, bridge manifest, ``GPUBatchBridge``,
``MDGPUBatchModel`` -- and checks that a batch of flexible-CO2 systems drives a cavity
mode without the total energy drifting.
"""

from __future__ import annotations

import sys
import threading
from pathlib import Path

import numpy as np
import pytest

SRC_ROOT = Path(__file__).resolve().parents[2] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

import maxwelllink as mxl  # noqa: E402
from maxwelllink.sockets.aggregated import (  # noqa: E402
    AggregatedSocketHub,
    run_bridge_node,
)
from maxwelllink.units import unit  # noqa: E402

from socket_test_helpers import can_create_sockets, pick_free_port  # noqa: E402

pytestmark = pytest.mark.skipif(
    not can_create_sockets(),
    reason="socket creation is not permitted in this environment",
)

N_SYSTEMS = 8
DT_AU = 0.5 * unit("fs")
STEPS = 150
OMEGA_AU = 2320.0 * unit("cm_inv")  # the CO2 asymmetric stretch


def _xp_or_skip(name):
    """Return the array module for ``name`` or skip if the GPU stack is absent."""
    if name == "numpy":
        return np
    cp = pytest.importorskip("cupy", reason="cupy not installed")
    pytest.importorskip("numba", reason="numba not installed")
    try:
        if cp.cuda.runtime.getDeviceCount() < 1:
            pytest.skip("No CUDA device available for the GPU batch model.")
    except Exception:  # pragma: no cover - cupy present but no usable driver
        pytest.skip("No usable CUDA runtime for the GPU batch model.")
    return cp


@pytest.mark.core
@pytest.mark.parametrize("xp_name", ["numpy", "cupy"])
def test_md_batch_drives_a_single_mode_cavity(xp_name, tmp_path):
    """The batched MD driver runs the whole socket path and conserves energy."""
    xp = _xp_or_skip(xp_name)

    hub = AggregatedSocketHub(
        host="127.0.0.1", port=pick_free_port(), timeout=120.0, latency=1e-4
    )
    molecules = [mxl.Molecule(hub=hub) for _ in range(N_SYSTEMS)]

    manifest = tmp_path / f"aggregation_md_{xp_name}.json"
    hub.init_remote_bridges(
        molecules,
        molecules_per_bridge=N_SYSTEMS,  # one group -> one batch model
        unix_prefix=f"mdgpu_{xp_name}_",
        save_file=str(manifest),
    )

    # the batch driver takes exactly the scalar MDModel's arguments
    bridge_thread = threading.Thread(
        target=run_bridge_node,
        kwargs=dict(
            info=str(manifest),
            idx=0,
            backend="gpu",
            model="md",
            driver_kwargs=dict(
                ff="co2jcp2021", thermostat="nve", pre_nvt=False, seed=3
            ),
            xp=xp,
        ),
        daemon=True,
    )
    bridge_thread.start()

    try:
        sim = mxl.SingleModeSimulation(
            dt_au=DT_AU,
            frequency_au=OMEGA_AU,
            damping_au=0.0,
            molecules=molecules,
            coupling_strength=2.0e-4,
            coupling_axis="z",
            hub=hub,
            qc_initial=[0.0, 0.0, 1.0e-2],
            include_dse=True,  # also exercises the force-time dipole
            record_history=True,
            drive=0.0,
        )
        init_payloads = {
            w.molecule_id: {**w.init_payload, "molecule_id": w.molecule_id}
            for w in sim.socket_wrappers
        }
        assert hub.wait_until_bound(
            init_payloads, require_init=True, timeout=60.0
        ), "GPU MD batch bridge failed to bind to the aggregate hub."
        sim.run(steps=STEPS)
    finally:
        hub.stop()
        bridge_thread.join(timeout=15.0)

    qc = np.asarray(sim.qc_history)[:, 2]
    energy = np.asarray(sim.energy_history)

    assert len(sim.time_history) == STEPS, "the run stopped early"
    assert qc.std() > 1e-3, "the cavity mode never responded to the molecules"
    drift = (energy.max() - energy.min()) / max(abs(energy).max(), 1e-30)
    assert drift < 1e-2, f"total energy drift {drift:.2e} is too large"
