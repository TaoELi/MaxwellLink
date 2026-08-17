# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""End-to-end tests: cpu/gpu batch drivers selected via the ``mxl_bridge`` CLI.

The batch backend and molecular model are chosen at bridge launch
(``run_bridge_node(..., backend=, model=, driver_kwargs=)`` — i.e.
``mxl_bridge --backend --model --param``), NOT in any hub API or manifest.  Both
aggregate hubs are exercised through the identical bridge command:

- ``AggregatedSocketHub`` (SingleMode / MultiMode): consumes amp + dipole JSON.
- ``AggregatedSusceptibilitySocketHub`` (Meep): consumes only ``dmu/dt``.

GPU runs inject ``xp=numpy`` so the vectorized model runs on the CPU here; the
identical path uses CuPy on a CUDA device in production.
"""

from __future__ import annotations

import json
import sys
import threading
import time
from pathlib import Path

import numpy as np
import pytest

SRC_ROOT = Path(__file__).resolve().parents[2] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

import maxwelllink as mxl
from maxwelllink.mxl_drivers.python.mxl_driver import _read_args_kwargs
from maxwelllink.mxl_drivers.python.models.sho_model import SHOModel
from maxwelllink.sockets.aggregated import AggregatedSocketHub, run_bridge_node
from maxwelllink.sockets.aggregated_susceptibility import (
    AggregatedSusceptibilitySocketHub,
)

from socket_test_helpers import (
    FakeMeepRank,
    can_create_sockets,
    pick_free_port,
    wait_for_path,
)

pytestmark = pytest.mark.skipif(
    not can_create_sockets(),
    reason="socket creation is not permitted in this environment",
)

# A MIXED positional+keyword --param string (omega positional; mu0/orientation as
# key=value), so the whole chain exercises the same parsing/forwarding mxl_driver
# uses -- proving bare tokens are not dropped.
_PARAM = "0.2,mu0=2.0,orientation=1"
_DT_AU = 0.1


def _driver_params():
    return _read_args_kwargs(_PARAM)  # (args, kwargs)


def _bridge_kwargs(backend: str) -> dict:
    """run_bridge_node kwargs that select the batch backend/model from the CLI."""

    args, kwargs = _driver_params()
    bridge = dict(backend=backend, model="sho", driver_args=args, driver_kwargs=kwargs)
    if backend == "gpu":
        bridge["xp"] = np  # run the vectorized GPU model on the CPU for the test
    return bridge


def _scalar_amp_extra(mid: int, efield):
    """One scalar SHO step: the ground-truth (amp, additional_data)."""

    args, kwargs = _driver_params()
    model = SHOModel(*args, **kwargs)
    model.initialize(_DT_AU, mid)
    model.stage_step(np.asarray(efield, dtype=float))
    amp = np.asarray(model.commit_step(), dtype=float)
    return amp, model.append_additional_data()


# --------------------------------------------------------------------------- #
# AggregatedSocketHub (SingleMode / MultiMode): needs amp + dipole JSON
# --------------------------------------------------------------------------- #


@pytest.mark.core
@pytest.mark.parametrize("backend", ["cpu", "gpu"])
def test_aggregated_socket_hub_batch_via_cli(backend, tmp_path):
    hub = AggregatedSocketHub(
        host="127.0.0.1", port=pick_free_port(), timeout=10.0, latency=1e-4
    )
    manifest = tmp_path / "aggregation.json"
    molecules = [mxl.Molecule(hub=hub, store_additional_data=False) for _ in range(3)]
    hub.init_remote_bridges(
        molecules,
        molecules_per_bridge=len(molecules),
        unix_prefix="unused_",
        save_file=str(manifest),
    )

    # Manifest is topology-only: the model/params live on the CLI, not here.
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    assert all("batch" not in entry for entry in payload["bridges"])

    bridge_thread = threading.Thread(
        target=run_bridge_node,
        kwargs={"info": str(manifest), "idx": 0, **_bridge_kwargs(backend)},
        daemon=True,
    )
    bridge_thread.start()

    init_payloads = {
        molecule.molecule_id: {**molecule.init_payload, "dt_au": _DT_AU}
        for molecule in molecules
    }
    fields = {
        molecules[0].molecule_id: np.array([0.0, 0.5, 0.0]),
        molecules[1].molecule_id: np.array([0.0, -0.25, 0.0]),
        molecules[2].molecule_id: np.array([0.0, 0.1, 0.0]),
    }
    expected = {mid: _scalar_amp_extra(mid, vec) for mid, vec in fields.items()}

    try:
        assert hub.wait_until_bound(init_payloads, require_init=True, timeout=10.0)
        responses = hub.step_barrier(
            {mid: {"efield_au": vec} for mid, vec in fields.items()}, timeout=10.0
        )
        assert set(responses) == set(fields)
        for mid in fields:
            amp_ref, extra_ref = expected[mid]
            np.testing.assert_allclose(
                responses[mid]["amp"], amp_ref, rtol=0.0, atol=1e-12
            )
            got = json.loads(responses[mid]["extra"].decode("utf-8"))
            assert got == pytest.approx(extra_ref, rel=0.0, abs=1e-10)
    finally:
        hub.stop()
        bridge_thread.join(timeout=5.0)
    assert not bridge_thread.is_alive()


# --------------------------------------------------------------------------- #
# AggregatedSusceptibilitySocketHub (Meep): needs only dmu/dt
# --------------------------------------------------------------------------- #


@pytest.mark.core
@pytest.mark.parametrize("backend", ["cpu", "gpu"])
def test_meep_susceptibility_hub_batch_via_cli(backend, tmp_path):
    prefix = f"aggbatch{time.time_ns() % 1_000_000}_"
    manifest = tmp_path / "aggregation.json"
    hub = AggregatedSusceptibilitySocketHub(
        host="127.0.0.1",
        port=pick_free_port(),
        timeout=60.0,
        latency=1e-3,
        num_bridges=1,
        unix_prefix=prefix,
        bridge_manifest=str(tmp_path / "bootstrap.json"),
        init_grace_seconds=0.1,
    )
    rank = None
    bridge_thread = None
    try:
        specs = hub.init_remote_bridges(
            molecules_per_bridge=3, unix_prefix=prefix, save_file=str(manifest)
        )
        assert specs == []  # deferred; manifest written after the rank announces

        def run_batch_bridge():
            wait_for_path(manifest, timeout=60.0)
            run_bridge_node(info=str(manifest), idx=0, **_bridge_kwargs(backend))

        bridge_thread = threading.Thread(target=run_batch_bridge, daemon=True)
        bridge_thread.start()

        rank = FakeMeepRank(
            hub.host,
            hub.port,
            molecule_ids=[0, 1, 2],
            rank=0,
            dt_au=_DT_AU,
            expected_total_molecules=3,
        )
        rank.wait_ready()

        # One batch bridge for the whole group; manifest carries no batch block.
        payload = json.loads(manifest.read_text(encoding="utf-8"))
        assert all("batch" not in entry for entry in payload["bridges"])

        fields = {
            0: np.array([0.0, 0.5, 0.0]),
            1: np.array([0.0, -0.25, 0.0]),
            2: np.array([0.0, 0.1, 0.0]),
        }
        responses = rank.step(fields)
        assert set(responses) == {0, 1, 2}
        for mid, vec in fields.items():
            amp_ref, _ = _scalar_amp_extra(mid, vec)
            np.testing.assert_allclose(
                responses[mid]["amp"], amp_ref, rtol=0.0, atol=1e-12
            )
    finally:
        if rank is not None:
            rank.close()
        hub.stop()
        if bridge_thread is not None:
            bridge_thread.join(timeout=10.0)
