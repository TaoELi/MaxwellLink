# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
End-to-end test for ``AggregatedSusceptibilitySocketHub``.

Exercises the full production topology in-process:

    fake Meep rank ==MXLINIT/AGGSTEP==> child-process aggregate hub
        ==AGGHELLO/AGGINIT==> run_bridge_node bridges (from the manifest)
            ==UNIX sockets==> real run_driver clients

including the deferred ``init_remote_bridges`` policy, whose manifest is only
finalized once the rank announces ``expected_total_molecules``.
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

from maxwelllink.sockets.aggregated import run_bridge_node
from maxwelllink.sockets.aggregated_susceptibility import (
    AggregatedSusceptibilitySocketHub,
)

from socket_test_helpers import (
    EchoModel,
    FakeMeepRank,
    can_create_sockets,
    pick_free_port,
    wait_for_path,
)
from maxwelllink.mxl_drivers.python.mxl_driver import run_driver

pytestmark = pytest.mark.skipif(
    not can_create_sockets(),
    reason="socket creation is not permitted in this environment",
)


def _bridge_when_manifest(manifest: Path, idx: int):
    """Start run_bridge_node(idx) as soon as the finalized manifest appears."""

    def runner():
        wait_for_path(manifest, timeout=60.0)
        run_bridge_node(info=str(manifest), idx=idx)

    thread = threading.Thread(target=runner, daemon=True)
    thread.start()
    return thread


def _driver_when_unixsocket(unix_name: str):
    """Start one run_driver client once the bridge's UNIX socket exists."""

    def runner():
        wait_for_path(f"/tmp/socketmxl_{unix_name}", timeout=60.0)
        run_driver(unix=True, address=unix_name, timeout=60.0, driver=EchoModel())

    thread = threading.Thread(target=runner, daemon=True)
    thread.start()
    return thread


@pytest.mark.core
def test_deferred_remote_bridges_round_trip(tmp_path):
    """Deferred policy -> finalized manifest -> bridges -> drivers -> steps."""

    prefix = f"aggsus{time.time_ns() % 1_000_000}_"
    manifest = tmp_path / "aggregation.json"
    port = pick_free_port()

    hub = AggregatedSusceptibilitySocketHub(
        host="127.0.0.1",
        port=port,
        timeout=60.0,
        latency=1e-3,
        num_bridges=1,
        unix_prefix=prefix,
        bridge_manifest=str(tmp_path / "bootstrap_manifest.json"),
        init_grace_seconds=0.1,
    )
    rank = None
    threads = []
    try:
        assert hub.host == "127.0.0.1"
        assert hub.port == port

        # Deferred partitioning: no specs yet, manifest written by the child
        # only after a Meep rank announces expected_total_molecules.
        specs = hub.init_remote_bridges(
            molecules_per_bridge=1, unix_prefix=prefix, save_file=str(manifest)
        )
        assert specs == []
        assert not manifest.exists()

        # expected_total=2, one molecule per bridge -> two bridge nodes.
        threads = [
            _bridge_when_manifest(manifest, 0),
            _bridge_when_manifest(manifest, 1),
            _driver_when_unixsocket(f"{prefix}0"),
            _driver_when_unixsocket(f"{prefix}1"),
        ]

        rank = FakeMeepRank(
            hub.host,
            hub.port,
            molecule_ids=[0, 1],
            rank=0,
            dt_au=1.0,
            rescaling_factor=2.5,
            time_units_fs=0.1,
            expected_total_molecules=2,
        )
        rank.wait_ready()

        payload = json.loads(manifest.read_text(encoding="utf-8"))
        assert payload["molecules_per_bridge"] == 1
        assert payload["unix_prefix"] == prefix
        assert payload["hub_host"] == "127.0.0.1"
        assert payload["hub_port"] == port
        bridges = sorted(payload["bridges"], key=lambda spec: spec["idx"])
        assert [b["group_id"] for b in bridges] == [f"{prefix}0", f"{prefix}1"]
        assert [b["n_molecules"] for b in bridges] == [1, 1]

        fields = {0: np.array([1.0, -2.0, 0.5]), 1: np.array([0.25, 4.0, -1.0])}
        responses = rank.step(fields)
        assert set(responses.keys()) == {0, 1}
        for mid in (0, 1):
            np.testing.assert_allclose(
                responses[mid]["amp"], (mid + 1.0) * fields[mid], rtol=0.0, atol=1e-12
            )

        # Dedup path across two different bridges in one timestep.
        same = {0: np.array([3.0, 3.0, 3.0]), 1: np.array([3.0, 3.0, 3.0])}
        responses2 = rank.step(same)
        np.testing.assert_allclose(responses2[0]["amp"], [3.0, 3.0, 3.0])
        np.testing.assert_allclose(responses2[1]["amp"], [6.0, 6.0, 6.0])

        # rank_stats records both aggregate groups serving this rank.
        deadline = time.time() + 10.0
        stats = hub.rank_stats
        while time.time() < deadline:
            stats = hub.rank_stats
            if 0 in stats and stats[0]["steps"] >= 2:
                break
            time.sleep(0.1)
        assert stats[0]["molecule_count"] == 2
        assert stats[0]["steps"] >= 2
        assert sorted(stats[0]["aggregate_groups"]) == [f"{prefix}0", f"{prefix}1"]
    finally:
        if rank is not None:
            rank.close()
        hub.stop()
        for thread in threads:
            thread.join(timeout=10.0)

    hub.stop()
    assert hub._process is not None
    assert not hub._process.is_alive()
