# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

import sys
import threading
from pathlib import Path

import pytest

SRC_ROOT = Path(__file__).resolve().parents[2] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from maxwelllink.sockets.aggregated_susceptibility import (
    _AggregatedSusceptibilitySocketHubServer,
)


@pytest.mark.core
def test_aggregated_susceptibility_barrier_allows_multiple_clients_per_rank():
    """Socket clients from one Meep rank should synchronize by rank-local order."""

    server = object.__new__(_AggregatedSusceptibilitySocketHubServer)
    server._meep_lock = threading.RLock()
    server._global_step_cond = threading.Condition(server._meep_lock)
    server._client_init_payloads = {0: {}, 1: {}, 2: {}, 3: {}}
    server._client_ordinals = {0: 0, 1: 0, 2: 1, 3: 1}
    server._client_steps = {0: 0, 1: 0, 2: 0, 3: 0}
    server._global_pending_key = None
    server._global_pending_requests = {}
    server._global_pending_mids = {}
    server._global_results = {}
    server._global_error = None
    server._global_running = False
    server._stop = False
    server.timeout = 5.0
    server.latency = 1e-4

    calls = []

    def merged_step(requests, deadline):
        del deadline
        calls.append(tuple(sorted(requests)))
        return {mid: {"amp_au": [float(mid), 0.0, 0.0]} for mid in requests}

    server._run_merged_susceptibility_step = merged_step
    errors = []

    def run_rank0():
        try:
            server._run_global_susceptibility_step(0, {0: {}})
            server._run_global_susceptibility_step(2, {2: {}})
            server._run_global_susceptibility_step(0, {4: {}})
        except Exception as exc:
            errors.append(exc)

    def run_rank1():
        try:
            server._run_global_susceptibility_step(1, {1: {}})
            server._run_global_susceptibility_step(3, {3: {}})
            server._run_global_susceptibility_step(1, {5: {}})
        except Exception as exc:
            errors.append(exc)

    threads = [
        threading.Thread(target=run_rank0, daemon=True),
        threading.Thread(target=run_rank1, daemon=True),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10.0)

    assert not errors
    assert all(not thread.is_alive() for thread in threads)
    assert calls == [(0, 1), (2, 3), (4, 5)]
    assert server._client_steps == {0: 2, 1: 2, 2: 1, 3: 1}


@pytest.mark.core
def test_aggregated_susceptibility_remote_layout_uses_announced_capacity():
    """Lazy Meep registrations should fill bridge capacities from the manifest."""

    server = object.__new__(_AggregatedSusceptibilitySocketHubServer)
    server._remote_bridge_policy = {
        "molecules_per_bridge": 3,
        "unix_prefix": "bridge_",
        "save_file": "unused.json",
    }
    server._mxl_molecule_to_group = {}
    server._meep_lock = threading.RLock()

    server._configure_remote_bridge_layout_locked(8)
    assigned = [server._molecule_group(mid) for mid in range(8)]

    assert server._group_capacities == {
        "bridge_0": 3,
        "bridge_1": 3,
        "bridge_2": 2,
    }
    assert assigned == [
        "bridge_0",
        "bridge_0",
        "bridge_0",
        "bridge_1",
        "bridge_1",
        "bridge_1",
        "bridge_2",
        "bridge_2",
    ]
    with pytest.raises(RuntimeError, match="expected_total_molecules"):
        server._molecule_group(8)
