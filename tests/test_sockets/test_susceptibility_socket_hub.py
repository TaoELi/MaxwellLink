# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
End-to-end tests for ``SusceptibilitySocketHub``.

This is the hub behind every production Meep+LAMMPS run in the plasmonic VSC
project. The tests drive it exactly the way a real run does: the hub spawns
its child-process server, ordinary drivers connect silently over TCP, and a
fake Meep rank speaks the C-level ``MXLINIT``/``AGGSTEP`` protocol. The Slurm
contract (``num_socket_molecule`` content) is asserted explicitly.
"""

from __future__ import annotations

import socket
import sys
import time
from pathlib import Path

import numpy as np
import pytest

SRC_ROOT = Path(__file__).resolve().parents[2] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from maxwelllink.sockets.susceptibility import SusceptibilitySocketHub

from socket_test_helpers import (
    FakeMeepRank,
    can_create_sockets,
    pick_free_port,
    start_driver_thread,
)

pytestmark = pytest.mark.skipif(
    not can_create_sockets(),
    reason="socket creation is not permitted in this environment",
)


@pytest.mark.core
def test_meep_rank_round_trip_with_real_drivers(tmp_path):
    """MXLINIT -> driver binding -> MXLREADY -> AGGSTEP round trips."""

    port = pick_free_port()
    count_file = tmp_path / "num_socket_molecule"
    hub = SusceptibilitySocketHub(
        host="127.0.0.1",
        port=port,
        timeout=60.0,
        latency=1e-3,
        driver_count_file=str(count_file),
    )
    rank = None
    threads = []
    try:
        assert hub.host == "127.0.0.1"
        assert hub.port == port
        assert hub.address == hub.host

        # Ordinary drivers connect silently and are parked by the classifier.
        threads = [
            start_driver_thread(unix=False, address="127.0.0.1", port=port),
            start_driver_thread(unix=False, address="127.0.0.1", port=port),
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

        # Slurm contract: the hub announces how many drivers Meep needs,
        # exactly once, as a single integer line.
        assert count_file.read_text(encoding="utf-8") == "2\n"

        fields = {0: np.array([1.0, -2.0, 0.5]), 1: np.array([0.25, 4.0, -1.0])}
        responses = rank.step(fields)
        assert set(responses.keys()) == {0, 1}
        for mid in (0, 1):
            np.testing.assert_allclose(
                responses[mid]["amp"], (mid + 1.0) * fields[mid], rtol=0.0, atol=1e-12
            )
            assert b"molecule_id" in responses[mid]["extra"]

        # Second step, including the dedup path (both molecules same field).
        same = {0: np.array([3.0, 3.0, 3.0]), 1: np.array([3.0, 3.0, 3.0])}
        responses2 = rank.step(same)
        np.testing.assert_allclose(responses2[0]["amp"], [3.0, 3.0, 3.0])
        np.testing.assert_allclose(responses2[1]["amp"], [6.0, 6.0, 6.0])

        # rank_stats snapshots flow back from the child process.
        deadline = time.time() + 10.0
        stats = hub.rank_stats
        while time.time() < deadline:
            stats = hub.rank_stats
            if 0 in stats and stats[0]["steps"] >= 2:
                break
            time.sleep(0.1)
        assert stats[0]["molecule_count"] == 2
        assert stats[0]["steps"] >= 2
        assert stats[0]["requests"] >= 4
    finally:
        if rank is not None:
            rank.close()
        hub.stop()
        for thread in threads:
            thread.join(timeout=5.0)

    # stop() must be idempotent and must not leave an orphan child process.
    hub.stop()
    assert hub._process is not None
    assert not hub._process.is_alive()


@pytest.mark.core
def test_startup_failure_raises_runtime_error():
    """A bind failure in the child must surface as RuntimeError in the parent."""

    blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    blocker.bind(("127.0.0.1", 0))
    blocker.listen(1)
    busy_port = int(blocker.getsockname()[1])
    try:
        with pytest.raises(RuntimeError, match="failed to start"):
            SusceptibilitySocketHub(
                host="127.0.0.1", port=busy_port, timeout=5.0, latency=1e-3
            )
    finally:
        blocker.close()


@pytest.mark.core
def test_unixsocket_is_rejected():
    """The susceptibility hubs are TCP-only; unixsocket must raise ValueError."""

    with pytest.raises(ValueError):
        SusceptibilitySocketHub(unixsocket="not_supported")
