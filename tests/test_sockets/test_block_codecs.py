# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
Tests for the vectorized (block) AGG frame codecs and the hub block fast path.

The block codecs must stay byte-identical to the per-record struct codecs:
``send_block`` output is compared against ``send`` golden bytes, and each
block decoder is fed frames produced by the legacy encoder (and vice versa).
The barrier test pins that a block-path client and a legacy client can share
one global timestep cohort.
"""

from __future__ import annotations

import socket
import sys
import threading
from pathlib import Path

import numpy as np
import pytest

SRC_ROOT = Path(__file__).resolve().parents[2] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from maxwelllink.sockets.protocol import (
    _ResultCodec,
    _StepCodec,
    _STEP_RECORD_DTYPE,
    _resolve_step_records,
)
from maxwelllink.sockets.aggregated_susceptibility import (
    _BLOCK_RESULT,
    _AggregatedSusceptibilitySocketHubServer,
)


class _CaptureSock:
    """Capture sendall payloads so frames can be compared byte-for-byte."""

    def __init__(self):
        self.data = b""

    def sendall(self, payload):
        self.data += bytes(payload)


def _identity_records(mids: np.ndarray) -> bytes:
    records = np.empty((mids.size, 2), dtype="<i4")
    records[:, 0] = mids
    records[:, 1] = np.arange(mids.size, dtype="<i4")
    return records.tobytes()


@pytest.mark.core
def test_step_send_block_matches_legacy_bytes():
    rng = np.random.default_rng(7)
    n = 11
    mids = np.arange(50, 50 + n, dtype="<i4")
    fields = rng.normal(size=(n, 3))
    requests = {int(mid): {"efield_au": fields[i]} for i, mid in enumerate(mids)}

    legacy, block = _CaptureSock(), _CaptureSock()
    _StepCodec().send(legacy, requests)
    _StepCodec().send_block(block, _identity_records(mids), fields)
    assert legacy.data == block.data


@pytest.mark.core
def test_step_recv_block_round_trips_legacy_frame():
    rng = np.random.default_rng(8)
    n = 5
    mids = np.array([3, 9, 1, 7, 4], dtype="<i4")
    fields = rng.normal(size=(n, 3))
    requests = {int(mid): {"efield_au": fields[i]} for i, mid in enumerate(mids)}

    a, b = socket.socketpair()
    try:
        _StepCodec().send(a, requests)
        nreq, records, decoded = _StepCodec().recv_block(b)
    finally:
        a.close()
        b.close()

    assert nreq == n
    assert records == _identity_records(mids)
    np.testing.assert_array_equal(np.asarray(decoded), fields)

    resolved = _resolve_step_records(records, decoded)
    for i, mid in enumerate(mids):
        np.testing.assert_array_equal(resolved[int(mid)], fields[i])


@pytest.mark.core
def test_step_recv_block_exposes_dedup_frames_for_fallback():
    """A deduplicating sender must be detectable from the record table."""

    shared = np.array([1.0, 2.0, 3.0])
    requests = {0: {"efield_au": shared}, 1: {"efield_au": shared}}

    a, b = socket.socketpair()
    try:
        _StepCodec().send(a, requests)
        nreq, records, fields = _StepCodec().recv_block(b)
    finally:
        a.close()
        b.close()

    assert nreq == 2 and fields.shape[0] == 1
    recs = np.frombuffer(records, dtype=_STEP_RECORD_DTYPE)
    assert list(recs["field_idx"]) == [0, 0]
    resolved = _resolve_step_records(records, fields)
    np.testing.assert_array_equal(resolved[0], shared)
    assert resolved[0] is resolved[1]  # aliasing contract preserved


@pytest.mark.core
def test_result_send_block_matches_legacy_bytes_and_round_trips():
    rng = np.random.default_rng(9)
    n = 6
    mids = np.array([12, 5, 33, 2, 40, 8], dtype="<i4")
    amps = rng.normal(size=(n, 3))
    responses = {int(mid): {"amp": amps[i], "extra": b""} for i, mid in enumerate(mids)}

    legacy, block = _CaptureSock(), _CaptureSock()
    _ResultCodec().send(legacy, responses)
    _ResultCodec().send_block(block, mids, amps)
    assert legacy.data == block.data

    # block -> legacy decoder
    a, b = socket.socketpair()
    try:
        _ResultCodec().send_block(a, mids, amps)
        decoded = _ResultCodec().recv(b)
        # legacy (with extras) -> block decoder
        extras = {
            int(mid): {"amp": amps[i], "extra": b"x" * i}
            for i, mid in enumerate(mids)
        }
        _ResultCodec().send(a, extras)
        rmids, ramps, extra_lens, extra_blob = _ResultCodec().recv_block(b)
    finally:
        a.close()
        b.close()

    for i, mid in enumerate(mids):
        np.testing.assert_array_equal(decoded[int(mid)]["amp"], amps[i])
        assert decoded[int(mid)]["extra"] == b""
    np.testing.assert_array_equal(rmids, mids)
    np.testing.assert_array_equal(ramps, amps)
    np.testing.assert_array_equal(extra_lens, np.arange(n))
    assert extra_blob == b"x" * (n * (n - 1) // 2)


def _make_barrier_server() -> _AggregatedSusceptibilitySocketHubServer:
    server = object.__new__(_AggregatedSusceptibilitySocketHubServer)
    server._meep_lock = threading.RLock()
    server._global_step_cond = threading.Condition(server._meep_lock)
    server._client_init_payloads = {0: {}, 1: {}}
    server._client_ordinals = {0: 0, 1: 0}
    server._client_steps = {0: 0, 1: 0}
    server._global_pending_key = None
    server._global_pending_requests = {}
    server._global_pending_mids = {}
    server._global_results = {}
    server._global_error = None
    server._global_running = False
    server._client_block_info = {}
    server._block_merged_cache = {}
    server._request_caches = {}
    server._stop = False
    server.timeout = 5.0
    server.latency = 1e-4
    return server


@pytest.mark.core
def test_block_and_legacy_clients_share_one_barrier_cohort():
    """A block-path client and a legacy client must merge into one step."""

    server = _make_barrier_server()
    server._client_block_info[0] = server._build_client_block_info([10, 11])

    merged_seen = []

    def merged_step(requests, deadline):
        del deadline
        merged_seen.append(sorted(requests))
        return {
            mid: {"amp": np.array([float(mid), 0.0, 0.0]), "extra": b""}
            for mid in requests
        }

    server._run_merged_susceptibility_step = merged_step

    results = {}
    errors = []

    def run_block_client():
        try:
            fields = np.array([[1.0, 0.0, 0.0], [2.0, 0.0, 0.0]])
            results["block"] = server._run_block_step(0, fields).copy()
        except Exception as exc:  # pragma: no cover - failure reporting
            errors.append(exc)

    def run_legacy_client():
        try:
            results["legacy"] = server._run_global_susceptibility_step(
                1, {20: {"efield_au": np.zeros(3)}}
            )
        except Exception as exc:  # pragma: no cover - failure reporting
            errors.append(exc)

    threads = [
        threading.Thread(target=run_block_client, daemon=True),
        threading.Thread(target=run_legacy_client, daemon=True),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10.0)

    assert not errors
    assert merged_seen == [[10, 11, 20]]
    np.testing.assert_array_equal(
        results["block"], [[10.0, 0.0, 0.0], [11.0, 0.0, 0.0]]
    )
    np.testing.assert_array_equal(results["legacy"][20]["amp"], [20.0, 0.0, 0.0])
    assert server._client_steps == {0: 1, 1: 1}
    # The mixed cohort must not have been cached as an all-block cohort.
    assert server._block_merged_cache == {}


@pytest.mark.core
def test_all_block_cohort_reuses_cached_merge():
    """Two block clients should hit the cached cohort merge on every step."""

    server = _make_barrier_server()
    server._client_block_info[0] = server._build_client_block_info([10, 11])
    server._client_block_info[1] = server._build_client_block_info([20])

    merged_ids = []

    def merged_step(requests, deadline):
        del deadline
        merged_ids.append(id(requests))
        return {
            mid: {
                "amp": np.asarray(requests[mid]["efield_au"], dtype=float) * 2.0,
                "extra": b"",
            }
            for mid in requests
        }

    server._run_merged_susceptibility_step = merged_step

    out = {}
    errors = []

    def run_client(cid, fields):
        try:
            for step in range(3):
                out[(cid, step)] = server._run_block_step(
                    cid, fields + float(step)
                ).copy()
        except Exception as exc:  # pragma: no cover - failure reporting
            errors.append(exc)

    fields0 = np.array([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]])
    fields1 = np.array([[0.0, 0.0, 1.0]])
    threads = [
        threading.Thread(target=run_client, args=(0, fields0), daemon=True),
        threading.Thread(target=run_client, args=(1, fields1), daemon=True),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10.0)

    assert not errors
    # One cached merged dict, reused on every step of the cohort.
    assert len(set(merged_ids)) == 1
    assert len(merged_ids) == 3
    for step in range(3):
        np.testing.assert_array_equal(out[(0, step)], (fields0 + step) * 2.0)
        np.testing.assert_array_equal(out[(1, step)], (fields1 + step) * 2.0)
    assert server._client_steps == {0: 3, 1: 3}


@pytest.mark.core
def test_block_plan_scatters_shuffled_bridge_replies():
    """The vectorized plan must map out-of-order bridge replies correctly."""

    import time as _time

    from maxwelllink.sockets.aggregated import _AggregateGroupState
    from maxwelllink.sockets.protocol import _recv_msg, AGGSTEP
    from maxwelllink.sockets.sockets import _ClientState

    server = _make_barrier_server()
    server._lock = threading.RLock()
    server.paused = False
    server._step_lock = threading.RLock()

    # One client with deliberately unsorted molecule ids, one bridge group.
    mids = [5, 3, 9]
    server._client_block_info[0] = server._build_client_block_info(mids)
    server._molecule_to_group = {mid: "g0" for mid in mids}

    hub_end, bridge_end = socket.socketpair()
    state = _ClientState(sock=hub_end, address="test", molecule_id=5)
    state.alive = True
    state.initialized = True
    group = _AggregateGroupState("g0")
    group.molecule_ids = list(mids)
    group.bridge = state
    server._groups = {"g0": group}

    cohort = (0,)
    server._block_merged_cache[cohort] = {"merged": {}, "rank_mids": {}, "plan": None}
    plan = server._block_plan_for_cohort(cohort)
    assert plan is not None
    assert plan["mid_list"] == mids

    shuffles = [[9, 5, 3], [3, 9, 5]]
    errors = []

    def bridge_loop():
        try:
            codec_in = _StepCodec()
            codec_out = _ResultCodec()
            for shuffle in shuffles:
                header = _recv_msg(bridge_end)
                assert header == AGGSTEP
                efields = codec_in.recv(bridge_end, header_already_read=True)
                responses = {
                    mid: {
                        "amp": 10.0 * np.asarray(efields[mid], dtype=float),
                        "extra": b"z" * (mid % 3),
                    }
                    for mid in shuffle
                }
                codec_out.send(bridge_end, responses)
        except Exception as exc:  # pragma: no cover - failure reporting
            errors.append(exc)

    thread = threading.Thread(target=bridge_loop, daemon=True)
    thread.start()

    info = server._client_block_info[0]
    try:
        for step, base in enumerate((1.0, 2.0)):
            fields = base * np.arange(9, dtype=float).reshape(3, 3)
            np.copyto(info["fields_stage"], fields)
            server._run_merged_block_step(plan, _time.time() + 5.0)
            np.testing.assert_allclose(plan["merged_amps"], 10.0 * fields)
    finally:
        thread.join(timeout=5.0)
        hub_end.close()
        bridge_end.close()

    assert not errors
