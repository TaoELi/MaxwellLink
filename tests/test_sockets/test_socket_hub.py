# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
Public-API tests for the base ``SocketHub``.

``SocketHub`` is the root of the planned hub hierarchy and the transport for
every EM solver (`em_solvers/*` all import it). These tests pin its public
contract — registration, binding, the step barrier, disconnect recovery, and
shutdown — against real ``run_driver`` clients over both TCP and UNIX sockets.
"""

from __future__ import annotations

import json
import socket
import sys
import time
from pathlib import Path

import numpy as np
import pytest

SRC_ROOT = Path(__file__).resolve().parents[2] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from maxwelllink.sockets.sockets import (
    SocketHub,
    get_available_host_port,
    _recv_bytes,
    _recv_int,
    _recv_msg,
)

from socket_test_helpers import (
    can_create_sockets,
    pick_free_port,
    start_driver_thread,
)

pytestmark = pytest.mark.skipif(
    not can_create_sockets(),
    reason="socket creation is not permitted in this environment",
)


def _make_tcp_hub(**overrides):
    """Return (hub, port). Characterization note: unlike every other hub class,
    the base SocketHub does NOT expose host/port attributes (only ``_where``),
    so tests must carry the chosen port themselves."""

    kwargs = dict(host="127.0.0.1", port=pick_free_port(), timeout=10.0, latency=1e-4)
    kwargs.update(overrides)
    hub = SocketHub(**kwargs)
    assert not hasattr(hub, "port")
    return hub, kwargs["port"]


def _step_until_complete(hub, init_payloads, requests, deadline_s=15.0):
    """Drive wait_until_bound + step_barrier the way EM solvers do."""

    deadline = time.time() + deadline_s
    responses = {}
    while not responses and time.time() < deadline:
        hub.wait_until_bound(init_payloads, require_init=True, timeout=5.0)
        responses = hub.step_barrier(requests, timeout=5.0)
    return responses


@pytest.mark.core
def test_tcp_round_trip_two_drivers():
    """Two TCP drivers bind, step, and report per-molecule extras."""

    hub, port = _make_tcp_hub()
    threads = [
        start_driver_thread(unix=False, address="127.0.0.1", port=port),
        start_driver_thread(unix=False, address="127.0.0.1", port=port),
    ]
    try:
        mid0 = hub.register_molecule_return_id()
        mid1 = hub.register_molecule_return_id()
        assert (mid0, mid1) == (0, 1)

        init_payloads = {mid0: {"dt_au": 0.25}, mid1: {"dt_au": 0.25}}
        fields = {
            mid0: np.array([1.5, -0.5, 0.25]),
            mid1: np.array([-2.0, 1.0, 0.75]),
        }
        requests = {
            mid: {"efield_au": fields[mid], "init": init_payloads[mid]}
            for mid in (mid0, mid1)
        }

        assert hub.wait_until_bound(init_payloads, require_init=True, timeout=10.0)
        assert hub.all_bound([mid0, mid1], require_init=True)

        responses = _step_until_complete(hub, init_payloads, requests)
        assert set(responses.keys()) == {mid0, mid1}
        for mid in (mid0, mid1):
            np.testing.assert_allclose(
                responses[mid]["amp"], (mid + 1.0) * fields[mid], rtol=0.0, atol=1e-12
            )
            extra = json.loads(responses[mid]["extra"].decode("utf-8"))
            assert extra["molecule_id"] == mid

        # A second step with new fields reuses the same bound clients.
        fields2 = {mid: -3.0 * fields[mid] for mid in fields}
        requests2 = {
            mid: {"efield_au": fields2[mid], "init": init_payloads[mid]}
            for mid in (mid0, mid1)
        }
        responses2 = _step_until_complete(hub, init_payloads, requests2)
        for mid in (mid0, mid1):
            np.testing.assert_allclose(
                responses2[mid]["amp"],
                (mid + 1.0) * fields2[mid],
                rtol=0.0,
                atol=1e-12,
            )
    finally:
        hub.stop()
        for thread in threads:
            thread.join(timeout=5.0)
    # stop() must have told the drivers to exit (STOP -> BYE).
    assert all(not thread.is_alive() for thread in threads)


@pytest.mark.core
def test_unix_round_trip_and_socket_path_cleanup():
    """A UNIX-socket hub serves a driver and unlinks its path on stop()."""

    unix_name = f"hub_lock_{time.time_ns()}"
    hub = SocketHub(unixsocket=unix_name, timeout=10.0, latency=1e-4)
    socket_path = Path(f"/tmp/socketmxl_{unix_name}")
    assert socket_path.exists()

    thread = start_driver_thread(unix=True, address=unix_name)
    try:
        mid = hub.register_molecule_return_id()
        init_payloads = {mid: {"dt_au": 0.5}}
        field = np.array([0.5, 0.0, -1.0])
        requests = {mid: {"efield_au": field, "init": init_payloads[mid]}}

        responses = _step_until_complete(hub, init_payloads, requests)
        np.testing.assert_allclose(
            responses[mid]["amp"], (mid + 1.0) * field, rtol=0.0, atol=1e-12
        )
    finally:
        hub.stop()
        thread.join(timeout=5.0)
    assert not socket_path.exists()


@pytest.mark.core
def test_register_molecule_semantics():
    """IDs are unique, duplicates raise, and manual + auto IDs coexist."""

    hub, _port = _make_tcp_hub()
    try:
        hub.register_molecule(5)
        with pytest.raises(ValueError):
            hub.register_molecule(5)
        auto_ids = [hub.register_molecule_return_id() for _ in range(3)]
        assert auto_ids == [0, 1, 2]
        assert 5 in hub.expected
        # The auto allocator must skip manually reserved IDs.
        hub.register_molecule(3)
        assert hub.register_molecule_return_id() == 4
        assert hub.register_molecule_return_id() == 6
    finally:
        hub.stop()


@pytest.mark.core
def test_get_available_host_port_file_format(tmp_path):
    """Slurm driver scripts parse this file: line 1 = host, line 2 = port."""

    save = tmp_path / "tcp_host_port_info.txt"
    host, port = get_available_host_port(localhost=True, save_to_file=str(save))
    assert host == "127.0.0.1"
    assert 0 < int(port) < 65536
    assert save.read_text(encoding="utf-8") == f"{host}\n{port}\n"


def _fragile_client(host: str, port: int):
    """Connect, accept INIT like a driver, then die before serving a step."""

    sock = socket.create_connection((host, port), timeout=10.0)
    sock.settimeout(10.0)
    try:
        msg = _recv_msg(sock)
        assert msg == b"INIT"
        _recv_int(sock)
        _recv_bytes(sock)
    finally:
        sock.close()


@pytest.mark.core
def test_disconnect_pauses_and_frozen_barrier_recovers():
    """A mid-step disconnect pauses the hub; a reconnect replays the SAME field."""

    hub, port = _make_tcp_hub()
    try:
        mid = hub.register_molecule_return_id()
        init_payloads = {mid: {"dt_au": 0.25}}
        original_field = np.array([2.0, -1.0, 0.5])
        requests = {mid: {"efield_au": original_field, "init": init_payloads[mid]}}

        # Bind a client that dies immediately after INIT.
        import threading as _threading

        fragile = _threading.Thread(
            target=_fragile_client, args=("127.0.0.1", port), daemon=True
        )
        fragile.start()
        assert hub.wait_until_bound(init_payloads, require_init=True, timeout=10.0)
        fragile.join(timeout=5.0)

        # The step against the dead client must fail softly: empty result + pause.
        deadline = time.time() + 10.0
        first = hub.step_barrier(requests, timeout=5.0)
        while first and time.time() < deadline:  # pragma: no cover - timing guard
            first = hub.step_barrier(requests, timeout=5.0)
        assert first == {}
        assert hub.paused is True

        # Recovery: a healthy driver binds and the frozen barrier replays the
        # ORIGINAL field even though the retry passes a different one.
        thread = start_driver_thread(unix=False, address="127.0.0.1", port=port)
        try:
            decoy_requests = {
                mid: {
                    "efield_au": np.array([99.0, 99.0, 99.0]),
                    "init": init_payloads[mid],
                }
            }
            responses = _step_until_complete(hub, init_payloads, decoy_requests)
            assert hub.paused is False
            np.testing.assert_allclose(
                responses[mid]["amp"],
                (mid + 1.0) * original_field,
                rtol=0.0,
                atol=1e-12,
            )
        finally:
            thread.join(timeout=5.0)
    finally:
        hub.stop()
