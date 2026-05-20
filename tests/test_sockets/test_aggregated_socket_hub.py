# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

import json
import socket
import threading
import time
import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

SRC_ROOT = Path(__file__).resolve().parents[2] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from maxwelllink.mxl_drivers.python.mxl_driver import run_driver
from maxwelllink.mxl_drivers.python.models.dummy_model import DummyModel
from maxwelllink.sockets.aggregated import AggregatedSocketHub, LocalSocketHubBridge


def _can_create_sockets() -> bool:
    """Return whether this environment permits opening localhost sockets."""

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except PermissionError:
        return False
    try:
        sock.close()
    except OSError:
        pass
    return True


pytestmark = pytest.mark.skipif(
    not _can_create_sockets(),
    reason="socket creation is not permitted in this environment",
)


def _pick_free_port() -> int:
    """Ask the OS for a free localhost TCP port."""

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class EchoModel(DummyModel):
    """
    Lightweight test driver that returns a molecule-dependent copy of the E field.

    This lets the test verify that the aggregated bridge preserves per-molecule
    routing while still reusing the normal driver loop and downstream SocketHub.
    """

    def __init__(self):
        super().__init__(verbose=False)
        self.last_e = np.zeros(3, dtype=float)

    def _snapshot(self):
        snapshot = super()._snapshot()
        snapshot["last_e"] = self.last_e.copy()
        return snapshot

    def _restore(self, snapshot):
        super()._restore(snapshot)
        self.last_e = np.asarray(snapshot["last_e"], dtype=float).copy()

    def propagate(self, effective_efield_vec):
        self.last_e = np.asarray(effective_efield_vec, dtype=float).copy()
        self.t += self.dt

    def calc_amp_vector(self):
        return (self.molecule_id + 1.0) * self.last_e

    def append_additional_data(self):
        return {"molecule_id": int(self.molecule_id), "time_au": float(self.t)}


def _start_driver_thread(*, unix: bool, address: str, port: int | None = None):
    """Start one blocking MaxwellLink driver loop in a daemon thread."""

    kwargs = {
        "unix": unix,
        "address": address,
        "timeout": 10.0,
        "driver": EchoModel(),
    }
    if port is not None:
        kwargs["port"] = int(port)

    thread = threading.Thread(target=run_driver, kwargs=kwargs, daemon=True)
    thread.start()
    return thread


@pytest.mark.core
def test_aggregated_socket_hub_convenience_bridge_api():
    """
    End-to-end test for the simplified hub-owned bridge workflow.

    This mirrors the intended user-facing path:

        hub = AggregatedSocketHub(...)
        node = hub.add_bridge("unix-name")
        node.append(molecule_or_list)
    """

    upstream_port = _pick_free_port()
    hub = AggregatedSocketHub(
        host="127.0.0.1",
        port=upstream_port,
        timeout=10.0,
        latency=1e-4,
    )

    node = hub.add_bridge(f"agg_helper_{time.time_ns()}")
    driver_threads = [
        _start_driver_thread(unix=True, address=node.address),
        _start_driver_thread(unix=True, address=node.address),
    ]

    mol0 = SimpleNamespace(hub=hub, init_payload={"dt_au": 0.25})
    mol1 = SimpleNamespace(hub=hub, init_payload={"dt_au": 0.25})

    node.append(mol0)
    node.append([mol1])

    assert mol0.init_payload["aggregate_group"] == node.group_id
    assert mol1.init_payload["aggregate_group"] == node.group_id
    assert node.unixsocket == node.address
    assert node.unixsocket_path is not None

    mid0 = hub.register_molecule_return_id()
    mid1 = hub.register_molecule_return_id()
    init_payloads = {
        mid0: dict(mol0.init_payload),
        mid1: dict(mol1.init_payload),
    }
    requests = {
        mid0: {
            "efield_au": np.array([1.5, -0.5, 0.25]),
            "init": init_payloads[mid0],
        },
        mid1: {
            "efield_au": np.array([-2.0, 1.0, 0.75]),
            "init": init_payloads[mid1],
        },
    }

    try:
        assert hub.wait_until_bound(init_payloads, require_init=True, timeout=10.0)
        responses = hub.step_barrier(requests, timeout=10.0)

        assert set(responses.keys()) == {mid0, mid1}
        np.testing.assert_allclose(
            responses[mid0]["amp"],
            (mid0 + 1.0) * requests[mid0]["efield_au"],
            rtol=0.0,
            atol=1e-12,
        )
        np.testing.assert_allclose(
            responses[mid1]["amp"],
            (mid1 + 1.0) * requests[mid1]["efield_au"],
            rtol=0.0,
            atol=1e-12,
        )
    finally:
        unixsocket_path = node.unixsocket_path
        try:
            hub.stop()
        except Exception:
            pass
        for thread in driver_threads:
            thread.join(timeout=2.0)

    assert unixsocket_path is not None
    assert not Path(unixsocket_path).exists()


@pytest.mark.core
@pytest.mark.parametrize("local_transport", ["unix", "tcp"])
def test_aggregated_socket_hub_roundtrip(local_transport: str):
    """
    End-to-end test for the two-layer aggregate socket flow.

    Topology:
        AggregatedSocketHub (upstream TCP)
            -> one LocalSocketHubBridge
            -> two normal downstream drivers through an ordinary SocketHub
    """

    upstream_port = _pick_free_port()
    hub = AggregatedSocketHub(
        host="127.0.0.1",
        port=upstream_port,
        timeout=10.0,
        latency=1e-4,
    )

    if local_transport == "unix":
        unix_name = f"agg_bridge_{time.time_ns()}"
        bridge = LocalSocketHubBridge(
            group_id="node-a",
            upstream_host="127.0.0.1",
            upstream_port=upstream_port,
            timeout=10.0,
            latency=1e-4,
            local_unixsocket=unix_name,
        )
        driver_threads = [
            _start_driver_thread(unix=True, address=unix_name),
            _start_driver_thread(unix=True, address=unix_name),
        ]
    else:
        downstream_port = _pick_free_port()
        bridge = LocalSocketHubBridge(
            group_id="node-a",
            upstream_host="127.0.0.1",
            upstream_port=upstream_port,
            timeout=10.0,
            latency=1e-4,
            local_host="127.0.0.1",
            local_port=downstream_port,
        )
        driver_threads = [
            _start_driver_thread(
                unix=False, address="127.0.0.1", port=downstream_port
            ),
            _start_driver_thread(
                unix=False, address="127.0.0.1", port=downstream_port
            ),
        ]

    bridge.start()

    mid0 = hub.register_molecule_return_id()
    mid1 = hub.register_molecule_return_id()
    init_payloads = {
        mid0: {"aggregate_group": "node-a", "dt_au": 0.25},
        mid1: {"aggregate_group": "node-a", "dt_au": 0.25},
    }

    try:
        assert hub.wait_until_bound(init_payloads, require_init=True, timeout=10.0)

        requests = {
            mid0: {
                "efield_au": np.array([1.0, -2.0, 0.5]),
                "init": init_payloads[mid0],
            },
            mid1: {
                "efield_au": np.array([0.25, 4.0, -1.5]),
                "init": init_payloads[mid1],
            },
        }
        responses = hub.step_barrier(requests, timeout=10.0)

        assert set(responses.keys()) == {mid0, mid1}
        np.testing.assert_allclose(
            responses[mid0]["amp"],
            (mid0 + 1.0) * requests[mid0]["efield_au"],
            rtol=0.0,
            atol=1e-12,
        )
        np.testing.assert_allclose(
            responses[mid1]["amp"],
            (mid1 + 1.0) * requests[mid1]["efield_au"],
            rtol=0.0,
            atol=1e-12,
        )

        extra0 = json.loads(responses[mid0]["extra"].decode("utf-8"))
        extra1 = json.loads(responses[mid1]["extra"].decode("utf-8"))
        assert extra0["molecule_id"] == mid0
        assert extra1["molecule_id"] == mid1
        assert extra0["time_au"] == pytest.approx(0.25)
        assert extra1["time_au"] == pytest.approx(0.25)

        # After the aggregate group is established, a later barrier should not
        # need the init payload again; the hub should reuse the saved mapping.
        second_requests = {
            mid0: {
                "efield_au": np.array([-3.0, 0.0, 2.0]),
            },
            mid1: {
                "efield_au": np.array([5.0, -1.0, 0.25]),
            },
        }
        second_responses = hub.step_barrier(second_requests, timeout=10.0)
        np.testing.assert_allclose(
            second_responses[mid0]["amp"],
            (mid0 + 1.0) * second_requests[mid0]["efield_au"],
            rtol=0.0,
            atol=1e-12,
        )
        np.testing.assert_allclose(
            second_responses[mid1]["amp"],
            (mid1 + 1.0) * second_requests[mid1]["efield_au"],
            rtol=0.0,
            atol=1e-12,
        )

        extra0_b = json.loads(second_responses[mid0]["extra"].decode("utf-8"))
        extra1_b = json.loads(second_responses[mid1]["extra"].decode("utf-8"))
        assert extra0_b["time_au"] == pytest.approx(0.50)
        assert extra1_b["time_au"] == pytest.approx(0.50)

    finally:
        try:
            hub.stop()
        except Exception:
            pass
        try:
            bridge.stop()
        except Exception:
            pass
        for thread in driver_threads:
            thread.join(timeout=2.0)
