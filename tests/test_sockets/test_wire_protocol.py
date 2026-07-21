# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
Golden-bytes tests for the socket wire formats.

Every byte layout asserted here is shared with external clients (the LAMMPS
``fix mxl`` driver, ``mxl_bridge`` nodes, and Meep's C-level
``MXLSocketSusceptibility``). These tests pin the formats so the planned
sockets/ refactor (protocol.py extraction, hub re-basing) provably moves code
without touching the wire. Expected buffers are built by hand with ``struct``
on purpose — round-tripping through the same codec would not catch a layout
change.
"""

from __future__ import annotations

import json
import socket
import struct
import sys
import threading
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

SRC_ROOT = Path(__file__).resolve().parents[2] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from maxwelllink.sockets import sockets as S
from maxwelllink.sockets import aggregated as A
from maxwelllink.sockets.aggregated_susceptibility import (
    AggregatedSusceptibilitySocketHub,
    _AggregatedSusceptibilitySocketHubServer,
)
from maxwelllink.sockets.susceptibility import (
    SusceptibilitySocketHub,
    _SusceptibilitySocketHubServer,
)

from socket_test_helpers import can_create_sockets, pick_free_port

pytestmark = pytest.mark.skipif(
    not can_create_sockets(),
    reason="socket creation is not permitted in this environment",
)

EYE3_BYTES = np.ascontiguousarray(np.eye(3, dtype=np.float64)).tobytes()


# ---------------------------------------------------------------------------
# i-PI fast path: the 196-byte request and 124-byte reply used by step_barrier
# ---------------------------------------------------------------------------


@pytest.mark.core
def test_fast_path_send_blob_is_frozen():
    """The FIELDDATA+GETSOURCE template must keep its exact byte layout."""

    expected = (
        b"POSDATA".ljust(12, b" ")
        + EYE3_BYTES
        + EYE3_BYTES
        + struct.pack("<i", 1)
        + b"\x00" * 24
        + b"GETFORCE".ljust(12, b" ")
    )
    assert bytes(S._SEND_TEMPLATE) == expected
    assert S._SEND_FIELD_OFFSET == 160
    assert S._SEND_TOTAL_LEN == 196
    assert S._REPLY_FIXED_LEN == 124

    blob = bytearray(S._SEND_TEMPLATE)
    struct.pack_into("<3d", blob, S._SEND_FIELD_OFFSET, 1.5, -2.0, 0.25)
    assert struct.unpack_from("<3d", blob, 160) == (1.5, -2.0, 0.25)
    # Patching the field window must not disturb the surrounding template.
    assert blob[:160] == expected[:160]
    assert blob[184:] == expected[184:]


@pytest.mark.core
def test_source_ready_reply_bytes_and_hub_parse():
    """_send_force_ready bytes match the documented layout and the hub's parser."""

    amp = np.array([[0.5, -1.25, 2.0]])
    extra = json.dumps({"energy_au": 0.125}, separators=(",", ":")).encode()
    left, right = socket.socketpair()
    try:
        sender = threading.Thread(
            target=S._send_force_ready,
            kwargs=dict(
                sock=left,
                energy_ha=3.5,
                forces_Nx3_ha_per_bohr=amp,
                virial_3x3_ha=np.zeros((3, 3)),
                more=extra,
            ),
        )
        sender.start()

        raw = b""
        expected_len = 124 + len(extra)
        while len(raw) < expected_len:
            chunk = right.recv(expected_len - len(raw))
            assert chunk, "peer closed early"
            raw += chunk
        sender.join(timeout=5.0)

        assert raw[:12] == b"FORCEREADY".ljust(12, b" ")
        assert struct.unpack_from("<d", raw, 12)[0] == 3.5
        assert struct.unpack_from("<i", raw, 20)[0] == 1
        assert struct.unpack_from("<3d", raw, 24) == (0.5, -1.25, 2.0)
        assert struct.unpack_from("<i", raw, 120)[0] == len(extra)
        assert raw[124:] == extra
    finally:
        left.close()
        right.close()

    # Feed the identical bytes to the hub-side fast-path parser.
    left, right = socket.socketpair()
    try:
        hub = object.__new__(S.SocketHub)
        hub._scratch_recv = bytearray(S._REPLY_FIXED_LEN)
        hub._scratch_recv_mv = memoryview(hub._scratch_recv)
        st = S._ClientState(sock=right, address="test", molecule_id=0)

        left.sendall(raw)
        got_amp, got_extra = hub._read_source_ready(st)
        np.testing.assert_allclose(got_amp, amp[0], rtol=0.0, atol=0.0)
        assert got_extra == extra
        assert st.pending_send is False
        np.testing.assert_allclose(st.last_amp, amp[0], rtol=0.0, atol=0.0)
    finally:
        left.close()
        right.close()


@pytest.mark.core
def test_source_ready_rejects_wrong_header_and_nat():
    """The fast-path parser must reject non-SOURCEREADY headers and nat != 1."""

    for bad in (
        b"BANANAS".ljust(12, b" ") + b"\x00" * 112,
        b"FORCEREADY".ljust(12, b" ")
        + struct.pack("<d", 0.0)
        + struct.pack("<i", 2)  # nat=2 violates the EM contract
        + b"\x00" * 100,
    ):
        left, right = socket.socketpair()
        try:
            hub = object.__new__(S.SocketHub)
            hub._scratch_recv = bytearray(S._REPLY_FIXED_LEN)
            hub._scratch_recv_mv = memoryview(hub._scratch_recv)
            st = S._ClientState(sock=right, address="test", molecule_id=0)
            left.sendall(bad)
            with pytest.raises(S._SocketClosed):
                hub._read_source_ready(st)
        finally:
            left.close()
            right.close()


@pytest.mark.core
def test_pack_init_wire_format():
    """INIT = header + molecule_id int32 + length-prefixed JSON payload."""

    payload = {"molecule_id": 7, "dt_au": 0.5, "rescaling_factor": 3.0}
    left, right = socket.socketpair()
    try:
        S._pack_init(left, payload)
        assert S._recv_msg(right) == S.INIT
        assert S._recv_int(right) == 7
        assert json.loads(S._recv_bytes(right).decode("utf-8")) == payload
    finally:
        left.close()
        right.close()


# ---------------------------------------------------------------------------
# Aggregate frames: AGGSTEP fan-out and AGGRESULT replies
# ---------------------------------------------------------------------------


def _recv_exactly(sock: socket.socket, n: int) -> bytes:
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        assert chunk, "peer closed early"
        data += chunk
    return data


@pytest.mark.core
def test_aggstep_frame_bytes_and_dedup():
    """AGGSTEP packs unique fields once and references them per molecule."""

    requests = {
        3: {"efield_au": np.array([1.0, 2.0, 3.0])},
        5: {"efield_au": np.array([1.0, 2.0, 3.0])},  # shares field with 3
        9: {"efield_au": np.array([-4.0, 0.5, 6.0])},
    }
    left, right = socket.socketpair()
    try:
        A._StepCodec().send(left, requests)
        head = _recv_exactly(right, 20)
        assert head[:12] == b"AGGSTEP".ljust(12, b" ")
        nreq = struct.unpack_from("<i", head, 12)[0]
        nuniq = struct.unpack_from("<i", head, 16)[0]
        assert (nreq, nuniq) == (3, 2)

        body = _recv_exactly(right, 24 * nuniq + 8 * nreq)
        assert struct.unpack_from("<3d", body, 0) == (1.0, 2.0, 3.0)
        assert struct.unpack_from("<3d", body, 24) == (-4.0, 0.5, 6.0)
        members = [struct.unpack_from("<2i", body, 48 + 8 * k) for k in range(nreq)]
        assert members == [(3, 0), (5, 0), (9, 1)]
    finally:
        left.close()
        right.close()

    # Decode a hand-built frame and verify per-molecule routing.
    frame = (
        b"AGGSTEP".ljust(12, b" ")
        + struct.pack("<2i", 2, 1)
        + struct.pack("<3d", 7.0, -8.0, 9.0)
        + struct.pack("<2i", 11, 0)
        + struct.pack("<2i", 4, 0)
    )
    left, right = socket.socketpair()
    try:
        left.sendall(frame)
        decoded = A._StepCodec().recv(right)
        assert set(decoded.keys()) == {11, 4}
        np.testing.assert_allclose(decoded[11], [7.0, -8.0, 9.0])
        np.testing.assert_allclose(decoded[4], [7.0, -8.0, 9.0])
        # Characterization note: molecules sharing an upstream field alias the
        # SAME decoded array (the recv docstring claims separate copies — the
        # docstring is wrong, the aliasing is the actual contract and is safe
        # because step_barrier copies fields into its frozen barrier).
        assert decoded[11] is decoded[4]
    finally:
        left.close()
        right.close()


@pytest.mark.core
def test_aggresult_frame_bytes_and_decode():
    """AGGRESULT packs fixed records first, then concatenated extra blobs."""

    responses = {
        2: {"amp": np.array([0.1, 0.2, 0.3]), "extra": b"abc"},
        8: {"amp": np.array([-1.0, 0.0, 1.0]), "extra": ""},
    }
    left, right = socket.socketpair()
    try:
        A._ResultCodec().send(left, responses)
        head = _recv_exactly(right, 16)
        assert head[:12] == b"AGGRESULT".ljust(12, b" ")
        nresp = struct.unpack_from("<i", head, 12)[0]
        assert nresp == 2

        fixed = _recv_exactly(right, 32 * nresp)
        assert struct.unpack_from("<i", fixed, 0)[0] == 2
        assert struct.unpack_from("<3d", fixed, 4) == (0.1, 0.2, 0.3)
        assert struct.unpack_from("<i", fixed, 28)[0] == 3
        assert struct.unpack_from("<i", fixed, 32)[0] == 8
        assert struct.unpack_from("<3d", fixed, 36) == (-1.0, 0.0, 1.0)
        assert struct.unpack_from("<i", fixed, 60)[0] == 0
        assert _recv_exactly(right, 3) == b"abc"
    finally:
        left.close()
        right.close()

    frame = (
        b"AGGRESULT".ljust(12, b" ")
        + struct.pack("<i", 1)
        + struct.pack("<i", 6)
        + struct.pack("<3d", 4.0, 5.0, 6.0)
        + struct.pack("<i", 2)
        + b"hi"
    )
    left, right = socket.socketpair()
    try:
        left.sendall(frame)
        decoded = A._ResultCodec().recv(right)
        assert set(decoded.keys()) == {6}
        np.testing.assert_allclose(decoded[6]["amp"], [4.0, 5.0, 6.0])
        assert decoded[6]["extra"] == b"hi"
    finally:
        left.close()
        right.close()


# ---------------------------------------------------------------------------
# Driver INIT payloads built by the Meep susceptibility servers
# ---------------------------------------------------------------------------

RANK_INIT = {
    "protocol": "mxl_socket_susceptibility_v1",
    "rank": 2,
    "molecule_ids": [4, 7],
    "dt_au": 0.5,
    "rescaling_factor": 3.0,
    "time_units_fs": 3.33564,
}


@pytest.mark.core
def test_susceptibility_driver_init_payload_keys():
    """LAMMPS/SHO drivers rely on exactly these INIT payload keys."""

    server = object.__new__(_SusceptibilitySocketHubServer)
    server._lock = threading.RLock()
    server.expected = set()
    server.bound = {}

    payloads = server._register_rank_molecules(dict(RANK_INIT), [4, 7])
    assert set(payloads.keys()) == {4, 7}
    assert payloads[4] == {
        "molecule_id": 4,
        "dt_au": 0.5,
        "mxl_rank": 2,
        "rescaling_factor": 3.0,
        "time_units_fs": 3.33564,
    }
    assert server.expected == {4, 7}
    assert server.bound == {4: None, 7: None}


@pytest.mark.core
def test_aggregated_susceptibility_driver_init_payload_keys():
    """The aggregated server adds exactly one key: the aggregate_group."""

    server = object.__new__(_AggregatedSusceptibilitySocketHubServer)
    server._lock = threading.RLock()
    server._meep_lock = threading.RLock()
    server.expected = set()
    server.bound = {}
    server._mxl_molecule_to_group = {}
    server._group_ids = ["g0"]
    server._group_loads = {"g0": 0}
    server._group_capacities = None

    payloads = server._register_rank_molecules(dict(RANK_INIT), [4, 7])
    assert payloads[4] == {
        "molecule_id": 4,
        "dt_au": 0.5,
        "mxl_rank": 2,
        "rescaling_factor": 3.0,
        "time_units_fs": 3.33564,
        "aggregate_group": "g0",
    }


# ---------------------------------------------------------------------------
# Bridge manifest schema and operator-facing command strings
# ---------------------------------------------------------------------------


@pytest.mark.core
def test_init_remote_bridges_manifest_schema(tmp_path):
    """mxl_bridge --info parses this manifest; its schema is frozen."""

    hub = A.AggregatedSocketHub(
        host="127.0.0.1", port=pick_free_port(), timeout=5.0, latency=1e-4
    )
    try:
        molecules = [
            SimpleNamespace(hub=hub, init_payload={"dt_au": 0.25}) for _ in range(3)
        ]
        save_file = tmp_path / "aggregation.json"
        specs = hub.init_remote_bridges(
            molecules, molecules_per_bridge=2, unix_prefix="b_", save_file=save_file
        )

        assert [spec.to_dict() for spec in specs] == [
            {"idx": 0, "group_id": "b_0", "unixsocket": "b_0", "n_molecules": 2},
            {"idx": 1, "group_id": "b_1", "unixsocket": "b_1", "n_molecules": 1},
        ]
        assert [mol.init_payload["aggregate_group"] for mol in molecules] == [
            "b_0",
            "b_0",
            "b_1",
        ]

        manifest = json.loads(save_file.read_text(encoding="utf-8"))
        assert set(manifest.keys()) == {
            "version",
            "hub_host",
            "hub_port",
            "timeout",
            "latency",
            "unix_prefix",
            "molecules_per_bridge",
            "bridges",
        }
        assert manifest["version"] == A.AGGREGATION_INFO_VERSION == 1
        assert manifest["hub_host"] == "127.0.0.1"
        assert manifest["hub_port"] == hub._bridge_connect_port
        assert manifest["molecules_per_bridge"] == 2
        assert manifest["bridges"] == [spec.to_dict() for spec in specs]
        assert A.RemoteBridgeSpec.from_dict(manifest["bridges"][0]) == specs[0]
    finally:
        hub.stop()


@pytest.mark.core
def test_bridge_and_driver_command_strings():
    """Operators copy-paste these command strings; their shape is frozen."""

    hub = object.__new__(AggregatedSusceptibilitySocketHub)
    hub.timeout = 10.0
    hub.bridge_manifest = "mxl_bridge_manifest.json"
    hub._bridge_info = {}

    assert (
        hub.bridge_command(3) == "mxl_bridge --info mxl_bridge_manifest.json --idx 3"
    )
    assert (
        hub.bridge_command(0, info="custom.json")
        == "mxl_bridge --info custom.json --idx 0"
    )

    template = hub.driver_command_template(
        omega_au=1.5198298460570259, mu0_au=187.0819866, orientation=2
    )
    assert template.startswith("/bin/bash -c ")
    assert "{unixsocket}" in template
    assert (
        "mxl_driver --unix --address {unixsocket} --model sho "
        "--param omega=1.5198298460570259,mu0=187.08198659999999,orientation=2"
        in template
    )


# ---------------------------------------------------------------------------
# Lorentzian -> SHO conversion: the physics constants behind rescaling_factor
# ---------------------------------------------------------------------------

LORENTZIAN_ARGS = dict(
    frequency=1.0,
    sigma=0.3,
    resolution=25.0,
    dimensions=3,
    time_units_fs=0.1,
    mu0_au=187.0819866,
    orientation=2,
)
GOLDEN_RESCALING = 0.13540603890461023
GOLDEN_OMEGA_AU = 1.5198298460570259


def _fake_simple_hub():
    hub = object.__new__(SusceptibilitySocketHub)
    hub.host, hub.port = "127.0.0.1", 12345
    return hub


def _fake_aggregated_hub():
    hub = object.__new__(AggregatedSusceptibilitySocketHub)
    hub.timeout = 10.0
    hub.bridge_manifest = "mxl_bridge_manifest.json"
    hub._bridge_info = {}
    return hub


@pytest.mark.core
def test_lorentzian_conversion_golden_values():
    """Both hubs must keep producing today's numbers for the same Lorentzian."""

    simple = SusceptibilitySocketHub.lorentzian_conversion(
        _fake_simple_hub(), **LORENTZIAN_ARGS
    )
    assert simple["rescaling_factor"] == pytest.approx(GOLDEN_RESCALING, rel=1e-15)
    assert simple["driver_command"] == (
        "mxl_driver --model sho --address 127.0.0.1 --port 12345 "
        f'--param "omega={GOLDEN_OMEGA_AU:.17g},mu0=187.08198659999999,orientation=2"'
    )

    aggregated = AggregatedSusceptibilitySocketHub.lorentzian_conversion(
        _fake_aggregated_hub(), **LORENTZIAN_ARGS
    )
    assert aggregated["rescaling_factor"] == simple["rescaling_factor"]
    assert aggregated["bridge_manifest"] == "mxl_bridge_manifest.json"
    assert aggregated["bridge_commands"] == []
    assert aggregated["bridge_specs"] == []


@pytest.mark.core
@pytest.mark.parametrize(
    "bad_kwargs",
    [
        {"frequency": 0.0},
        {"sigma": -0.1},
        {"resolution": 0.0},
        {"gamma": -1.0},
        {"dimensions": 0},
        {"mu0_au": 0.0},
        {"orientation": 3},
        {"time_units_fs": 0.0},
    ],
)
def test_lorentzian_conversion_validation(bad_kwargs):
    """Both hubs validate identically and raise ValueError on bad input."""

    args = {**LORENTZIAN_ARGS, **bad_kwargs}
    with pytest.raises(ValueError):
        SusceptibilitySocketHub.lorentzian_conversion(_fake_simple_hub(), **args)
    with pytest.raises(ValueError):
        AggregatedSusceptibilitySocketHub.lorentzian_conversion(
            _fake_aggregated_hub(), **args
        )
