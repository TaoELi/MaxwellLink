import json

import numpy as np
import pytest

from maxwelllink.sockets.ucx_protocol import (
    BYE,
    HELLO,
    INIT,
    STEP_REQUEST,
    STEP_RESPONSE,
    pack_bye,
    pack_hello,
    pack_init,
    pack_message,
    pack_step_request,
    pack_step_response,
    unpack_hello,
    unpack_init,
    unpack_message,
    unpack_step_request,
    unpack_step_response,
)


def test_ucx_protocol_roundtrip_init_and_hello():
    hello = pack_hello({"hostname": "node0", "pid": 42, "transport": "ucx"})
    message = unpack_message(hello)
    assert message.opcode == HELLO
    assert unpack_hello(message.payload) == {
        "hostname": "node0",
        "pid": 42,
        "transport": "ucx",
    }

    init = pack_init({"molecule_id": 7, "dt_au": 0.25, "label": "mol7"})
    message = unpack_message(init)
    assert message.opcode == INIT
    assert unpack_init(message.payload) == {
        "dt_au": 0.25,
        "label": "mol7",
        "molecule_id": 7,
    }


def test_ucx_protocol_roundtrip_step_messages():
    request = pack_step_request([1.0, -2.0, 3.5])
    message = unpack_message(request)
    assert message.opcode == STEP_REQUEST
    np.testing.assert_allclose(unpack_step_request(message.payload), [1.0, -2.0, 3.5])

    extra = json.dumps({"energy_au": -1.23}).encode("utf-8")
    response = pack_step_response([0.1, 0.2, 0.3], extra=extra)
    message = unpack_message(response)
    assert message.opcode == STEP_RESPONSE
    amp, more = unpack_step_response(message.payload)
    np.testing.assert_allclose(amp, [0.1, 0.2, 0.3])
    assert more == extra

    bye = unpack_message(pack_bye())
    assert bye.opcode == BYE
    assert bye.payload == b""


def test_ucx_protocol_rejects_bad_lengths():
    broken = pack_message(STEP_RESPONSE, b"\x00" * 5)
    message = unpack_message(broken)
    assert message.opcode == STEP_RESPONSE
    with pytest.raises(ValueError):
        unpack_step_response(message.payload)

    with pytest.raises(ValueError):
        unpack_message(b"short")
