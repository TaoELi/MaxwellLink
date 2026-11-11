# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Round-trip tests for the hot-path JSON helpers.
"""

import numpy as np
import pytest

pytest.importorskip("maxwelllink", reason="maxwelllink is required for this test")

from maxwelllink.sockets.protocol import _json_dumps_bytes  # noqa: E402
from maxwelllink.tools.fast_json import json_dumps, json_loads  # noqa: E402


@pytest.mark.core
def test_round_trip_of_driver_payload_types():
    """Every scalar type a driver's append_additional_data() may return survives."""
    payload = {
        "time_au": 1.5,
        "energy_au": np.float64(-0.25),  # stdlib json accepts this; orjson needs a flag
        "potential_au": np.float32(0.5),
        "n_steps": np.int64(7),
        "mu_au": np.zeros(3),
        "label": "co2jcp2021",
        "converged": True,
        "missing": None,
    }
    decoded = json_loads(json_dumps(payload))
    assert decoded["time_au"] == 1.5
    assert decoded["energy_au"] == -0.25
    assert decoded["potential_au"] == 0.5
    assert decoded["n_steps"] == 7
    assert decoded["mu_au"] == [0.0, 0.0, 0.0]
    assert decoded["label"] == "co2jcp2021"
    assert decoded["converged"] is True
    assert decoded["missing"] is None


@pytest.mark.core
def test_keys_are_sorted_so_framing_is_deterministic():
    """The HELLO/INIT framing relies on the same payload giving the same bytes."""
    assert json_dumps({"z": 1.0, "a": 2.0}) == b'{"a":2.0,"z":1.0}'
    assert json_dumps({"a": 2.0, "z": 1.0}) == json_dumps({"z": 1.0, "a": 2.0})


@pytest.mark.core
def test_protocol_helper_uses_the_shared_encoder():
    """``_json_dumps_bytes`` is the same encoder, so both paths stay in step."""
    payload = {"group_id": "bridge_0", "version": 1}
    assert _json_dumps_bytes(payload) == json_dumps(payload)


@pytest.mark.core
def test_float_values_survive_the_round_trip_exactly():
    """Formatting may differ from stdlib json, but the decoded floats must not."""
    rng = np.random.default_rng(0)
    values = rng.normal(0.0, 1.0, 2000) * 10.0 ** rng.integers(-20, 20, 2000)
    for value in values:
        assert json_loads(json_dumps({"v": float(value)}))["v"] == float(value)
