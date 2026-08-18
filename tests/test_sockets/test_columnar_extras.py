# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Equivalence tests for the columnar additional-data path.
"""

from __future__ import annotations

import sys
import threading
from pathlib import Path

import numpy as np
import pytest

SRC_ROOT = Path(__file__).resolve().parents[2] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

import maxwelllink as mxl  # noqa: E402
from maxwelllink.mxl_drivers.python.models import __drivers__  # noqa: E402
from maxwelllink.mxl_drivers.python.mxl_driver import run_driver  # noqa: E402
from maxwelllink.sockets.aggregated import (  # noqa: E402
    AggregatedSocketHub,
    LocalSocketHubBridge,
    run_bridge_node,
)
from maxwelllink.units import unit  # noqa: E402

from socket_test_helpers import can_create_sockets, pick_free_port  # noqa: E402

pytestmark = pytest.mark.skipif(
    not can_create_sockets(),
    reason="socket creation is not permitted in this environment",
)

N_SIDE = 6
N_MOLECULES = N_SIDE * N_SIDE
STEPS = 30
DT_AU = 0.5 * unit("fs")
OMEGA_AU = 2320.0 * unit("cm_inv")  # the CO2 asymmetric stretch


def _run(columnar, tmp_path, tag, extra_molecule=False):
    """Run one short multimode simulation and return its final state."""
    hub = AggregatedSocketHub(
        host="127.0.0.1", port=pick_free_port(), timeout=120.0, latency=1e-4
    )
    # the cavity grid is fixed, so one in-process driver replaces one socket one
    n_socket = N_MOLECULES - 1 if extra_molecule else N_MOLECULES
    molecules = [
        mxl.Molecule(hub=hub, store_additional_data=False) for _ in range(n_socket)
    ]
    if extra_molecule:  # an in-process driver no batch bridge can serve
        molecules.append(mxl.Molecule(driver="sho", driver_kwargs={}))

    manifest = tmp_path / f"aggregation_{tag}.json"
    hub.init_remote_bridges(
        [m for m in molecules if m.mode == "socket"],
        molecules_per_bridge=n_socket,
        unix_prefix=f"col_{tag}_",
        save_file=str(manifest),
    )
    bridge_thread = threading.Thread(
        target=run_bridge_node,
        kwargs=dict(
            info=str(manifest),
            idx=0,
            backend="gpu",
            model="md",
            driver_kwargs=dict(
                ff="co2jcp2021", thermostat="nve", pre_nvt=False, seed=3
            ),
            xp=np,  # the compiled CPU path: deterministic, and needs no CUDA
        ),
        daemon=True,
    )
    bridge_thread.start()

    cavity = mxl.FabryPerotCavity(
        frequency_au=OMEGA_AU,
        coupling_strength=1.0e-4,
        coupling_axis="xy",
        n_grid_x=N_SIDE,
        n_grid_y=N_SIDE,
        delta_omega_x_au=12.5 * unit("cm_inv"),
        delta_omega_y_au=12.5 * unit("cm_inv"),
        n_mode_x=N_SIDE,
        n_mode_y=N_SIDE,
        save_mode_functions=False,
    )
    try:
        sim = mxl.MultiModeSimulation(
            hub=hub,
            molecules=molecules,
            dt_au=DT_AU,
            damping_au=0.0,
            include_dse=True,  # also exercises the force-time dipole column
            cavity_geometry=cavity,
        )
        if not columnar:
            # the block reply is asked for automatically, so the reference run
            # takes it back before the bridge binds and gets JSON per molecule
            hub.request_columnar_extras(())
            sim.columnar_extras = False
        init_payloads = {
            w.molecule_id: {**w.init_payload, "molecule_id": w.molecule_id}
            for w in sim.socket_wrappers
        }
        assert hub.wait_until_bound(
            init_payloads, require_init=True, timeout=60.0
        ), "batch bridge failed to bind to the aggregate hub."
        sim.run(steps=STEPS, record_list=["energy"])
        state = (
            sim.qc.copy(),
            sim.pc.copy(),
            sim.dipole.copy(),
            sim.dipole_next.copy(),
            sim.dmudt.copy(),
            np.asarray(sim.energy_history),  # the molecular energy column too
            sim.columnar_extras,
        )
    finally:
        hub.stop()
        bridge_thread.join(timeout=15.0)
    return state


@pytest.mark.core
def test_columnar_reply_reproduces_the_json_reply(tmp_path):
    """The packed block gives exactly the trajectory the JSON documents give."""
    *json_state, json_used = _run(False, tmp_path, "json")
    *columnar_state, columnar_used = _run(True, tmp_path, "columnar")

    assert not json_used and columnar_used, "the two runs took the same path"
    names = ("qc", "pc", "dipole", "dipole_next", "dmudt")
    for name, expected, actual in zip(names, json_state, columnar_state):
        assert np.array_equal(expected, actual), f"{name} differs between the paths"

    # the energy is the one reduction: the block path sums the column with NumPy,
    # the JSON path adds the molecules one at a time, so they differ in the last bits
    np.testing.assert_allclose(json_state[-1], columnar_state[-1], rtol=0.0, atol=1e-12)


@pytest.mark.core
def test_columnar_falls_back_when_a_molecule_is_not_batched(tmp_path):
    """An in-process driver disables the fast path instead of dropping data."""
    *_, columnar_used = _run(True, tmp_path, "mixed", extra_molecule=True)
    assert not columnar_used


@pytest.mark.core
def test_individual_driver_bridge_keeps_the_per_molecule_reply():
    """A bridge that fans out to single drivers ignores the columnar request."""
    port = pick_free_port()
    hub = AggregatedSocketHub(host="127.0.0.1", port=port, timeout=10.0, latency=1e-4)
    hub.request_columnar_extras(("mux_au", "energy_au"))

    unix_name = f"col_single_{port}"
    bridge = LocalSocketHubBridge(
        group_id="node-a",
        upstream_host="127.0.0.1",
        upstream_port=port,
        timeout=10.0,
        latency=1e-4,
        local_unixsocket=unix_name,
    )
    threading.Thread(
        target=run_driver,
        kwargs=dict(
            unix=True,
            address=unix_name,
            timeout=10.0,
            driver=__drivers__["sho"](),
        ),
        daemon=True,
    ).start()
    bridge.start()

    mid = hub.register_molecule_return_id()
    init_payloads = {mid: {"aggregate_group": "node-a", "dt_au": DT_AU}}
    try:
        assert hub.wait_until_bound(init_payloads, require_init=True, timeout=10.0)
        responses = hub.step_barrier(
            {mid: {"efield_au": np.zeros(3), "init": init_payloads[mid]}},
            timeout=10.0,
        )
        assert set(responses) == {mid}  # answered molecule by molecule
        assert not hub.last_columnar  # and never as a block
    finally:
        hub.stop()
        bridge.stop(wait=1.0)


@pytest.mark.core
def test_hub_request_selects_the_columnar_result_format():
    """The request travels as a result format plus the column order."""
    hub = AggregatedSocketHub(host="127.0.0.1", port=0)
    assert hub._bridge_result_format == "full"
    hub.request_columnar_extras(("mux_au", "energy_au"))
    assert hub._bridge_result_format == "columnar"
    assert hub._bridge_extra_keys == ("mux_au", "energy_au")
    hub.request_columnar_extras(())  # asking for nothing restores the JSON reply
    assert hub._bridge_result_format == "full"
