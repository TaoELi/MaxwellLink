# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
The batched RT-TDDFTB driver over the whole aggregate socket path.
"""

import sys
import threading
from pathlib import Path

import numpy as np
import pytest

SRC_ROOT = Path(__file__).resolve().parents[2] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

import maxwelllink as mxl  # noqa: E402
from maxwelllink.sockets.aggregated import (  # noqa: E402
    AggregatedSocketHub,
    run_bridge_node,
)
from maxwelllink.units import unit  # noqa: E402

from socket_test_helpers import can_create_sockets, pick_free_port  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "test_dftb"))
from slko_helpers import sk_path  # noqa: E402

pytestmark = pytest.mark.skipif(
    not can_create_sockets(),
    reason="socket creation is not permitted in this environment",
)

N_SYSTEMS = 4
DT_AU = 0.2  # atomic units, the step the DFTB+ references were generated at
STEPS = 120
OMEGA_AU = 23.0 * unit("eV")  # onto the water transition the 3ob set gives

_ELEMENTS = ["O", "H", "H"]
_POSITIONS = [[0.0, 0.0, 0.1173], [0.0, 0.7572, -0.4692], [0.0, -0.7572, -0.4692]]


def _xp_or_skip(name):
    """Return the array module for ``name`` or skip if the GPU stack is absent."""

    if name == "numpy":
        return np
    cp = pytest.importorskip("cupy", reason="cupy not installed")
    pytest.importorskip("numba", reason="numba not installed")
    try:
        if cp.cuda.runtime.getDeviceCount() < 1:
            pytest.skip("No CUDA device available for the GPU batch model.")
    except Exception:  # pragma: no cover - cupy present but no usable driver
        pytest.skip("No usable CUDA runtime for the GPU batch model.")
    return cp


def _run_cavity(xp, tmp_path, tag, coupling_strength):
    """Drive ``N_SYSTEMS`` batched DFTB systems with one cavity mode, over the socket."""

    hub = AggregatedSocketHub(
        host="127.0.0.1", port=pick_free_port(), timeout=180.0, latency=1e-4
    )
    molecules = [mxl.Molecule(hub=hub) for _ in range(N_SYSTEMS)]

    manifest = tmp_path / f"aggregation_rtdftb_{tag}.json"
    hub.init_remote_bridges(
        molecules,
        molecules_per_bridge=N_SYSTEMS,  # one group -> one batch model
        unix_prefix=f"rtdftbgpu_{tag}_",
        save_file=str(manifest),
    )

    # the batch driver takes exactly the scalar RTDFTBModel's arguments
    bridge_thread = threading.Thread(
        target=run_bridge_node,
        kwargs=dict(
            info=str(manifest),
            idx=0,
            backend="gpu",
            model="rtdftb",
            driver_kwargs=dict(
                sk_path=sk_path("3ob-3-1", skip=pytest.skip),
                elements=_ELEMENTS,
                positions=_POSITIONS,
            ),
            xp=xp,
        ),
        daemon=True,
    )
    bridge_thread.start()

    try:
        sim = mxl.SingleModeSimulation(
            dt_au=DT_AU,
            frequency_au=OMEGA_AU,
            damping_au=0.0,
            molecules=molecules,
            coupling_strength=coupling_strength,
            coupling_axis="z",
            hub=hub,
            qc_initial=[0.0, 0.0, 1.0e-2],
            record_history=True,
            drive=0.0,
        )
        init_payloads = {
            w.molecule_id: {**w.init_payload, "molecule_id": w.molecule_id}
            for w in sim.socket_wrappers
        }
        assert hub.wait_until_bound(
            init_payloads, require_init=True, timeout=120.0
        ), "GPU RT-DFTB batch bridge failed to bind to the aggregate hub."
        sim.run(steps=STEPS)
    finally:
        hub.stop()
        bridge_thread.join(timeout=20.0)

    return (
        np.asarray(sim.qc_history)[:, 2],
        np.asarray(sim.energy_history),
        len(sim.time_history),
    )


@pytest.mark.core
@pytest.mark.parametrize("xp_name", ["numpy", "cupy"])
def test_rtdftb_batch_drives_a_single_mode_cavity(xp_name, tmp_path):
    """The batched RT-TDDFTB driver runs the whole socket path and couples to the mode.

    The coupled run is compared against an uncoupled control rather than against a
    threshold on the cavity coordinate: with ``qc_initial`` non-zero and no damping, an
    *empty* cavity oscillates forever, so "the mode moved" is not evidence that the
    molecules were driven at all. The difference between the two runs is.
    """

    xp = _xp_or_skip(xp_name)

    qc, energy, n_recorded = _run_cavity(xp, tmp_path, f"{xp_name}_on", 5.0e-3)
    qc_free, _, _ = _run_cavity(xp, tmp_path, f"{xp_name}_off", 0.0)

    assert n_recorded == STEPS, "the run stopped early"
    assert np.all(np.isfinite(qc)), "the cavity coordinate diverged"
    assert np.all(np.isfinite(energy)), "the reported energy diverged"
    assert np.all(np.isfinite(qc_free))

    # the uncoupled control is a free oscillator: it moves, but the molecules cannot
    # have touched it
    assert qc_free.std() > 1e-6, "the control cavity did not oscillate"
    response = np.abs(qc - qc_free).max()
    assert response > 1e-6, (
        f"the molecules did not act back on the cavity (max |qc - qc_free| = "
        f"{response:.3e})"
    )
    # and the molecular energy must move too, not just the mode
    assert np.abs(energy - energy[0]).max() > 1e-12, "the molecules never responded"
