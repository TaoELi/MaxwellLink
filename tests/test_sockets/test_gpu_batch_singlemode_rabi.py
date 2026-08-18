# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""Integrated physics test of the GPU-batched SHO driver.

This test wires the **whole stack** together exactly as a production run would 
and checks the collective (N-molecule) Rabi splitting.
"""

from __future__ import annotations

import sys
import threading
from pathlib import Path

import numpy as np
import pytest

# Make ``maxwelllink`` and the shared socket helpers importable from the tree.
SRC_ROOT = Path(__file__).resolve().parents[2] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

import maxwelllink as mxl  # noqa: E402
from maxwelllink.sockets.aggregated import (  # noqa: E402
    AggregatedSocketHub,
    run_bridge_node,
)

from socket_test_helpers import can_create_sockets, pick_free_port  # noqa: E402

pytestmark = pytest.mark.skipif(
    not can_create_sockets(),
    reason="socket creation is not permitted in this environment",
)

# --------------------------------------------------------------------------- #
# Physical parameters (atomic units)                                          #
# --------------------------------------------------------------------------- #
OMEGA = 0.242  # SHO frequency == cavity frequency (exact resonance)
MU0 = 187.0  # SHO dipole prefactor  mu(t) = mu0 * q(t)
DT_AU = 0.5  # EM/molecular time step
EPS0 = 3.2e-5  # single-molecule coupling_strength (g0 = EPS0*MU0 ~ 6e-3 a.u.)
QC0 = 1.0e-3  # initial cavity displacement along z; molecules start at rest
STEPS = 800  # ~1.5 Rabi periods for the resonant single-molecule coupling


def _xp_or_skip(name: str):
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


def _run_coupled_rabi(xp, n_osc, coupling_strength, tmp_path, tag):
    """Run one single-mode-cavity + ``n_osc`` GPU-batched SHO simulation.

    The cavity is excited (``qc0`` along z) while every oscillator starts at
    rest, so energy flows cavity -> molecules -> cavity at the Rabi period.

    Returns
    -------
    (t, e_cav, e_tot) : tuple of numpy.ndarray
        Time axis, bare cavity energy ``0.5 (pc^2 + omega^2 qc^2)`` along the
        coupling axis, and the total (cavity + molecular) energy per step.
    """

    hub = AggregatedSocketHub(
        host="127.0.0.1", port=pick_free_port(), timeout=60.0, latency=1e-4
    )
    # Socket-mode molecules: their driver lives behind the bridge, not in-process.
    molecules = [mxl.Molecule(hub=hub) for _ in range(n_osc)]

    # Topology-only manifest; the model/params are chosen on the bridge below.
    manifest = tmp_path / f"aggregation_{tag}.json"
    hub.init_remote_bridges(
        molecules,
        molecules_per_bridge=n_osc,  # one group -> one batch model
        unix_prefix=f"rabi_{tag}_",
        save_file=str(manifest),
    )

    # One GPU batch bridge serves the whole group.  xp=numpy runs the vectorized
    # model on the CPU; xp=cupy runs the fused numba.cuda kernel on the device.
    driver_kwargs = {"omega": OMEGA, "mu0": MU0, "orientation": 2}
    bridge_thread = threading.Thread(
        target=run_bridge_node,
        kwargs=dict(
            info=str(manifest),
            idx=0,
            backend="gpu",
            model="sho",
            driver_kwargs=driver_kwargs,
            xp=xp,
        ),
        daemon=True,
    )
    bridge_thread.start()

    try:
        sim = mxl.SingleModeSimulation(
            dt_au=DT_AU,
            frequency_au=OMEGA,
            damping_au=0.0,
            molecules=molecules,
            coupling_strength=coupling_strength,
            coupling_axis="z",  # matches the SHO dipole orientation (z)
            hub=hub,
            qc_initial=[0.0, 0.0, QC0],
            include_dse=False,
            record_history=True,
            drive=0.0,
        )

        # Bounded bind so a broken bridge fails fast instead of hanging forever.
        init_payloads = {
            w.molecule_id: {**w.init_payload, "molecule_id": w.molecule_id}
            for w in sim.socket_wrappers
        }
        assert hub.wait_until_bound(
            init_payloads, require_init=True, timeout=30.0
        ), "GPU batch bridge failed to bind to the aggregate hub."

        sim.run(steps=STEPS)  # SingleModeSimulation.run() stops the hub at the end
    finally:
        hub.stop()
        bridge_thread.join(timeout=10.0)

    t = np.asarray(sim.time_history)
    qc = np.asarray(sim.qc_history)[:, 2]  # z component of the cavity coordinate
    pc = np.asarray(sim.pc_history)[:, 2]
    e_cav = 0.5 * (pc**2 + OMEGA**2 * qc**2)
    e_tot = np.asarray(sim.energy_history)
    return t, e_cav, e_tot


def _smooth(y, window):
    """Odd-length moving average (removes the fast 2*omega ripple on E_cav)."""

    window = int(window) | 1
    kernel = np.ones(window) / window
    return np.convolve(y, kernel, mode="same")


def _first_transfer_time(t, e_cav):
    """Time of the first Rabi energy transfer out of the cavity (~ T_Rabi / 2).

    ``E_cav`` carries a fast ``2*omega`` ripple on top of the slow Rabi
    envelope, so it is smoothed over roughly one bare period before the first
    minimum that drops well below the starting cavity energy is located.
    """

    dt = t[1] - t[0]
    bare_period_steps = (2.0 * np.pi / OMEGA) / dt
    env = _smooth(e_cav, bare_period_steps)
    threshold = 0.5 * env[0]
    for i in range(1, len(env) - 1):
        if env[i] < threshold and env[i] <= env[i - 1] and env[i] <= env[i + 1]:
            return t[i]
    return t[int(np.argmin(env))]


def _analytic_half_rabi(coupling_strength, n_osc):
    """Analytic time of the first energy transfer, ``pi / Omega_Rabi``.

    Two resonant oscillators coupled by the collective ``g_N`` have polariton
    frequencies ``sqrt(omega^2 +/- g_N)``; their difference is the Rabi
    splitting ``Omega_Rabi`` and energy fully returns to the cavity after
    ``2*pi / Omega_Rabi``.
    """

    g_collective = coupling_strength * MU0 * np.sqrt(n_osc)
    omega_plus = np.sqrt(OMEGA**2 + g_collective)
    omega_minus = np.sqrt(OMEGA**2 - g_collective)
    rabi = omega_plus - omega_minus
    return np.pi / rabi


# --------------------------------------------------------------------------- #
# Positive test: with coupling ~ 1/sqrt(N) the Rabi dynamics are N-invariant
# --------------------------------------------------------------------------- #


@pytest.mark.core
@pytest.mark.parametrize("xp_name", ["numpy", "cupy"])
def test_rabi_dynamics_invariant_under_sqrtN_coupling(xp_name, tmp_path):
    xp = _xp_or_skip(xp_name)

    n_list = [1, 2, 4]
    curves = {}  # N -> (t, e_cav)
    transfer_times = {}
    for n_osc in n_list:
        coupling = EPS0 / np.sqrt(n_osc)  # <-- 1/sqrt(N) rescaling
        t, e_cav, e_tot = _run_coupled_rabi(
            xp, n_osc, coupling, tmp_path, tag=f"pos{n_osc}"
        )

        # A genuine Rabi transfer must occur: the cavity energy has to nearly
        # empty (guards against a dead / decoupled simulation trivially passing).
        assert (
            e_cav.min() < 0.2 * e_cav[0]
        ), f"N={n_osc}: cavity never released its energy (no Rabi oscillation)."
        # Undamped, undriven -> energy is conserved (guards against blow-up).
        drift = (e_tot.max() - e_tot.min()) / max(abs(e_tot).max(), 1e-30)
        assert drift < 1e-2, f"N={n_osc}: energy drift {drift:.2e} too large."
        # The measured transfer time matches the analytic collective Rabi result.
        t_transfer = _first_transfer_time(t, e_cav)
        t_analytic = _analytic_half_rabi(coupling, n_osc)
        assert abs(t_transfer - t_analytic) < 0.2 * t_analytic, (
            f"N={n_osc}: transfer time {t_transfer:.1f} a.u. disagrees with "
            f"analytic {t_analytic:.1f} a.u."
        )

        curves[n_osc] = (t, e_cav)
        transfer_times[n_osc] = t_transfer

    # (1) The transfer time is the same for every N (the collective coupling,
    #     and hence the Rabi period, is held fixed by the 1/sqrt(N) rescaling).
    spread = max(transfer_times.values()) - min(transfer_times.values())
    assert (
        spread <= 4.0 * DT_AU
    ), f"Rabi transfer time is not N-invariant: {transfer_times}."

    # (2) The full cavity-energy trajectories overlap across N.  For this linear
    #     light-matter system the invariance is exact, so the tolerance is tight.
    t1, c1 = curves[1]
    c1n = c1 / c1[0]
    for n_osc in n_list[1:]:
        tn, cn = curves[n_osc]
        m = min(len(c1n), len(cn))
        max_diff = float(np.max(np.abs(c1n[:m] - cn[:m] / cn[0])))
        assert max_diff < 1e-6, (
            f"N={n_osc}: cavity-energy trajectory deviates from N=1 by "
            f"{max_diff:.2e} despite 1/sqrt(N) coupling."
        )


# --------------------------------------------------------------------------- #
# Negative control: WITHOUT the 1/sqrt(N) rescaling the period must change,
# proving the test above actually depends on the collective coupling physics.
# --------------------------------------------------------------------------- #


@pytest.mark.core
@pytest.mark.parametrize("xp_name", ["numpy", "cupy"])
def test_rabi_period_scales_without_coupling_rescale(xp_name, tmp_path):
    xp = _xp_or_skip(xp_name)

    # Same per-molecule coupling for both runs -> collective coupling grows as
    # sqrt(N), so the Rabi period must shrink as 1/sqrt(N).
    t1, e_cav1, _ = _run_coupled_rabi(xp, 1, EPS0, tmp_path, tag="neg1")
    t4, e_cav4, _ = _run_coupled_rabi(xp, 4, EPS0, tmp_path, tag="neg4")

    transfer1 = _first_transfer_time(t1, e_cav1)
    transfer4 = _first_transfer_time(t4, e_cav4)
    ratio = transfer1 / transfer4

    # Expected ratio is sqrt(4) = 2 (approached exactly in the weak-coupling
    # limit).  The wide bracket still cleanly excludes the "no scaling" value 1.
    assert 1.7 < ratio < 2.4, (
        f"Without 1/sqrt(N) rescaling the transfer-time ratio was {ratio:.3f}; "
        f"expected ~sqrt(4)=2 (a value near 1 would mean the coupling is ignored)."
    )
