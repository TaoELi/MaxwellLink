# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
SocketHub for Meep ``MXLSocketSusceptibility`` with direct driver connections.

This is the hub behind the production Meep+LAMMPS workflow: one TCP listener
serves both ordinary ``mxl_driver``/LAMMPS clients (silent at connect time)
and Meep rank clients (which announce themselves with an ``MXLINIT`` banner)::

    Meep rank (C client) ==MXLINIT/AGGSTEP==> _SusceptibilitySocketHubServer
    mxl_driver / LAMMPS  ==i-PI protocol===>      (child process, SocketHub)

The user-facing :class:`SusceptibilitySocketHub` is a thin proxy: the real
server runs in a child process because Meep's C-level time-step loop holds the
GIL while waiting on the socket (see ``_meep_hub_base.py`` for the shared
layer and the full class diagram).
"""

from __future__ import annotations

import time
from typing import Dict, Optional

import numpy as np

from ._meep_hub_base import (
    _HubProcessProxy,
    _MeepRankServerMixin,
    _pump_rank_stats,
    _resolve_bound_endpoint,
    lorentzian_to_sho_parameters,
)

# Names that historically lived in this module; re-exported so existing
# imports (tests, aggregated_susceptibility, user scripts) keep working.
from ._meep_hub_base import (  # noqa: F401  re-exported for backward compatibility
    FS_TO_AU,
    MEEP_EFIELD_TO_AU_PREFAC,
    MXL_SOURCE_AMP_AU_TO_MEEP,
    MXLINIT,
    MXLREADY,
    _choose_ephemeral_port,
    _copy_rank_stats,
    _restore_env,
    _strip_mpi_env_for_child_start,
)
from .protocol import _close_socket
from .sockets import SocketHub, _ClientState, am_master


# ----------------------------------------------------------------------
# Child-process entry point
# ----------------------------------------------------------------------


def _run_susceptibility_socket_hub_server(
    host: Optional[str],
    port: int,
    timeout: float,
    latency: float,
    driver_count_file: Optional[str],
    ready_queue,
    stats_queue,
    stop_event,
) -> None:
    """
    Child-process entry point that runs the susceptibility hub server.

    Constructs the server, reports the bound endpoint back to the parent
    through ``ready_queue`` (or ``{"error": ...}`` on failure), then forwards
    per-rank statistics until ``stop_event`` is set.
    """

    try:
        server = _SusceptibilitySocketHubServer(
            host=host,
            port=port,
            timeout=timeout,
            latency=latency,
            driver_count_file=driver_count_file,
        )
        ready_queue.put({"host": server.host, "port": server.port})
    except Exception as exc:
        ready_queue.put({"error": repr(exc)})
        return

    _pump_rank_stats(server, stats_queue, stop_event)


# ----------------------------------------------------------------------
# In-child server
# ----------------------------------------------------------------------


class _SusceptibilitySocketHubServer(_MeepRankServerMixin, SocketHub):
    """
    Meep-facing hub server with direct (non-bridged) driver connections.

    The Meep-rank protocol comes from :class:`_MeepRankServerMixin`; this
    subclass supplies the direct-driver transport: silent sockets are parked
    as unbound ``SocketHub`` clients, and each timestep runs through the
    inherited :meth:`SocketHub.step_barrier`.

    Parameters
    ----------
    host : str or None, optional
        Bind host. ``None`` uses the inherited default.
    port : int or None, default: 31415
        Bind port.
    timeout : float, default: 60000.0
        Socket timeout in seconds applied to bound clients.
    latency : float, default: 0.05
        Polling interval in seconds for the accept/bind loops.
    unixsocket : str or None, optional
        Reserved; must be falsy. UNIX sockets are intentionally unsupported
        because Meep's ``MXLSocketSusceptibility`` C client connects by
        host/port only.
    driver_count_file : str or None, default: "num_socket_molecule"
        File that receives the total socket molecule count advertised by Meep
        in ``MXLINIT`` (one integer, written once). Slurm driver arrays read
        it to size themselves. Disabled when ``None``.

    Notes
    -----
    The inherited ``SocketHub`` accept thread starts during ``__init__``; no
    separate ``start()`` call is needed.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = 31415,
        timeout: float = 60000.0,
        latency: float = 0.05,
        unixsocket: Optional[str] = None,
        driver_count_file: Optional[str] = "num_socket_molecule",
    ):
        if unixsocket:
            raise ValueError(
                "SusceptibilitySocketHub currently supports TCP host/port only."
            )

        self._init_rank_tracking()
        self._rank_payloads: dict[int, dict] = {}
        self._rank_init_payloads: dict[int, dict[int, dict]] = {}
        self.driver_count_file = driver_count_file
        self._driver_count_file_written = False

        super().__init__(host=host, port=port, timeout=timeout, latency=latency)

        self.host, self.port = _resolve_bound_endpoint(self.serversock, host, port)
        self.timeout = float(timeout)

    # -------------- accept / classify --------------

    def _classify_silent(self, csock, peer: str) -> None:
        """
        Park a silent socket as an ordinary ``mxl_driver`` client.

        Drivers send nothing at connect time, so a socket that produced no
        ``MXLINIT`` banner within the classify window is handed to the
        inherited SocketHub pool as an unbound client (``molecule_id=-1``)
        for later binding.
        """

        if self._stop:
            _close_socket(csock)
            return
        csock.settimeout(self.timeout)
        st = _ClientState(sock=csock, address=peer, molecule_id=-1)
        with self._lock:
            self.clients[id(csock)] = st

    # -------------- MXLINIT handshake hooks --------------

    def _before_rank_registration(self, init_payload: dict) -> None:
        """Announce the driver count to Slurm before binding starts."""

        with self._meep_lock:
            self._maybe_write_driver_count_file(init_payload)

    def _on_rank_registered(self, ctx, init_payload: dict) -> None:
        """Record this rank's payloads and start its statistics row."""

        with self._meep_lock:
            self._rank_payloads[ctx.rank] = init_payload
            self._rank_init_payloads[ctx.rank] = ctx.init_payloads
            self.rank_stats[ctx.rank] = {
                "molecule_count": len(ctx.molecule_ids),
                "steps": 0,
                "requests": 0,
                "peer": ctx.peer,
            }
        print(
            f"[{self._log_prefix}] Meep rank {ctx.rank} requested "
            f"{len(ctx.molecule_ids)} drivers from {ctx.peer}.",
            flush=True,
        )

    def _wait_for_rank_drivers(self, ctx) -> None:
        """
        Bind this rank's molecules to driver sockets before MXLREADY.

        Drivers are passive at connect time, so their classifier workers
        register them only after the silent-classify window. Give any
        already-accepted driver sockets one classifier window before the
        rank enters the binding wait loop.
        """

        time.sleep(self._classify_window())
        ok = self.wait_until_bound(
            ctx.init_payloads,
            require_init=True,
            timeout=None,
        )
        if not ok:
            raise RuntimeError(f"Timed out waiting for rank {ctx.rank} drivers.")

    def _maybe_write_driver_count_file(self, init_payload: dict) -> None:
        """
        Write the total socket molecule count from ``MXLINIT`` to disk, once.

        Caller must hold ``self._meep_lock``. A no-op when the feature is
        disabled, the file has already been written, or Meep omits / reports a
        non-positive ``expected_total_molecules``.
        """

        if self.driver_count_file is None or self._driver_count_file_written:
            return
        total = int(init_payload.get("expected_total_molecules", 0) or 0)
        if total <= 0:
            return
        with open(self.driver_count_file, "w", encoding="utf-8") as handle:
            handle.write(f"{total}\n")
        self._driver_count_file_written = True

    # -------------- per-timestep barrier --------------

    def _handle_step(self, ctx, efields: Dict[int, np.ndarray]) -> Dict[int, dict]:
        """Run one timestep for this rank through the direct-driver barrier."""

        return self._run_susceptibility_step(efields, ctx.init_payloads)

    def _run_susceptibility_step(
        self, efields: Dict[int, np.ndarray], init_payloads: dict[int, dict]
    ) -> Dict[int, dict]:
        """
        Run one barrier step: fields in, source amplitudes out.

        Wraps the per-molecule fields into request dicts and drives the
        inherited :meth:`SocketHub.step_barrier`. If the barrier returns empty
        (a driver dropped or is not yet bound), it re-binds and retries until
        ``timeout`` elapses.

        Raises
        ------
        TimeoutError
            If no complete set of driver responses arrives within ``timeout``.
        """

        requests = {
            int(mid): {
                "efield_au": np.asarray(field, dtype=float).reshape(3),
                "init": init_payloads[int(mid)],
            }
            for mid, field in efields.items()
        }

        deadline = time.time() + self.timeout
        with self._step_lock:
            while not self._stop:
                remaining = max(0.0, deadline - time.time())
                if remaining <= 0.0:
                    break
                responses = self.step_barrier(requests, timeout=remaining)
                if responses:
                    return responses
                self.wait_until_bound(
                    {mid: init_payloads[mid] for mid in requests.keys()},
                    require_init=True,
                    timeout=min(1.0, remaining),
                )
        raise TimeoutError("Timed out waiting for susceptibility driver responses.")


# ----------------------------------------------------------------------
# User-facing hub (proxy to the child-process server)
# ----------------------------------------------------------------------


class SusceptibilitySocketHub(_HubProcessProxy):
    """
    Process-backed hub for Meep ``MXLSocketSusceptibility`` connections.

    The hub starts immediately during construction and exposes the endpoint
    fields consumed by ``mp.MXLSocketSusceptibility(hub=hub)``. The actual
    server (:class:`_SusceptibilitySocketHubServer`) runs in a child process;
    see ``_meep_hub_base.py`` for why and for the shared proxy machinery.

    Parameters
    ----------
    host : str or None, optional
        Bind host for the server. ``None`` uses the server default.
    port : int or None, default: 31415
        Bind port. ``0`` requests an OS-chosen ephemeral port.
    timeout : float, default: 60000.0
        Socket timeout in seconds passed to the server.
    latency : float, default: 0.05
        Polling interval in seconds passed to the server.
    unixsocket : str or None, optional
        Reserved; must be falsy (TCP only).
    driver_count_file : str or None, default: "num_socket_molecule"
        File that receives the total number of socket molecules required by
        Meep, written by the child server as a single integer after
        ``MXLINIT``. Set to ``None`` to disable.

    Attributes
    ----------
    host : str
        Resolved bind host of the running server.
    port : int
        Resolved bind port of the running server.
    address : str
        Alias of ``host``.
    rank_stats : dict[int, dict]
        Latest per-Meep-rank statistics drained from the child process.

    Raises
    ------
    ValueError
        If ``unixsocket`` is given.
    RuntimeError
        If the child server fails to start.
    """

    _log_prefix = "SusceptibilitySocketHub"

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = 31415,
        timeout: float = 60000.0,
        latency: float = 0.05,
        unixsocket: Optional[str] = None,
        driver_count_file: Optional[str] = "num_socket_molecule",
    ):
        if unixsocket:
            raise ValueError(
                "SusceptibilitySocketHub currently supports TCP host/port only."
            )

        self.timeout = float(timeout)
        self.latency = float(latency)
        self.driver_count_file = driver_count_file
        self._start_server_process(host, port)

    def _server_runner(self):
        return _run_susceptibility_socket_hub_server

    def _server_config(self) -> tuple:
        return (self.driver_count_file,)

    def lorentzian_conversion(
        self,
        frequency: float,
        sigma: float,
        resolution: float,
        *,
        gamma: float = 0.0,
        dimensions: int = 1,
        time_units_fs: float = 0.1,
        mu0_au: float = 187.0819866,
        orientation: int = 0,
    ) -> dict:
        """
        Convert a Meep Lorentzian susceptibility to SHO driver parameters.

        The numerical mapping is :func:`lorentzian_to_sho_parameters`; this
        method adds the ready-to-run ``mxl_driver --model sho`` command line
        targeting this hub's endpoint.

        Returns
        -------
        dict
            ``{"rescaling_factor", "driver_command"}`` where
            ``rescaling_factor`` is the symmetric bright-state coupling scale
            to pass to ``mp.MXLSocketSusceptibility(rescaling_factor=...)``.

        Raises
        ------
        ValueError
            If any argument is outside its documented valid range.
        """

        converted = lorentzian_to_sho_parameters(
            frequency,
            sigma,
            resolution,
            gamma=gamma,
            dimensions=dimensions,
            time_units_fs=time_units_fs,
            mu0_au=mu0_au,
            orientation=orientation,
        )
        rescaling_factor = converted["rescaling_factor"]
        driver_command = (
            f"mxl_driver --model sho --address {self.host} --port {self.port} "
            f'--param "{converted["driver_param"]}"'
        )

        if am_master():
            if gamma != 0.0:
                print(
                    f"[{self._log_prefix}] gamma={gamma} ignored "
                    "(SHO driver is lossless).",
                    flush=True,
                )
            print(
                f"[{self._log_prefix}] rescaling_factor={rescaling_factor:.12g}",
                flush=True,
            )
            print(f"[{self._log_prefix}] {driver_command}", flush=True)

        return {"rescaling_factor": rescaling_factor, "driver_command": driver_command}


__all__ = ["SusceptibilitySocketHub"]
