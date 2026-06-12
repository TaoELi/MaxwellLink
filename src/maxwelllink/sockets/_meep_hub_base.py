# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
Shared layer for the Meep ``MXLSocketSusceptibility`` socket hubs.

Meep's C-level susceptibility client connects from inside the FDTD time-step
loop while holding Python's GIL, so an in-process Python server thread could
never answer its ``MXLINIT``/``MXLREADY`` handshake. Both Meep-facing hubs
therefore run their real server in a ``spawn``-ed child process and hand the
user a thin proxy. This module holds everything the two hubs share::

    SocketHub                                  in-process server (sockets.py)
    ├── AggregatedSocketHub                    + bridge transport (aggregated.py)
    │   └── _AggregatedSusceptibilitySocketHubServer(_MeepRankServerMixin, ...)
    └── _SusceptibilitySocketHubServer(_MeepRankServerMixin, ...)

    _HubProcessProxy                           child-process launcher (this file)
    ├── SusceptibilitySocketHub                susceptibility.py
    └── AggregatedSusceptibilitySocketHub      aggregated_susceptibility.py

``_MeepRankServerMixin`` implements the Meep-rank protocol (accept, classify,
MXLINIT handshake, AGGSTEP serve loop) as a template method with a small set
of hooks, in the same must/may/never-override style as
``mxl_drivers/python/models/dummy_model.py``. ``_HubProcessProxy`` implements
the child-process lifecycle (spawn with MPI-environment stripping, the ready
handshake, statistics draining, and shutdown).
"""

from __future__ import annotations

import math
import multiprocessing as mp
import os
import queue
import socket
import threading
from types import SimpleNamespace
from typing import Dict, Optional

import numpy as np

from .protocol import (
    AGGSTEP,
    _ResultCodec,
    _SocketClosed,
    _StepCodec,
    _close_socket,
    _json_loads_bytes,
    _recv_bytes,
    _recv_msg,
    _send_msg,
)
from .sockets import am_master, mpi_bcast_from_master

# ----------------------------------------------------------------------
# Protocol constants shared with meep/src/susceptibility.cpp (frozen)
# ----------------------------------------------------------------------

MXLINIT = b"MXLINIT"
MXLREADY = b"MXLREADY"
MXL_SUSCEPTIBILITY_PROTOCOL = "mxl_socket_susceptibility_v1"

# Unit conversions used by the Lorentzian -> SHO driver mapping.
FS_TO_AU = 41.34137333518211
MEEP_EFIELD_TO_AU_PREFAC = 1.2929541569381223e-6
MXL_SOURCE_AMP_AU_TO_MEEP = 0.002209799779149953

# Ordinary drivers say nothing at connect time, so a fresh socket is held this
# long waiting for an MXLINIT/AGGHELLO banner before it is classified as a
# silent client. The same window doubles as the grace period that lets every
# already-accepted driver finish classification before a rank starts binding.
_CLASSIFY_WINDOW_FLOOR_S = 0.25


# ----------------------------------------------------------------------
# Child-process environment handling
# ----------------------------------------------------------------------

# Environment variables that MPI launchers (mpirun, srun, Hydra, ...) inject to
# wire a process into the MPI job. They are inherited by ``spawn``-ed children,
# where they make the child look like an extra rank and can wedge MPI startup.
# We strip anything matching these prefixes / exact names while forking the hub
# server child, then restore them in the parent.
_MPI_ENV_PREFIXES = (
    "PMI_",
    "PMIX_",
    "OMPI_",
    "MPI_",
    "MPICH_",
    "I_MPI_",
    "HYDRA_",
    "SLURM_",
    "FI_",
    "UCX_",
    "PSM2_",
    "PMI",
)
_MPI_ENV_EXACT = ("PMI_FD", "PMI_PORT", "PMI_ID", "PMI_RANK", "PMI_SIZE")


def _strip_mpi_env_for_child_start() -> dict[str, str]:
    """Remove MPI launcher variables from ``os.environ`` and return them."""

    saved = {}
    for key in list(os.environ.keys()):
        if key in _MPI_ENV_EXACT or any(
            key.startswith(prefix) for prefix in _MPI_ENV_PREFIXES
        ):
            saved[key] = os.environ.pop(key)
    return saved


def _restore_env(saved: dict[str, str]) -> None:
    """Put back variables removed by :func:`_strip_mpi_env_for_child_start`."""

    os.environ.update(saved)


# ----------------------------------------------------------------------
# Small shared helpers
# ----------------------------------------------------------------------


def _copy_rank_stats(stats: dict[int, dict]) -> dict[int, dict]:
    """
    Copy a per-rank statistics mapping so callers cannot mutate hub state.

    List-valued entries (``peers``, ``aggregate_groups``) are copied as well,
    so the returned rows share nothing with the hub's internal rows.
    """

    return {
        int(rank): {
            key: list(value) if isinstance(value, list) else value
            for key, value in row.items()
        }
        for rank, row in stats.items()
    }


def _choose_ephemeral_port(host: Optional[str]) -> int:
    """
    Ask the OS for a free TCP port on ``host`` and return it.

    The probe socket is bound to port 0 (let the kernel pick) and then closed;
    the chosen port number is returned so the caller can bind it explicitly.
    """

    bind_host = "127.0.0.1" if host in (None, "", "0.0.0.0", "::") else str(host)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((bind_host, 0))
        return int(sock.getsockname()[1])


def _resolve_bound_endpoint(serversock, host, port) -> tuple[str, int]:
    """
    Return the ``(host, port)`` a server actually bound, normalized for peers.

    Wildcard binds (``None``/``""``/``"0.0.0.0"``/``"::"``) are reported as
    ``127.0.0.1`` because that is the address local clients connect back to.
    """

    sockname = serversock.getsockname()
    actual_host = sockname[0] if isinstance(sockname, tuple) else host
    actual_port = sockname[1] if isinstance(sockname, tuple) else port
    if actual_host in (None, "", "0.0.0.0", "::"):
        actual_host = "127.0.0.1"
    return actual_host, int(actual_port)


def _pump_rank_stats(server, stats_queue, stop_event, tick=None) -> None:
    """
    Forward per-rank statistics snapshots to the parent until shutdown.

    Runs in the child process: every 0.25 s it optionally calls ``tick()``
    (used to drain the aggregated hub's control queue) and pushes the current
    statistics whenever they changed. On exit the server is stopped and one
    final snapshot is flushed so closing counters reach the parent.
    """

    last_stats = None
    try:
        while not stop_event.wait(0.25):
            if tick is not None:
                tick()
            stats = _copy_rank_stats(server.rank_stats)
            if stats != last_stats:
                stats_queue.put(stats)
                last_stats = stats
    finally:
        server.stop()
        stats_queue.put(_copy_rank_stats(server.rank_stats))


def lorentzian_to_sho_parameters(
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
    Map a Meep Lorentzian susceptibility onto SHO driver parameters.

    The mapping converts the Meep Lorentzian (resonance ``frequency``,
    oscillator strength ``sigma``) into a simple-harmonic-oscillator driver:
    a resonance ``omega_au`` in atomic units plus the symmetric bright-state
    ``rescaling_factor`` (the square root of the direct density/current scale)
    that Meep applies to both the socket drive field and the returned
    molecular amplitude, so one socket molecule reproduces the per-cell
    Lorentzian response in the linear-response limit.

    Parameters
    ----------
    frequency : float
        Lorentzian resonance frequency in Meep units (must be > 0).
    sigma : float
        Lorentzian oscillator strength (must be >= 0).
    resolution : float
        Meep grid resolution in pixels per unit length (must be > 0); sets
        the grid-cell measure ``(1 / resolution) ** dimensions``.
    gamma : float, default: 0.0
        Lorentzian damping rate. Accepted for API symmetry but ignored: the
        current SHO driver is lossless (callers print a warning if nonzero).
    dimensions : int, default: 1
        Simulation dimensionality, used to form the cell measure (>= 1).
    time_units_fs : float, default: 0.1
        Meep time unit in femtoseconds; sets the Meep<->a.u. conversions.
    mu0_au : float, default: 187.0819866
        SHO transition dipole moment in atomic units (must be nonzero).
    orientation : int, default: 0
        Dipole axis: ``0`` (x), ``1`` (y), or ``2`` (z).

    Returns
    -------
    dict
        ``{"rescaling_factor", "omega_au", "driver_param"}`` where
        ``driver_param`` is the ``--param`` string for ``mxl_driver --model sho``.

    Raises
    ------
    ValueError
        If any argument is outside its documented valid range.
    """

    if frequency <= 0.0 or resolution <= 0.0 or time_units_fs <= 0.0:
        raise ValueError("frequency, resolution, time_units_fs must be positive.")
    if sigma < 0.0 or gamma < 0.0:
        raise ValueError("sigma and gamma must be nonnegative.")
    if dimensions < 1:
        raise ValueError("dimensions must be at least 1.")
    if mu0_au == 0.0:
        raise ValueError("mu0_au must be nonzero.")
    if orientation not in (0, 1, 2):
        raise ValueError("orientation must be 0, 1, or 2.")

    omega_au = 2.0 * math.pi * frequency / (time_units_fs * FS_TO_AU)
    cell_measure = (1.0 / resolution) ** int(dimensions)
    efield_factor = MEEP_EFIELD_TO_AU_PREFAC / (time_units_fs * time_units_fs)
    density_scale = (
        sigma
        * cell_measure
        * omega_au
        * omega_au
        * (time_units_fs * FS_TO_AU)
        / (MXL_SOURCE_AMP_AU_TO_MEEP * efield_factor * mu0_au * mu0_au)
    )
    rescaling_factor = math.sqrt(density_scale)
    driver_param = (
        f"omega={omega_au:.17g},mu0={mu0_au:.17g},orientation={int(orientation)}"
    )
    return {
        "rescaling_factor": rescaling_factor,
        "omega_au": omega_au,
        "driver_param": driver_param,
    }


# ----------------------------------------------------------------------
# In-child server side: the Meep-rank protocol as a template method
# ----------------------------------------------------------------------


class _MeepRankServerMixin:
    """
    Meep-rank protocol layer mixed into a ``SocketHub``-derived server.

    The mixin owns the accept loop, socket classification, the ``MXLINIT``
    handshake, and the ``AGGSTEP`` serve loop. Subclasses supply the transport
    behind each timestep through a small hook set:

    must override
        ``_handle_step``, ``_wait_for_rank_drivers``, ``_on_rank_registered``
    may override
        ``_classify_silent``, ``_classify_other``,
        ``_before_rank_registration``, ``_molecule_init_payload_extras``,
        ``_on_rank_closed``, ``_wake_step_waiters``
    do not override
        ``_accept_loop``, ``_classify_socket``, ``_serve_meep_rank``,
        ``_register_rank_molecules``, ``stop``

    The hooks receive a per-connection ``ctx`` namespace carrying ``sock``,
    ``peer``, ``rank``, ``molecule_ids``, and ``init_payloads``; subclasses may
    stash extra fields (e.g. ``client_id``) on it.
    """

    _log_prefix = "SusceptibilitySocketHub"

    # -------------- construction (called before SocketHub.__init__) --------------

    def _init_rank_tracking(self) -> None:
        """Initialize the rank/classifier bookkeeping shared by both servers."""

        self._rank_threads: list[threading.Thread] = []
        self._classifier_threads: list[threading.Thread] = []
        self._rank_sockets: list[socket.socket] = []
        self.rank_stats: dict[int, dict] = {}
        self._step_lock = threading.RLock()
        self._meep_lock = threading.RLock()

    def _classify_window(self) -> float:
        """Seconds a fresh socket may stay silent before classification."""

        return max(self.latency, _CLASSIFY_WINDOW_FLOOR_S)

    # -------------- accept / classify --------------

    def _accept_loop(self) -> None:
        """Accept sockets and classify each one on its own daemon thread."""

        while not self._stop:
            try:
                csock, addr = self.serversock.accept()
            except socket.timeout:
                continue
            except OSError:
                break

            try:
                csock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                csock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except (OSError, AttributeError):
                pass

            peer = addr if isinstance(addr, str) else f"{addr[0]}:{addr[1]}"
            thread = threading.Thread(
                target=self._classify_socket,
                args=(csock, peer),
                daemon=True,
            )
            thread.start()
            with self._meep_lock:
                self._classifier_threads = [
                    t for t in self._classifier_threads if t.is_alive()
                ]
                self._classifier_threads.append(thread)

    def _classify_socket(self, csock: socket.socket, peer: str) -> None:
        """
        Route one accepted socket by its first protocol banner.

        ``MXLINIT`` marks a Meep rank; silence within the classify window is
        delegated to :meth:`_classify_silent`; any other banner goes to
        :meth:`_classify_other`; a read error closes the socket.
        """

        csock.settimeout(self._classify_window())
        try:
            header = _recv_msg(csock)
        except socket.timeout:
            self._classify_silent(csock, peer)
            return
        except (_SocketClosed, OSError, RuntimeError):
            _close_socket(csock)
            return

        if header == MXLINIT:
            csock.settimeout(self.timeout)
            self._register_meep_rank_socket(csock, peer)
        else:
            self._classify_other(header, csock, peer)

    def _classify_silent(self, csock: socket.socket, peer: str) -> None:
        """Handle a client that sent no banner. Default: close it."""

        _close_socket(csock)

    def _classify_other(self, header: bytes, csock: socket.socket, peer: str) -> None:
        """Handle a non-MXLINIT banner. Default: close the socket."""

        _close_socket(csock)

    def _register_meep_rank_socket(self, csock: socket.socket, peer: str) -> None:
        """Track a Meep rank socket and spawn its dedicated handler thread."""

        if self._stop:
            _close_socket(csock)
            return
        with self._meep_lock:
            self._rank_sockets.append(csock)
        thread = threading.Thread(
            target=self._serve_meep_rank,
            args=(csock, peer),
            daemon=True,
        )
        thread.start()
        with self._meep_lock:
            self._rank_threads.append(thread)

    # -------------- MXLINIT handshake and AGGSTEP serve loop --------------

    def _recv_mxl_init_payload(self, sock: socket.socket) -> dict:
        """
        Read and validate the JSON ``MXLINIT`` payload from a Meep rank.

        Raises
        ------
        RuntimeError
            If the payload does not declare
            ``protocol == "mxl_socket_susceptibility_v1"``.
        """

        payload = _json_loads_bytes(_recv_bytes(sock))
        if payload.get("protocol") != MXL_SUSCEPTIBILITY_PROTOCOL:
            raise RuntimeError(
                f"Expected protocol='{MXL_SUSCEPTIBILITY_PROTOCOL}' in MXLINIT."
            )
        return payload

    def _serve_meep_rank(self, sock: socket.socket, peer: str) -> None:
        """
        Handle one Meep rank connection after its ``MXLINIT`` banner.

        Template method: reads the init payload, registers the rank's
        molecules, defers to the subclass for bookkeeping and driver binding,
        replies ``MXLREADY``, then services ``AGGSTEP`` frames through
        :meth:`_handle_step` until the socket closes or the server stops.
        The socket is always closed and de-registered on exit.
        """

        ctx = SimpleNamespace(
            sock=sock,
            peer=peer,
            rank=-1,
            molecule_ids=[],
            init_payloads={},
        )
        try:
            step_codec = _StepCodec()
            result_codec = _ResultCodec()
            init_payload = self._recv_mxl_init_payload(sock)
            ctx.rank = int(init_payload.get("rank", -1))
            ctx.molecule_ids = [
                int(mid) for mid in init_payload.get("molecule_ids", [])
            ]
            if not ctx.molecule_ids:
                raise RuntimeError("MXLINIT payload did not include molecule_ids.")

            self._before_rank_registration(init_payload)
            ctx.init_payloads = self._register_rank_molecules(
                init_payload, ctx.molecule_ids
            )
            self._on_rank_registered(ctx, init_payload)
            self._wait_for_rank_drivers(ctx)
            _send_msg(sock, MXLREADY)

            while not self._stop:
                try:
                    header = _recv_msg(sock)
                except socket.timeout:
                    continue
                if header != AGGSTEP:
                    raise RuntimeError(
                        f"Unexpected Meep susceptibility header {header!r}."
                    )
                efields = step_codec.recv(sock, header_already_read=True)
                responses = self._handle_step(ctx, efields)
                result_codec.send(sock, responses)
                self._note_step_served(ctx, len(efields))

        except (_SocketClosed, OSError):
            pass
        except Exception as exc:
            print(
                f"[{self._log_prefix}] Meep rank connection {peer} failed: {exc!r}",
                flush=True,
            )
        finally:
            _close_socket(sock)
            with self._meep_lock:
                try:
                    self._rank_sockets.remove(sock)
                except ValueError:
                    pass
            self._on_rank_closed(ctx)

    def _note_step_served(self, ctx, n_requests: int) -> None:
        """Update the rank's step/request counters after one served step."""

        with self._meep_lock:
            stats = self.rank_stats.get(ctx.rank)
            if stats is not None:
                stats["steps"] += 1
                stats["requests"] += n_requests

    # -------------- driver INIT payloads --------------

    def _register_rank_molecules(
        self, init_payload: dict, molecule_ids: list[int]
    ) -> dict[int, dict]:
        """
        Register a rank's molecule IDs and build their per-driver INIT payloads.

        Each molecule ID is added to the inherited ``expected``/``bound`` bind
        bookkeeping, and an INIT payload is assembled carrying the timestep and
        rescaling metadata the drivers need. The payload key set is part of the
        driver protocol and pinned by tests; subclasses add transport-specific
        keys through :meth:`_molecule_init_payload_extras` only.
        """

        dt_au = float(init_payload.get("dt_au", 0.0))
        rank = int(init_payload.get("rank", -1))
        payloads: dict[int, dict] = {}
        for mid in molecule_ids:
            extras = self._molecule_init_payload_extras(int(mid))
            with self._lock:
                if mid not in self.expected:
                    self.expected.add(mid)
                    self.bound.setdefault(mid, None)
            payloads[mid] = {
                "molecule_id": mid,
                "dt_au": dt_au,
                "mxl_rank": rank,
                "rescaling_factor": float(init_payload.get("rescaling_factor", 1.0)),
                "time_units_fs": float(init_payload.get("time_units_fs", 0.0)),
                **extras,
            }
        return payloads

    def _molecule_init_payload_extras(self, molecule_id: int) -> dict:
        """Extra INIT payload entries for one molecule. Default: none."""

        return {}

    # -------------- hooks implemented by the concrete servers --------------

    def _before_rank_registration(self, init_payload: dict) -> None:
        """Run before molecules are registered. Default: no-op."""

    def _on_rank_registered(self, ctx, init_payload: dict) -> None:
        """Record per-rank bookkeeping and statistics. Must be overridden."""

        raise NotImplementedError

    def _wait_for_rank_drivers(self, ctx) -> None:
        """Block until the rank's transport is bound. Must be overridden."""

        raise NotImplementedError

    def _handle_step(self, ctx, efields: Dict[int, np.ndarray]) -> Dict[int, dict]:
        """Run one timestep for this rank's fields. Must be overridden."""

        raise NotImplementedError

    def _on_rank_closed(self, ctx) -> None:
        """Run after a rank connection is torn down. Default: no-op."""

    # -------------- shutdown --------------

    def _wake_step_waiters(self) -> None:
        """Wake threads blocked on step synchronization. Default: no-op."""

    def stop(self):
        """Stop Meep rank handlers, then the underlying hub, then join threads."""

        self._stop = True
        self._wake_step_waiters()
        with self._meep_lock:
            rank_sockets = list(self._rank_sockets)
            rank_threads = list(self._rank_threads)
            classifier_threads = list(self._classifier_threads)
        for sock in rank_sockets:
            _close_socket(sock)
        super().stop()
        for thread in classifier_threads:
            thread.join(timeout=0.2)
        for thread in rank_threads:
            thread.join(timeout=1.0)


# ----------------------------------------------------------------------
# User side: proxy to the server running in a child process
# ----------------------------------------------------------------------


class _HubProcessProxy:
    """
    Base for the user-facing hubs that proxy a child-process server.

    Subclasses set ``_log_prefix`` and implement :meth:`_server_runner` and
    :meth:`_server_config` (plus :meth:`_create_extra_queues` when the child
    needs more channels), then call :meth:`_start_server_process` from their
    ``__init__``. Under MPI only the master rank launches the child; every
    rank then learns the resolved endpoint through a broadcast, so all ranks
    agree on the ``host``/``port`` to connect to.
    """

    _log_prefix = "SusceptibilitySocketHub"

    # -------------- hooks implemented by the concrete hubs --------------

    def _server_runner(self):
        """Return the picklable module-level child entry point."""

        raise NotImplementedError

    def _server_config(self) -> tuple:
        """Hub-specific runner arguments placed after ``latency``."""

        raise NotImplementedError

    def _create_extra_queues(self, ctx) -> tuple:
        """Extra runner channels placed after the stats queue. Default: none."""

        return ()

    # -------------- child-process lifecycle --------------

    def _start_server_process(self, host, port) -> dict:
        """
        Launch the child server and resolve the bound endpoint on every rank.

        Returns the child's ready payload (``host``/``port`` plus any
        hub-specific extras) after broadcasting it from the MPI master.

        Raises
        ------
        RuntimeError
            If the child server fails to start; the hub is stopped first so
            no half-started child process is leaked.
        """

        if port is None:
            port = 31415
        if int(port) == 0:
            port = _choose_ephemeral_port(host)

        self._stats_cache: dict[int, dict] = {}
        self._stopped = False
        self._is_master = am_master()
        self._ready_queue = None
        self._stats_queue = None
        self._stop_event = None
        self._process = None

        ready = None
        if self._is_master:
            ctx = mp.get_context("spawn")
            self._ready_queue = ctx.Queue()
            self._stats_queue = ctx.Queue()
            self._stop_event = ctx.Event()
            extra_queues = self._create_extra_queues(ctx)
            self._process = ctx.Process(
                target=self._server_runner(),
                args=(
                    host,
                    int(port),
                    self.timeout,
                    self.latency,
                    *self._server_config(),
                    self._ready_queue,
                    self._stats_queue,
                    *extra_queues,
                    self._stop_event,
                ),
                daemon=False,
            )

            try:
                saved_env = _strip_mpi_env_for_child_start()
                try:
                    self._process.start()
                finally:
                    _restore_env(saved_env)
                ready = self._ready_queue.get(
                    timeout=min(max(self.timeout, 1.0), 30.0)
                )
            except queue.Empty:
                ready = {"error": f"{self._log_prefix} server did not start."}
            except Exception as exc:
                ready = {"error": repr(exc)}

        ready = mpi_bcast_from_master(ready)
        if not ready or "error" in ready:
            self.stop()
            msg = (
                ready.get("error", "unknown startup error")
                if ready
                else "unknown startup error"
            )
            raise RuntimeError(f"{self._log_prefix} server failed to start: {msg}")

        self.host = str(ready["host"])
        self.address = self.host
        self.port = int(ready["port"])
        return ready

    # -------------- statistics --------------

    def _drain_stats(self) -> None:
        """Cache the latest stats snapshot from the child (master only)."""

        if not self._is_master or self._stats_queue is None:
            return
        while True:
            try:
                stats = self._stats_queue.get_nowait()
            except queue.Empty:
                break
            self._stats_cache = _copy_rank_stats(stats)

    @property
    def rank_stats(self) -> dict[int, dict]:
        """
        Latest per-Meep-rank statistics from the running server.

        Returns
        -------
        dict[int, dict]
            Mapping from rank to its stats row (``molecule_count``, ``steps``,
            ``requests``, ``peer``, ...). Empty on non-master ranks.
        """

        self._drain_stats()
        return _copy_rank_stats(self._stats_cache)

    # -------------- shutdown --------------

    def stop(self) -> None:
        """
        Stop the hub and tear down the child server process.

        Idempotent and safe on non-master ranks. Signals the child via the stop
        event, joins it, and falls back to ``terminate()`` if it does not exit;
        a final stats drain captures any closing counters.
        """

        if self._stopped:
            return
        self._stopped = True
        if not self._is_master:
            return
        try:
            if self._stop_event is not None:
                self._stop_event.set()
        except Exception:
            pass
        # A pid of None means the child was never started (startup failure).
        if self._process is not None and self._process.pid is not None:
            self._process.join(timeout=2.0)
            if self._process.is_alive():
                self._process.terminate()
                self._process.join(timeout=2.0)
        self._drain_stats()

    def __del__(self):
        """Best-effort cleanup so a dropped reference still stops the child."""

        try:
            self.stop()
        except Exception:
            pass
