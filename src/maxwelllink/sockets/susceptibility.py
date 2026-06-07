# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
SocketHub for Meep ``MXLSocketSusceptibility`` connections.

This module bridges Meep's C-level ``MXLSocketSusceptibility`` to MaxwellLink
socket molecule drivers.  A single TCP listener accepts two kinds of clients
and tells them apart by what they send first:

- Ordinary ``mxl_driver`` clients stay silent at connect time and are handed to
  the inherited :class:`~maxwelllink.sockets.sockets.SocketHub` binding/stepping
  machinery (see :meth:`_SusceptibilitySocketHubServer._register_driver_socket`).
- Meep rank clients send an ``MXLINIT`` header immediately and are served by a
  dedicated per-rank handler (see
  :meth:`_SusceptibilitySocketHubServer._serve_meep_rank`).

The user-facing entry point is :class:`SusceptibilitySocketHub`, which launches
the server in a child process (so Meep holding the GIL cannot deadlock the
handshake) and exposes the ``host``/``port`` consumed by
``mp.MXLSocketSusceptibility(hub=hub)``.
"""

from __future__ import annotations

import json
import math
import multiprocessing as mp
import os
import queue
import socket
import threading
import time
from typing import Dict, Optional

import numpy as np

from .aggregated import AGGSTEP, _ResultCodec, _StepCodec
from .sockets import (
    SocketHub,
    _ClientState,
    _SocketClosed,
    am_master,
    mpi_bcast_from_master,
    _recv_bytes,
    _recv_msg,
    _send_msg,
)

MXLINIT = b"MXLINIT"
MXLREADY = b"MXLREADY"

FS_TO_AU = 41.34137333518211
MEEP_EFIELD_TO_AU_PREFAC = 1.2929541569381223e-6
MXL_SOURCE_AMP_AU_TO_MEEP = 0.002209799779149953


# Environment variables that MPI launchers (mpirun, srun, Hydra, ...) inject to
# wire a process into the MPI job.  They are inherited by ``spawn``-ed children,
# where they make the child look like an extra rank and can wedge MPI startup.
# We strip anything matching these prefixes / exact names while forking the hub
# server child, then restore them in the parent (see
# ``_strip_mpi_env_for_child_start`` / ``_restore_env``).
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
    """
    Remove MPI launcher variables from ``os.environ`` and return the removed set.

    Returns
    -------
    dict[str, str]
        The variables that were removed, suitable for passing back to
        :func:`_restore_env` once the child process has been started.
    """

    saved = {}
    for key in list(os.environ.keys()):
        if key in _MPI_ENV_EXACT or any(key.startswith(prefix) for prefix in _MPI_ENV_PREFIXES):
            saved[key] = os.environ.pop(key)
    return saved


def _restore_env(saved: dict[str, str]) -> None:
    """
    Put back environment variables previously removed by
    :func:`_strip_mpi_env_for_child_start`.

    Parameters
    ----------
    saved : dict[str, str]
        Variables to restore into ``os.environ``.
    """

    os.environ.update(saved)


def _copy_rank_stats(stats: dict[int, dict]) -> dict[int, dict]:
    """
    Deep-copy a per-rank statistics mapping so callers cannot mutate hub state.

    Parameters
    ----------
    stats : dict[int, dict]
        Mapping from Meep rank to its per-rank stats row.

    Returns
    -------
    dict[int, dict]
        An independent copy with integer keys and copied rows.
    """

    return {int(rank): dict(row) for rank, row in stats.items()}


def _choose_ephemeral_port(host: Optional[str]) -> int:
    """
    Ask the OS for a free TCP port on ``host`` and return it.

    The probe socket is bound to port 0 (let the kernel pick) and then closed;
    the chosen port number is returned so the caller can bind it explicitly.

    Parameters
    ----------
    host : str or None
        Target host.  ``None``, ``""``, ``"0.0.0.0"`` and ``"::"`` are treated
        as loopback for the probe.

    Returns
    -------
    int
        A port number that was free at probe time.
    """

    bind_host = "127.0.0.1" if host in (None, "", "0.0.0.0", "::") else str(host)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((bind_host, 0))
        return int(sock.getsockname()[1])


def _run_susceptibility_socket_hub_server(
    host: Optional[str],
    port: int,
    timeout: float,
    latency: float,
    ready_queue,
    stats_queue,
    stop_event,
) -> None:
    """
    Child-process entry point that runs the susceptibility hub server.

    Constructs a :class:`_SusceptibilitySocketHubServer`, reports the bound
    endpoint back to the parent through ``ready_queue``, then loops forwarding
    per-rank statistics until ``stop_event`` is set.  Runs in a separate process
    started by :class:`SusceptibilitySocketHub`.

    Parameters
    ----------
    host : str or None
        Bind host for the server (``None`` selects the default).
    port : int
        Bind port for the server.
    timeout : float
        Socket timeout in seconds applied to bound clients.
    latency : float
        Polling interval in seconds for the server's accept/bind loops.
    ready_queue : multiprocessing.Queue
        Queue used once to report ``{"host", "port"}`` on success, or
        ``{"error": repr}`` if construction failed.
    stats_queue : multiprocessing.Queue
        Queue onto which updated per-rank statistics snapshots are pushed.
    stop_event : multiprocessing.Event
        When set by the parent, the server is stopped and the loop exits.
    """

    server = None
    last_stats = None
    try:
        server = _SusceptibilitySocketHubServer(
            host=host,
            port=port,
            timeout=timeout,
            latency=latency,
        )
        ready_queue.put({"host": server.host, "port": server.port})
    except Exception as exc:
        ready_queue.put({"error": repr(exc)})
        return

    try:
        while not stop_event.wait(0.25):
            stats = _copy_rank_stats(server.rank_stats)
            if stats != last_stats:
                stats_queue.put(stats)
                last_stats = stats
    finally:
        if server is not None:
            server.stop()
            stats_queue.put(_copy_rank_stats(server.rank_stats))


class _SusceptibilitySocketHubServer(SocketHub):
    """
    SocketHub variant for C-level Meep susceptibility coupling.

    Extends :class:`~maxwelllink.sockets.sockets.SocketHub` so a single listener
    can serve both ordinary ``mxl_driver`` clients and Meep rank connections (the
    latter identified by an ``MXLINIT`` header).  Driver binding and the per-step
    barrier reuse the inherited machinery; this subclass only adds the Meep-rank
    accept/classify/serve path and the per-rank statistics it exposes.

    Parameters
    ----------
    host : str or None, optional
        Bind host.  ``None`` uses the inherited default.
    port : int or None, default: 31415
        Bind port.
    timeout : float, default: 60000.0
        Socket timeout in seconds applied to bound clients.
    latency : float, default: 0.05
        Polling interval in seconds for the accept/bind loops.
    unixsocket : str or None, optional
        Reserved; must be falsy.  UNIX sockets are intentionally unsupported
        because Meep's ``MXLSocketSusceptibility`` C client connects by
        host/port only.

    Attributes
    ----------
    rank_stats : dict[int, dict]
        Per-Meep-rank counters (molecule count, steps, requests, peer).
    host, port, timeout
        The actually-bound endpoint, resolved after the listener opens.

    Notes
    -----
    The inherited ``SocketHub`` accept thread starts during ``__init__``.  No
    separate ``start()`` call is needed.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = 31415,
        timeout: float = 60000.0,
        latency: float = 0.05,
        unixsocket: Optional[str] = None,
    ):
        if unixsocket:
            raise ValueError(
                "SusceptibilitySocketHub currently supports TCP host/port only."
            )

        self._rank_threads: list[threading.Thread] = []
        self._classifier_threads: list[threading.Thread] = []
        self._rank_sockets: list[socket.socket] = []
        self._rank_payloads: dict[int, dict] = {}
        self._rank_init_payloads: dict[int, dict[int, dict]] = {}
        self.rank_stats: dict[int, dict] = {}
        self._step_lock = threading.RLock()
        self._meep_lock = threading.RLock()

        super().__init__(host=host, port=port, timeout=timeout, latency=latency)

        sockname = self.serversock.getsockname()
        actual_host = sockname[0] if isinstance(sockname, tuple) else host
        actual_port = sockname[1] if isinstance(sockname, tuple) else port
        self.host = "127.0.0.1" if actual_host in (None, "", "0.0.0.0", "::") else actual_host
        self.port = int(actual_port)
        self.timeout = float(timeout)

    def _accept_loop(self) -> None:
        """Accept ordinary drivers and Meep susceptibility clients."""

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
        Classify one accepted socket without blocking the accept loop.

        Waits briefly for a first message: an ``MXLINIT`` header marks a Meep
        rank client, a no-header timeout marks an ordinary (passive) driver, and
        any error closes the socket.

        Parameters
        ----------
        csock : socket.socket
            The freshly accepted socket.
        peer : str
            Human-readable peer address for logging/state.
        """

        csock.settimeout(max(self.latency, 0.25))
        try:
            header = _recv_msg(csock)
        except socket.timeout:
            self._register_driver_socket(csock, peer)
            return
        except (_SocketClosed, OSError, RuntimeError):
            try:
                csock.close()
            except OSError:
                pass
            return

        if header == MXLINIT:
            csock.settimeout(self.timeout)
            self._register_meep_rank_socket(csock, peer)
        else:
            try:
                csock.close()
            except OSError:
                pass

    def _register_driver_socket(self, csock: socket.socket, peer: str) -> None:
        """
        Hand an ordinary ``mxl_driver`` socket to the inherited SocketHub pool.

        Drivers send nothing at connect time, so a socket that produced no
        ``MXLINIT`` header within the classify window lands here and is parked as
        an unbound client (``molecule_id=-1``) for later binding.

        Parameters
        ----------
        csock : socket.socket
            The accepted driver socket.
        peer : str
            Human-readable peer address for logging/state.
        """

        if self._stop:
            try:
                csock.close()
            except OSError:
                pass
            return
        csock.settimeout(self.timeout)
        st = _ClientState(sock=csock, address=peer, molecule_id=-1)
        with self._lock:
            self.clients[id(csock)] = st

    def _register_meep_rank_socket(self, csock: socket.socket, peer: str) -> None:
        """
        Track a Meep rank socket and spawn its dedicated handler thread.

        Parameters
        ----------
        csock : socket.socket
            The accepted socket that sent an ``MXLINIT`` header.
        peer : str
            Human-readable peer address for logging.
        """

        if self._stop:
            try:
                csock.close()
            except OSError:
                pass
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

    def _serve_meep_rank(self, sock: socket.socket, peer: str) -> None:
        """
        Handle one Meep rank connection after its ``MXLINIT`` header.

        Reads the init payload, registers the rank's molecule IDs, waits for the
        matching drivers to bind, replies ``MXLREADY``, then services
        ``AGGSTEP``/``AGGRESULT`` frames in a loop until the socket closes or the
        server stops.  Cleans up the rank socket on exit.

        Parameters
        ----------
        sock : socket.socket
            The rank socket, positioned just after its ``MXLINIT`` header.
        peer : str
            Human-readable peer address for logging.
        """

        try:
            step_codec = _StepCodec()
            result_codec = _ResultCodec()
            init_payload = self._recv_mxl_init_payload(sock)
            rank = int(init_payload.get("rank", -1))
            molecule_ids = [int(mid) for mid in init_payload.get("molecule_ids", [])]
            if not molecule_ids:
                raise RuntimeError("MXLINIT payload did not include molecule_ids.")

            init_payloads = self._register_rank_molecules(init_payload, molecule_ids)
            with self._meep_lock:
                self._rank_payloads[rank] = init_payload
                self._rank_init_payloads[rank] = init_payloads
                self.rank_stats[rank] = {
                    "molecule_count": len(molecule_ids),
                    "steps": 0,
                    "requests": 0,
                    "peer": peer,
                }

            print(
                f"[SusceptibilitySocketHub] Meep rank {rank} requested "
                f"{len(molecule_ids)} drivers from {peer}.",
                flush=True,
            )
            # Drivers are passive at connect time, so their classifier workers
            # register them only after a short no-header timeout.  Give any
            # already-accepted driver sockets one classifier window before the
            # rank enters the binding wait loop.
            time.sleep(max(self.latency, 0.25))
            ok = self.wait_until_bound(
                init_payloads,
                require_init=True,
                timeout=None,
            )
            if not ok:
                raise RuntimeError(f"Timed out waiting for rank {rank} drivers.")
            _send_msg(sock, MXLREADY)

            while not self._stop:
                try:
                    header = _recv_msg(sock)
                except socket.timeout:
                    continue
                if header != AGGSTEP:
                    raise RuntimeError(f"Unexpected Meep susceptibility header {header!r}.")
                efields = step_codec.recv(sock, header_already_read=True)
                responses = self._run_susceptibility_step(efields, init_payloads)
                result_codec.send(sock, responses)
                with self._meep_lock:
                    stats = self.rank_stats.get(rank)
                    if stats is not None:
                        stats["steps"] += 1
                        stats["requests"] += len(efields)

        except (_SocketClosed, OSError):
            pass
        except Exception as exc:
            print(
                f"[SusceptibilitySocketHub] Meep rank connection {peer} failed: {exc!r}",
                flush=True,
            )
        finally:
            try:
                sock.close()
            except OSError:
                pass
            with self._meep_lock:
                try:
                    self._rank_sockets.remove(sock)
                except ValueError:
                    pass

    def _recv_mxl_init_payload(self, sock: socket.socket) -> dict:
        """
        Read and validate the JSON ``MXLINIT`` payload from a Meep rank.

        Parameters
        ----------
        sock : socket.socket
            The rank socket, positioned just after its ``MXLINIT`` header.

        Returns
        -------
        dict
            The decoded init payload (rank, molecule_ids, dt_au, ...).

        Raises
        ------
        RuntimeError
            If the payload does not declare
            ``protocol == "mxl_socket_susceptibility_v1"``.
        """

        payload = json.loads(_recv_bytes(sock).decode("utf-8") or "{}")
        if payload.get("protocol") != "mxl_socket_susceptibility_v1":
            raise RuntimeError(
                "Expected protocol='mxl_socket_susceptibility_v1' in MXLINIT."
            )
        return payload

    def _register_rank_molecules(
        self, init_payload: dict, molecule_ids: list[int]
    ) -> dict[int, dict]:
        """
        Register a rank's molecule IDs as expected and build per-ID INIT payloads.

        Each molecule ID is added to the inherited ``expected``/``bound`` bind
        bookkeeping, and a per-driver INIT payload is assembled carrying the
        timestep and rescaling metadata the SHO drivers need.

        Parameters
        ----------
        init_payload : dict
            The rank-level ``MXLINIT`` payload.
        molecule_ids : list[int]
            Molecule IDs owned by this rank.

        Returns
        -------
        dict[int, dict]
            Mapping from molecule ID to its INIT payload (``dt_au``,
            ``rescaling_factor``, ``time_units_fs``, ``mxl_rank``).
        """

        dt_au = float(init_payload.get("dt_au", 0.0))
        rank = int(init_payload.get("rank", -1))
        payloads: dict[int, dict] = {}
        for mid in molecule_ids:
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
            }
        return payloads

    def _run_susceptibility_step(
        self, efields: Dict[int, np.ndarray], init_payloads: dict[int, dict]
    ) -> Dict[int, dict]:
        """
        Run one barrier step: fields in, source amplitudes out.

        Wraps the per-molecule fields into request dicts and drives the inherited
        :meth:`step_barrier`.  If the barrier returns empty (a driver dropped or
        is not yet bound), it re-binds and retries until ``timeout`` elapses.

        Parameters
        ----------
        efields : dict[int, numpy.ndarray]
            Mapping from molecule ID to its ``(3,)`` electric field in a.u.
        init_payloads : dict[int, dict]
            Per-molecule INIT payloads from :meth:`_register_rank_molecules`.

        Returns
        -------
        dict[int, dict]
            Mapping ``molecule_id -> {"amp": ndarray(3,), "extra": bytes}``.

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

    def stop(self):
        """Stop Meep rank handlers and ordinary SocketHub clients."""

        self._stop = True
        with self._meep_lock:
            rank_sockets = list(self._rank_sockets)
            rank_threads = list(self._rank_threads)
            classifier_threads = list(self._classifier_threads)
        for sock in rank_sockets:
            try:
                sock.close()
            except OSError:
                pass
        super().stop()
        for thread in classifier_threads:
            thread.join(timeout=0.2)
        for thread in rank_threads:
            thread.join(timeout=1.0)


class SusceptibilitySocketHub:
    """
    Process-backed hub for Meep ``MXLSocketSusceptibility`` connections.

    The public object starts immediately during construction and exposes the
    endpoint fields consumed by ``mp.MXLSocketSusceptibility(hub=hub)``.  The
    actual server runs in a child process because Meep's long C-level time-step
    loop can hold Python's GIL while waiting on the socket; an in-process Python
    thread would therefore deadlock at the first ``MXLINIT``/``MXLREADY``
    handshake.  The child server is ``_SusceptibilitySocketHubServer``, which is
    the ``SocketHub``-derived implementation that reuses MaxwellLink's optimized
    driver binding and stepping path.

    Under MPI only the master rank launches the child server; every rank then
    learns the resolved endpoint through :func:`mpi_bcast_from_master`, so all
    ranks agree on the ``host``/``port`` to connect to.

    Parameters
    ----------
    host : str or None, optional
        Bind host for the server.  ``None`` uses the server default.
    port : int or None, default: 31415
        Bind port.  ``0`` requests an OS-chosen ephemeral port.
    timeout : float, default: 60000.0
        Socket timeout in seconds passed to the server.
    latency : float, default: 0.05
        Polling interval in seconds passed to the server.
    unixsocket : str or None, optional
        Reserved; must be falsy (TCP only).

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

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = 31415,
        timeout: float = 60000.0,
        latency: float = 0.05,
        unixsocket: Optional[str] = None,
    ):
        if unixsocket:
            raise ValueError(
                "SusceptibilitySocketHub currently supports TCP host/port only."
            )

        if port is None:
            port = 31415
        if int(port) == 0:
            port = _choose_ephemeral_port(host)

        self.timeout = float(timeout)
        self.latency = float(latency)
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
            self._process = ctx.Process(
                target=_run_susceptibility_socket_hub_server,
                args=(
                    host,
                    int(port),
                    self.timeout,
                    self.latency,
                    self._ready_queue,
                    self._stats_queue,
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
                ready = {"error": "SusceptibilitySocketHub server did not start."}
            except Exception as exc:
                ready = {"error": repr(exc)}

        ready = mpi_bcast_from_master(ready)
        if not ready or "error" in ready:
            self.stop()
            msg = ready.get("error", "unknown startup error") if ready else "unknown startup error"
            raise RuntimeError(f"SusceptibilitySocketHub server failed to start: {msg}")

        self.host = str(ready["host"])
        self.address = self.host
        self.port = int(ready["port"])

    def _drain_stats(self) -> None:
        """
        Pull any pending stats snapshots from the child and cache the latest.

        No-op on non-master ranks (which never started the child process).
        """

        if not self._is_master or self._stats_queue is None:
            return
        while True:
            try:
                stats = self._stats_queue.get_nowait()
            except queue.Empty:
                break
            self._stats_cache = {int(rank): dict(row) for rank, row in stats.items()}

    @property
    def rank_stats(self) -> dict[int, dict]:
        """
        Latest per-Meep-rank statistics from the running server.

        Returns
        -------
        dict[int, dict]
            Mapping from rank to its stats row (``molecule_count``, ``steps``,
            ``requests``, ``peer``).  Empty on non-master ranks.
        """

        self._drain_stats()
        return _copy_rank_stats(self._stats_cache)

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

        Maps the standard Meep Lorentzian dielectric parameters (resonance
        ``frequency``, oscillator strength ``sigma``) onto the simple-harmonic
        oscillator (SHO) molecular driver: a resonance ``omega`` in atomic units,
        the fixed transition dipole ``mu0_au``, and the ``rescaling_factor`` that
        the Meep ``MXLSocketSusceptibility`` applies so one socket molecule
        reproduces the per-cell Lorentzian response.

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
            Lorentzian damping rate.  Accepted for API symmetry but ignored: the
            current SHO driver is lossless (a warning is printed if nonzero).
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
            Dictionary with two keys:

            - ``"rescaling_factor"`` : float
              Value to pass to ``mp.MXLSocketSusceptibility(rescaling_factor=...)``.
            - ``"driver_command"`` : str
              Ready-to-run shell command that launches one matching
              ``mxl_driver --model sho`` client against this hub.

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
        rescaling_factor = (
            sigma * cell_measure * omega_au * omega_au * (time_units_fs * FS_TO_AU)
            / (MXL_SOURCE_AMP_AU_TO_MEEP * efield_factor * mu0_au * mu0_au)
        )

        driver_param = f"omega={omega_au:.17g},mu0={mu0_au:.17g},orientation={int(orientation)}"
        driver_command = (
            f"mxl_driver --model sho --address {self.host} --port {self.port} "
            f'--param "{driver_param}"'
        )

        if am_master():
            if gamma != 0.0:
                print(
                    f"[SusceptibilitySocketHub] gamma={gamma} ignored "
                    "(SHO driver is lossless).",
                    flush=True,
                )
            print(
                f"[SusceptibilitySocketHub] rescaling_factor={rescaling_factor:.12g}",
                flush=True,
            )
            print(f"[SusceptibilitySocketHub] {driver_command}", flush=True)

        return {"rescaling_factor": rescaling_factor, "driver_command": driver_command}

    def stop(self) -> None:
        """
        Stop the hub and tear down the child server process.

        Idempotent and safe on non-master ranks.  Signals the child via the stop
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


__all__ = ["SusceptibilitySocketHub"]
