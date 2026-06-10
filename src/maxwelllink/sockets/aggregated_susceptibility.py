# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
Aggregated SocketHub for Meep ``MXLSocketSusceptibility`` connections.

This module combines the Meep-facing ``MXLINIT`` protocol from
``SusceptibilitySocketHub`` with the bridge transport from
``AggregatedSocketHub``.  Meep ranks still connect to one TCP endpoint, but the
hub forwards rank requests to a small number of aggregate bridge connections;
each bridge fans out locally to many ordinary ``mxl_driver`` clients over UNIX
sockets.
"""

from __future__ import annotations

import json
import math
import multiprocessing as mp
import os
import queue
import shlex
import socket
import threading
import time
from collections import Counter
from typing import Dict, Optional

import numpy as np

from .aggregated import (
    AGGHELLO,
    AGGREGATION_INFO_VERSION,
    AGGSTEP,
    AggregatedSocketHub,
    _AggregateGroupState,
    RemoteBridgeSpec,
    _ResultCodec,
    _StepCodec,
    _close_socket,
    _json_loads_bytes,
)
from .sockets import (
    _ClientState,
    _SocketClosed,
    _recv_bytes,
    _recv_msg,
    _send_msg,
    am_master,
    mpi_bcast_from_master,
)
from .susceptibility import (
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


def _aggregation_manifest(
    *,
    hub_host,
    hub_port,
    timeout: float,
    latency: float,
    unix_prefix: str,
    molecules_per_bridge: Optional[int],
    bridges: list,
) -> dict:
    """
    Build one aggregation manifest payload in the canonical key order.

    This is the single place that defines the manifest schema shared by the
    child hub's ``bridge_info``, the finalized on-disk manifest, and the public
    hub's ``init_remote_bridges`` placeholder.

    Parameters
    ----------
    hub_host : str
        Host name or IP that bridge processes connect back to.
    hub_port : int
        TCP port of the upstream aggregate hub.
    timeout : float
        Operation timeout (seconds) recorded for downstream bridge nodes;
        coerced to ``float``.
    latency : float
        Polling interval (seconds) recorded for downstream bridge nodes;
        coerced to ``float``.
    unix_prefix : str
        Prefix used to generate the per-bridge UNIX-socket names.
    molecules_per_bridge : int or None
        Target molecule count per bridge, or ``None`` when partitioning is still
        deferred (placeholder manifest).
    bridges : list
        Per-bridge specification dicts, as produced by
        :meth:`RemoteBridgeSpec.to_dict`.

    Returns
    -------
    dict
        Manifest mapping with ``version``, ``hub_host``, ``hub_port``,
        ``timeout``, ``latency``, ``unix_prefix``, ``molecules_per_bridge``, and
        ``bridges`` entries, in that key order.
    """

    return {
        "version": AGGREGATION_INFO_VERSION,
        "hub_host": hub_host,
        "hub_port": hub_port,
        "timeout": float(timeout),
        "latency": float(latency),
        "unix_prefix": unix_prefix,
        "molecules_per_bridge": molecules_per_bridge,
        "bridges": bridges,
    }


def _write_manifest(path: str, payload: dict) -> None:
    """
    Write one aggregation manifest to ``path`` as pretty, sorted JSON.

    Parameters
    ----------
    path : str or path-like
        Destination file path; any existing content is overwritten.
    payload : dict
        Manifest mapping to serialize, typically from
        :func:`_aggregation_manifest`.
    """

    with open(os.fspath(path), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=True)


def _run_aggregated_susceptibility_socket_hub_server(
    host: Optional[str],
    port: int,
    timeout: float,
    latency: float,
    num_bridges: int,
    unix_prefix: str,
    init_grace_seconds: float,
    ready_queue,
    stats_queue,
    control_queue,
    stop_event,
) -> None:
    """
    Child-process entry point for :class:`AggregatedSusceptibilitySocketHub`.

    Parameters
    ----------
    host : str or None
        Interface the upstream TCP server binds to.
    port : int
        TCP port for the upstream server.
    timeout : float
        Operation timeout (seconds) for binding and stepping.
    latency : float
        Polling interval (seconds) for the bind/step loops.
    num_bridges : int
        Initial number of aggregate bridge groups.
    unix_prefix : str
        Prefix used to generate aggregate bridge group ids.
    init_grace_seconds : float
        Grace period (seconds) for collecting the first burst of rank INITs when
        the expected molecule total is not announced up front.
    ready_queue : multiprocessing.Queue
        Startup channel: the child puts ``{"host", "port", "bridge_info"}`` on
        success or ``{"error": ...}`` on failure.
    stats_queue : multiprocessing.Queue
        Channel the child pushes per-rank statistics snapshots onto whenever they
        change, plus a final snapshot on shutdown.
    control_queue : multiprocessing.Queue
        Channel the parent uses to send control commands (e.g.
        ``init_remote_bridges``) into the running hub.
    stop_event : multiprocessing.Event
        Event set by the parent to request shutdown.

    Notes
    -----
    Runs until ``stop_event`` is set, then stops the hub and flushes a final
    statistics snapshot. A startup exception is reported through ``ready_queue``
    instead of raised, so the parent process can surface it.
    """

    server = None
    last_stats = None
    try:
        server = _AggregatedSusceptibilitySocketHubServer(
            host=host,
            port=port,
            timeout=timeout,
            latency=latency,
            num_bridges=num_bridges,
            unix_prefix=unix_prefix,
            init_grace_seconds=init_grace_seconds,
        )
        server._control_queue = control_queue
        ready_queue.put(
            {
                "host": server.host,
                "port": server.port,
                "bridge_info": server.bridge_info,
            }
        )
    except Exception as exc:
        ready_queue.put({"error": repr(exc)})
        return

    try:
        while not stop_event.wait(0.25):
            server.drain_control_queue()
            stats = _copy_rank_stats(server.rank_stats)
            if stats != last_stats:
                stats_queue.put(stats)
                last_stats = stats
    finally:
        if server is not None:
            server.stop()
            stats_queue.put(_copy_rank_stats(server.rank_stats))


class _AggregatedSusceptibilitySocketHubServer(AggregatedSocketHub):
    """
    Meep-facing aggregate hub for C-level ``MXLSocketSusceptibility`` clients.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = 31415,
        timeout: float = 60000.0,
        latency: float = 0.05,
        num_bridges: int = 1,
        unix_prefix: str = "mxl_bridge_",
        init_grace_seconds: float = 0.5,
    ):
        """
        Bind the upstream TCP server and initialize hub bookkeeping.

        Parameters
        ----------
        host : str or None, optional
            Interface to bind the upstream TCP server to. ``None``, ``""``,
            ``"0.0.0.0"``, or ``"::"`` bind all interfaces; bridges and ranks
            then connect back over ``127.0.0.1``.
        port : int or None, default: 31415
            TCP port for the upstream server. ``0`` selects an ephemeral port.
        timeout : float, default: 60000.0
            Operation timeout (seconds) for binding and stepping.
        latency : float, default: 0.05
            Polling interval (seconds) for the bind/step loops.
        num_bridges : int, default: 1
            Initial number of aggregate bridge groups (overridden later if a
            remote-bridge policy is configured).
        unix_prefix : str, default: ``"mxl_bridge_"``
            Prefix used to generate aggregate bridge group ids
            ``f"{unix_prefix}{idx}"``.
        init_grace_seconds : float, default: 0.5
            Grace period (seconds) for collecting the first burst of rank INITs
            when the expected molecule total is not announced up front.

        Raises
        ------
        ValueError
            If ``num_bridges`` is not positive.
        """

        nbridge = int(num_bridges)
        if nbridge <= 0:
            raise ValueError("num_bridges must be positive.")

        self._rank_threads: list[threading.Thread] = []
        self._classifier_threads: list[threading.Thread] = []
        self._rank_sockets: list[socket.socket] = []
        self._client_init_payloads: dict[int, dict[int, dict]] = {}
        self._client_ordinals: dict[int, int] = {}
        self._client_steps: dict[int, int] = {}
        self._rank_client_counts: dict[int, int] = {}
        self._next_client_id = 0
        self.rank_stats: dict[int, dict] = {}
        self._step_lock = threading.RLock()
        self._meep_lock = threading.RLock()
        self._global_step_cond = threading.Condition(self._meep_lock)
        self._global_pending_key: Optional[tuple[int, int]] = None
        self._global_pending_requests: dict[int, dict[int, dict]] = {}
        self._global_pending_mids: dict[int, set[int]] = {}
        self._global_results: dict[int, dict[int, dict]] = {}
        self._global_error: Optional[BaseException] = None
        self._global_running = False
        self._unix_prefix = str(unix_prefix)
        self._group_ids = [f"{self._unix_prefix}{idx}" for idx in range(nbridge)]
        self._group_loads = {group_id: 0 for group_id in self._group_ids}
        self._group_capacities: Optional[dict[str, int]] = None
        self._mxl_molecule_to_group: dict[int, str] = {}
        self._request_caches: dict[int, dict[int, dict]] = {}
        self._init_grace_seconds = max(0.0, float(init_grace_seconds))
        self._ordinal_first_init_time: dict[int, float] = {}
        self._expected_total_molecules: Optional[int] = None
        self._remote_bridge_policy: Optional[dict] = None
        self._bridge_manifest_info: Optional[dict] = None
        self._bridge_manifest_written = False
        self._control_queue = None

        super().__init__(host=host, port=port, timeout=timeout, latency=latency)

        sockname = self.serversock.getsockname()
        actual_host = sockname[0] if isinstance(sockname, tuple) else host
        actual_port = sockname[1] if isinstance(sockname, tuple) else port
        self.host = "127.0.0.1" if actual_host in (None, "", "0.0.0.0", "::") else actual_host
        self.port = int(actual_port)
        self.timeout = float(timeout)

    @property
    def bridge_info(self) -> dict:
        """
        Manifest payload consumed by ``mxl_bridge --info ...``.

        Returns
        -------
        dict
            A copy of the finalized manifest once it has been written, otherwise
            a placeholder manifest enumerating the currently configured groups
            with zero molecules each.
        """

        if self._bridge_manifest_info is not None:
            return dict(self._bridge_manifest_info)
        return _aggregation_manifest(
            hub_host=self._bridge_connect_host,
            hub_port=self._bridge_connect_port,
            timeout=self.timeout,
            latency=self.latency,
            unix_prefix=self._unix_prefix,
            molecules_per_bridge=None,
            bridges=[
                RemoteBridgeSpec(
                    idx=idx,
                    group_id=group_id,
                    unixsocket=group_id,
                    n_molecules=0,
                ).to_dict()
                for idx, group_id in enumerate(self._group_ids)
            ],
        )

    def configure_remote_bridges(
        self,
        *,
        molecules_per_bridge: int,
        unix_prefix: str,
        save_file: str,
    ) -> None:
        """
        Record the ``init_remote_bridges`` policy sent by the public hub.

        Parameters
        ----------
        molecules_per_bridge : int
            Target number of socket molecules per aggregate bridge.
        unix_prefix : str
            Prefix used to generate aggregate bridge group ids.
        save_file : str or path-like
            Path where the finalized bridge manifest will be written.

        Raises
        ------
        ValueError
            If ``molecules_per_bridge`` is not a positive integer.

        Notes
        -----
        If the expected molecule total is already known, the group layout is
        recomputed immediately; otherwise it is deferred until the first
        ``MXLINIT`` reports ``expected_total_molecules``.
        """

        per_bridge = int(molecules_per_bridge)
        if per_bridge <= 0:
            raise ValueError("molecules_per_bridge must be a positive integer.")
        with self._meep_lock:
            self._remote_bridge_policy = {
                "molecules_per_bridge": per_bridge,
                "unix_prefix": str(unix_prefix),
                "save_file": os.fspath(save_file),
            }
            if self._expected_total_molecules is not None:
                self._configure_remote_bridge_layout_locked(
                    self._expected_total_molecules
                )

    def drain_control_queue(self) -> None:
        """
        Apply any pending control commands from the parent process.

        Notes
        -----
        Drains the control queue non-blockingly and handles
        ``init_remote_bridges`` commands by forwarding them to
        :meth:`configure_remote_bridges`. Called both from the child's main loop
        and from rank handlers so a late policy still takes effect. A no-op when
        no control queue is attached.
        """

        q = self._control_queue
        if q is None:
            return
        while True:
            try:
                msg = q.get_nowait()
            except queue.Empty:
                break
            if isinstance(msg, dict) and msg.get("cmd") == "init_remote_bridges":
                self.configure_remote_bridges(
                    molecules_per_bridge=msg["molecules_per_bridge"],
                    unix_prefix=msg["unix_prefix"],
                    save_file=msg["save_file"],
                )

    def _configure_remote_bridge_layout_locked(self, total_molecules: int) -> None:
        """
        Recompute group ids and loads from the remote-bridge policy.

        Parameters
        ----------
        total_molecules : int
            Expected total number of socket molecules across all ranks, used to
            size the bridge count as ``ceil(total / molecules_per_bridge)``.

        Notes
        -----
        Caller must hold ``self._meep_lock``. A no-op when no remote-bridge
        policy is set, when molecules have already been assigned to groups, or
        when ``total_molecules`` is non-positive. The resulting per-group
        capacities are also used by lazy Meep registrations so manifest driver
        counts and later molecule assignments stay consistent.
        """

        if self._remote_bridge_policy is None or self._mxl_molecule_to_group:
            return
        total = int(total_molecules)
        if total <= 0:
            return
        per_bridge = int(self._remote_bridge_policy["molecules_per_bridge"])
        nbridge = max(1, int(math.ceil(total / per_bridge)))
        self._unix_prefix = str(self._remote_bridge_policy["unix_prefix"])
        self._group_ids = [f"{self._unix_prefix}{idx}" for idx in range(nbridge)]
        self._group_loads = {group_id: 0 for group_id in self._group_ids}
        remaining = total
        capacities = {}
        for group_id in self._group_ids:
            capacity = min(per_bridge, remaining)
            capacities[group_id] = max(0, capacity)
            remaining -= capacity
        self._group_capacities = capacities

    def _note_expected_total_molecules_locked(self, init_payload: dict) -> None:
        """
        Record the expected molecule total advertised in an INIT payload.

        Parameters
        ----------
        init_payload : dict
            MXLINIT payload, optionally carrying an ``"expected_total_molecules"``
            entry.

        Raises
        ------
        RuntimeError
            If two INIT payloads report different positive totals.

        Notes
        -----
        Caller must hold ``self._meep_lock``. A no-op when the payload omits the
        total or reports a non-positive value. On the first valid total it also
        triggers the deferred group-layout computation.
        """

        raw_total = init_payload.get("expected_total_molecules")
        if raw_total is None:
            return
        total = int(raw_total)
        if total <= 0:
            return
        if (
            self._expected_total_molecules is not None
            and self._expected_total_molecules != total
        ):
            raise RuntimeError(
                "Inconsistent expected_total_molecules values in MXLINIT "
                f"payloads: {self._expected_total_molecules} vs {total}."
            )
        self._expected_total_molecules = total
        self._configure_remote_bridge_layout_locked(total)

    def _registered_molecule_count_locked(self) -> int:
        """
        Count socket molecules registered across all Meep clients so far.

        Returns
        -------
        int
            Total number of molecules across every client's INIT payload set.

        Notes
        -----
        Caller must hold ``self._meep_lock``.
        """

        return sum(len(payloads) for payloads in self._client_init_payloads.values())

    def _write_final_bridge_manifest_locked(self) -> None:
        """
        Write the finalized bridge manifest once bridge counts are known.

        Notes
        -----
        Caller must hold ``self._meep_lock``. Idempotent and a no-op until a
        remote-bridge policy is configured. If the total molecule count has
        already produced per-group capacities, those capacities are written
        immediately so remote bridge jobs can start before later lazy Meep
        socket clients register. Without capacities, the manifest waits until
        every expected molecule has registered and uses the observed group
        loads. On success it writes the manifest to disk, caches it on
        ``self._bridge_manifest_info``, and latches
        ``self._bridge_manifest_written`` so it runs at most once.
        """

        if self._remote_bridge_policy is None or self._bridge_manifest_written:
            return
        expected = self._expected_total_molecules
        capacities = self._group_capacities
        if (
            capacities is None
            and expected is not None
            and self._registered_molecule_count_locked() < expected
        ):
            return
        specs = [
            RemoteBridgeSpec(
                idx=idx,
                group_id=group_id,
                unixsocket=group_id,
                n_molecules=int(
                    capacities.get(group_id, 0)
                    if capacities is not None
                    else self._group_loads.get(group_id, 0)
                ),
            )
            for idx, group_id in enumerate(self._group_ids)
        ]
        payload = _aggregation_manifest(
            hub_host=self._bridge_connect_host,
            hub_port=self._bridge_connect_port,
            timeout=self.timeout,
            latency=self.latency,
            unix_prefix=str(self._remote_bridge_policy["unix_prefix"]),
            molecules_per_bridge=int(self._remote_bridge_policy["molecules_per_bridge"]),
            bridges=[spec.to_dict() for spec in specs],
        )
        path = os.fspath(self._remote_bridge_policy["save_file"])
        _write_manifest(path, payload)
        self._bridge_manifest_info = payload
        self._bridge_manifest_written = True
        print(
            "[AggregatedSusceptibilitySocketHub] finalized aggregate bridge "
            f"manifest {path!r} with {len(specs)} bridge(s) for "
            f"{sum(spec.n_molecules for spec in specs)} socket molecule(s).",
            flush=True,
        )

    def _accept_loop(self) -> None:
        """
        Accept aggregate bridge clients and Meep susceptibility clients.

        Notes
        -----
        Overrides the base accept loop. Each accepted socket is handed to a
        daemon classifier thread (:meth:`_classify_socket`) so a single slow peer
        cannot stall the listener, and dead classifier threads are reaped on each
        new connection.
        """

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
        Route one accepted socket by its first protocol header.

        Parameters
        ----------
        csock : socket.socket
            Freshly accepted connection awaiting classification.
        peer : str
            Human-readable peer address used for logging.

        Notes
        -----
        An ``MXLINIT`` header routes to :meth:`_register_meep_rank_socket`, an
        ``AGGHELLO`` header to :meth:`_register_bridge_socket_after_hello`, and
        anything else (or a read error/timeout) closes the socket.
        """

        csock.settimeout(max(self.latency, 0.25))
        try:
            header = _recv_msg(csock)
        except (socket.timeout, _SocketClosed, OSError, RuntimeError):
            _close_socket(csock)
            return

        if header == MXLINIT:
            csock.settimeout(self.timeout)
            self._register_meep_rank_socket(csock, peer)
        elif header == AGGHELLO:
            self._register_bridge_socket_after_hello(csock, peer)
        else:
            _close_socket(csock)

    def _register_bridge_socket_after_hello(self, csock: socket.socket, peer: str) -> None:
        """
        Register an aggregate bridge after its ``AGGHELLO`` header.

        Parameters
        ----------
        csock : socket.socket
            Bridge connection that has already sent its ``AGGHELLO`` banner.
        peer : str
            Human-readable peer address used for logging.

        Notes
        -----
        Reads the HELLO JSON payload and registers the socket as an unbound
        client tagged with its ``aggregate_group``; the bind-loop later promotes
        it to a group. The socket is closed if the payload is malformed, names an
        unknown group, or arrives while the hub is stopping.
        """

        try:
            hello = _json_loads_bytes(_recv_bytes(csock))
        except (_SocketClosed, OSError, RuntimeError):
            _close_socket(csock)
            return

        group_id = str(hello.get("group_id", "")).strip()
        if group_id not in self._group_ids or self._stop:
            _close_socket(csock)
            return

        csock.settimeout(self.timeout)
        st = _ClientState(sock=csock, address=peer, molecule_id=-1)
        st.extras["aggregate_group"] = group_id
        with self._lock:
            self.clients[id(csock)] = st
        self._log(f"HELLO: aggregate group {group_id!r} <- {peer}")

    def _register_meep_rank_socket(self, csock: socket.socket, peer: str) -> None:
        """
        Track a Meep rank socket and spawn its handler thread.

        Parameters
        ----------
        csock : socket.socket
            Meep rank connection that has already sent its ``MXLINIT`` banner.
        peer : str
            Human-readable peer address used for logging.

        Notes
        -----
        Records the socket and starts a daemon :meth:`_serve_meep_rank` thread to
        handle the connection. The socket is closed if the hub is stopping.
        """

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

    def _serve_meep_rank(self, sock: socket.socket, peer: str) -> None:
        """
        Handle one Meep rank connection after its ``MXLINIT`` header.

        Parameters
        ----------
        sock : socket.socket
            Meep rank connection positioned just past its ``MXLINIT`` banner.
        peer : str
            Human-readable peer address used for logging and statistics.

        Notes
        -----
        Reads the INIT payload, registers the rank's molecules onto aggregate
        groups, waits for the bridge set to come up, replies ``MXLREADY``, then
        serves ``AGGSTEP`` frames in a loop, decoding e-fields, running the
        global timestep barrier, and returning packed amplitudes until the peer
        disconnects or the hub stops. Multiple socket clients from the same rank
        are assigned rank-local ordinals so they can advance in Meep's serial
        update order. The socket is always closed and de-registered on exit.
        """

        client_id = None
        try:
            step_codec = _StepCodec()
            result_codec = _ResultCodec()
            init_payload = self._recv_mxl_init_payload(sock)
            rank = int(init_payload.get("rank", -1))
            molecule_ids = [int(mid) for mid in init_payload.get("molecule_ids", [])]
            if not molecule_ids:
                raise RuntimeError("MXLINIT payload did not include molecule_ids.")

            self.drain_control_queue()
            if self._remote_bridge_policy is None:
                time.sleep(min(max(self.latency, 0.05), 0.25))
                self.drain_control_queue()
            with self._meep_lock:
                self._note_expected_total_molecules_locked(init_payload)

            init_payloads = self._register_rank_molecules(init_payload, molecule_ids)
            with self._meep_lock:
                client_id = self._next_client_id
                self._next_client_id += 1
                ordinal = self._rank_client_counts.get(rank, 0)
                self._rank_client_counts[rank] = ordinal + 1
                self._client_init_payloads[client_id] = init_payloads
                self._client_ordinals[client_id] = ordinal
                self._client_steps[client_id] = 0
                aggregate_groups = {
                    payload.get("aggregate_group") for payload in init_payloads.values()
                }
                stats = self.rank_stats.setdefault(
                    rank,
                    {
                        "molecule_count": 0,
                        "steps": 0,
                        "requests": 0,
                        "peer": peer,
                        "peers": [],
                        "client_count": 0,
                        "aggregate_groups": [],
                    },
                )
                stats["molecule_count"] += len(molecule_ids)
                stats["client_count"] += 1
                stats["peers"].append(peer)
                stats["aggregate_groups"] = sorted(
                    set(stats["aggregate_groups"]) | aggregate_groups
                )

            group_counts = Counter(
                str(payload["aggregate_group"]) for payload in init_payloads.values()
            )
            for group_id, count in sorted(group_counts.items()):
                print(
                    f"[AggregatedSusceptibilitySocketHub] Meep rank {rank} "
                    f"socket {ordinal} requested {count} drivers from {peer}; "
                    f"group={group_id!r}.",
                    flush=True,
                )

            self._wait_for_rank_ordinal_burst(ordinal)
            with self._meep_lock:
                self._write_final_bridge_manifest_locked()
            all_init_payloads = self._snapshot_rank_init_payloads()
            with self._step_lock:
                ok = self.wait_until_bound(
                    all_init_payloads,
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
                responses = self._run_susceptibility_step(client_id, efields)
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
                f"[AggregatedSusceptibilitySocketHub] Meep rank connection {peer} "
                f"failed: {exc!r}",
                flush=True,
            )
        finally:
            _close_socket(sock)
            with self._meep_lock:
                try:
                    self._rank_sockets.remove(sock)
                except ValueError:
                    pass
            if client_id is not None:
                self._retire_client(client_id)

    def _recv_mxl_init_payload(self, sock: socket.socket) -> dict:
        """
        Read and validate one ``MXLINIT`` JSON payload.

        Parameters
        ----------
        sock : socket.socket
            Meep rank connection positioned at the INIT JSON frame.

        Returns
        -------
        dict
            The decoded INIT payload.

        Raises
        ------
        RuntimeError
            If the payload's ``"protocol"`` is not
            ``"mxl_socket_susceptibility_v1"``.
        """

        payload = _json_loads_bytes(_recv_bytes(sock))
        if payload.get("protocol") != "mxl_socket_susceptibility_v1":
            raise RuntimeError(
                "Expected protocol='mxl_socket_susceptibility_v1' in MXLINIT."
            )
        return payload

    def _wait_for_rank_ordinal_burst(self, ordinal: int) -> None:
        """
        Sleep through the INIT grace window for one rank-local socket ordinal.

        Meep initializes multiple ``MXLSocketSusceptibility`` objects on a rank
        sequentially.  The first object on every rank has ordinal 0, the second
        has ordinal 1, and so on.  Pausing briefly when the first client for an
        ordinal arrives lets peer ranks register the same ordinal before the
        first timestep barrier is formed.
        """

        if self._init_grace_seconds <= 0.0:
            return
        with self._meep_lock:
            first = self._ordinal_first_init_time.setdefault(ordinal, time.time())
            deadline = first + self._init_grace_seconds
        remaining = deadline - time.time()
        if remaining > 0.0:
            time.sleep(remaining)

    def _snapshot_rank_init_payloads(self) -> dict[int, dict]:
        """
        Return all Meep-rank INIT payloads collected during the startup burst.

        Returns
        -------
        dict[int, dict]
            Mapping from molecule ID to its INIT payload, merged across every
            registered Meep socket client.
        """

        payloads: dict[int, dict] = {}
        with self._meep_lock:
            for client_payloads in self._client_init_payloads.values():
                payloads.update(client_payloads)
        return payloads

    def _molecule_group(self, molecule_id: int) -> str:
        """
        Assign one socket molecule to the least-loaded aggregate bridge group.

        Parameters
        ----------
        molecule_id : int
            Socket molecule ID to place on a bridge group.

        Returns
        -------
        str
            The aggregate group id the molecule is assigned to.

        Notes
        -----
        Assignment is sticky: a molecule keeps its first group. New molecules go
        to the group with the fewest molecules (ties broken by group id), and the
        chosen group's load counter is incremented. Holds ``self._meep_lock``.
        """

        with self._meep_lock:
            group_id = self._mxl_molecule_to_group.get(molecule_id)
            if group_id is None:
                if self._group_capacities is None:
                    group_id = min(
                        self._group_ids,
                        key=lambda gid: (self._group_loads.get(gid, 0), gid),
                    )
                else:
                    group_id = next(
                        (
                            gid
                            for gid in self._group_ids
                            if self._group_loads.get(gid, 0)
                            < self._group_capacities.get(gid, 0)
                        ),
                        None,
                    )
                    if group_id is None:
                        raise RuntimeError(
                            "Registered more MXLSocket molecules than the "
                            "announced expected_total_molecules capacity."
                        )
                self._mxl_molecule_to_group[molecule_id] = group_id
                self._group_loads[group_id] = self._group_loads.get(group_id, 0) + 1
            return group_id

    def _register_rank_molecules(
        self, init_payload: dict, molecule_ids: list[int]
    ) -> dict[int, dict]:
        """
        Map a rank's molecules onto groups and build their INIT payloads.

        Parameters
        ----------
        init_payload : dict
            The rank's MXLINIT payload, supplying shared fields such as
            ``dt_au``, ``rank``, ``rescaling_factor``, and ``time_units_fs``.
        molecule_ids : list[int]
            Socket molecule IDs owned by this rank.

        Returns
        -------
        dict[int, dict]
            Mapping from molecule ID to its per-molecule INIT payload, including
            the assigned ``aggregate_group``.

        Notes
        -----
        Side effect: each molecule is registered in ``self.expected`` and seeded
        in ``self.bound`` (under ``self._lock``) so the bind loop tracks it.
        """

        dt_au = float(init_payload.get("dt_au", 0.0))
        rank = int(init_payload.get("rank", -1))
        payloads: dict[int, dict] = {}
        for mid in molecule_ids:
            group_id = self._molecule_group(int(mid))
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
                "aggregate_group": group_id,
            }
        return payloads

    def _prepare_groups_locked(self, init_payloads):
        """
        Build group metadata, then force re-init of groups whose membership grew.

        Parameters
        ----------
        init_payloads : Mapping[int, dict]
            Mapping from molecule ID to its INIT payload to fold into the group
            metadata.

        Notes
        -----
        Caller must hold ``self._lock``. Extends the base implementation in two
        ways: every configured group id is materialized (so empty bridges still
        exist to bind against), and any group whose molecule set changed while a
        bridge is already attached is marked uninitialized. The latter is needed
        because Meep ranks publish molecules incrementally, so a bridge can
        connect before its full membership is known and must replay AGGINIT once
        the remaining molecules arrive.
        """

        before = {
            group_id: set(group.molecule_ids)
            for group_id, group in self._groups.items()
        }
        super()._prepare_groups_locked(init_payloads)
        for group_id in self._group_ids:
            self._groups.setdefault(group_id, _AggregateGroupState(group_id))
        for group_id, group in self._groups.items():
            previous = before.get(group_id, set())
            current = set(group.molecule_ids)
            if current != previous and group.bridge is not None:
                group.bridge.initialized = False

    def _initialize_groups(self, group_ids: list[str]) -> None:
        """
        Run AGGINIT for several groups, in parallel when there is more than one.

        Parameters
        ----------
        group_ids : list[str]
            Aggregate group ids to initialize. An empty or single-element list is
            handled inline; longer lists are initialized on one daemon thread per
            group and joined before returning.

        Notes
        -----
        Parallelizing matters because each :meth:`_initialize_group` call can
        block on its bridge's local drivers, so a serial loop would add up the
        per-bridge startup latencies.
        """

        if len(group_ids) <= 1:
            for group_id in group_ids:
                self._initialize_group(group_id)
            return

        threads = [
            threading.Thread(
                target=self._initialize_group,
                args=(group_id,),
                daemon=True,
            )
            for group_id in group_ids
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    def wait_until_bound(self, init_payloads: dict, require_init=True, timeout=None):
        """
        Wait until requested molecules are backed by initialized aggregate bridges.

        Parameters
        ----------
        init_payloads : dict
            Mapping from molecule ID to its INIT payload for the molecules that
            must be bound before returning.
        require_init : bool, default: True
            Require each backing bridge to have completed AGGINIT, not merely be
            connected.
        timeout : float or None, optional
            Maximum time (seconds) to wait. ``None`` waits up to ``self.timeout``
            without ever returning ``False``.

        Returns
        -------
        bool
            ``True`` once all requested molecules are bound (and initialized when
            ``require_init``); ``False`` if a finite ``timeout`` elapsed first.

        Notes
        -----
        The susceptibility hub accepts bridge sockets in classifier threads, so a
        bridge can send HELLO before all Meep ranks have published their full
        molecule-to-group map. The base aggregate hub initializes any bound group
        immediately; for this Meep susceptibility layout that can block on one
        bridge's local drivers before the remaining HELLO sockets are promoted to
        CONNECTED. This override defers AGGINIT until the configured bridge set is
        live, then initializes only the groups that actually own requested
        molecules.
        """

        wanted = {int(mid) for mid in init_payloads.keys()}
        deadline = self._deadline(timeout)
        payloads = {
            int(mid): {**dict(init_payloads[mid]), "molecule_id": int(mid)}
            for mid in init_payloads.keys()
        }

        with self._lock:
            self._prepare_groups_locked(payloads)

        while True:
            if self.all_bound(wanted, require_init=require_init):
                self._resume()
                return True

            self._try_identify_fresh_clients()
            self._progress_group_binds()

            with self._lock:
                missing_groups = [
                    group_id
                    for group_id in self._group_ids
                    if self._groups[group_id].bridge is None
                    or not self._groups[group_id].bridge.alive
                ]
                molecule_groups = [
                    group_id
                    for group_id, group in self._groups.items()
                    if any(mid in wanted for mid in group.molecule_ids)
                ]
                if missing_groups:
                    groups_needing_init = []
                else:
                    groups_needing_init = [
                        group_id
                        for group_id in molecule_groups
                        if self._groups[group_id].bridge is not None
                        and self._groups[group_id].bridge.alive
                        and not self._groups[group_id].bridge.initialized
                    ]

            self._initialize_groups(groups_needing_init)

            if timeout is not None and time.time() > deadline:
                return False
            time.sleep(self.latency)

    def _make_rank_requests(
        self, client_id: int, efields: Dict[int, np.ndarray]
    ) -> dict[int, dict]:
        """
        Build this client's per-step requests, reusing cached arrays in place.

        Parameters
        ----------
        client_id : int
            Internal id of the Meep socket client this step belongs to.
        efields : dict[int, numpy.ndarray]
            Mapping from molecule ID to its ``(3,)`` electric-field vector in
            atomic units for the current timestep.

        Returns
        -------
        dict[int, dict]
            Mapping from molecule ID to a request dict with an ``"efield_au"``
            ``(3,)`` array. The dict is owned by the per-client cache and reused
            across steps, so callers must not retain it past the step.

        Notes
        -----
        The molecule set for a client is fixed after INIT, so the request dict
        and its ``(3,)`` field buffers are allocated once and overwritten on each
        step. ``"init"`` is intentionally omitted: every molecule is already
        mapped to a group by the startup bind, so the downstream planner can skip
        per-step group preparation.
        """

        cache = self._request_caches.get(client_id)
        reuse = (
            cache is not None
            and len(cache) == len(efields)
            and all(int(mid) in cache for mid in efields)
        )
        if not reuse:
            cache = {
                int(mid): {"efield_au": np.zeros(3, dtype=float)} for mid in efields
            }
            self._request_caches[client_id] = cache
        for mid, field in efields.items():
            np.copyto(
                cache[int(mid)]["efield_au"],
                np.asarray(field, dtype=float).reshape(3),
            )
        return cache

    def _run_merged_susceptibility_step(
        self, requests: dict[int, dict], deadline: float
    ) -> Dict[int, dict]:
        """
        Run one merged bridge step for all Meep-rank requests in a timestep.

        Parameters
        ----------
        requests : dict[int, dict]
            Merged molecule requests for every participating rank in this
            timestep.
        deadline : float
            Absolute ``time.time()`` deadline for completing the step.

        Returns
        -------
        dict[int, dict]
            Mapping from molecule ID to its bridge response.

        Raises
        ------
        TimeoutError
            If no complete set of responses arrives before ``deadline``.

        Notes
        -----
        Drives the base hub's :meth:`step_barrier`; if a bridge dropped and the
        barrier returns empty, it rebinds the affected molecules via
        :meth:`wait_until_bound` and retries until the deadline.
        """

        while not self._stop:
            remaining = max(0.0, deadline - time.time())
            if remaining <= 0.0:
                break
            responses = self.step_barrier(requests, timeout=remaining)
            if responses:
                return responses
            rebind_payloads = self._snapshot_rank_init_payloads()
            with self._step_lock:
                self.wait_until_bound(
                    {
                        mid: rebind_payloads[mid]
                        for mid in requests
                        if mid in rebind_payloads
                    },
                    require_init=True,
                    timeout=min(1.0, remaining),
                )
        raise TimeoutError("Timed out waiting for aggregate susceptibility responses.")

    def _client_barrier_key_locked(self, client_id: int) -> tuple[int, int]:
        """
        Return the global-barrier key for one socket client.

        The key is ``(step_index, rank_local_socket_ordinal)``.  This lets Meep
        ranks with multiple socket-backed susceptibilities advance them in the
        same serial order Meep calls ``update_P`` without forcing two sockets on
        the same rank to enter the barrier at the same time.

        Notes
        -----
        Caller must hold ``self._global_step_cond``.
        """

        return (
            int(self._client_steps.get(client_id, 0)),
            int(self._client_ordinals.get(client_id, 0)),
        )

    def _expected_clients_for_key_locked(self, key: tuple[int, int]) -> set[int]:
        """
        Return registered clients participating in one barrier phase.

        Parameters
        ----------
        key : tuple[int, int]
            ``(step_index, rank_local_socket_ordinal)`` phase key.

        Returns
        -------
        set[int]
            Client ids whose next request belongs to the same phase.

        Notes
        -----
        Caller must hold ``self._global_step_cond``.
        """

        step_index, ordinal = key
        expected = {
            cid
            for cid, client_ordinal in self._client_ordinals.items()
            if client_ordinal == ordinal
            and int(self._client_steps.get(cid, 0)) == step_index
        }
        return expected

    def _consume_global_result_locked(self, client_id: int) -> Dict[int, dict]:
        """
        Pop one client's slice of the completed global-barrier result.

        Parameters
        ----------
        client_id : int
            Client whose result slice is being collected.

        Returns
        -------
        dict[int, dict]
            This client's molecule responses for the timestep.

        Notes
        -----
        Caller must hold ``self._global_step_cond``. When the last pending slice
        is consumed, the shared error flag is cleared and all waiters are woken
        so the next timestep can start.
        """

        result = self._global_results.pop(client_id)
        self._client_steps[client_id] = self._client_steps.get(client_id, 0) + 1
        if not self._global_results:
            self._global_error = None
            self._global_pending_key = None
            self._global_step_cond.notify_all()
        return result

    def _run_global_susceptibility_step(
        self, client_id: int, requests: dict[int, dict]
    ) -> Dict[int, dict]:
        """
        Run one timestep through the global cross-rank barrier.

        Parameters
        ----------
        client_id : int
            Internal id of the calling Meep socket client.
        requests : dict[int, dict]
            This client's molecule requests for the current timestep.

        Returns
        -------
        dict[int, dict]
            This client's slice of the merged step responses.

        Raises
        ------
        RuntimeError
            If a client joins after the barrier is already active, if a client
            sends two ``AGGSTEP`` frames in one timestep, or if the runner's
            merged step failed.
        TimeoutError
            If the expected clients do not all arrive before ``self.timeout``.

        Notes
        -----
        Every Meep rank drives the same FDTD timestep, but multiple socket
        susceptibilities on one rank are called serially. This gathers one
        ``requests`` dict per expected client for the same
        ``(step_index, rank_local_socket_ordinal)`` phase under
        ``_global_step_cond``; the call that completes the expected set becomes
        the "runner", which merges all requests, runs the shared bridge step via
        :meth:`_run_merged_susceptibility_step`, then publishes each client's
        slice into ``_global_results`` and wakes the waiters. The non-runner
        clients block on the condition and return their slice once the runner
        stores it. A timeout or a runner error is recorded in ``_global_error``
        and re-raised on every participant so no socket client is left waiting
        on the barrier.
        """

        deadline = time.time() + self.timeout
        merged_requests = None
        rank_mids = None

        with self._global_step_cond:
            key = self._client_barrier_key_locked(client_id)
            while not self._stop:
                if client_id in self._global_results:
                    return self._consume_global_result_locked(client_id)
                if (
                    self._global_pending_key is None
                    or self._global_pending_key == key
                ) and not self._global_running:
                    break
                self._global_step_cond.wait(timeout=self.latency)

            if client_id in self._global_results:
                return self._consume_global_result_locked(client_id)
            if self._global_pending_key is None:
                self._global_pending_key = key
            elif self._global_pending_key != key:
                raise RuntimeError(
                    "Meep socket client entered a different timestep barrier "
                    "phase while another phase was active."
                )
            if client_id in self._global_pending_requests:
                raise RuntimeError(
                    "Received two AGGSTEP frames from one Meep socket client "
                    "before completing the global timestep barrier."
                )

            self._global_pending_requests[client_id] = requests
            self._global_pending_mids[client_id] = set(requests.keys())

            while not self._stop:
                expected = self._expected_clients_for_key_locked(key)
                if (
                    not self._global_running
                    and expected.issubset(self._global_pending_requests.keys())
                ):
                    merged_requests = {}
                    rank_mids = {}
                    for cid in sorted(expected):
                        cid_requests = self._global_pending_requests[cid]
                        merged_requests.update(cid_requests)
                        rank_mids[cid] = set(self._global_pending_mids[cid])
                    self._global_running = True
                    break

                remaining = deadline - time.time()
                if remaining <= 0.0:
                    exc = TimeoutError(
                        "Timed out waiting for all Meep socket clients at the "
                        "global timestep barrier."
                    )
                    self._global_error = exc
                    self._global_pending_requests.clear()
                    self._global_pending_mids.clear()
                    self._global_running = False
                    self._global_pending_key = None
                    self._global_step_cond.notify_all()
                    raise exc

                self._global_step_cond.wait(timeout=min(self.latency, remaining))
                if client_id in self._global_results:
                    return self._consume_global_result_locked(client_id)
                if self._global_error is not None and not self._global_running:
                    raise RuntimeError(
                        "Global aggregate susceptibility timestep failed."
                    ) from self._global_error

        error = None
        responses = None
        try:
            responses = self._run_merged_susceptibility_step(merged_requests, deadline)
        except BaseException as exc:
            error = exc

        with self._global_step_cond:
            if error is None:
                self._global_results = {
                    cid: {mid: responses[mid] for mid in mids}
                    for cid, mids in rank_mids.items()
                }
                self._global_error = None
            else:
                self._global_results = {}
                self._global_error = error

            self._global_pending_requests.clear()
            self._global_pending_mids.clear()
            self._global_running = False
            self._global_step_cond.notify_all()

            if error is not None:
                self._global_pending_key = None
                raise error
            return self._consume_global_result_locked(client_id)

    def _run_susceptibility_step(
        self,
        client_id: int,
        efields: Dict[int, np.ndarray],
    ) -> Dict[int, dict]:
        """
        Build this client's requests and run them through the global barrier.

        Parameters
        ----------
        client_id : int
            Internal id of the calling Meep socket client.
        efields : dict[int, numpy.ndarray]
            Mapping from molecule ID to its ``(3,)`` electric-field vector in
            atomic units for the current timestep.

        Returns
        -------
        dict[int, dict]
            This client's slice of the merged step responses.
        """

        requests = self._make_rank_requests(client_id, efields)
        return self._run_global_susceptibility_step(client_id, requests)

    def _retire_client(self, client_id: int) -> None:
        """
        Remove a closed Meep socket client from hub bookkeeping.

        The method leaves molecule-to-group assignments intact so an already
        initialized aggregate bridge can continue serving the remaining clients,
        but it removes the client from future timestep-barrier cohorts.
        """

        with self._global_step_cond:
            self._client_init_payloads.pop(client_id, None)
            self._client_ordinals.pop(client_id, None)
            self._client_steps.pop(client_id, None)
            self._request_caches.pop(client_id, None)
            self._global_pending_requests.pop(client_id, None)
            self._global_pending_mids.pop(client_id, None)
            self._global_results.pop(client_id, None)
            if (
                not self._global_pending_requests
                and not self._global_running
                and not self._global_results
            ):
                self._global_pending_key = None
            self._global_step_cond.notify_all()

    def stop(self):
        """
        Stop the hub, close rank sockets, and join worker threads.

        Notes
        -----
        Sets the stop flag, wakes any threads blocked on the global barrier,
        closes the tracked Meep rank sockets, then defers to the base hub's
        :meth:`stop` before joining the classifier and rank handler threads.
        """

        self._stop = True
        with self._global_step_cond:
            self._global_step_cond.notify_all()
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


class AggregatedSusceptibilitySocketHub:
    """
    Process-backed aggregate hub for Meep ``MXLSocketSusceptibility``.

    The hub runs in a dedicated ``spawn`` child process so its background socket
    threads never contend with Meep's own threads or MPI state. The public
    object lives in the Meep process and proxies to that child: it exposes the
    bound ``host``/``port``, the bridge manifest, helpers to build bridge and
    driver launch commands, and aggregated per-rank statistics. Only the MPI
    master starts the child; non-master ranks receive the bound endpoint via a
    broadcast.

    Parameters
    ----------
    host : str or None, optional
        Interface to bind the upstream TCP server to. ``None``, ``""``,
        ``"0.0.0.0"``, or ``"::"`` bind all interfaces; peers connect back over
        ``127.0.0.1``.
    port : int or None, default: 31415
        TCP port for the upstream server. ``None`` falls back to 31415 and ``0``
        selects an ephemeral port.
    timeout : float, default: 60000.0
        Operation timeout (seconds) for binding and stepping.
    latency : float, default: 0.05
        Polling interval (seconds) for the bind/step loops.
    num_bridges : int, default: 10
        Initial number of aggregate bridge groups.
    unix_prefix : str, default: ``"mxl_bridge_"``
        Prefix used to generate aggregate bridge group ids.
    bridge_manifest : str, default: ``"mxl_bridge_manifest.json"``
        Path the bridge manifest is written to after startup.
    init_grace_seconds : float, default: 0.5
        Grace period (seconds) for collecting the first burst of rank INITs when
        the expected molecule total is not announced up front.
    unixsocket : str or None, optional
        Reserved for API symmetry; must be falsy because this hub only supports
        TCP host/port upstream.

    Raises
    ------
    ValueError
        If ``unixsocket`` is provided.
    RuntimeError
        If the child hub process fails to start.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = 31415,
        timeout: float = 60000.0,
        latency: float = 0.05,
        num_bridges: int = 10,
        unix_prefix: str = "mxl_bridge_",
        bridge_manifest: str = "mxl_bridge_manifest.json",
        init_grace_seconds: float = 0.5,
        unixsocket: Optional[str] = None,
    ):
        if unixsocket:
            raise ValueError(
                "AggregatedSusceptibilitySocketHub supports TCP host/port upstream only."
            )
        if port is None:
            port = 31415
        if int(port) == 0:
            port = _choose_ephemeral_port(host)

        self.timeout = float(timeout)
        self.latency = float(latency)
        self.num_bridges = int(num_bridges)
        self.unix_prefix = str(unix_prefix)
        self.bridge_manifest = str(bridge_manifest)
        self._stats_cache: dict[int, dict] = {}
        self._bridge_info: Optional[dict] = None
        self._stopped = False
        self._is_master = am_master()
        self._ready_queue = None
        self._stats_queue = None
        self._control_queue = None
        self._stop_event = None
        self._process = None

        ready = None
        if self._is_master:
            ctx = mp.get_context("spawn")
            self._ready_queue = ctx.Queue()
            self._stats_queue = ctx.Queue()
            self._control_queue = ctx.Queue()
            self._stop_event = ctx.Event()
            self._process = ctx.Process(
                target=_run_aggregated_susceptibility_socket_hub_server,
                args=(
                    host,
                    int(port),
                    self.timeout,
                    self.latency,
                    self.num_bridges,
                    self.unix_prefix,
                    float(init_grace_seconds),
                    self._ready_queue,
                    self._stats_queue,
                    self._control_queue,
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
                ready = {
                    "error": "AggregatedSusceptibilitySocketHub server did not start."
                }
            except Exception as exc:
                ready = {"error": repr(exc)}

        ready = mpi_bcast_from_master(ready)
        if not ready or "error" in ready:
            self.stop()
            msg = ready.get("error", "unknown startup error") if ready else "unknown startup error"
            raise RuntimeError(
                f"AggregatedSusceptibilitySocketHub server failed to start: {msg}"
            )

        self.host = str(ready["host"])
        self.address = self.host
        self.port = int(ready["port"])
        self._bridge_info = dict(ready["bridge_info"])
        self._bridge_info["hub_host"] = self.host
        self._bridge_info["hub_port"] = self.port
        if self._is_master:
            self.write_bridge_manifest(self.bridge_manifest)

    @property
    def bridge_info(self) -> dict:
        """
        Current bridge manifest payload.

        Returns
        -------
        dict
            A copy of the manifest reported by the child hub (empty when the hub
            has not started).
        """

        return dict(self._bridge_info or {})

    @property
    def bridge_specs(self) -> list[dict]:
        """
        Per-bridge specification entries from the manifest.

        Returns
        -------
        list[dict]
            A copy of the manifest's ``bridges`` list (empty when unavailable).
        """

        return list((self._bridge_info or {}).get("bridges", []))

    def write_bridge_manifest(self, path: str) -> dict:
        """
        Write the current bridge manifest to ``path``.

        Parameters
        ----------
        path : str or path-like
            Destination file for the manifest JSON.

        Returns
        -------
        dict
            The manifest payload that was written.
        """

        payload = self.bridge_info
        _write_manifest(path, payload)
        return payload

    def init_remote_bridges(
        self,
        susceptibility=None,
        *,
        molecules_per_bridge: int,
        unix_prefix: str = "bridge_",
        save_file: str = "aggregation.json",
    ) -> list[RemoteBridgeSpec]:
        """
        Configure delayed bridge partitioning for ``MXLSocketSusceptibility``.

        Parameters
        ----------
        susceptibility : object, optional
            Accepted and ignored, for API symmetry with
            :meth:`AggregatedSocketHub.init_remote_bridges`.
        molecules_per_bridge : int
            Target number of socket molecules per aggregate bridge.
        unix_prefix : str, default: ``"bridge_"``
            Prefix used to generate downstream UNIX-socket names.
        save_file : str or path-like, default: ``"aggregation.json"``
            Path the finalized bridge manifest will be written to.

        Returns
        -------
        list[RemoteBridgeSpec]
            Always empty; the concrete bridge specs are only known later and are
            written to ``save_file`` by the child hub.

        Raises
        ------
        ValueError
            If ``molecules_per_bridge`` is not a positive integer.

        Notes
        -----
        Meep generates the actual socket molecule ids later, during its first
        polarization update, so this method only records the bridge policy (and
        forwards it to the child hub). The child writes the final manifest once
        ``MXLINIT`` reports ``expected_total_molecules``. On the MPI master any
        stale ``save_file`` is removed up front.
        """

        del susceptibility
        per_bridge = int(molecules_per_bridge)
        if per_bridge <= 0:
            raise ValueError("molecules_per_bridge must be a positive integer.")
        self.unix_prefix = str(unix_prefix)
        self.bridge_manifest = os.fspath(save_file)
        self._bridge_info = _aggregation_manifest(
            hub_host=self.host,
            hub_port=self.port,
            timeout=self.timeout,
            latency=self.latency,
            unix_prefix=self.unix_prefix,
            molecules_per_bridge=per_bridge,
            bridges=[],
        )
        if self._is_master:
            try:
                os.remove(self.bridge_manifest)
            except FileNotFoundError:
                pass
            if self._control_queue is not None:
                self._control_queue.put(
                    {
                        "cmd": "init_remote_bridges",
                        "molecules_per_bridge": per_bridge,
                        "unix_prefix": self.unix_prefix,
                        "save_file": self.bridge_manifest,
                    }
                )
        return []

    def bridge_command(self, idx: int, *, info: Optional[str] = None) -> str:
        """
        Build the shell command that launches one aggregate bridge node.

        Parameters
        ----------
        idx : int
            Zero-based bridge index within the manifest.
        info : str or None, optional
            Manifest path to reference. Defaults to ``self.bridge_manifest``.

        Returns
        -------
        str
            An ``mxl_bridge --info ... --idx ...`` command line.
        """

        manifest = self.bridge_manifest if info is None else str(info)
        return f"mxl_bridge --info {manifest} --idx {int(idx)}"

    def driver_command_template(
        self,
        *,
        omega_au: float,
        mu0_au: float,
        orientation: int,
    ) -> str:
        """
        Build the shell template that launches one SHO driver against a socket.

        Parameters
        ----------
        omega_au : float
            SHO resonance angular frequency in atomic units.
        mu0_au : float
            Transition dipole magnitude in atomic units.
        orientation : int
            Cartesian axis (0, 1, or 2) the dipole points along.

        Returns
        -------
        str
            A ``/bin/bash -c ...`` command with a ``{unixsocket}`` placeholder.
            The wrapper waits for the UNIX socket to appear, jitters its start,
            and restarts the driver until it exits cleanly or the timeout
            (clamped to ``[30, 600]`` seconds) elapses.
        """

        driver_param = (
            f"omega={omega_au:.17g},mu0={mu0_au:.17g},orientation={int(orientation)}"
        )
        driver_command = (
            "mxl_driver --unix --address {unixsocket} --model sho "
            f"--param {shlex.quote(driver_param)}"
        )
        wait_seconds = min(
            600,
            max(30, int(math.ceil(float(self.timeout)))),
        )
        wait_script = (
            'socket="/tmp/socketmxl_$1"; '
            "shift; "
            f"deadline=$((SECONDS + {wait_seconds})); "
            'while [[ ! -S "$socket" ]]; do '
            "if (( SECONDS >= deadline )); then "
            'echo "Timed out waiting for MaxwellLink UNIX socket $socket" >&2; '
            "exit 124; "
            "fi; "
            "sleep 0.05; "
            "done; "
            'sleep "0.$((RANDOM % 100))"; '
            "while true; do "
            '"$@"; status=$?; '
            "if (( status == 0 )); then exit 0; fi; "
            "if (( SECONDS >= deadline )); then exit \"$status\"; fi; "
            "sleep 0.1; "
            "done"
        )
        return (
            f"/bin/bash -c {shlex.quote(wait_script)} _ "
            f"{{unixsocket}} {driver_command}"
        )

    def _drain_stats(self) -> None:
        """
        Pull the latest per-rank statistics snapshots from the child process.

        Notes
        -----
        Non-blocking and master-only. Each queued snapshot replaces the cached
        statistics, so after draining ``self._stats_cache`` holds the most recent
        snapshot the child published. A no-op on non-master ranks.
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
        Latest per-rank statistics gathered from the child hub.

        Returns
        -------
        dict[int, dict]
            Mapping from Meep rank to its statistics row (molecule count, step
            and request counters, peer address, and aggregate groups). Empty on
            non-master ranks.
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
        Convert a Meep Lorentzian susceptibility to aggregate SHO parameters.

        Parameters
        ----------
        frequency : float
            Meep Lorentzian resonance frequency (Meep units); must be positive.
        sigma : float
            Meep Lorentzian oscillator strength; must be nonnegative.
        resolution : float
            Meep grid resolution (pixels per unit length); must be positive.
        gamma : float, default: 0.0
            Meep Lorentzian damping. Must be nonnegative and is ignored (the SHO
            driver is lossless) apart from a warning when nonzero.
        dimensions : int, default: 1
            Spatial dimensionality used to scale the per-cell measure; at least 1.
        time_units_fs : float, default: 0.1
            Femtoseconds represented by one Meep time unit; must be positive.
        mu0_au : float, default: 187.0819866
            SHO transition dipole magnitude in atomic units; must be nonzero.
        orientation : int, default: 0
            Cartesian axis (0, 1, or 2) the dipole points along.

        Returns
        -------
        dict
            Mapping with symmetric bright-state ``rescaling_factor``,
            ``driver_command``,
            ``bridge_manifest``, ``bridge_commands``, and ``bridge_specs`` for
            wiring up the SHO drivers and bridges.

        Raises
        ------
        ValueError
            If any argument violates the bounds listed above.

        Notes
        -----
        The returned ``rescaling_factor`` is the symmetric bright-state coupling
        scale applied by Meep to both the socket drive field and returned
        molecular amplitude. It is the square root of the direct density/current
        scale needed to reproduce the requested Lorentzian response in the
        linear-response limit. Diagnostic lines are printed on the MPI master
        only.
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
            sigma * cell_measure * omega_au * omega_au * (time_units_fs * FS_TO_AU)
            / (MXL_SOURCE_AMP_AU_TO_MEEP * efield_factor * mu0_au * mu0_au)
        )
        rescaling_factor = math.sqrt(density_scale)
        driver_command = self.driver_command_template(
            omega_au=omega_au,
            mu0_au=mu0_au,
            orientation=orientation,
        )

        if am_master():
            if gamma != 0.0:
                print(
                    f"[AggregatedSusceptibilitySocketHub] gamma={gamma} ignored "
                    "(SHO driver is lossless).",
                    flush=True,
                )
            print(
                f"[AggregatedSusceptibilitySocketHub] rescaling_factor="
                f"{rescaling_factor:.12g}",
                flush=True,
            )
            print(
                f"[AggregatedSusceptibilitySocketHub] bridge_manifest="
                f"{self.bridge_manifest}",
                flush=True,
            )
            print(
                f"[AggregatedSusceptibilitySocketHub] driver_template="
                f"{driver_command}",
                flush=True,
            )

        return {
            "rescaling_factor": rescaling_factor,
            "driver_command": driver_command,
            "bridge_manifest": self.bridge_manifest,
            "bridge_commands": [
                self.bridge_command(spec["idx"]) for spec in self.bridge_specs
            ],
            "bridge_specs": self.bridge_specs,
        }

    def stop(self) -> None:
        """
        Shut down the child hub process and flush final statistics.

        Notes
        -----
        Idempotent and master-only past the guard. Signals the child via its stop
        event, joins it, and falls back to ``terminate()`` if it does not exit in
        time, then drains any remaining statistics snapshots.
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
        """Best-effort :meth:`stop` when the handle is garbage-collected."""

        try:
            self.stop()
        except Exception:
            pass


__all__ = ["AggregatedSusceptibilitySocketHub"]
