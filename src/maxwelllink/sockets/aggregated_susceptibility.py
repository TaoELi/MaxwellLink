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
import queue
import shlex
import socket
import threading
import time
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
    stop_event,
) -> None:
    """
    Child-process entry point for :class:`AggregatedSusceptibilitySocketHub`.
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
        nbridge = int(num_bridges)
        if nbridge <= 0:
            raise ValueError("num_bridges must be positive.")

        self._rank_threads: list[threading.Thread] = []
        self._classifier_threads: list[threading.Thread] = []
        self._rank_sockets: list[socket.socket] = []
        self._rank_payloads: dict[int, dict] = {}
        self._rank_init_payloads: dict[int, dict[int, dict]] = {}
        self._client_init_payloads: dict[int, dict[int, dict]] = {}
        self._client_ranks: dict[int, int] = {}
        self._next_client_id = 0
        self.rank_stats: dict[int, dict] = {}
        self._step_lock = threading.RLock()
        self._meep_lock = threading.RLock()
        self._global_step_cond = threading.Condition(self._meep_lock)
        self._global_expected_clients: Optional[set[int]] = None
        self._global_pending_requests: dict[int, dict[int, dict]] = {}
        self._global_pending_mids: dict[int, set[int]] = {}
        self._global_results: dict[int, dict[int, dict]] = {}
        self._global_error: Optional[BaseException] = None
        self._global_running = False
        self._global_step_index = 0
        self._group_ids = [f"{unix_prefix}{idx}" for idx in range(nbridge)]
        self._group_loads = {group_id: 0 for group_id in self._group_ids}
        self._mxl_molecule_to_group: dict[int, str] = {}
        self._init_grace_seconds = max(0.0, float(init_grace_seconds))
        self._first_rank_init_time: Optional[float] = None

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
        """

        return {
            "version": AGGREGATION_INFO_VERSION,
            "hub_host": self._bridge_connect_host,
            "hub_port": self._bridge_connect_port,
            "timeout": float(self.timeout),
            "latency": float(self.latency),
            "unix_prefix": self._group_ids[0].rsplit("0", 1)[0] if self._group_ids else "",
            "molecules_per_bridge": None,
            "bridges": [
                RemoteBridgeSpec(
                    idx=idx,
                    group_id=group_id,
                    unixsocket=group_id,
                    n_molecules=0,
                ).to_dict()
                for idx, group_id in enumerate(self._group_ids)
            ],
        }

    def _accept_loop(self) -> None:
        """Accept aggregate bridge clients and Meep susceptibility clients."""

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
        """

        csock.settimeout(max(self.latency, 0.25))
        try:
            header = _recv_msg(csock)
        except socket.timeout:
            self._close_unclassified(csock)
            return
        except (_SocketClosed, OSError, RuntimeError):
            self._close_unclassified(csock)
            return

        if header == MXLINIT:
            csock.settimeout(self.timeout)
            self._register_meep_rank_socket(csock, peer)
        elif header == AGGHELLO:
            self._register_bridge_socket_after_hello(csock, peer)
        else:
            self._close_unclassified(csock)

    @staticmethod
    def _close_unclassified(sock: socket.socket) -> None:
        try:
            sock.close()
        except OSError:
            pass

    def _register_bridge_socket_after_hello(self, csock: socket.socket, peer: str) -> None:
        """
        Register an aggregate bridge after its ``AGGHELLO`` header.
        """

        try:
            hello = _json_loads_bytes(_recv_bytes(csock))
        except (_SocketClosed, OSError, RuntimeError):
            self._close_unclassified(csock)
            return

        group_id = str(hello.get("group_id", "")).strip()
        if group_id not in self._group_ids or self._stop:
            self._close_unclassified(csock)
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
        """

        if self._stop:
            self._close_unclassified(csock)
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
                client_id = self._next_client_id
                self._next_client_id += 1
                self._client_init_payloads[client_id] = init_payloads
                self._client_ranks[client_id] = rank
                self._rank_payloads[rank] = init_payload
                self._rank_init_payloads[rank] = init_payloads
                self.rank_stats[rank] = {
                    "molecule_count": len(molecule_ids),
                    "steps": 0,
                    "requests": 0,
                    "peer": peer,
                    "aggregate_groups": sorted(
                        {payload.get("aggregate_group") for payload in init_payloads.values()}
                    ),
                }

            group_counts: dict[str, int] = {}
            for payload in init_payloads.values():
                group_id = str(payload["aggregate_group"])
                group_counts[group_id] = group_counts.get(group_id, 0) + 1
            for group_id, count in sorted(group_counts.items()):
                print(
                    f"[AggregatedSusceptibilitySocketHub] Meep rank {rank} requested "
                    f"{count} drivers from {peer}; group={group_id!r}.",
                    flush=True,
                )

            self._wait_for_initial_rank_burst()
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
                responses = self._run_susceptibility_step(
                    client_id, efields, init_payloads
                )
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
        payload = json.loads(_recv_bytes(sock).decode("utf-8") or "{}")
        if payload.get("protocol") != "mxl_socket_susceptibility_v1":
            raise RuntimeError(
                "Expected protocol='mxl_socket_susceptibility_v1' in MXLINIT."
            )
        return payload

    def _wait_for_initial_rank_burst(self) -> None:
        if self._init_grace_seconds <= 0.0:
            return
        with self._meep_lock:
            if self._first_rank_init_time is None:
                self._first_rank_init_time = time.time()
            deadline = self._first_rank_init_time + self._init_grace_seconds
        remaining = deadline - time.time()
        if remaining > 0.0:
            time.sleep(remaining)

    def _snapshot_rank_init_payloads(self) -> dict[int, dict]:
        """
        Return all Meep-rank INIT payloads collected during the startup burst.
        """

        payloads: dict[int, dict] = {}
        with self._meep_lock:
            for client_payloads in self._client_init_payloads.values():
                payloads.update(client_payloads)
        return payloads

    def _molecule_group(self, molecule_id: int) -> str:
        """
        Assign one socket molecule to the least-loaded aggregate bridge group.
        """

        with self._meep_lock:
            if molecule_id not in self._mxl_molecule_to_group:
                group_id = min(
                    self._group_ids,
                    key=lambda gid: (self._group_loads.get(gid, 0), gid),
                )
                self._mxl_molecule_to_group[molecule_id] = group_id
                self._group_loads[group_id] = self._group_loads.get(group_id, 0) + 1
            group_id = self._mxl_molecule_to_group[molecule_id]
            return group_id

    def _register_rank_molecules(
        self, init_payload: dict, molecule_ids: list[int]
    ) -> dict[int, dict]:
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

        The susceptibility hub accepts bridge sockets in classifier threads. A
        bridge can send HELLO before all Meep ranks have published their full
        molecule-to-group map. The base aggregate hub initializes any bound
        group immediately; for this Meep susceptibility layout that can block on
        one bridge's local drivers before the remaining HELLO sockets are
        promoted to CONNECTED. Defer AGGINIT until the configured bridge set is
        live, then initialize only groups that actually own requested molecules.
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
        self, efields: Dict[int, np.ndarray], init_payloads: dict[int, dict]
    ) -> dict[int, dict]:
        return {
            int(mid): {
                "efield_au": np.asarray(field, dtype=float).reshape(3),
                "init": init_payloads[int(mid)],
            }
            for mid, field in efields.items()
        }

    def _run_merged_susceptibility_step(
        self, requests: dict[int, dict], deadline: float
    ) -> Dict[int, dict]:
        """
        Run one merged bridge step for all Meep-rank requests in a timestep.
        """

        while not self._stop:
            remaining = max(0.0, deadline - time.time())
            if remaining <= 0.0:
                break
            responses = self.step_barrier(requests, timeout=remaining)
            if responses:
                return responses
            with self._step_lock:
                self.wait_until_bound(
                    {mid: requests[mid]["init"] for mid in requests.keys()},
                    require_init=True,
                    timeout=min(1.0, remaining),
                )
        raise TimeoutError("Timed out waiting for aggregate susceptibility responses.")

    def _expected_clients_locked(self) -> set[int]:
        if self._global_expected_clients is None:
            self._global_expected_clients = set(self._client_init_payloads.keys())
            if self._global_expected_clients:
                print(
                    "[AggregatedSusceptibilitySocketHub] global timestep barrier "
                    f"enabled for {len(self._global_expected_clients)} Meep "
                    "socket clients.",
                    flush=True,
                )
        return set(self._global_expected_clients)

    def _consume_global_result_locked(self, client_id: int) -> Dict[int, dict]:
        result = self._global_results.pop(client_id)
        if not self._global_results:
            self._global_error = None
            self._global_step_cond.notify_all()
        return result

    def _run_global_susceptibility_step(
        self, client_id: int, requests: dict[int, dict]
    ) -> Dict[int, dict]:
        deadline = time.time() + self.timeout
        merged_requests = None
        rank_mids = None

        with self._global_step_cond:
            while (
                self._global_results
                and client_id not in self._global_results
                and not self._stop
            ):
                self._global_step_cond.wait(timeout=self.latency)

            expected = self._expected_clients_locked()
            if client_id not in expected:
                if (
                    not self._global_pending_requests
                    and not self._global_running
                    and not self._global_results
                ):
                    self._global_expected_clients.add(client_id)
                    expected.add(client_id)
                else:
                    raise RuntimeError(
                        "Meep socket client joined after the global timestep "
                        "barrier was already active."
                    )

            if client_id in self._global_results:
                return self._consume_global_result_locked(client_id)
            if client_id in self._global_pending_requests:
                raise RuntimeError(
                    "Received two AGGSTEP frames from one Meep socket client "
                    "before completing the global timestep barrier."
                )

            self._global_pending_requests[client_id] = requests
            self._global_pending_mids[client_id] = set(requests.keys())

            while not self._stop:
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
            self._global_step_index += 1
            self._global_step_cond.notify_all()

            if error is not None:
                raise error
            return self._consume_global_result_locked(client_id)

    def _run_susceptibility_step(
        self,
        client_id: int,
        efields: Dict[int, np.ndarray],
        init_payloads: dict[int, dict],
    ) -> Dict[int, dict]:
        requests = self._make_rank_requests(efields, init_payloads)
        return self._run_global_susceptibility_step(client_id, requests)

    def stop(self):
        self._stop = True
        with self._global_step_cond:
            self._global_step_cond.notify_all()
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


class AggregatedSusceptibilitySocketHub:
    """
    Process-backed aggregate hub for Meep ``MXLSocketSusceptibility``.
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
        self._stop_event = None
        self._process = None

        ready = None
        if self._is_master:
            ctx = mp.get_context("spawn")
            self._ready_queue = ctx.Queue()
            self._stats_queue = ctx.Queue()
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
        return dict(self._bridge_info or {})

    @property
    def bridge_specs(self) -> list[dict]:
        return list((self._bridge_info or {}).get("bridges", []))

    def write_bridge_manifest(self, path: str) -> dict:
        payload = self.bridge_info
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=True)
        return payload

    def bridge_command(self, idx: int, *, info: Optional[str] = None) -> str:
        manifest = self.bridge_manifest if info is None else str(info)
        return f"mxl_bridge --info {manifest} --idx {int(idx)}"

    def driver_command_template(
        self,
        *,
        omega_au: float,
        mu0_au: float,
        orientation: int,
    ) -> str:
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
        try:
            self.stop()
        except Exception:
            pass


__all__ = ["AggregatedSusceptibilitySocketHub"]
