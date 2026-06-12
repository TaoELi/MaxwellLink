# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
Aggregated SocketHub for Meep ``MXLSocketSusceptibility`` connections.

Same Meep-facing ``MXLINIT`` protocol as ``susceptibility.py``, but the
timestep traffic is fanned out through a small number of aggregate bridges
instead of one socket per driver::

    Meep ranks ==MXLINIT/AGGSTEP==> _AggregatedSusceptibilitySocketHubServer
        ==AGGHELLO/AGGINIT==> mxl_bridge nodes ==UNIX==> many local drivers

The user-facing :class:`AggregatedSusceptibilitySocketHub` subclasses
:class:`SusceptibilitySocketHub` (same proxy surface, different downstream
transport) and adds the bridge manifest and launch-command helpers. The two
hub-specific hard parts live here and only here: the *global timestep
barrier*, which lets several socket susceptibilities per Meep rank advance in
Meep's serial ``update_P`` order, and the *deferred bridge manifest*, which is
finalized only once Meep announces how many socket molecules exist.
"""

from __future__ import annotations

import json
import math
import os
import queue
import shlex
import threading
import time
from collections import Counter
from typing import Dict, Optional

import numpy as np

from ._meep_hub_base import (
    _MeepRankServerMixin,
    _pump_rank_stats,
    _resolve_bound_endpoint,
    lorentzian_to_sho_parameters,
)

# Names that historically lived in this module; re-exported so existing
# imports keep working.
from ._meep_hub_base import (  # noqa: F401  re-exported for backward compatibility
    FS_TO_AU,
    MEEP_EFIELD_TO_AU_PREFAC,
    MXL_SOURCE_AMP_AU_TO_MEEP,
    MXLINIT,
    MXLREADY,
)
from .aggregated import (
    AGGREGATION_INFO_VERSION,
    AggregatedSocketHub,
    RemoteBridgeSpec,
    _AggregateGroupState,
)
from .protocol import _close_socket, _json_loads_bytes, _recv_bytes
from .sockets import _ClientState, am_master
from .susceptibility import SusceptibilitySocketHub


# ----------------------------------------------------------------------
# Bridge manifest helpers
# ----------------------------------------------------------------------


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
    Build one aggregation manifest payload.

    This is the single place that defines the manifest schema shared by the
    child hub's ``bridge_info``, the finalized on-disk manifest, and the public
    hub's ``init_remote_bridges`` placeholder. On disk the keys are written
    alphabetically (:func:`_write_manifest` uses ``sort_keys=True``).
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
    """Write one aggregation manifest to ``path`` as pretty, sorted JSON."""

    with open(os.fspath(path), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=True)


# ----------------------------------------------------------------------
# Child-process entry point
# ----------------------------------------------------------------------


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

    Constructs the server, reports ``{"host", "port", "bridge_info"}`` back to
    the parent through ``ready_queue`` (or ``{"error": ...}`` on failure), then
    forwards per-rank statistics until ``stop_event`` is set, draining the
    parent's control commands on every tick so a late
    ``init_remote_bridges`` policy still takes effect.
    """

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

    _pump_rank_stats(server, stats_queue, stop_event, tick=server.drain_control_queue)


# ----------------------------------------------------------------------
# In-child server
# ----------------------------------------------------------------------


class _AggregatedSusceptibilitySocketHubServer(_MeepRankServerMixin, AggregatedSocketHub):
    """
    Meep-facing hub server that fans timesteps out through aggregate bridges.

    The Meep-rank protocol comes from :class:`_MeepRankServerMixin`; this
    subclass supplies the bridge transport (group assignment, AGGHELLO
    classification, deferred manifest) and the global cross-rank timestep
    barrier.

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

    _log_prefix = "AggregatedSusceptibilitySocketHub"

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

        self._init_rank_tracking()

        # One Meep rank may open several socket clients (one per socket-backed
        # susceptibility); each gets a client id and a rank-local ordinal.
        self._client_init_payloads: dict[int, dict[int, dict]] = {}
        self._client_ordinals: dict[int, int] = {}
        self._client_steps: dict[int, int] = {}
        self._rank_client_counts: dict[int, int] = {}
        self._next_client_id = 0
        self._request_caches: dict[int, dict[int, dict]] = {}

        # Global timestep barrier state (guarded by _global_step_cond, which
        # shares _meep_lock so stats and barrier updates cannot interleave).
        self._global_step_cond = threading.Condition(self._meep_lock)
        self._global_pending_key: Optional[tuple[int, int]] = None
        self._global_pending_requests: dict[int, dict[int, dict]] = {}
        self._global_pending_mids: dict[int, set[int]] = {}
        self._global_results: dict[int, dict[int, dict]] = {}
        self._global_error: Optional[BaseException] = None
        self._global_running = False

        # Bridge-group layout and the deferred remote-bridge manifest.
        self._unix_prefix = str(unix_prefix)
        self._group_ids = [f"{self._unix_prefix}{idx}" for idx in range(nbridge)]
        self._group_loads = {group_id: 0 for group_id in self._group_ids}
        self._group_capacities: Optional[dict[str, int]] = None
        self._mxl_molecule_to_group: dict[int, str] = {}
        self._init_grace_seconds = max(0.0, float(init_grace_seconds))
        self._ordinal_first_init_time: dict[int, float] = {}
        self._expected_total_molecules: Optional[int] = None
        self._remote_bridge_policy: Optional[dict] = None
        self._bridge_manifest_info: Optional[dict] = None
        self._bridge_manifest_written = False
        self._control_queue = None

        super().__init__(host=host, port=port, timeout=timeout, latency=latency)

        self.host, self.port = _resolve_bound_endpoint(self.serversock, host, port)
        self.timeout = float(timeout)

    # -------------- remote-bridge policy and manifest --------------

    @property
    def bridge_info(self) -> dict:
        """
        Manifest payload consumed by ``mxl_bridge --info ...``.

        Returns a copy of the finalized manifest once it has been written,
        otherwise a placeholder manifest enumerating the currently configured
        groups with zero molecules each.
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

        If the expected molecule total is already known, the group layout is
        recomputed immediately; otherwise it is deferred until the first
        ``MXLINIT`` reports ``expected_total_molecules``.

        Raises
        ------
        ValueError
            If ``molecules_per_bridge`` is not a positive integer.
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

        Called from the child's stats loop and from rank handlers so a late
        ``init_remote_bridges`` policy still takes effect. A no-op when no
        control queue is attached.
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
        Recompute group ids, loads, and capacities from the remote policy.

        Caller must hold ``self._meep_lock``. A no-op when no remote-bridge
        policy is set, when molecules have already been assigned to groups, or
        when ``total_molecules`` is non-positive. The bridge count becomes
        ``ceil(total / molecules_per_bridge)`` and the resulting per-group
        capacities keep manifest driver counts and later lazy Meep
        registrations consistent.
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

        Caller must hold ``self._meep_lock``. A no-op when the payload omits
        the total or reports a non-positive value. On the first valid total it
        also triggers the deferred group-layout computation.

        Raises
        ------
        RuntimeError
            If two INIT payloads report different positive totals.
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
        """Count molecules registered across all Meep clients (holds lock)."""

        return sum(len(payloads) for payloads in self._client_init_payloads.values())

    def _write_final_bridge_manifest_locked(self) -> None:
        """
        Write the finalized bridge manifest once bridge counts are known.

        Caller must hold ``self._meep_lock``. Idempotent and a no-op until a
        remote-bridge policy is configured. If the total molecule count has
        already produced per-group capacities, those capacities are written
        immediately so remote bridge jobs can start before later lazy Meep
        socket clients register. Without capacities, the manifest waits until
        every expected molecule has registered and uses the observed group
        loads.
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
            molecules_per_bridge=int(
                self._remote_bridge_policy["molecules_per_bridge"]
            ),
            bridges=[spec.to_dict() for spec in specs],
        )
        path = os.fspath(self._remote_bridge_policy["save_file"])
        _write_manifest(path, payload)
        self._bridge_manifest_info = payload
        self._bridge_manifest_written = True
        print(
            f"[{self._log_prefix}] finalized aggregate bridge "
            f"manifest {path!r} with {len(specs)} bridge(s) for "
            f"{sum(spec.n_molecules for spec in specs)} socket molecule(s).",
            flush=True,
        )

    # -------------- accept / classify --------------

    def _classify_other(self, header: bytes, csock, peer: str) -> None:
        """Route ``AGGHELLO`` banners to bridge registration; close the rest."""

        from .protocol import AGGHELLO

        if header == AGGHELLO:
            self._register_bridge_socket_after_hello(csock, peer)
        else:
            _close_socket(csock)

    def _register_bridge_socket_after_hello(self, csock, peer: str) -> None:
        """
        Register an aggregate bridge after its ``AGGHELLO`` banner.

        Reads the HELLO JSON payload and registers the socket as an unbound
        client tagged with its ``aggregate_group``; the bind loop later
        promotes it to a group. The socket is closed if the payload is
        malformed, names an unknown group, or arrives while stopping.
        """

        try:
            hello = _json_loads_bytes(_recv_bytes(csock))
        except (OSError, RuntimeError):
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

    # -------------- MXLINIT handshake hooks --------------

    def _before_rank_registration(self, init_payload: dict) -> None:
        """Pick up any pending bridge policy, then note the molecule total."""

        self.drain_control_queue()
        if self._remote_bridge_policy is None:
            time.sleep(min(max(self.latency, 0.05), 0.25))
            self.drain_control_queue()
        with self._meep_lock:
            self._note_expected_total_molecules_locked(init_payload)

    def _molecule_init_payload_extras(self, molecule_id: int) -> dict:
        """Tag each molecule with the aggregate group that will serve it."""

        return {"aggregate_group": self._molecule_group(molecule_id)}

    def _on_rank_registered(self, ctx, init_payload: dict) -> None:
        """Assign this socket client its id/ordinal and update rank stats."""

        with self._meep_lock:
            ctx.client_id = self._next_client_id
            self._next_client_id += 1
            ctx.ordinal = self._rank_client_counts.get(ctx.rank, 0)
            self._rank_client_counts[ctx.rank] = ctx.ordinal + 1
            self._client_init_payloads[ctx.client_id] = ctx.init_payloads
            self._client_ordinals[ctx.client_id] = ctx.ordinal
            self._client_steps[ctx.client_id] = 0
            aggregate_groups = {
                payload.get("aggregate_group")
                for payload in ctx.init_payloads.values()
            }
            stats = self.rank_stats.setdefault(
                ctx.rank,
                {
                    "molecule_count": 0,
                    "steps": 0,
                    "requests": 0,
                    "peer": ctx.peer,
                    "peers": [],
                    "client_count": 0,
                    "aggregate_groups": [],
                },
            )
            stats["molecule_count"] += len(ctx.molecule_ids)
            stats["client_count"] += 1
            stats["peers"].append(ctx.peer)
            stats["aggregate_groups"] = sorted(
                set(stats["aggregate_groups"]) | aggregate_groups
            )

        group_counts = Counter(
            str(payload["aggregate_group"]) for payload in ctx.init_payloads.values()
        )
        for group_id, count in sorted(group_counts.items()):
            print(
                f"[{self._log_prefix}] Meep rank {ctx.rank} "
                f"socket {ctx.ordinal} requested {count} drivers from {ctx.peer}; "
                f"group={group_id!r}.",
                flush=True,
            )

    def _wait_for_rank_drivers(self, ctx) -> None:
        """
        Bind the full bridge set before MXLREADY.

        Ranks register molecules incrementally, so the rank waits through the
        INIT grace window, finalizes the manifest if possible, and then binds
        against the union of every registered client's molecules.
        """

        self._wait_for_rank_ordinal_burst(ctx.ordinal)
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
            raise RuntimeError(f"Timed out waiting for rank {ctx.rank} drivers.")

    def _on_rank_closed(self, ctx) -> None:
        """Remove a closed Meep socket client from barrier bookkeeping."""

        client_id = getattr(ctx, "client_id", None)
        if client_id is not None:
            self._retire_client(client_id)

    def _wait_for_rank_ordinal_burst(self, ordinal: int) -> None:
        """
        Sleep through the INIT grace window for one rank-local socket ordinal.

        Meep initializes multiple ``MXLSocketSusceptibility`` objects on a rank
        sequentially. The first object on every rank has ordinal 0, the second
        has ordinal 1, and so on. Pausing briefly when the first client for an
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
        """Merge the INIT payloads of every registered Meep socket client."""

        payloads: dict[int, dict] = {}
        with self._meep_lock:
            for client_payloads in self._client_init_payloads.values():
                payloads.update(client_payloads)
        return payloads

    # -------------- molecule-to-group assignment --------------

    def _molecule_group(self, molecule_id: int) -> str:
        """
        Assign one socket molecule to an aggregate bridge group.

        Assignment is sticky: a molecule keeps its first group. With announced
        capacities, molecules fill groups in order; otherwise new molecules go
        to the group with the fewest molecules (ties broken by group id).

        Raises
        ------
        RuntimeError
            If more molecules register than the announced
            ``expected_total_molecules`` capacity allows.
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

    # -------------- bridge binding --------------

    def _prepare_groups_locked(self, init_payloads):
        """
        Build group metadata, then force re-init of groups whose membership grew.

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
        Wait until requested molecules are backed by initialized bridges.

        The susceptibility hub accepts bridge sockets in classifier threads, so
        a bridge can send HELLO before all Meep ranks have published their full
        molecule-to-group map. The base aggregate hub initializes any bound
        group immediately; for this Meep layout that can block on one bridge's
        local drivers before the remaining HELLO sockets are promoted. This
        override defers AGGINIT until the configured bridge set is live, then
        initializes only the groups that actually own requested molecules.

        Parameters
        ----------
        init_payloads : dict
            Mapping from molecule ID to its INIT payload for the molecules that
            must be bound before returning.
        require_init : bool, default: True
            Require each backing bridge to have completed AGGINIT, not merely
            be connected.
        timeout : float or None, optional
            Maximum time (seconds) to wait. ``None`` waits up to
            ``self.timeout`` without ever returning ``False``.

        Returns
        -------
        bool
            ``True`` once all requested molecules are bound (and initialized
            when ``require_init``); ``False`` if a finite ``timeout`` elapsed.
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

    # -------------- per-timestep barrier --------------

    def _handle_step(self, ctx, efields: Dict[int, np.ndarray]) -> Dict[int, dict]:
        """Run one timestep for this socket client through the global barrier."""

        return self._run_susceptibility_step(ctx.client_id, efields)

    def _make_rank_requests(
        self, client_id: int, efields: Dict[int, np.ndarray]
    ) -> dict[int, dict]:
        """
        Build this client's per-step requests, reusing cached arrays in place.

        The molecule set for a client is fixed after INIT, so the request dict
        and its ``(3,)`` field buffers are allocated once and overwritten on
        each step; callers must not retain it past the step. ``"init"`` is
        intentionally omitted: every molecule is already mapped to a group by
        the startup bind, so the downstream planner can skip per-step group
        preparation.
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

        Drives the base hub's :meth:`step_barrier`; if a bridge dropped and the
        barrier returns empty, it rebinds the affected molecules via
        :meth:`wait_until_bound` and retries until the deadline.

        Raises
        ------
        TimeoutError
            If no complete set of responses arrives before ``deadline``.
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
        Return the global-barrier key ``(step_index, rank_local_ordinal)``.

        This lets Meep ranks with multiple socket-backed susceptibilities
        advance them in the same serial order Meep calls ``update_P`` without
        forcing two sockets on the same rank to enter the barrier at the same
        time. Caller must hold ``self._global_step_cond``.
        """

        return (
            int(self._client_steps.get(client_id, 0)),
            int(self._client_ordinals.get(client_id, 0)),
        )

    def _expected_clients_for_key_locked(self, key: tuple[int, int]) -> set[int]:
        """
        Return registered clients participating in one barrier phase.

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

        Caller must hold ``self._global_step_cond``. When the last pending
        slice is consumed, the shared error flag is cleared and all waiters are
        woken so the next timestep can start.
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

        Every Meep rank drives the same FDTD timestep, but multiple socket
        susceptibilities on one rank are called serially. This gathers one
        ``requests`` dict per expected client for the same
        ``(step_index, rank_local_socket_ordinal)`` phase under
        ``_global_step_cond``; the call that completes the expected set becomes
        the "runner", which merges all requests, runs the shared bridge step
        via :meth:`_run_merged_susceptibility_step`, then publishes each
        client's slice into ``_global_results`` and wakes the waiters. The
        non-runner clients block on the condition and return their slice once
        the runner stores it. A timeout or a runner error is recorded in
        ``_global_error`` and re-raised on every participant so no socket
        client is left waiting on the barrier.

        Raises
        ------
        RuntimeError
            If a client joins after the barrier is already active, if a client
            sends two ``AGGSTEP`` frames in one timestep, or if the runner's
            merged step failed.
        TimeoutError
            If the expected clients do not all arrive before ``self.timeout``.
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
        """Build this client's requests and run them through the global barrier."""

        requests = self._make_rank_requests(client_id, efields)
        return self._run_global_susceptibility_step(client_id, requests)

    def _retire_client(self, client_id: int) -> None:
        """
        Remove a closed Meep socket client from hub bookkeeping.

        Molecule-to-group assignments stay intact so an already initialized
        aggregate bridge can continue serving the remaining clients, but the
        client is removed from future timestep-barrier cohorts.
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

    # -------------- shutdown --------------

    def _wake_step_waiters(self) -> None:
        """Wake every thread blocked on the global timestep barrier."""

        with self._global_step_cond:
            self._global_step_cond.notify_all()


# ----------------------------------------------------------------------
# User-facing hub (proxy to the child-process server)
# ----------------------------------------------------------------------


class AggregatedSusceptibilitySocketHub(SusceptibilitySocketHub):
    """
    Process-backed aggregate hub for Meep ``MXLSocketSusceptibility``.

    Same user-facing surface as :class:`SusceptibilitySocketHub` (endpoint
    fields, ``rank_stats``, ``lorentzian_conversion``, ``stop``); the
    downstream transport runs through aggregate bridges instead of direct
    driver sockets, and this subclass adds the bridge manifest plus the
    bridge/driver launch-command helpers.

    Parameters
    ----------
    host : str or None, optional
        Interface to bind the upstream TCP server to. ``None``, ``""``,
        ``"0.0.0.0"``, or ``"::"`` bind all interfaces; peers connect back
        over ``127.0.0.1``.
    port : int or None, default: 31415
        TCP port for the upstream server. ``None`` falls back to 31415 and
        ``0`` selects an ephemeral port.
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
        Grace period (seconds) for collecting the first burst of rank INITs
        when the expected molecule total is not announced up front.
    unixsocket : str or None, optional
        Reserved for API symmetry; must be falsy (TCP upstream only).

    Raises
    ------
    ValueError
        If ``unixsocket`` is provided.
    RuntimeError
        If the child hub process fails to start.
    """

    _log_prefix = "AggregatedSusceptibilitySocketHub"

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

        self.timeout = float(timeout)
        self.latency = float(latency)
        self.num_bridges = int(num_bridges)
        self.unix_prefix = str(unix_prefix)
        self.bridge_manifest = str(bridge_manifest)
        self._init_grace_seconds = float(init_grace_seconds)
        # The aggregate hub announces driver needs through the bridge
        # manifest rather than the inherited driver-count file.
        self.driver_count_file = None
        self._bridge_info: Optional[dict] = None
        self._control_queue = None

        ready = self._start_server_process(host, port)

        self._bridge_info = dict(ready["bridge_info"])
        self._bridge_info["hub_host"] = self.host
        self._bridge_info["hub_port"] = self.port
        if self._is_master:
            self.write_bridge_manifest(self.bridge_manifest)

    def _server_runner(self):
        return _run_aggregated_susceptibility_socket_hub_server

    def _server_config(self) -> tuple:
        return (self.num_bridges, self.unix_prefix, self._init_grace_seconds)

    def _create_extra_queues(self, ctx) -> tuple:
        self._control_queue = ctx.Queue()
        return (self._control_queue,)

    # -------------- bridge manifest and launch commands --------------

    @property
    def bridge_info(self) -> dict:
        """A copy of the manifest reported by the child hub (may be empty)."""

        return dict(self._bridge_info or {})

    @property
    def bridge_specs(self) -> list[dict]:
        """A copy of the manifest's ``bridges`` list (empty when unavailable)."""

        return list((self._bridge_info or {}).get("bridges", []))

    def write_bridge_manifest(self, path: str) -> dict:
        """Write the current bridge manifest to ``path`` and return it."""

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

        Meep generates the actual socket molecule ids later, during its first
        polarization update, so this method only records the bridge policy
        (and forwards it to the child hub). The child writes the final
        manifest to ``save_file`` once ``MXLINIT`` reports
        ``expected_total_molecules``; any stale ``save_file`` is removed up
        front on the MPI master.

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
            Always empty; the concrete bridge specs are only known later and
            are written to ``save_file`` by the child hub.

        Raises
        ------
        ValueError
            If ``molecules_per_bridge`` is not a positive integer.
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

        Returns a ``/bin/bash -c ...`` command with a ``{unixsocket}``
        placeholder. The wrapper waits for the UNIX socket to appear, jitters
        its start, and restarts the driver until it exits cleanly or the
        timeout (clamped to ``[30, 600]`` seconds) elapses.
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

        The numerical mapping is :func:`lorentzian_to_sho_parameters`; this
        method adds the bridge manifest, per-bridge launch commands, and the
        UNIX-socket driver command template.

        Returns
        -------
        dict
            Mapping with ``rescaling_factor``, ``driver_command``,
            ``bridge_manifest``, ``bridge_commands``, and ``bridge_specs``.

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
        driver_command = self.driver_command_template(
            omega_au=converted["omega_au"],
            mu0_au=mu0_au,
            orientation=orientation,
        )

        if am_master():
            if gamma != 0.0:
                print(
                    f"[{self._log_prefix}] gamma={gamma} ignored "
                    "(SHO driver is lossless).",
                    flush=True,
                )
            print(
                f"[{self._log_prefix}] rescaling_factor="
                f"{rescaling_factor:.12g}",
                flush=True,
            )
            print(
                f"[{self._log_prefix}] bridge_manifest="
                f"{self.bridge_manifest}",
                flush=True,
            )
            print(
                f"[{self._log_prefix}] driver_template="
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


__all__ = ["AggregatedSusceptibilitySocketHub"]
