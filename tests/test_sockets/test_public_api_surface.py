# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
Characterization tests freezing the public surface of ``maxwelllink.sockets``.

The name snapshots below were generated mechanically from each module's
``__dict__`` before the sockets/ internal refactor. They pin three contracts:

1. every name reachable from the five hub modules keeps resolving (tests,
   ``mxl_driver.py``, and user scripts import underscore-prefixed helpers
   directly, so the "private" convention is not a license to drop names);
2. the ten lazy exports of ``maxwelllink.sockets`` and the installed
   ``mxl_bridge`` console-script target keep importing;
3. every public method keeps its exact call signature (parameter names,
   kinds, and defaults — annotations are deliberately not pinned).

If a refactor legitimately adds new names, extend the snapshots; a test
failure here must never be silenced by deleting an entry.
"""

from __future__ import annotations

import importlib
import inspect
import sys
from pathlib import Path

import pytest

SRC_ROOT = Path(__file__).resolve().parents[2] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


# names that are incidental stdlib/typing imports, not API — excluded so the
# refactor may reorganize import lines freely
_INCIDENTAL = {
    "Dict",
    "Optional",
    "Tuple",
    "Mapping",
    "Iterable",
    "Counter",
    "SimpleNamespace",
    "dataclass",
    "field",
    "annotations",
}

# module -> every non-dunder, non-module name present before the refactor
_NAMESPACE_SNAPSHOT = {
    "maxwelllink.sockets.sockets": [
        "BYE",
        "DT_FLOAT",
        "DT_INT",
        "FIELDDATA",
        "FORCEREADY",
        "GETFORCE",
        "GETSOURCE",
        "HAVEDATA",
        "HEADER_LEN",
        "INIT",
        "NEEDINIT",
        "POSDATA",
        "READY",
        "SOURCEREADY",
        "STATUS",
        "STOP",
        "SocketHub",
        "_ClientState",
        "_EYE3_BYTES",
        "_FIELDDATA_HDR",
        "_FLOAT64",
        "_GETSOURCE_HDR",
        "_INT32",
        "_NAT1_BYTES",
        "_REPLY_EXTRA_LEN_OFFSET",
        "_REPLY_FIXED_LEN",
        "_REPLY_FORCES_OFFSET",
        "_REPLY_NAT_OFFSET",
        "_SEND_FIELD_OFFSET",
        "_SEND_TEMPLATE",
        "_SEND_TOTAL_LEN",
        "_STRUCT_3D",
        "_STRUCT_I",
        "_SocketClosed",
        "_mpi_comm",
        "_pack_init",
        "_pad12",
        "_recv_array",
        "_recv_bytes",
        "_recv_int",
        "_recv_msg",
        "_recv_posdata",
        "_recvall",
        "_send_array",
        "_send_bytes",
        "_send_force_ready",
        "_send_int",
        "_send_msg",
        "am_master",
        "get_available_host_port",
        "mpi_bcast_from_master",
    ],
    "maxwelllink.sockets.aggregated": [
        "AGGHELLO",
        "AGGINIT",
        "AGGREADY",
        "AGGREGATION_INFO_VERSION",
        "AGGRESULT",
        "AGGSTEP",
        "AggregatedBridge",
        "AggregatedSocketHub",
        "BYE",
        "DT_FLOAT",
        "LocalSocketHubBridge",
        "RemoteBridgeSpec",
        "STOP",
        "SocketHub",
        "_AggregateGroupState",
        "_ClientState",
        "_FrameCodec",
        "_ResultCodec",
        "_SELECTOR_ERRORS",
        "_SocketClosed",
        "_StepCodec",
        "_as_molecule_list",
        "_assign_molecule_to_group",
        "_close_socket",
        "_coerce_remote_bridge_specs",
        "_connect_tcp_with_retry",
        "_expect_header",
        "_json_dumps_bytes",
        "_json_loads_bytes",
        "_load_aggregation_info",
        "_recv_bytes",
        "_recv_exact_into",
        "_recv_msg",
        "_recv_msg_with_timeout",
        "_send_aggregate_hello",
        "_send_aggregate_init",
        "_send_bytes",
        "_send_msg",
        "mxl_bridge_main",
        "run_bridge_node",
    ],
    "maxwelllink.sockets.susceptibility": [
        "FS_TO_AU",
        "MEEP_EFIELD_TO_AU_PREFAC",
        "MXLINIT",
        "MXLREADY",
        "MXL_SOURCE_AMP_AU_TO_MEEP",
        "SocketHub",
        "SusceptibilitySocketHub",
        "_ClientState",
        "_HubProcessProxy",
        "_MeepRankServerMixin",
        "_SusceptibilitySocketHubServer",
        "_choose_ephemeral_port",
        "_close_socket",
        "_copy_rank_stats",
        "_pump_rank_stats",
        "_resolve_bound_endpoint",
        "_restore_env",
        "_run_susceptibility_socket_hub_server",
        "_strip_mpi_env_for_child_start",
        "am_master",
        "lorentzian_to_sho_parameters",
    ],
    "maxwelllink.sockets.aggregated_susceptibility": [
        "AGGREGATION_INFO_VERSION",
        "AggregatedSocketHub",
        "AggregatedSusceptibilitySocketHub",
        "FS_TO_AU",
        "MEEP_EFIELD_TO_AU_PREFAC",
        "MXLINIT",
        "MXLREADY",
        "MXL_SOURCE_AMP_AU_TO_MEEP",
        "RemoteBridgeSpec",
        "SusceptibilitySocketHub",
        "_AggregateGroupState",
        "_AggregatedSusceptibilitySocketHubServer",
        "_BLOCK_RESULT",
        "_ClientState",
        "_MeepRankServerMixin",
        "_SocketClosed",
        "_aggregation_manifest",
        "_close_socket",
        "_json_loads_bytes",
        "_pump_rank_stats",
        "_recv_bytes",
        "_resolve_bound_endpoint",
        "_resolve_step_records",
        "_run_aggregated_susceptibility_socket_hub_server",
        "_write_manifest",
        "am_master",
        "lorentzian_to_sho_parameters",
    ],
    "maxwelllink.sockets._meep_hub_base": [
        "AGGSTEP",
        "FS_TO_AU",
        "MEEP_EFIELD_TO_AU_PREFAC",
        "MXLINIT",
        "MXLREADY",
        "MXL_SOURCE_AMP_AU_TO_MEEP",
        "MXL_SUSCEPTIBILITY_PROTOCOL",
        "_CLASSIFY_WINDOW_FLOOR_S",
        "_HubProcessProxy",
        "_MPI_ENV_EXACT",
        "_MPI_ENV_PREFIXES",
        "_MeepRankServerMixin",
        "_ResultCodec",
        "_SocketClosed",
        "_StepCodec",
        "_choose_ephemeral_port",
        "_close_socket",
        "_copy_rank_stats",
        "_json_loads_bytes",
        "_pump_rank_stats",
        "_recv_bytes",
        "_recv_msg",
        "_resolve_bound_endpoint",
        "_restore_env",
        "_send_msg",
        "_strip_mpi_env_for_child_start",
        "am_master",
        "lorentzian_to_sho_parameters",
        "mpi_bcast_from_master",
    ],
}

# the ten names promised by maxwelllink.sockets.__init__
_PACKAGE_EXPORTS = [
    "get_available_host_port",
    "am_master",
    "mpi_bcast_from_master",
    "SocketHub",
    "AggregatedSocketHub",
    "LocalSocketHubBridge",
    "RemoteBridgeSpec",
    "run_bridge_node",
    "SusceptibilitySocketHub",
    "AggregatedSusceptibilitySocketHub",
]

# "ClassName.method" or "function" -> (parameter name, kind, repr(default)) tuples.
# Return annotations and parameter annotations are intentionally not pinned.
_SIGNATURE_SNAPSHOT = {
    "SocketHub.__init__": "(self, host=None, port=31415, unixsocket=None, timeout=60000.0, latency=0.01)",
    "SocketHub.register_molecule": "(self, molecule_id)",
    "SocketHub.register_molecule_return_id": "(self)",
    "SocketHub.step_barrier": "(self, requests, timeout=None)",
    "SocketHub.all_bound": "(self, molecule_ids, require_init=True)",
    "SocketHub.wait_until_bound": "(self, init_payloads, require_init=True, timeout=None)",
    "SocketHub.graceful_shutdown": "(self, reason=None, wait=2.0)",
    "SocketHub.stop": "(self)",
    "AggregatedSocketHub.__init__": "(self, host=None, port=31415, timeout=60000.0, latency=0.01)",
    "AggregatedSocketHub.add_bridge": "(self, local_unixsocket)",
    "AggregatedSocketHub.init_remote_bridges": "(self, molecules, *, molecules_per_bridge, unix_prefix='bridge_', save_file='aggregation.json')",
    "AggregatedSocketHub.wait_until_bound": "(self, init_payloads, require_init=True, timeout=None)",
    "AggregatedSocketHub.step_barrier": "(self, requests, timeout=None)",
    "AggregatedSocketHub.stop": "(self)",
    "AggregatedBridge.__init__": "(self, *, hub, group_id, bridge)",
    "AggregatedBridge.append": "(self, molecules)",
    "AggregatedBridge.start": "(self)",
    "AggregatedBridge.stop": "(self, wait=2.0)",
    "LocalSocketHubBridge.__init__": "(self, *, group_id, upstream_host, upstream_port, timeout=60.0, latency=0.01, local_host='127.0.0.1', local_port=None, local_unixsocket=None)",
    "LocalSocketHubBridge.run": "(self)",
    "LocalSocketHubBridge.start": "(self)",
    "LocalSocketHubBridge.stop": "(self, wait=2.0)",
    "RemoteBridgeSpec.__init__": "(self, idx, group_id, unixsocket, n_molecules)",
    "RemoteBridgeSpec.to_dict": "(self)",
    "RemoteBridgeSpec.from_dict": "(cls, payload)",
    "SusceptibilitySocketHub.__init__": "(self, host=None, port=31415, timeout=60000.0, latency=0.05, unixsocket=None, driver_count_file='num_socket_molecule')",
    "SusceptibilitySocketHub.lorentzian_conversion": "(self, frequency, sigma, resolution, *, gamma=0.0, dimensions=1, time_units_fs=0.1, mu0_au=187.0819866, orientation=0)",
    "SusceptibilitySocketHub.stop": "(self)",
    "AggregatedSusceptibilitySocketHub.__init__": "(self, host=None, port=31415, timeout=60000.0, latency=0.05, num_bridges=10, unix_prefix='mxl_bridge_', bridge_manifest='mxl_bridge_manifest.json', init_grace_seconds=0.5, unixsocket=None)",
    "AggregatedSusceptibilitySocketHub.bridge_command": "(self, idx, *, info=None)",
    "AggregatedSusceptibilitySocketHub.driver_command_template": "(self, *, omega_au, mu0_au, orientation)",
    "AggregatedSusceptibilitySocketHub.init_remote_bridges": "(self, susceptibility=None, *, molecules_per_bridge, unix_prefix='bridge_', save_file='aggregation.json')",
    "AggregatedSusceptibilitySocketHub.lorentzian_conversion": "(self, frequency, sigma, resolution, *, gamma=0.0, dimensions=1, time_units_fs=0.1, mu0_au=187.0819866, orientation=0)",
    "AggregatedSusceptibilitySocketHub.write_bridge_manifest": "(self, path)",
    "AggregatedSusceptibilitySocketHub.stop": "(self)",
    "get_available_host_port": "(localhost=True, save_to_file=None)",
    "am_master": "()",
    "mpi_bcast_from_master": "(value)",
    "run_bridge_node": "(info='aggregation.json', *, idx=0)",
    "mxl_bridge_main": "(argv=None)",
    "lorentzian_to_sho_parameters": "(frequency, sigma, resolution, *, gamma=0.0, dimensions=1, time_units_fs=0.1, mu0_au=187.0819866, orientation=0)",
}

# properties that must stay properties (attribute access, not calls)
_PROPERTY_SNAPSHOT = [
    "AggregatedBridge.address",
    "AggregatedBridge.unixsocket",
    "AggregatedBridge.unixsocket_path",
    "AggregatedBridge.local_endpoint",
    "LocalSocketHubBridge.local_endpoint",
    "SusceptibilitySocketHub.rank_stats",
    "AggregatedSusceptibilitySocketHub.rank_stats",
    "AggregatedSusceptibilitySocketHub.bridge_info",
    "AggregatedSusceptibilitySocketHub.bridge_specs",
]


def _bare_signature(obj) -> str:
    """Render a signature with annotations stripped (names, kinds, defaults only)."""

    sig = inspect.signature(obj)
    params = [
        p.replace(annotation=inspect.Parameter.empty) for p in sig.parameters.values()
    ]
    return str(
        sig.replace(parameters=params, return_annotation=inspect.Signature.empty)
    )


@pytest.mark.core
@pytest.mark.parametrize("module_name", sorted(_NAMESPACE_SNAPSHOT))
def test_module_namespace_preserved(module_name):
    module = importlib.import_module(module_name)
    missing = [
        name
        for name in _NAMESPACE_SNAPSHOT[module_name]
        if name not in _INCIDENTAL and not hasattr(module, name)
    ]
    assert not missing, f"{module_name} lost pinned names: {missing}"


@pytest.mark.core
def test_package_exports_resolve():
    package = importlib.import_module("maxwelllink.sockets")
    assert sorted(package.__all__) == sorted(_PACKAGE_EXPORTS)
    for name in _PACKAGE_EXPORTS:
        assert getattr(package, name) is not None


@pytest.mark.core
def test_console_script_target_resolves():
    # exactly how the installed `mxl_bridge` entry point resolves it
    module = importlib.import_module("maxwelllink.sockets.aggregated")
    assert callable(module.mxl_bridge_main)


@pytest.mark.core
def test_top_level_reexports_resolve():
    import maxwelllink

    for name in _PACKAGE_EXPORTS:
        if name in ("am_master", "mpi_bcast_from_master"):
            continue  # MPI helpers are not re-exported at top level
        assert getattr(maxwelllink, name) is not None


@pytest.mark.core
@pytest.mark.parametrize("dotted", sorted(_SIGNATURE_SNAPSHOT))
def test_public_signatures_preserved(dotted):
    from maxwelllink.sockets import aggregated as A
    from maxwelllink.sockets import sockets as S
    from maxwelllink.sockets import _meep_hub_base as M
    from maxwelllink.sockets.aggregated_susceptibility import (
        AggregatedSusceptibilitySocketHub,
    )
    from maxwelllink.sockets.susceptibility import SusceptibilitySocketHub

    namespace = {
        "SocketHub": S.SocketHub,
        "AggregatedSocketHub": A.AggregatedSocketHub,
        "AggregatedBridge": A.AggregatedBridge,
        "LocalSocketHubBridge": A.LocalSocketHubBridge,
        "RemoteBridgeSpec": A.RemoteBridgeSpec,
        "SusceptibilitySocketHub": SusceptibilitySocketHub,
        "AggregatedSusceptibilitySocketHub": AggregatedSusceptibilitySocketHub,
        "get_available_host_port": S.get_available_host_port,
        "am_master": S.am_master,
        "mpi_bcast_from_master": S.mpi_bcast_from_master,
        "run_bridge_node": A.run_bridge_node,
        "mxl_bridge_main": A.mxl_bridge_main,
        "lorentzian_to_sho_parameters": M.lorentzian_to_sho_parameters,
    }
    if "." in dotted:
        cls_name, method_name = dotted.split(".")
        target = inspect.getattr_static(namespace[cls_name], method_name)
        if isinstance(target, (staticmethod, classmethod)):
            target = target.__func__
    else:
        target = namespace[dotted]
    assert (
        _bare_signature(target) == _SIGNATURE_SNAPSHOT[dotted]
    ), f"signature of {dotted} changed"


@pytest.mark.core
@pytest.mark.parametrize("dotted", _PROPERTY_SNAPSHOT)
def test_public_properties_preserved(dotted):
    from maxwelllink.sockets import aggregated as A
    from maxwelllink.sockets.aggregated_susceptibility import (
        AggregatedSusceptibilitySocketHub,
    )
    from maxwelllink.sockets.susceptibility import SusceptibilitySocketHub

    namespace = {
        "AggregatedBridge": A.AggregatedBridge,
        "LocalSocketHubBridge": A.LocalSocketHubBridge,
        "SusceptibilitySocketHub": SusceptibilitySocketHub,
        "AggregatedSusceptibilitySocketHub": AggregatedSusceptibilitySocketHub,
    }
    cls_name, attr_name = dotted.split(".")
    assert isinstance(
        inspect.getattr_static(namespace[cls_name], attr_name), property
    ), f"{dotted} is no longer a property"
