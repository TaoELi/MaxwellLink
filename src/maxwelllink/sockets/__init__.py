# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
Socket hubs connecting MaxwellLink EM solvers to molecular drivers.

The package is organized as follows::

    protocol.py                     frozen byte formats (i-PI + AGG frames)
    sockets.py                      SocketHub - the base in-process server
    aggregated.py                   AggregatedSocketHub + bridge transport
    _meep_hub_base.py               shared Meep MXLINIT layer (mixin + proxy)
    susceptibility.py               SusceptibilitySocketHub (Meep, direct drivers)
    aggregated_susceptibility.py    AggregatedSusceptibilitySocketHub (Meep, bridges)

Attributes are loaded lazily so that importing :mod:`maxwelllink.sockets`
stays cheap for driver-side processes.
"""

from __future__ import annotations

__all__ = [
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

# module that provides each lazily loaded public name
_LAZY_ATTRS = {
    "get_available_host_port": ".sockets",
    "am_master": ".sockets",
    "mpi_bcast_from_master": ".sockets",
    "SocketHub": ".sockets",
    "AggregatedSocketHub": ".aggregated",
    "LocalSocketHubBridge": ".aggregated",
    "RemoteBridgeSpec": ".aggregated",
    "run_bridge_node": ".aggregated",
    "SusceptibilitySocketHub": ".susceptibility",
    "AggregatedSusceptibilitySocketHub": ".aggregated_susceptibility",
}


def __getattr__(name: str):
    """
    Import and return a public socket-hub name on first access.

    Parameters
    ----------
    name : str
        The attribute requested (e.g., ``"SocketHub"``).

    Returns
    -------
    object
        The requested class or function.

    Raises
    ------
    AttributeError
        If the requested attribute is not a known public name.
    """

    module_name = _LAZY_ATTRS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from importlib import import_module

    return getattr(import_module(module_name, package=__name__), name)


def __dir__() -> list[str]:
    return sorted(__all__)
