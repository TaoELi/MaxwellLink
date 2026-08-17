# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

from __future__ import annotations
from importlib import import_module
from functools import lru_cache
from typing import Callable, Dict

__all__ = [
    "TLSMolecule",
    "SocketMolecule",
    "update_molecules",
    "update_molecules_no_mpi",
    "update_molecules_no_socket",
    "Molecule",
    "Vector3",
]


# --- Lazy attribute loader for direct class access ---
def __getattr__(name: str):
    """
    Lazy attribute loader that imports and returns model classes on demand.

    Parameters
    ----------
    name : str
        The attribute name requested (e.g., ``"DummyModel"``, ``"TLSModel"``).

    Returns
    -------
    type
        The requested class object.

    Raises
    ------
    AttributeError
        If the requested attribute is not a known model class.
    """

    if name in {
        "TLSMolecule",
        "SocketMolecule",
        "update_molecules",
        "update_molecules_no_mpi",
        "update_molecules_no_socket",
    }:
        from .molecule_legacy import (
            TLSMolecule,
            SocketMolecule,
            update_molecules,
            update_molecules_no_mpi,
            update_molecules_no_socket,
        )

        return locals()[name]

    if name in {
        "Molecule",
        "Vector3",
    }:
        from .molecule import (
            Molecule,
            Vector3,
        )

        return locals()[name]
