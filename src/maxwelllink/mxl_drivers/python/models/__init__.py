# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

from __future__ import annotations
from importlib import import_module
from functools import lru_cache
from typing import Callable, Dict

__all__ = [
    "DummyModel",
    "TLSModel",
    "QuTiPModel",
    "RTTDDFTModel",
    "RTEhrenfestModel",
    "ASEModel",
    "__drivers__",
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

    if name == "DummyModel":
        from .dummy_model import DummyModel

        return DummyModel
    if name == "TLSModel":
        from .tls_model import TLSModel

        return TLSModel
    if name == "QuTiPModel":
        from .qutip_model import QuTiPModel

        return QuTiPModel
    if name == "RTTDDFTModel":
        from .rttddft_model import RTTDDFTModel

        return RTTDDFTModel
    if name == "RTEhrenfestModel":
        from .rt_ehrenfest_model import RTEhrenfestModel

        return RTEhrenfestModel
    if name == "ASEModel":
        from .ase_model import ASEModel

        return ASEModel
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# --- Tiny helpers to import classes only when needed ---
@lru_cache(maxsize=None)
def _load(cls_path: str):
    """
    Import a class specified by ``'package.module:ClassName'`` exactly once and
    cache the class object.

    Parameters
    ----------
    cls_path : str
        Dotted path with a colon separating module and class, e.g.
        ``'.tls_model:TLSModel'``.

    Returns
    -------
    type
        The imported class object.

    Examples
    --------
    >>> Cls = _load('.tls_model:TLSModel')
    >>> isinstance(Cls, type)
    True
    """

    mod, cls = cls_path.split(":")
    module = import_module(mod, package=__name__)
    return getattr(module, cls)


def _factory(cls_path: str) -> Callable:
    """
    Create a lazy factory callable that imports the class at first use and
    constructs an instance when invoked.

    Parameters
    ----------
    cls_path : str
        Dotted path with a colon separating module and class, e.g.
        ``'.qutip_model:QuTiPModel'``.

    Returns
    -------
    Callable
        A callable ``ctor(*args, **kwargs)`` that returns an instance of the class.
    """

    def _ctor(*args, **kwargs):
        Cls = _load(cls_path)
        return Cls(*args, **kwargs)

    return _ctor


# --- Public registry used by mxl_driver.py; all entries are LAZY factories ---
__drivers__: Dict[str, Callable] = {
    "dummy": _factory(".dummy_model:DummyModel"),
    "tls": _factory(".tls_model:TLSModel"),
    "qutip": _factory(".qutip_model:QuTiPModel"),
    "rttddft": _factory(".rttddft_model:RTTDDFTModel"),
    "rtehrenfest": _factory(".rt_ehrenfest_model:RTEhrenfestModel"),
    "ase": _factory(".ase_model:ASEModel"),
}
