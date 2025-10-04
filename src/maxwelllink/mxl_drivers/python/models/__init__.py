from __future__ import annotations
from importlib import import_module
from functools import lru_cache
from typing import Callable, Dict

__all__ = ["DummyModel", "TLSModel", "RTTDDFTModel", "__drivers__"]

# --- Lazy attribute loader for direct class access ---
def __getattr__(name: str):
    if name == "DummyModel":
        from .dummy_model import DummyModel
        return DummyModel
    if name == "TLSModel":
        from .tls_model import TLSModel
        return TLSModel
    if name == "RTTDDFTModel":
        from .rttddft_model import RTTDDFTModel
        return RTTDDFTModel
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

# --- Tiny helpers to import classes only when needed ---
@lru_cache(maxsize=None)
def _load(cls_path: str):
    """
    Import 'package.module:ClassName' once and cache the class.
    Example: '.tls_model:TLSModel'
    """
    mod, cls = cls_path.split(":")
    module = import_module(mod, package=__name__)
    return getattr(module, cls)

def _factory(cls_path: str) -> Callable:
    """
    Return a callable that, when invoked, imports the class and constructs it.
    This matches mxl_driver.py's expectation that __drivers__[name](...) is callable.
    """
    def _ctor(*args, **kwargs):
        Cls = _load(cls_path)
        return Cls(*args, **kwargs)
    return _ctor

# --- Public registry used by mxl_driver.py; all entries are LAZY factories ---
__drivers__: Dict[str, Callable] = {
    "dummy":   _factory(".dummy_model:DummyModel"),
    "tls":     _factory(".tls_model:TLSModel"),
    "rttddft": _factory(".rttddft_model:RTTDDFTModel"),
}

