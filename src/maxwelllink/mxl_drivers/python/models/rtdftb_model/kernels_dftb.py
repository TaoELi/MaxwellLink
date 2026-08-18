# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Compilation of the DFTB scalar-loop kernels, for the CPU and for the GPU.

The kernel bodies live next to the physics they belong to; this module only compiles
them. :func:`kernel` registers a body and returns its ``numba.njit`` form.
:func:`device_kernels` rebuilds every registered body against a patched copy of its
module globals, so kernels that call other kernels resolve to the CUDA versions. One body
serves both targets, and they cannot drift apart.

The one rule this imposes: a kernel may call another only by bare name, never through a
module attribute, or the call would resolve to the CPU version on the GPU.
"""

import types

#: Raw, uncompiled kernel bodies keyed by name, for the second compilation target.
KERNELS = {}

_DEVICE_KERNELS = None


def kernel(function):
    """Register one scalar-loop kernel and return its numba-compiled form.

    Falls back to the plain Python body when numba is not installed, which is also what
    ``NUMBA_DISABLE_JIT=1`` gives, so the two can be cross-checked against each other.
    """

    KERNELS[function.__name__] = function
    try:
        from numba import njit
    except ImportError:
        return function
    return njit(cache=False)(function)


def device_kernels():
    """Compile every registered kernel as a CUDA device function, once per process.

    Import the whole package before calling this: a body that has not been registered
    yet is simply absent from the table, and a kernel that calls it will fail to compile
    with an untyped-global error rather than anything more helpful.

    Returns
    -------
    dict
        The CUDA device functions, keyed by kernel name.

    Raises
    ------
    ImportError
        If numba's CUDA target is unavailable.
    """

    global _DEVICE_KERNELS
    if _DEVICE_KERNELS is not None:
        return _DEVICE_KERNELS

    try:
        from numba import cuda
    except Exception as exc:  # ImportError, or a numba/CUDA runtime failure
        raise ImportError(
            "The GPU batch backend requires numba's CUDA target. Install it "
            "alongside CuPy, e.g. 'pip install maxwelllink[gpu-cuda12]'. On hosts "
            "without CUDA, inject xp=numpy to run the compiled CPU kernels instead."
        ) from exc

    # one patched namespace per defining module, so module constants still resolve
    namespaces = {}
    for body in KERNELS.values():
        namespaces.setdefault(id(body.__globals__), dict(body.__globals__))

    device = {}
    for name, body in KERNELS.items():
        rebuilt = types.FunctionType(
            body.__code__,
            namespaces[id(body.__globals__)],
            name,
            body.__defaults__,
            body.__closure__,
        )
        device[name] = cuda.jit(rebuilt, device=True)

    # publish after every dispatcher exists; compilation is lazy, so cross-kernel and
    # cross-module calls all resolve to the device versions when they are first used
    for namespace in namespaces.values():
        namespace.update(device)

    _DEVICE_KERNELS = device
    return device
