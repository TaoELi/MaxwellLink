# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Fast JSON helpers for hot-path serialization.

Encoding and decoding live together so a single place decides whether orjson is used.
Aggregate bridges encode one payload per molecule every step, so at tens of thousands
of molecules this is a measurable part of the step.
"""

try:
    # a few times faster than json.loads()
    from orjson import loads as json_loads
except ImportError:
    from json import loads as json_loads

try:
    from orjson import OPT_SERIALIZE_NUMPY, OPT_SORT_KEYS, dumps as _orjson_dumps

    # OPT_SERIALIZE_NUMPY is required, not a nicety: without it orjson rejects
    # numpy scalars, which stdlib json accepts because numpy.float64 subclasses
    # float. Drivers do return them, and with the flag orjson also handles
    # float32, int64 and arrays, which stdlib json does not.
    _ORJSON_OPTIONS = OPT_SORT_KEYS | OPT_SERIALIZE_NUMPY

    def json_dumps(payload):
        """
        Encode a mapping into compact, sorted UTF-8 JSON bytes.

        Notes
        -----
        orjson always emits compact UTF-8, so it needs no ``ensure_ascii`` or
        ``separators`` argument. It writes ``null`` for a non-finite float where
        stdlib json writes ``NaN``; ``null`` is the parseable one, since
        :func:`json_loads` is orjson too.

        Parameters
        ----------
        payload : Mapping
            JSON-serializable mapping to encode.

        Returns
        -------
        bytes
            Compact UTF-8 JSON with sorted keys.
        """

        return _orjson_dumps(payload, option=_ORJSON_OPTIONS)

except ImportError:
    from json import dumps as _json_dumps

    def json_dumps(payload):
        """
        Encode a mapping into compact, sorted UTF-8 JSON bytes.

        Parameters
        ----------
        payload : Mapping
            JSON-serializable mapping to encode.

        Returns
        -------
        bytes
            Compact UTF-8 JSON with sorted keys.
        """

        return _json_dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")


__all__ = ["json_loads", "json_dumps"]
