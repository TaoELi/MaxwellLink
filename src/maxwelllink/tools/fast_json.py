# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Fast JSON parsing helper for hot-path deserialization.
"""

try:
    # a few times faster than json.loads()
    from orjson import loads as json_loads
except ImportError:
    from json import loads as json_loads

__all__ = ["json_loads"]
