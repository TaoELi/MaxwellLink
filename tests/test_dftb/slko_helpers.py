# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Slater-Koster parameter sets for the tests.
"""

import pytest

from maxwelllink.tools.slko import resolve


def sk_path(name="3ob-3-1", skip=pytest.skip):
    """Return the directory of one parameter set, or skip when it cannot be had."""

    try:
        return resolve(name)
    except (RuntimeError, ValueError) as exc:
        skip(str(exc))


def sk_path_for(reference_name, elements, skip=pytest.skip):
    """Return the set a bundled system needs; ``elements`` is accepted for symmetry."""

    return sk_path(reference_name, skip=skip)
