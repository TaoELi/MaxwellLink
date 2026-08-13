# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""Tests for conda recipe packaging configuration."""

from __future__ import annotations

from pathlib import Path


def test_conda_recipe_uses_path_python_for_build_script() -> None:
    """Ensure the conda build script does not hardcode a rendered Python path."""
    recipe = (
        Path(__file__).resolve().parents[2] / "conda.recipe" / "meta.yaml"
    ).read_text(encoding="utf-8")

    assert (
        "script: python -m pip install . -vv --no-deps --no-build-isolation" in recipe
    )
    assert "{{ PYTHON }}" not in recipe
