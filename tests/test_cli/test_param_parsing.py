# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""The ``--param`` syntax of ``mxl_driver`` and ``mxl_bridge``: scalars, booleans, strings and bracketed lists."""

import pytest

from maxwelllink.mxl_drivers.python.mxl_driver import _read_args_kwargs, _read_value


@pytest.mark.core
def test_scalars_booleans_and_strings():
    args, kwargs = _read_args_kwargs(
        "3, 2.5, omega=0.21, name=sho, flag=True, off=false"
    )
    assert args == [3, 2.5]
    assert kwargs == {"omega": 0.21, "name": "sho", "flag": True, "off": False}


@pytest.mark.core
def test_bracketed_lists_keep_their_commas():
    args, kwargs = _read_args_kwargs(
        "dipole_self_energy=[0,8.2807e-4,0],kpoints=[2, 2, 2],xyz=cell.xyz,empty=[]"
    )
    assert args == []
    assert kwargs["dipole_self_energy"] == [0, 8.2807e-4, 0]
    assert kwargs["kpoints"] == [2, 2, 2]
    assert kwargs["xyz"] == "cell.xyz"
    assert kwargs["empty"] == []
    assert _read_value("[[1,2],[3]]") == [[1, 2], [3]]
