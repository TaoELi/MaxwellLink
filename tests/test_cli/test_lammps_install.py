import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from maxwelllink.mxl_drivers.lammps.install import (
    _fix_files_for_transport,
    _ucxx_cmake_args,
)


def test_fix_files_for_transport():
    files, binary = _fix_files_for_transport("socket")
    assert files == ("fix_maxwelllink.cpp", "fix_maxwelllink.h")
    assert binary == "lmp_mxl"

    files, binary = _fix_files_for_transport("ucx")
    assert files == ("fix_maxwelllink_ucx.cpp", "fix_maxwelllink_ucx.h")
    assert binary == "lmp_mxl_ucx"

    files, binary = _fix_files_for_transport("both")
    assert files == (
        "fix_maxwelllink.cpp",
        "fix_maxwelllink.h",
        "fix_maxwelllink_ucx.cpp",
        "fix_maxwelllink_ucx.h",
    )
    assert binary == "lmp_mxl_ucx"


def test_ucxx_cmake_args_from_env(monkeypatch):
    monkeypatch.setenv("CONDA_PREFIX", "/tmp/conda")
    monkeypatch.setenv("MXL_LAMMPS_UCXX_CFLAGS", "-I/tmp/conda/include")
    monkeypatch.setenv("MXL_LAMMPS_UCXX_LDFLAGS", "-L/tmp/conda/lib -lucxx")

    args = _ucxx_cmake_args()
    assert "-DCMAKE_PREFIX_PATH=/tmp/conda" in args
    assert "-DCMAKE_CXX_FLAGS=-I/tmp/conda/include" in args
    assert "-DCMAKE_EXE_LINKER_FLAGS=-L/tmp/conda/lib -lucxx" in args
    assert "-DCMAKE_SHARED_LINKER_FLAGS=-L/tmp/conda/lib -lucxx" in args
    assert "-DCMAKE_MODULE_LINKER_FLAGS=-L/tmp/conda/lib -lucxx" in args
