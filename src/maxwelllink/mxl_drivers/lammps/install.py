# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""Build and install a custom LAMMPS binary with MaxwellLink support."""

import os, shutil, subprocess, sys, sysconfig, tarfile
from pathlib import Path
import platform
import argparse

try:
    import requests
except Exception:
    requests = None

# LAMMPS release in Github
TARBALL_URL = (
    "https://github.com/lammps/lammps/releases/download/"
    "stable_29Aug2024_update1/lammps-src-29Aug2024_update1.tar.gz"
)
# directory name inside the tarball
SRC_DIR_NAME = "lammps-29Aug2024"


def _repo_root_from_here() -> Path:
    """Walk up from this file until we find pyproject.toml; return its parent."""
    here = Path(__file__).resolve()
    for p in [here] + list(here.parents):
        if (p / "pyproject.toml").exists():
            if (p / "src" / "maxwelllink").exists():
                return p
            return p.parent
    # Fallback: 4 levels up from .../src/maxwelllink/mxl_drivers/lammps/install.py
    return Path(__file__).resolve().parents[4]


def _default_build_root() -> Path:
    return _repo_root_from_here() / "build"


def _scripts_dir() -> Path:
    return Path(sysconfig.get_path("scripts"))


def _run(cmd):
    print("[mxl] $", " ".join(map(str, cmd)))
    subprocess.run(cmd, check=True)


def _pkg_config_value(package: str, flag: str) -> str:
    try:
        return subprocess.check_output(["pkg-config", flag, package], text=True).strip()
    except (FileNotFoundError, subprocess.CalledProcessError):
        return ""


def _fix_files_for_transport(transport: str):
    if transport == "socket":
        return ("fix_maxwelllink.cpp", "fix_maxwelllink.h"), "lmp_mxl"
    if transport == "ucx":
        return ("fix_maxwelllink_ucx.cpp", "fix_maxwelllink_ucx.h"), "lmp_mxl_ucx"
    if transport == "both":
        return (
            "fix_maxwelllink.cpp",
            "fix_maxwelllink.h",
            "fix_maxwelllink_ucx.cpp",
            "fix_maxwelllink_ucx.h",
        ), "lmp_mxl_ucx"
    raise ValueError(f"Unsupported LAMMPS transport: {transport}")


def _ucxx_cmake_args() -> list[str]:
    cflags = os.environ.get("MXL_LAMMPS_UCXX_CFLAGS", "").strip()
    ldflags = os.environ.get("MXL_LAMMPS_UCXX_LDFLAGS", "").strip()

    if not cflags and not ldflags:
        pkg_name = os.environ.get("MXL_LAMMPS_UCXX_PKGCONFIG", "ucxx")
        cflags = _pkg_config_value(pkg_name, "--cflags")
        ldflags = _pkg_config_value(pkg_name, "--libs")

    if not cflags and not ldflags:
        raise RuntimeError(
            "UCX transport requested, but UCXX compile/link flags were not found. "
            "Install the UCXX C++ library and pkg-config metadata, or set "
            "`MXL_LAMMPS_UCXX_CFLAGS` and `MXL_LAMMPS_UCXX_LDFLAGS`."
        )

    args = []
    conda_prefix = os.environ.get("CONDA_PREFIX", "").strip()
    if conda_prefix:
        args.append(f"-DCMAKE_PREFIX_PATH={conda_prefix}")
    if cflags:
        args.append(f"-DCMAKE_CXX_FLAGS={cflags}")
    if ldflags:
        args.append(f"-DCMAKE_EXE_LINKER_FLAGS={ldflags}")
        args.append(f"-DCMAKE_SHARED_LINKER_FLAGS={ldflags}")
        args.append(f"-DCMAKE_MODULE_LINKER_FLAGS={ldflags}")
    return args


def _download_tarball(url: str, out: Path):
    if requests is not None:
        r = requests.get(url, stream=True)
        r.raise_for_status()
        with open(out, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
    else:
        if shutil.which("wget"):
            _run(["wget", "-c", "-O", str(out), url])
        elif shutil.which("curl"):
            _run(["curl", "-L", "-o", str(out), url])
        else:
            print(
                "[mxl] ERROR: Need 'requests' (pip install .[lammps]) or wget/curl to fetch LAMMPS.",
                file=sys.stderr,
            )
            raise SystemExit(2)


def mxl_lammps_main(argv=None):
    parser = argparse.ArgumentParser(
        description="Build and install lmp_mxl (custom LAMMPS)"
    )
    parser.add_argument(
        "--build-dir",
        type=str,
        default=os.environ.get("MXL_BUILD_DIR", ""),
        help="Directory to place/download LAMMPS sources (default: <REPO>/build)",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Delete any previous LAMMPS source/build before building",
    )
    parser.add_argument(
        "--transport",
        choices=("socket", "ucx", "both"),
        default=os.environ.get("MXL_LAMMPS_TRANSPORT", "socket"),
        help="Transport backend to compile into the LAMMPS binary.",
    )
    parser.add_argument(
        "--cmake-arg",
        action="append",
        default=[],
        help="Extra argument to append to the CMake configure command. Repeatable.",
    )
    parser.add_argument(
        "--binary-name",
        default="",
        help="Override the installed LAMMPS binary name.",
    )
    args = parser.parse_args(argv)

    if shutil.which("cmake") is None:
        print(
            "[mxl] ERROR: cmake not found. Try `pip install .[lammps]` or install CMake.",
            file=sys.stderr,
        )
        return 2

    build_root = Path(args.build_dir) if args.build_dir else _default_build_root()
    build_root.mkdir(parents=True, exist_ok=True)

    # Persisted source dir and tarball location
    src_root = build_root / SRC_DIR_NAME
    tarpath = build_root / "lammps.tar.gz"

    if args.clean and src_root.exists():
        print(f"[mxl] --clean: removing {src_root}")
        shutil.rmtree(src_root, ignore_errors=True)
    if args.clean and tarpath.exists():
        print(f"[mxl] --clean: removing {tarpath}")
        try:
            tarpath.unlink()
        except OSError:
            pass

    if not tarpath.exists():
        print(f"[mxl] Downloading LAMMPS tarball to {tarpath}")
        _download_tarball(TARBALL_URL, tarpath)
    else:
        print(f"[mxl] Reusing existing tarball: {tarpath}")

    if not src_root.exists():
        print(f"[mxl] Extracting to {build_root}")
        with tarfile.open(tarpath, "r:gz") as tf:
            tf.extractall(build_root)
    else:
        print(f"[mxl] Reusing existing source tree: {src_root}")

    fix_files, default_binary_name = _fix_files_for_transport(args.transport)

    # Copy our fix files
    here = Path(__file__).parent
    (src_root / "src" / "MISC").mkdir(parents=True, exist_ok=True)
    misc_dir = src_root / "src" / "MISC"
    for stale in misc_dir.glob("fix_maxwelllink*.cpp"):
        stale.unlink()
    for stale in misc_dir.glob("fix_maxwelllink*.h"):
        stale.unlink()
    for fn in fix_files:
        src = here / fn
        if not src.exists():
            print(f"[mxl] ERROR: missing {src}", file=sys.stderr)
            return 4
        shutil.copy2(src, misc_dir / fn)

    # Configure & build
    cmake_build_dir = src_root / "build"
    cmake_build_dir.mkdir(exist_ok=True)

    cmake_cfg = [
        "cmake",
        # LAMMPS uses subdir as project root
        "-S",
        str(src_root / "cmake"),
        "-B",
        str(cmake_build_dir),
        "-C",
        str(src_root / "cmake" / "presets" / "most.cmake"),
        "-C",
        str(src_root / "cmake" / "presets" / "nolib.cmake"),
        "-D",
        "PKG_GPU=off",
        # avoid FFTW/arch mismatch from tools like phana
        "-D",
        "BUILD_TOOLS=off",
        # do not use system FFTW, which may conflict with the arm64 vs x86_64 platforms
        "-D",
        "FFT=KISS",
        # remove libpng
        "-D",
        "WITH_PNG=off",
        # remove libjpeg
        "-D",
        "WITH_JPEG=off",
    ]
    if args.transport in {"ucx", "both"}:
        try:
            cmake_cfg.extend(_ucxx_cmake_args())
        except RuntimeError as exc:
            print(f"[mxl] ERROR: {exc}", file=sys.stderr)
            return 6
    cmake_cfg.extend(args.cmake_arg)
    if sys.platform == "darwin" and platform.machine() == "arm64":
        cmake_cfg += ["-D", "CMAKE_OSX_ARCHITECTURES=arm64"]

    _run(cmake_cfg)
    _run(["cmake", "--build", str(cmake_build_dir), "-j4"])

    # Find binary and install to environment's scripts dir
    candidates = [
        p for p in cmake_build_dir.glob("lmp*") if p.is_file() and os.access(p, os.X_OK)
    ]
    if not candidates:
        print("[mxl] ERROR: no 'lmp*' binary found in build dir", file=sys.stderr)
        return 5
    src_bin = candidates[0]

    scripts_dir = _scripts_dir()
    binary_name = args.binary_name.strip() or default_binary_name
    if os.name == "nt" and not binary_name.endswith(".exe"):
        binary_name += ".exe"
    dest_bin = scripts_dir / binary_name
    dest_bin.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src_bin, dest_bin)
    os.chmod(dest_bin, 0o755)

    print(f"[mxl] Built sources at: {src_root}")
    print(f"[mxl] Installed binary: {dest_bin}")
    print("[mxl] Try: lmp_mxl -h")
    return 0


if __name__ == "__main__":
    raise SystemExit(mxl_lammps_main())
