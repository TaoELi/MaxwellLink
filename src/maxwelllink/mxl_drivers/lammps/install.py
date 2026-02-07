# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""This file is a python version of mxl_install_lammps.sh
which is used to install a custom LAMMPS binary with fix_maxwelllink
support. It downloads a specific LAMMPS release tarball from GitHub,
adds the necessary source files, builds it with CMake, and places
the resulting binary in the same directory as other mxl scripts.
"""

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
            # pyproject is in repo root or in src; if it's in repo root, use it;
            # if it's in src/, go one level up
            return p if (p / "src").exists() is False else p.parent
    # Fallback: 4 levels up from .../src/maxwelllink/mxl_drivers/lammps/install.py
    return Path(__file__).resolve().parents[4]


def _default_build_root() -> Path:
    return _repo_root_from_here() / "build"


def _scripts_dir() -> Path:
    return Path(sysconfig.get_path("scripts"))


def _run(cmd):
    print("[mxl] $", " ".join(map(str, cmd)))
    subprocess.run(cmd, check=True)


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

    # Copy our fix files
    here = Path(__file__).parent
    (src_root / "src" / "MISC").mkdir(parents=True, exist_ok=True)
    for fn in ("fix_maxwelllink.cpp", "fix_maxwelllink.h"):
        src = here / fn
        if not src.exists():
            print(f"[mxl] ERROR: missing {src}", file=sys.stderr)
            return 4
        shutil.copy2(src, src_root / "src" / "MISC" / fn)

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
    dest_bin = scripts_dir / ("lmp_mxl.exe" if os.name == "nt" else "lmp_mxl")
    dest_bin.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src_bin, dest_bin)
    os.chmod(dest_bin, 0o755)

    print(f"[mxl] Built sources at: {src_root}")
    print(f"[mxl] Installed binary: {dest_bin}")
    print("[mxl] Try: lmp_mxl -h")
    return 0


if __name__ == "__main__":
    raise SystemExit(mxl_lammps_main())
