# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
Manage persistent MaxwellLink HPC profile settings.

This module provides the implementation behind ``mxl hpc`` and
``mxl hpc set``.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from . import mxl_init

_REQUIRED_HPC_KEYS = (
    "slurm_default_partition",
    "slurm_defaults",
    "slurm_resource_policy",
)


def _load_hpc_profile(path: Path) -> dict:
    """
    Load and return an HPC profile JSON object.

    Raises
    ------
    ValueError
        If the file does not contain a JSON object.
    """
    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        raise ValueError(f"HPC profile must be a JSON object: {path}")
    return data


def _validate_hpc_profile(data: dict) -> None:
    """
    Validate required keys and value types of an HPC profile.

    Parameters
    ----------
    data : dict
        Parsed profile object.

    Raises
    ------
    ValueError
        If required keys are missing or values are not strings.
    """
    missing = [key for key in _REQUIRED_HPC_KEYS if key not in data]
    if missing:
        raise ValueError(f"Missing required HPC profile keys: {', '.join(missing)}")

    bad = [key for key in _REQUIRED_HPC_KEYS if not isinstance(data[key], str)]
    if bad:
        raise ValueError("HPC profile keys must map to strings: " + ", ".join(bad))


def set_hpc_profile(source_file: Path, destination_file: Path | None = None) -> Path:
    """
    Install a persistent global HPC profile.

    Parameters
    ----------
    source_file : pathlib.Path
        User-provided JSON profile.
    destination_file : pathlib.Path or None, default=None
        Install destination. When ``None``, uses
        ``~/.maxwelllink/HPC_PROFILE.json``.

    Returns
    -------
    pathlib.Path
        Installed destination path.
    """
    if not source_file.exists():
        raise FileNotFoundError(f"Profile file not found: {source_file}")

    data = _load_hpc_profile(source_file)
    _validate_hpc_profile(data)

    destination = destination_file or mxl_init._global_hpc_profile_path()
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    return destination


def ensure_default_hpc_profile() -> tuple[Path, bool]:
    """
    Ensure the default global HPC profile exists.

    Returns
    -------
    tuple[pathlib.Path, bool]
        ``(profile_path, created)`` where ``created`` is ``True`` only when
        a new profile was copied from the MaxwellLink payload.
    """
    destination = mxl_init._global_hpc_profile_path()
    exists_before = destination.exists()
    payload_root = mxl_init._resolve_payload_root()
    profile_path = mxl_init._ensure_global_hpc_profile(payload_root)
    return profile_path, not exists_before


def mxl_hpc_main(argv: list[str] | None = None) -> int:
    """
    Run the ``mxl hpc`` CLI command family.

    Parameters
    ----------
    argv : list of str or None, default=None
        Optional command-line arguments. When ``None``, uses ``sys.argv``.

    Returns
    -------
    int
        ``0`` on success, otherwise non-zero on failure.
    """
    parser = argparse.ArgumentParser(
        prog="mxl hpc",
        description="Manage persistent HPC profile settings for MaxwellLink.",
    )
    subparsers = parser.add_subparsers(dest="hpc_command", required=False)
    set_parser = subparsers.add_parser(
        "set",
        help="Install a user HPC profile to ~/.maxwelllink/HPC_PROFILE.json",
    )
    set_parser.add_argument(
        "file",
        type=str,
        help="Path to a JSON profile file.",
    )

    args = parser.parse_args(argv)

    try:
        if args.hpc_command == "set":
            dest = set_hpc_profile(Path(args.file).expanduser().resolve())
            print("[mxl-hpc] Installed profile at", dest)
        else:
            _, created = ensure_default_hpc_profile()
            if created:
                print(
                    "[mxl-hpc] Created default HPC profile at "
                    "~/.maxwelllink/HPC_PROFILE.json"
                )
            else:
                print(
                    "[mxl-hpc] ~/.maxwelllink/HPC_PROFILE.json already exists. "
                    "No copy was made."
                )
            print(
                "[mxl-hpc] You can adjust ~/.maxwelllink/HPC_PROFILE.json as needed "
                "for your HPC environment."
            )
    except Exception as exc:
        print(f"[mxl-hpc] ERROR: {exc}", file=sys.stderr)
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(mxl_hpc_main())
