"""Clean local MaxwellLink workspace artifacts created by ``mxl-init``.

This module provides the implementation behind ``mxl-clean`` and
``mxl clean``.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

from . import mxl_init


def _remove_managed_symlink(
    target_path: Path, expected_source: Path, force: bool = False
) -> None:
    """Remove a managed symlink if it matches the expected source.

    Parameters
    ----------
    target_path : pathlib.Path
        Symlink path in the local workspace.
    expected_source : pathlib.Path
        Expected symlink source path.
    force : bool, default=False
        Whether to remove even when the symlink target does not match.

    Raises
    ------
    FileExistsError
        If a conflicting path exists and ``force`` is ``False``.
    """
    if not mxl_init._path_exists(target_path):
        return
    if force:
        mxl_init._remove_path(target_path)
        return
    if mxl_init._symlink_matches(target_path, expected_source):
        mxl_init._remove_path(target_path)
        return
    raise FileExistsError(
        f"Conflict at {target_path}: expected symlink to {expected_source}. "
        "Use --force to remove anyway."
    )


def _remove_managed_agents_file(
    target_path: Path, expected_source: Path, force: bool = False
) -> None:
    """Remove local ``AGENTS.md`` when it matches the managed content.

    Parameters
    ----------
    target_path : pathlib.Path
        Local ``AGENTS.md`` path.
    expected_source : pathlib.Path
        Reference ``AGENTS.md`` from payload.
    force : bool, default=False
        Whether to remove even if local content was modified.

    Raises
    ------
    FileExistsError
        If local ``AGENTS.md`` differs and ``force`` is ``False``.
    """
    if not mxl_init._path_exists(target_path):
        return
    if force:
        mxl_init._remove_path(target_path)
        return
    if (
        target_path.is_file()
        and not target_path.is_symlink()
        and expected_source.is_file()
        and target_path.read_bytes() == expected_source.read_bytes()
    ):
        mxl_init._remove_path(target_path)
        return
    raise FileExistsError(
        f"Conflict at {target_path}: not an unmodified AGENTS.md created by mxl-init. "
        "Use --force to remove anyway."
    )


def clean_workspace(
    destination: Path,
    payload_root: Path,
    force: bool = False,
) -> None:
    """Clean managed workspace files and symlinks from a destination folder.

    Parameters
    ----------
    destination : pathlib.Path
        Workspace directory to clean.
    payload_root : pathlib.Path
        Root directory containing installed payload targets.
    force : bool, default=False
        Whether to remove conflicting/modified managed paths.

    Raises
    ------
    FileNotFoundError
        If ``payload_root`` is invalid.
    FileExistsError
        If conflicting managed paths are found and ``force`` is ``False``.
    """
    if not mxl_init._is_valid_payload_root(payload_root):
        raise FileNotFoundError(
            f"Invalid payload root {payload_root}. "
            f"Expected: {', '.join(mxl_init._REQUIRED_PAYLOAD_ITEMS)}"
        )

    agents_src = payload_root / "AGENTS.md"
    agents_dst = destination / "AGENTS.md"

    for name in mxl_init._AGENT_LINKS:
        dst = destination / name
        _remove_managed_symlink(dst, agents_dst, force=force)

    for name in mxl_init._REPO_LINKS:
        src = payload_root / name
        dst = destination / name
        _remove_managed_symlink(dst, src, force=force)

    _remove_managed_agents_file(agents_dst, agents_src, force=force)


def mxl_clean_main(argv: list[str] | None = None) -> int:
    """Run the ``mxl-clean`` CLI entry point.

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
        prog="mxl-clean",
        description=(
            "Clean MaxwellLink agent workspace artifacts from the current "
            "directory (reverse of mxl-init)."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Remove conflicting managed paths even when they were modified.",
    )
    args = parser.parse_args(argv)

    try:
        payload_root = mxl_init._resolve_payload_root()
        clean_workspace(Path.cwd(), payload_root, force=args.force)
    except Exception as exc:
        print(f"[mxl-clean] ERROR: {exc}", file=sys.stderr)
        return 2

    print("[mxl-clean] Workspace cleaned in", Path.cwd())
    return 0


if __name__ == "__main__":
    raise SystemExit(mxl_clean_main())
