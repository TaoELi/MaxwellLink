"""Initialize local MaxwellLink workspace links for agent workflows.

This module provides the implementation behind ``mxl-init`` and ``mxl init``.
"""

from __future__ import annotations

import argparse
from importlib import resources
import os
from pathlib import Path
import shutil
import sys

_REPO_LINKS = ("src", "tests", "skills", "docs", "media", "tutorials", "README.md")
_AGENT_LINKS = ("CLAUDE.md", "GEMINI.md")
_REQUIRED_PAYLOAD_ITEMS = (
    "AGENTS.md",
    "README.md",
    "src",
    "tests",
    "skills",
    "docs",
    "media",
    "tutorials",
)


def _path_exists(path: Path) -> bool:
    """Return whether a path exists, including symlink entries.

    Parameters
    ----------
    path : pathlib.Path
        Path to inspect.

    Returns
    -------
    bool
        ``True`` when the target exists or is a symlink.
    """
    return path.exists() or path.is_symlink()


def _remove_path(path: Path) -> None:
    """Remove a file, symlink, or directory path.

    Parameters
    ----------
    path : pathlib.Path
        Path to remove.

    Raises
    ------
    FileNotFoundError
        If the path does not exist.
    """
    if path.is_symlink() or path.is_file():
        path.unlink()
        return
    if path.is_dir():
        shutil.rmtree(path)
        return
    raise FileNotFoundError(path)


def _is_valid_payload_root(path: Path) -> bool:
    """Check whether a payload root contains all required items.

    Parameters
    ----------
    path : pathlib.Path
        Candidate payload root.

    Returns
    -------
    bool
        ``True`` if all required files/directories are present.
    """
    return all((path / item).exists() for item in _REQUIRED_PAYLOAD_ITEMS)


def _resolve_payload_root() -> Path:
    """Resolve the installed (or local) workspace payload root.

    Returns
    -------
    pathlib.Path
        Path to a directory containing payload items needed for ``mxl-init``.

    Raises
    ------
    FileNotFoundError
        If no valid payload root can be located.
    """
    try:
        candidate = resources.files("maxwelllink").joinpath("_workspace_payload")
        candidate_path = Path(str(candidate))
    except Exception:
        candidate_path = None

    if candidate_path is not None and _is_valid_payload_root(candidate_path):
        return candidate_path

    repo_root = Path(__file__).resolve().parents[3]
    if _is_valid_payload_root(repo_root):
        return repo_root

    searched = []
    if candidate_path is not None:
        searched.append(str(candidate_path))
    searched.append(str(repo_root))
    raise FileNotFoundError(
        "Could not locate MaxwellLink workspace payload. "
        f"Searched: {', '.join(searched)}"
    )


def _symlink_matches(target_path: Path, source_path: Path) -> bool:
    """Check whether a symlink resolves to the expected source path.

    Parameters
    ----------
    target_path : pathlib.Path
        Existing symlink path to validate.
    source_path : pathlib.Path
        Expected symlink target.

    Returns
    -------
    bool
        ``True`` if ``target_path`` is a symlink resolving to ``source_path``.
    """
    if not target_path.is_symlink():
        return False
    resolved = (target_path.parent / target_path.readlink()).resolve()
    return resolved == source_path.resolve()


def _ensure_copied_file(source_path: Path, target_path: Path, force: bool) -> None:
    """Ensure a destination file is copied from source.

    Parameters
    ----------
    source_path : pathlib.Path
        Source file to copy.
    target_path : pathlib.Path
        Destination file path.
    force : bool
        Whether conflicting existing paths may be overwritten.

    Raises
    ------
    FileExistsError
        If ``target_path`` conflicts and ``force`` is ``False``.
    """
    if _path_exists(target_path):
        if (
            target_path.is_file()
            and not target_path.is_symlink()
            and target_path.read_bytes() == source_path.read_bytes()
        ):
            return
        if not force:
            raise FileExistsError(
                f"Conflict at {target_path}: already exists. Use --force to overwrite."
            )
        _remove_path(target_path)

    target_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, target_path)


def _ensure_symlink(source_path: Path, target_path: Path, force: bool) -> None:
    """Ensure a destination symlink points to the expected source path.

    Parameters
    ----------
    source_path : pathlib.Path
        Expected symlink source path.
    target_path : pathlib.Path
        Destination symlink path.
    force : bool
        Whether conflicting paths may be removed and replaced.

    Raises
    ------
    FileNotFoundError
        If ``source_path`` does not exist.
    FileExistsError
        If ``target_path`` conflicts and ``force`` is ``False``.
    """
    if not source_path.exists():
        raise FileNotFoundError(f"Missing source path for symlink: {source_path}")

    if _path_exists(target_path):
        if _symlink_matches(target_path, source_path):
            return
        if not force:
            raise FileExistsError(
                f"Conflict at {target_path}: already exists. Use --force to overwrite."
            )
        _remove_path(target_path)

    target_path.parent.mkdir(parents=True, exist_ok=True)
    relative_target = Path(os.path.relpath(source_path, start=target_path.parent))
    target_path.symlink_to(relative_target, target_is_directory=source_path.is_dir())


def initialize_workspace(
    destination: Path,
    payload_root: Path,
    force: bool = False,
) -> None:
    """Initialize a local workspace directory for MaxwellLink agent use.

    Parameters
    ----------
    destination : pathlib.Path
        Directory where files/symlinks will be created.
    payload_root : pathlib.Path
        Root directory containing the install-time workspace payload.
    force : bool, default=False
        Whether to overwrite conflicting files/symlinks.

    Raises
    ------
    FileNotFoundError
        If ``payload_root`` is invalid.
    FileExistsError
        If conflicts are found and ``force`` is ``False``.
    """
    if not _is_valid_payload_root(payload_root):
        raise FileNotFoundError(
            f"Invalid payload root {payload_root}. "
            f"Expected: {', '.join(_REQUIRED_PAYLOAD_ITEMS)}"
        )

    agents_src = payload_root / "AGENTS.md"
    agents_dst = destination / "AGENTS.md"
    _ensure_copied_file(agents_src, agents_dst, force=force)

    for name in _REPO_LINKS:
        src = payload_root / name
        dst = destination / name
        _ensure_symlink(src, dst, force=force)

    for name in _AGENT_LINKS:
        dst = destination / name
        _ensure_symlink(agents_dst, dst, force=force)


def mxl_init_main(argv: list[str] | None = None) -> int:
    """Run the ``mxl-init`` CLI entry point.

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
        prog="mxl-init",
        description=(
            "Initialize the current directory for MaxwellLink agent workflows by "
            "copying AGENTS.md and creating symlinks to installed resources."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite conflicting files/symlinks in the destination directory.",
    )
    args = parser.parse_args(argv)

    try:
        payload_root = _resolve_payload_root()
        initialize_workspace(Path.cwd(), payload_root, force=args.force)
    except Exception as exc:
        print(f"[mxl-init] ERROR: {exc}", file=sys.stderr)
        return 2

    print("[mxl-init] Workspace initialized in", Path.cwd())
    return 0


if __name__ == "__main__":
    raise SystemExit(mxl_init_main())
