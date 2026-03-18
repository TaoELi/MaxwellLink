"""Setuptools build hooks for packaging the MaxwellLink workspace payload.

This module customizes ``build_py`` so the source-tree payload used by
``mxl-init`` is copied into ``maxwelllink/_workspace_payload`` in the built
wheel.
"""

from __future__ import annotations

from pathlib import Path
import shutil

from setuptools import setup
from setuptools.command.build_py import build_py as _build_py


_WORKSPACE_ITEMS: tuple[tuple[str, str], ...] = (
    ("AGENTS.md", "AGENTS.md"),
    ("README.md", "README.md"),
    ("src", "src"),
    ("tests", "tests"),
    ("skills", "skills"),
    ("docs/source", "docs/source"),
    ("media", "media"),
    ("tutorials", "tutorials"),
)


def _copy_workspace_payload(repo_root: Path, payload_root: Path) -> None:
    """Copy workspace payload files into the wheel build directory.

    Parameters
    ----------
    repo_root : pathlib.Path
        Repository root containing source payload paths.
    payload_root : pathlib.Path
        Destination path inside ``build_lib`` where payload files are staged.

    Raises
    ------
    FileNotFoundError
        If any required payload item is missing in ``repo_root``.
    """
    if payload_root.exists():
        shutil.rmtree(payload_root)
    payload_root.mkdir(parents=True, exist_ok=True)

    ignore = shutil.ignore_patterns("__pycache__", "*.pyc", ".DS_Store", "*.egg-info")

    for src_rel, dst_rel in _WORKSPACE_ITEMS:
        src = repo_root / src_rel
        dst = payload_root / dst_rel
        if not src.exists():
            raise FileNotFoundError(f"Missing required workspace payload path: {src}")

        if src.is_dir():
            shutil.copytree(src, dst, dirs_exist_ok=True, ignore=ignore)
        else:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)


class build_py(_build_py):
    """Custom ``build_py`` command that stages the workspace payload."""

    def run(self) -> None:
        """Run standard build and then copy the workspace payload.

        Returns
        -------
        None
            This method updates files in ``self.build_lib`` in place.
        """
        super().run()
        repo_root = Path(__file__).resolve().parent
        payload_root = Path(self.build_lib) / "maxwelllink" / "_workspace_payload"
        _copy_workspace_payload(repo_root, payload_root)


setup(
    cmdclass={
        "build_py": build_py,
    }
)
