"""Tests for MaxwellLink workspace init/clean CLI helpers."""

from __future__ import annotations

from pathlib import Path
import tempfile

import pytest

import maxwelllink.cli.mxl as mxl
from maxwelllink.cli import mxl_clean
from maxwelllink.cli import mxl_init


def _symlink_supported() -> bool:
    """Check whether symlink creation is available in this environment.

    Returns
    -------
    bool
        ``True`` when a simple symlink can be created.
    """
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        source = base / "source.txt"
        source.write_text("x", encoding="utf-8")
        link = base / "link.txt"
        try:
            link.symlink_to(source.name)
        except (OSError, NotImplementedError):
            return False
        return link.is_symlink()


pytestmark = pytest.mark.skipif(
    not _symlink_supported(),
    reason="Symlink support is not available in this environment.",
)


def _assert_points_to(link_path: Path, expected_source: Path) -> None:
    """Assert that a symlink resolves to an expected source path.

    Parameters
    ----------
    link_path : pathlib.Path
        Symlink path to validate.
    expected_source : pathlib.Path
        Expected resolved source path.
    """
    assert link_path.is_symlink()
    resolved = (link_path.parent / link_path.readlink()).resolve()
    assert resolved == expected_source.resolve()


@pytest.fixture
def payload_root(tmp_path: Path) -> Path:
    """Create a minimal fake payload tree for CLI unit tests.

    Parameters
    ----------
    tmp_path : pathlib.Path
        Temporary root directory provided by pytest.

    Returns
    -------
    pathlib.Path
        Path to a payload root containing required files/directories.
    """
    payload = tmp_path / "payload"
    payload.mkdir(parents=True, exist_ok=True)

    (payload / "AGENTS.md").write_text("agents prompt", encoding="utf-8")
    (payload / "README.md").write_text("readme", encoding="utf-8")

    for folder in ("src", "tests", "skills", "media", "tutorials"):
        root = payload / folder
        root.mkdir(parents=True, exist_ok=True)
        (root / "placeholder.txt").write_text(folder, encoding="utf-8")

    docs_source = payload / "docs" / "source"
    docs_source.mkdir(parents=True, exist_ok=True)
    (docs_source / "index.rst").write_text("docs", encoding="utf-8")

    return payload


def test_initialize_workspace_creates_expected_tree(
    tmp_path: Path,
    payload_root: Path,
) -> None:
    workdir = tmp_path / "workspace"
    workdir.mkdir()

    mxl_init.initialize_workspace(workdir, payload_root, force=False)

    agents = workdir / "AGENTS.md"
    assert agents.exists()
    assert agents.is_file()
    assert not agents.is_symlink()
    assert agents.read_text(encoding="utf-8") == "agents prompt"

    for name in ("src", "tests", "skills", "docs", "media", "tutorials", "README.md"):
        _assert_points_to(workdir / name, payload_root / name)

    _assert_points_to(workdir / "CLAUDE.md", agents)
    _assert_points_to(workdir / "GEMINI.md", agents)


def test_initialize_workspace_is_idempotent(tmp_path: Path, payload_root: Path) -> None:
    workdir = tmp_path / "workspace"
    workdir.mkdir()

    mxl_init.initialize_workspace(workdir, payload_root, force=False)
    mxl_init.initialize_workspace(workdir, payload_root, force=False)

    for name in ("src", "tests", "skills", "docs", "media", "tutorials", "README.md"):
        _assert_points_to(workdir / name, payload_root / name)


def test_initialize_workspace_conflict_and_force(
    tmp_path: Path, payload_root: Path
) -> None:
    workdir = tmp_path / "workspace"
    workdir.mkdir()
    (workdir / "AGENTS.md").write_text("different", encoding="utf-8")
    (workdir / "src").mkdir(parents=True, exist_ok=True)

    with pytest.raises(FileExistsError):
        mxl_init.initialize_workspace(workdir, payload_root, force=False)

    mxl_init.initialize_workspace(workdir, payload_root, force=True)
    _assert_points_to(workdir / "src", payload_root / "src")
    assert (workdir / "AGENTS.md").read_text(encoding="utf-8") == "agents prompt"


def test_mxl_init_main_uses_payload_and_cwd(
    tmp_path: Path,
    payload_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workdir = tmp_path / "workspace"
    workdir.mkdir()

    monkeypatch.setattr(mxl_init, "_resolve_payload_root", lambda: payload_root)
    monkeypatch.chdir(workdir)

    rc = mxl_init.mxl_init_main([])
    assert rc == 0
    _assert_points_to(workdir / "README.md", payload_root / "README.md")


def test_clean_workspace_removes_init_artifacts(
    tmp_path: Path, payload_root: Path
) -> None:
    workdir = tmp_path / "workspace"
    workdir.mkdir()

    mxl_init.initialize_workspace(workdir, payload_root, force=False)
    mxl_clean.clean_workspace(workdir, payload_root, force=False)

    for name in (
        "src",
        "tests",
        "skills",
        "docs",
        "media",
        "tutorials",
        "README.md",
        "AGENTS.md",
        "CLAUDE.md",
        "GEMINI.md",
    ):
        assert not (workdir / name).exists()


def test_clean_workspace_conflict_and_force(tmp_path: Path, payload_root: Path) -> None:
    workdir = tmp_path / "workspace"
    workdir.mkdir()

    mxl_init.initialize_workspace(workdir, payload_root, force=False)
    (workdir / "AGENTS.md").write_text("edited", encoding="utf-8")

    with pytest.raises(FileExistsError):
        mxl_clean.clean_workspace(workdir, payload_root, force=False)

    mxl_clean.clean_workspace(workdir, payload_root, force=True)
    assert not (workdir / "AGENTS.md").exists()
    assert not (workdir / "src").exists()


def test_mxl_clean_main_uses_payload_and_cwd(
    tmp_path: Path,
    payload_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workdir = tmp_path / "workspace"
    workdir.mkdir()

    monkeypatch.setattr(mxl_init, "_resolve_payload_root", lambda: payload_root)
    monkeypatch.chdir(workdir)

    assert mxl_init.mxl_init_main([]) == 0
    assert (workdir / "src").is_symlink()

    rc = mxl_clean.mxl_clean_main([])
    assert rc == 0
    assert not (workdir / "src").exists()
    assert not (workdir / "AGENTS.md").exists()


def test_mxl_dispatcher_supports_init_and_clean(
    tmp_path: Path,
    payload_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workdir = tmp_path / "workspace"
    workdir.mkdir()

    monkeypatch.setattr(mxl_init, "_resolve_payload_root", lambda: payload_root)
    monkeypatch.chdir(workdir)

    assert mxl.main(["init"]) == 0
    assert (workdir / "src").is_symlink()
    assert mxl.main(["clean"]) == 0
    assert not (workdir / "src").exists()
