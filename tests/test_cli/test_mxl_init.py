# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""Tests for MaxwellLink workspace init/clean CLI helpers."""

from __future__ import annotations

import json
from pathlib import Path
import tempfile

import pytest

import maxwelllink.cli.mxl as mxl
from maxwelllink.cli import mxl_clean
from maxwelllink.cli import mxl_hpc
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
    (payload / "HPC_PROFILE.json").write_text(
        json.dumps(
            {
                "slurm_default_partition": "shared",
                "slurm_defaults": "--nodes=1 --ntasks=1 --time=01:00:00",
                "slurm_resource_policy": "serial-first",
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    for folder in ("src", "tests", "skills", "media", "tutorials"):
        root = payload / folder
        root.mkdir(parents=True, exist_ok=True)
        (root / "placeholder.txt").write_text(folder, encoding="utf-8")

    docs_source = payload / "docs" / "source"
    docs_source.mkdir(parents=True, exist_ok=True)
    (docs_source / "index.rst").write_text("docs", encoding="utf-8")

    return payload


@pytest.fixture(autouse=True)
def isolated_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Isolate home directory so tests do not modify user-global files.

    Parameters
    ----------
    tmp_path : pathlib.Path
        Temporary directory for the test.
    monkeypatch : pytest.MonkeyPatch
        Fixture used to patch environment variables.

    Returns
    -------
    pathlib.Path
        Isolated HOME path.
    """
    home = tmp_path / "home"
    home.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("HOME", str(home))
    return home


@pytest.fixture(autouse=True)
def force_slurm_available(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force ``sbatch`` availability for deterministic CLI tests.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Fixture used to patch module attributes.
    """
    monkeypatch.setattr(mxl_init, "_slurm_available", lambda: True)


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

    _assert_points_to(
        workdir / "HPC_PROFILE.json",
        mxl_init._global_hpc_profile_path(),
    )
    assert mxl_init._global_hpc_profile_path().is_file()
    assert mxl_init._global_hpc_profile_path().read_text(encoding="utf-8") == (
        payload_root / "HPC_PROFILE.json"
    ).read_text(encoding="utf-8")

    _assert_points_to(workdir / "CLAUDE.md", agents)
    _assert_points_to(workdir / "GEMINI.md", agents)


def test_initialize_workspace_is_idempotent(tmp_path: Path, payload_root: Path) -> None:
    workdir = tmp_path / "workspace"
    workdir.mkdir()

    mxl_init.initialize_workspace(workdir, payload_root, force=False)
    mxl_init.initialize_workspace(workdir, payload_root, force=False)

    for name in ("src", "tests", "skills", "docs", "media", "tutorials", "README.md"):
        _assert_points_to(workdir / name, payload_root / name)
    _assert_points_to(
        workdir / "HPC_PROFILE.json",
        mxl_init._global_hpc_profile_path(),
    )


def test_initialize_workspace_skips_hpc_profile_when_no_sbatch(
    tmp_path: Path,
    payload_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workdir = tmp_path / "workspace"
    workdir.mkdir()
    monkeypatch.setattr(mxl_init, "_slurm_available", lambda: False)

    mxl_init.initialize_workspace(workdir, payload_root, force=False)

    assert not (workdir / "HPC_PROFILE.json").exists()
    assert not mxl_init._global_hpc_profile_path().exists()


@pytest.mark.parametrize("slurm_available", [False, True])
def test_initialize_and_clean_without_bundled_hpc_profile(
    tmp_path: Path,
    payload_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    slurm_available: bool,
) -> None:
    (payload_root / "HPC_PROFILE.json").unlink()
    monkeypatch.setattr(mxl_init, "_slurm_available", lambda: slurm_available)
    workdir = tmp_path / "workspace"
    workdir.mkdir()

    mxl_init.initialize_workspace(workdir, payload_root)
    _assert_points_to(workdir / "src", payload_root / "src")
    assert not (workdir / "HPC_PROFILE.json").exists()
    assert not mxl_init._global_hpc_profile_path().exists()

    mxl_clean.clean_workspace(workdir, payload_root)
    assert not (workdir / "src").exists()


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
    _assert_points_to(workdir / "HPC_PROFILE.json", mxl_init._global_hpc_profile_path())


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
        "HPC_PROFILE.json",
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
    assert mxl_init._global_hpc_profile_path().exists()


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
    assert not (workdir / "HPC_PROFILE.json").exists()
    assert mxl_init._global_hpc_profile_path().exists()


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


def test_set_hpc_profile_and_dispatcher(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workdir = tmp_path / "workspace"
    workdir.mkdir()
    src = workdir / "custom_profile.json"
    src.write_text(
        json.dumps(
            {
                "slurm_default_partition": "debug",
                "slurm_defaults": "--nodes=2 --ntasks=64 --time=04:00:00",
                "slurm_resource_policy": "mpi-multinode",
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    dest = mxl_hpc.set_hpc_profile(src)
    assert dest == mxl_init._global_hpc_profile_path()
    assert dest.exists()
    assert '"slurm_default_partition": "debug"' in dest.read_text(encoding="utf-8")

    monkeypatch.chdir(workdir)
    rc = mxl.main(["hpc", "set", str(src)])
    assert rc == 0
    assert '"slurm_default_partition": "debug"' in dest.read_text(encoding="utf-8")


def test_mxl_hpc_creates_default_global_profile_when_missing(
    tmp_path: Path,
    payload_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    workdir = tmp_path / "workspace"
    workdir.mkdir()
    monkeypatch.chdir(workdir)
    monkeypatch.setattr(mxl_init, "_resolve_payload_root", lambda: payload_root)

    profile = mxl_init._global_hpc_profile_path()
    assert not profile.exists()

    rc = mxl.main(["hpc"])
    assert rc == 0
    assert profile.exists()
    assert profile.read_text(encoding="utf-8") == (
        payload_root / "HPC_PROFILE.json"
    ).read_text(encoding="utf-8")
    output = capsys.readouterr().out
    assert "Created default HPC profile" in output
    assert "You can adjust ~/.maxwelllink/HPC_PROFILE.json as needed" in output


def test_mxl_hpc_keeps_existing_global_profile(
    tmp_path: Path,
    payload_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    workdir = tmp_path / "workspace"
    workdir.mkdir()
    monkeypatch.chdir(workdir)
    monkeypatch.setattr(mxl_init, "_resolve_payload_root", lambda: payload_root)

    profile = mxl_init._global_hpc_profile_path()
    profile.parent.mkdir(parents=True, exist_ok=True)
    profile.write_text(
        json.dumps(
            {
                "slurm_default_partition": "custom",
                "slurm_defaults": "--nodes=1 --ntasks=8 --time=00:10:00",
                "slurm_resource_policy": "custom",
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    before = profile.read_text(encoding="utf-8")

    rc = mxl.main(["hpc"])
    assert rc == 0
    assert profile.read_text(encoding="utf-8") == before

    output = capsys.readouterr().out
    assert "already exists" in output
    assert "You can adjust ~/.maxwelllink/HPC_PROFILE.json as needed" in output


def test_set_hpc_profile_validation(tmp_path: Path) -> None:
    bad = tmp_path / "bad_profile.json"
    bad.write_text(
        json.dumps(
            {
                "slurm_default_partition": "shared",
                "slurm_defaults": "--nodes=1 --ntasks=1",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError):
        mxl_hpc.set_hpc_profile(bad)


def test_init_reuses_existing_global_hpc_profile(
    tmp_path: Path,
    payload_root: Path,
) -> None:
    (payload_root / "HPC_PROFILE.json").unlink()
    custom = tmp_path / "custom_profile.json"
    custom.write_text(
        json.dumps(
            {
                "slurm_default_partition": "debug",
                "slurm_defaults": "--nodes=2 --ntasks=16 --time=00:30:00",
                "slurm_resource_policy": "custom-policy",
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    dest = mxl_hpc.set_hpc_profile(custom)
    before = dest.read_text(encoding="utf-8")

    workdir = tmp_path / "workspace"
    workdir.mkdir()
    mxl_init.initialize_workspace(workdir, payload_root, force=False)

    assert dest.read_text(encoding="utf-8") == before
    _assert_points_to(workdir / "HPC_PROFILE.json", dest)
