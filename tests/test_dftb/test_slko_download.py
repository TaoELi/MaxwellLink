# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Tests for the Slater-Koster downloader that the DFTB drivers depend on.
"""

import os

import pytest

slko = pytest.importorskip(
    "maxwelllink.tools.slko", reason="maxwelllink is required for this test"
)


@pytest.mark.core
def test_every_advertised_set_has_a_download():
    """Names, aliases and the archive template have to agree with each other."""

    names = slko.available_sets()
    assert len(names) >= 20, "the published parameter sets are not all listed"
    assert "3ob" in names and "mio" in names
    for alias, target in slko.ALIASES.items():
        assert target in names, f"alias {alias} points at the unknown set {target}"
        assert slko.canonical_name(alias) == target
    # a name that is already canonical passes through untouched
    assert slko.canonical_name("3ob") == "3ob"


@pytest.mark.core
def test_a_directory_is_taken_as_given():
    """An existing directory is a parameter set, not a name to be downloaded."""

    here = os.path.dirname(os.path.abspath(__file__))
    assert slko.resolve(here) == here


@pytest.mark.core
def test_unknown_names_and_paths_are_rejected_clearly():
    """The two ways of getting it wrong each get their own message."""

    with pytest.raises(ValueError, match="unknown Slater-Koster set"):
        slko.resolve("not-a-real-parameter-set")
    with pytest.raises(ValueError, match="looks like a path"):
        slko.resolve(os.path.join("no", "such", "directory"))


@pytest.mark.core
def test_downloading_can_be_forbidden(tmp_path, monkeypatch):
    """``MAXWELLLINK_SLKO_NO_DOWNLOAD`` fails loudly instead of reaching the network."""

    monkeypatch.setenv("MAXWELLLINK_SLKO_DIR", str(tmp_path))
    monkeypatch.setenv("MAXWELLLINK_SLKO_NO_DOWNLOAD", "1")
    with pytest.raises(RuntimeError, match="MAXWELLLINK_SLKO_NO_DOWNLOAD"):
        slko.fetch("3ob")


@pytest.mark.core
def test_an_installed_set_is_reused_rather_than_refetched(tmp_path, monkeypatch):
    """A set already on disk is returned without touching the network.

    Downloading is forbidden for this call, so if the cache check were wrong the test
    would fail rather than quietly spend a minute re-fetching.
    """

    monkeypatch.setenv("MAXWELLLINK_SLKO_DIR", str(tmp_path))
    folder = tmp_path / "3ob"
    folder.mkdir()
    (folder / "H-H.skf").write_text("not a real parameter file\n")

    monkeypatch.setenv("MAXWELLLINK_SLKO_NO_DOWNLOAD", "1")
    assert slko.fetch("3ob") == str(folder)
    assert slko.fetch("3ob-3-1") == str(folder), "the alias must reach the same set"


@pytest.mark.core
def test_a_source_checkout_is_never_written_into():
    """Parameter files must not land in a working copy.

    Any test or script that puts ``src`` on ``sys.path`` imports this module from the
    checkout rather than from the installed package. Without this guard the downloader
    dropped 78 MB of .skf files into the repository, which is exactly what the download
    exists to avoid.
    """

    root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
    )
    if not os.path.exists(os.path.join(root, "pyproject.toml")):
        pytest.skip("not running from a source checkout")
    assert slko._is_source_checkout(os.path.join(root, "src", "maxwelllink"))
    # and an installed layout is still recognised as installable
    assert not slko._is_source_checkout(os.path.join("site-packages", "maxwelllink"))


@pytest.mark.slow
def test_the_default_set_downloads_and_parses(tmp_path, monkeypatch):
    """End to end: fetch 3ob into an empty directory and read a file out of it."""

    pytest.importorskip("maxwelllink.mxl_drivers.python.models.rtdftb_model")
    from maxwelllink.mxl_drivers.python.models.rtdftb_model import read_skf

    monkeypatch.setenv("MAXWELLLINK_SLKO_DIR", str(tmp_path))
    try:
        folder = slko.fetch("3ob")
    except RuntimeError as exc:
        pytest.skip(f"no network for the parameter-set download: {exc}")

    names = sorted(n for n in os.listdir(folder) if n.endswith(".skf"))
    assert len(names) > 100, "the downloaded set looks truncated"
    skf = read_skf(os.path.join(folder, "C-C.skf"), homonuclear=True)
    assert skf.n_grid > 0 and skf.mass > 0.0
    # nothing but .skf files, and no staging directory left behind
    assert all(n.endswith(".skf") for n in os.listdir(folder))
    assert not os.path.exists(str(tmp_path / "3ob.partial"))
