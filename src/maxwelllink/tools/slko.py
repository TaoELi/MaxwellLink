# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Slater-Koster parameter sets for the DFTB drivers fetched on first use.
"""

import os
import shutil
import tarfile
import tempfile
import urllib.error
import urllib.request

# The parameter sets published at ``github.com/dftbparams``
SETS = {
    "3ob": "H C N O P S, third-order corrected; the general-purpose organic set",
    "3ob-freq": "3ob variant reparameterised for vibrational frequencies",
    "3ob-hhmod": "3ob with a modified H-H repulsive",
    "3ob-nhmod": "3ob with a modified N-H repulsive",
    "3ob-ophyd": "3ob with a modified O-H repulsive for hydration",
    "auorg": "gold with organic elements",
    "auorgap": "auorg variant for gold-thiolate systems",
    "borg": "boron with organic elements",
    "chalc": "chalcogenides",
    "halorg": "halogens with organic elements",
    "hyb": "hybrid organic-inorganic, including Ag and Si",
    "magsil": "magnesium silicates",
    "matsci": "materials-science set for solids",
    "mio": "H C N O P S, the original organic set",
    "miomod-hh": "mio with a modified H-H repulsive",
    "miomod-nh": "mio with a modified N-H repulsive",
    "ob2": "range-separated-tuned organic set",
    "pbc": "solid-state set for periodic systems",
    "perov": "halide perovskites",
    "rare": "rare-earth elements",
    "siband": "silicon band structure",
    "tiorg": "titanium with organic elements",
    "trans3d": "3d transition metals",
    "znorg": "zinc with organic elements",
}

#: Versioned names people and older reference data use, mapped onto the repositories.
ALIASES = {
    "3ob-3-1": "3ob",
    "mio-1-1": "mio",
    "auorg-1-1": "auorg",
    "znorg-0-1": "znorg",
    "tiorg-0-1": "tiorg",
    "hyb-0-2": "hyb",
    "matsci-0-3": "matsci",
    "pbc-0-3": "pbc",
    "borg-0-1": "borg",
    "halorg-0-1": "halorg",
    "trans3d-0-1": "trans3d",
    "siband-1-1": "siband",
    "ob2-1-1": "ob2",
    "chalc-0-2": "chalc",
    "magsil-0-1": "magsil",
    "rare-0-2": "rare",
}

_ARCHIVE = "https://github.com/dftbparams/{name}/archive/refs/heads/main.tar.gz"
_SKF_DIRECTORY = "skfiles"
_DOWNLOAD_TIMEOUT = 900  # seconds; the archives run to tens of megabytes


def canonical_name(name):
    """Return the repository name of a set, resolving the versioned aliases."""

    key = str(name).strip()
    return ALIASES.get(key, key)


def available_sets():
    """Return the names of every parameter set that can be downloaded."""

    return tuple(sorted(SETS))


def install_root():
    """Directory the sets are stored in.

    ``MAXWELLLINK_SLKO_DIR`` wins if set. Otherwise the sets live beside the installed
    package so one download serves the whole environment, falling back to the user cache
    when that is not writable.
    """

    override = os.environ.get("MAXWELLLINK_SLKO_DIR")
    if override:
        return override

    package = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if not _is_source_checkout(package):
        beside_package = os.path.join(package, "data", "slko")
        if os.path.isdir(beside_package) or _is_writable(os.path.dirname(package)):
            return beside_package

    base = os.environ.get("XDG_CACHE_HOME") or os.path.join(
        os.path.expanduser("~"), ".cache"
    )
    return os.path.join(base, "maxwelllink", "slko")


def _is_source_checkout(package):
    """Whether ``package`` is the working copy rather than an installed package.

    Tens of megabytes of parameter files must never land in somebody's git checkout,
    and they would: a test or script that puts ``src`` on ``sys.path`` imports this
    module straight from the working copy. Those runs use the user cache instead.
    """

    parent = os.path.dirname(package)
    if os.path.basename(parent) != "src":
        return False
    repository = os.path.dirname(parent)
    return any(
        os.path.exists(os.path.join(repository, marker))
        for marker in ("pyproject.toml", "setup.py", ".git")
    )


def _is_writable(path):
    """Whether a new directory could be created under ``path``."""

    probe = path
    while probe and not os.path.exists(probe):
        parent = os.path.dirname(probe)
        if parent == probe:
            break
        probe = parent
    return os.access(probe, os.W_OK) if probe else False


def _has_skf(path):
    """Whether ``path`` is a directory holding at least one ``.skf`` file."""

    return os.path.isdir(path) and any(
        name.endswith(".skf") for name in os.listdir(path)
    )


def _download(name, destination):
    """Fetch one parameter set and flatten its ``.skf`` files into ``destination``."""

    url = _ARCHIVE.format(name=name)
    with tempfile.TemporaryDirectory() as work:
        archive = os.path.join(work, "set.tar.gz")
        with urllib.request.urlopen(url, timeout=_DOWNLOAD_TIMEOUT) as response:
            with open(archive, "wb") as handle:
                shutil.copyfileobj(response, handle)
        with tarfile.open(archive) as tar:
            # take only the .skf files, and refuse any member whose path would escape
            # the extraction directory
            members = [
                member
                for member in tar.getmembers()
                if member.isfile()
                and member.name.endswith(".skf")
                and ("/%s/" % _SKF_DIRECTORY) in member.name
                and not os.path.isabs(member.name)
                and ".." not in member.name.split("/")
            ]
            if not members:
                raise RuntimeError(f"no .skf files in the {name} archive at {url}")
            # filter="data" is Python 3.12+, and becomes the default in 3.14; it
            # refuses absolute paths, links and odd permissions on top of the checks
            # above
            try:
                tar.extractall(work, members=members, filter="data")
            except TypeError:  # Python < 3.12
                tar.extractall(work, members=members)
        staging = destination + ".partial"
        shutil.rmtree(staging, ignore_errors=True)
        os.makedirs(staging)
        for member in members:
            shutil.copy(
                os.path.join(work, member.name),
                os.path.join(staging, os.path.basename(member.name)),
            )
        # rename last, so an interrupted download never leaves a half-set behind
        os.replace(staging, destination)


def fetch(name, root=None):
    """
    Return the directory of one parameter set, downloading it if it is not there.

    Parameters
    ----------
    name : str
        Set name, either a repository name from :func:`available_sets` or one of the
        versioned aliases such as ``'3ob-3-1'``.
    root : str, optional
        Directory to store sets in; defaults to :func:`install_root`.

    Returns
    -------
    str
        Path to a directory of ``.skf`` files.

    Raises
    ------
    ValueError
        If ``name`` is not a known set.
    RuntimeError
        If the set is absent and cannot be downloaded.
    """

    repository = canonical_name(name)
    if repository not in SETS:
        raise ValueError(
            f"unknown Slater-Koster set {name!r}. Available: "
            f"{', '.join(available_sets())}"
        )

    root = root or install_root()
    destination = os.path.join(root, repository)
    if _has_skf(destination):
        return destination
    if os.environ.get("MAXWELLLINK_SLKO_NO_DOWNLOAD"):
        raise RuntimeError(
            f"the {repository} parameter set is not in {root} and downloading is "
            f"disabled by MAXWELLLINK_SLKO_NO_DOWNLOAD."
        )

    try:
        os.makedirs(root, exist_ok=True)
        _download(repository, destination)
    except (urllib.error.URLError, OSError, RuntimeError, tarfile.TarError) as exc:
        raise RuntimeError(
            f"could not obtain the {repository} parameter set: {exc}. Download it "
            f"from https://github.com/dftbparams/{repository} and either put its "
            f".skf files in {destination} or point MAXWELLLINK_SLKO_DIR at them."
        ) from exc
    return destination


def resolve(path_or_name, root=None):
    """
    Turn a directory or a set name into a directory of ``.skf`` files.

    An existing directory is returned unchanged, so a hand-installed parameter set keeps
    working; anything else is treated as a set name and downloaded on first use.

    Parameters
    ----------
    path_or_name : str
        Directory holding ``.skf`` files, or a name from :func:`available_sets`.
    root : str, optional
        Directory to store downloaded sets in; defaults to :func:`install_root`.

    Returns
    -------
    str
        Path to a directory of ``.skf`` files.
    """

    text = str(path_or_name)
    if os.path.isdir(text):
        return text
    if os.sep in text or (os.altsep and os.altsep in text):
        raise ValueError(
            f"{text!r} looks like a path but is not a directory. To download a "
            f"parameter set instead, pass its name: {', '.join(available_sets())}"
        )
    return fetch(text, root=root)


#: Package-level aliases
fetch_slko = fetch
resolve_slko = resolve
