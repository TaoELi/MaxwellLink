from __future__ import annotations
import os, sys
from pathlib import Path
from importlib.metadata import version as pkg_version, PackageNotFoundError

# --- Make the package importable ---
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

# --- Project info ---
project = "MaxwellLink"
author = "Tao E. Li"
try:
    release = pkg_version("maxwelllink")
    version = ".".join(release.split(".")[:2])
except PackageNotFoundError:
    release = version = "0.1.0"

# --- Extensions ---
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx.ext.todo",
    "sphinx.ext.viewcode",
    "myst_parser",           
    "sphinx.ext.mathjax",
    "nbsphinx",
]

mathjax3_config = {
    "tex": {
        "inlineMath": [["$", "$"], ["\\(", "\\)"]],
        "displayMath": [["$$", "$$"], ["\\[", "\\]"]],
    }
}

# Follow the links to examples/ in the root directory
followlinks = True
nbsphinx_execute = "never" 


# Build autosummary pages for modules/classes/functions automatically
autosummary_generate = True

# Autodoc settings: good defaults for scientific code
autodoc_default_options = {
    "members": True,
    "undoc-members": False,
    "inherited-members": True,
    "show-inheritance": True,
}
autodoc_typehints = "description"  
autodoc_class_signature = "separated"
autodoc_preserve_defaults = True

# For heavy dependencies that are not needed for docs building
autodoc_mock_imports = [
    "meep", "qutip", "ase", "psi4"
]

# Napoleon (Google/NumPy docstrings)
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_use_param = True
napoleon_use_rtype = True


# Theme
html_theme = "furo"

# General Sphinx settings
templates_path = ["_templates"]
exclude_patterns = ["_build"]
html_static_path = ["_static"]
