from __future__ import annotations
import sys
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
    release = version = "0.2.0"

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
# Store extracted notebook outputs directly in the published `_images` folder
nbsphinx_outputdir = "_images"


# Build autosummary pages for modules/classes/functions automatically
autosummary_generate = True

# Autodoc setting
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
autodoc_mock_imports = ["meep", "qutip", "ase", "psi4"]

# Napoleon (Google/NumPy docstrings)
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_use_param = True
napoleon_use_rtype = True


# Theme
html_theme = "furo"

html_theme_options = {
    "sidebar_hide_name": False,
    "top_of_page_button": "edit",
    "light_logo": "img/icon.png",
    "dark_logo": "img/icon.png",
    "light_css_variables": {
        "color-brand-primary": "#1264a3",
        "color-brand-content": "#0d2a4d",
        "color-sidebar-background": "#f6f9ff",
        "color-admonition-background": "rgba(18, 100, 163, 0.08)",
    },
    "dark_css_variables": {
        "color-brand-primary": "#66c7ff",
        "color-brand-content": "#d6ecff",
        "color-sidebar-background": "#0d1829",
        "color-admonition-background": "rgba(102, 199, 255, 0.12)",
    },
}

# General Sphinx settings
templates_path = ["_templates"]
exclude_patterns = ["_build"]
html_static_path = ["_static"]
html_css_files = [
    "css/custom.css",
]
