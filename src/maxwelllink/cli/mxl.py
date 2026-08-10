# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
Top-level ``mxl`` command dispatcher for MaxwellLink CLI actions.
"""

from __future__ import annotations

import argparse

from .mxl_clean import mxl_clean_main
from .mxl_hpc import mxl_hpc_main
from .mxl_init import mxl_init_main

# Map each subcommand to the CLI entry point that parses its own arguments.
_COMMANDS = {
    "init": mxl_init_main,
    "clean": mxl_clean_main,
    "hpc": mxl_hpc_main,
}


def main(argv: list[str] | None = None) -> int:
    """
    Run the ``mxl`` command dispatcher.

    The dispatcher selects a subcommand and forwards the remaining arguments to
    the matching entry point (``mxl init`` -> ``mxl-init``, ``mxl clean`` ->
    ``mxl-clean``, ``mxl hpc`` -> :func:`mxl_hpc_main`), which parses them itself.

    Parameters
    ----------
    argv : list of str or None, default=None
        Optional command-line arguments. When ``None``, uses ``sys.argv``.

    Returns
    -------
    int
        Exit status code from the selected subcommand.
    """
    parser = argparse.ArgumentParser(
        prog="mxl",
        description="MaxwellLink convenience CLI.",
    )
    parser.add_argument(
        "command",
        choices=tuple(_COMMANDS),
        help="Subcommand to run: init, clean, or hpc.",
    )
    parser.add_argument(
        "args",
        nargs=argparse.REMAINDER,
        help="Arguments forwarded to the subcommand (e.g. --force, or set FILE).",
    )

    parsed = parser.parse_args(argv)
    return _COMMANDS[parsed.command](parsed.args)


if __name__ == "__main__":
    raise SystemExit(main())
