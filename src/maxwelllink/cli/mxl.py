"""Top-level ``mxl`` command dispatcher for MaxwellLink CLI actions."""

from __future__ import annotations

import argparse

from .mxl_clean import mxl_clean_main
from .mxl_init import mxl_init_main


def main(argv: list[str] | None = None) -> int:
    """Run the ``mxl`` command dispatcher.

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
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser(
        "init",
        help="Initialize current folder for MaxwellLink agent workflows.",
    )
    init_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite conflicting files/symlinks in the destination directory.",
    )

    clean_parser = subparsers.add_parser(
        "clean",
        help="Clean workspace artifacts created by `mxl init` / `mxl-init`.",
    )
    clean_parser.add_argument(
        "--force",
        action="store_true",
        help="Remove conflicting managed paths even when they were modified.",
    )

    args = parser.parse_args(argv)
    passthrough = ["--force"] if args.force else []
    if args.command == "init":
        return mxl_init_main(passthrough)
    if args.command == "clean":
        return mxl_clean_main(passthrough)

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
