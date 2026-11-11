# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
XYZ readers shared by the molecular drivers: one geometry, or one frame per molecule.
"""

import numpy as np

__all__ = ["read_xyz", "read_xyz_frames", "read_xyz_trajectory"]


def read_xyz(path):
    """
    Read the first frame of an XYZ file.

    Parameters
    ----------
    path : str
        Path to the file.

    Returns
    -------
    elements : list of str
        Element symbol of every atom, in file order.
    positions : numpy.ndarray of float, shape (n_atom, 3)
        Coordinates in Angstrom.
    """

    elements, frames = read_xyz_frames(path)
    return elements, frames[0]


def read_xyz_frames(path):
    """
    Read a single- or multi-frame XYZ file (``n_atom`` / comment / atom lines, repeated).

    Drivers given such a file as ``batch_xyz`` start molecule ``m`` from frame ``m``,
    so every frame must list the same atoms in the same order.

    Parameters
    ----------
    path : str
        Path to the file.

    Returns
    -------
    elements : list of str
        Element symbols, the same in every frame.
    positions : numpy.ndarray of float, shape (n_frames, n_atom, 3)
        Coordinates of every frame in Angstrom.

    Raises
    ------
    ValueError
        If the file holds no frame, or a frame lists other atoms than the first.
    """

    with open(path) as handle:
        lines = handle.read().split("\n")
    elements, frames, cursor = None, [], 0
    while cursor < len(lines) and lines[cursor].strip():
        n_atom = int(lines[cursor].split()[0])
        block = lines[cursor + 2 : cursor + 2 + n_atom]
        symbols = [line.split()[0] for line in block]
        if elements is None:
            elements = symbols
        elif symbols != elements:
            raise ValueError(
                f"frame {len(frames)} of {path} lists other atoms than frame 0."
            )
        frames.append([[float(v) for v in line.split()[1:4]] for line in block])
        cursor += 2 + n_atom
    if not frames:
        raise ValueError(f"{path} holds no XYZ frame.")
    return elements, np.array(frames)


def read_xyz_trajectory(path, num):
    """
    Read a trajectory a driver wrote with ``traj_filename``, back into a batch array.

    Such a file is time-major with one frame per molecule per record; this undoes that.

    Parameters
    ----------
    path : str
        Path to the file.
    num : int
        Number of molecules the writing process owned (its ``molecule_ids``).

    Returns
    -------
    elements : list of str
        Element symbols of one molecule.
    positions : numpy.ndarray of float, shape (n_records, num, n_atom, 3)
        Coordinates in Angstrom.
    """

    elements, frames = read_xyz_frames(path)
    if frames.shape[0] % int(num):
        raise ValueError(
            f"{path} holds {frames.shape[0]} frames, not a multiple of {num} molecules."
        )
    return elements, frames.reshape(-1, int(num), frames.shape[1], 3)
