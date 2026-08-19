# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Reader for the Slater-Koster (.skf) parameter files used by DFTB+.

One .skf file holds the two-centre integrals for one ordered pair of elements on a
uniform radial grid, the atomic data for a homonuclear pair, and the repulsive pair
potential. The layout is Fortran list-directed input, so :class:`_ListDirected` mimics
one Fortran ``READ`` per call, discarding the rest of each record as Fortran does.
"""

import math
import re
from dataclasses import dataclass

import numpy as np

from .jit import kernel

MIN_NEIGH_DIST = 1.0e-2  # accuracy.F90:78, below this the repulsive is switched off

# constants.F90:49,52 -- e__amu = 0.00054857990945, amu__au = 1 / e__amu
AMU_TO_AU = 1.0 / 0.00054857990945

# oldskdata.F90:77 -- the 10 columns an spd ("simple") file stores, scattered into the
# 20-column table. Converted here to 0-based column indices.
_ISK_INTER_OLD = np.array([8, 9, 10, 13, 14, 15, 16, 18, 19, 20]) - 1

# parser.F90:3994-4001 -- skMap(m, lMax, lMin) -> 1-based column of the 20-wide table.
# The 20 columns in file order are
#   ff0 ff1 ff2 ff3 df0 df1 df2 dd0 dd1 dd2 pf0 pf1 pd0 pd1 pp0 pp1 sf0 sd0 sp0 ss0
_SK_MAP_COLUMNS = {
    (0, 0): (20,),
    (0, 1): (19,),
    (0, 2): (18,),
    (0, 3): (17,),
    (1, 1): (15, 16),
    (1, 2): (13, 14),
    (1, 3): (11, 12),
    (2, 2): (8, 9, 10),
    (2, 3): (5, 6, 7),
    (3, 3): (1, 2, 3, 4),
}

# SK_MAP[m, l_max, l_min] -> 0-based column; -1 where the combination does not exist.
SK_MAP = np.full((4, 4, 4), -1, dtype=np.int64)
for (_l_min, _l_max), _cols in _SK_MAP_COLUMNS.items():
    for _m, _col in enumerate(_cols):
        SK_MAP[_m, _l_max, _l_min] = _col - 1

_TOKEN = re.compile(r"[^\s,]+")
_REPEAT = re.compile(r"^(\d+)\*(.*)$")
_D_EXPONENT = re.compile(r"([0-9.])[dD]([+-]?\d)")


def _to_float(token):
    """Convert one Fortran numeric token; a null repeat (``n*``) counts as zero."""

    if token is None:
        return 0.0
    return float(_D_EXPONENT.sub(r"\1e\2", token.strip()))


class _ListDirected:
    """Minimal Fortran list-directed reader over a list of records (lines)."""

    def __init__(self, records, first=0):
        self.records = records
        self.i_record = first
        self.buffer = []
        self.i_token = 0

    def _refill(self):
        """Tokenize records until at least one unread token is buffered."""

        while self.i_token >= len(self.buffer):
            if self.i_record >= len(self.records):
                raise EOFError("unexpected end of .skf file")
            line = self.records[self.i_record].replace("\t", " ")
            self.i_record += 1
            tokens = []
            for raw in _TOKEN.findall(line):
                repeat = _REPEAT.match(raw)
                if repeat:
                    count, value = int(repeat.group(1)), repeat.group(2)
                    tokens += [value if value else None] * count
                else:
                    tokens.append(raw)
            self.buffer, self.i_token = tokens, 0

    def read(self, n_items):
        """One Fortran READ of ``n_items`` values, then skip the rest of the record."""

        values = []
        while len(values) < n_items:
            self._refill()
            take = min(n_items - len(values), len(self.buffer) - self.i_token)
            values += self.buffer[self.i_token : self.i_token + take]
            self.i_token += take
        self.buffer, self.i_token = [], 0  # Fortran advances to the next record
        return [_to_float(v) for v in values]


@dataclass
class SkfData:
    """
    Everything one .skf file contains, in Hartree atomic units.

    Attributes
    ----------
    path : str
        File the data came from.
    homonuclear : bool
        Whether the file carries the atomic record (shell energies / U / occupations).
        Decided by the *species pair*, never by file content.
    extended : bool
        True for the "@" (spdf) format, where every integral record holds 40 numbers.
    grid_dist : float
        Radial grid spacing in Bohr.
    n_grid : int
        Number of tabulated radial points used, i.e. the header count minus one.
        Table row ``k`` (0-based) sits at ``r = (k + 1) * grid_dist``; ``r = 0`` is
        never tabulated.
    e_onsite, hubbard_u, occupation : numpy.ndarray of float, shape (4,)
        Free-atom shell energy (Hartree), Hubbard U (Hartree) and occupation
        (electrons), by angular momentum ``l = 0, 1, 2, 3``. Zero if heteronuclear.
    spe : float
        Spin-polarisation error; read and discarded by DFTB+, kept for completeness.
    mass : float
        Atomic mass in electron masses (the file value in amu times ``AMU_TO_AU``).
    h_tab, s_tab : numpy.ndarray of float, shape (n_grid, 20)
        Hamiltonian and overlap integrals in the 20-column layout of ``SK_MAP``.
    poly_coeffs : numpy.ndarray of float, shape (8,)
        Polynomial repulsive coefficients ``c2 ... c9``; junk in every distributed set.
    poly_cutoff : float
        Cutoff of the polynomial repulsive in Bohr.
    spline_xstart, spline_xend : numpy.ndarray of float, shape (n_interval,)
        Interval boundaries of the repulsive spline in Bohr.
    spline_coeffs : numpy.ndarray of float, shape (n_interval - 1, 4)
        Cubic coefficients ``c0 ... c3`` of every interval but the last.
    spline_last : numpy.ndarray of float, shape (6,)
        Fifth-order coefficients ``c0 ... c5`` of the last interval.
    spline_exp : numpy.ndarray of float, shape (3,)
        Exponential-head coefficients ``a1, a2, a3``: ``E = exp(-a1 r + a2) + a3``.
    spline_cutoff : float
        End of the last spline interval in Bohr (the header cutoff is discarded).
    """

    path: str
    homonuclear: bool
    extended: bool
    grid_dist: float
    n_grid: int
    e_onsite: np.ndarray
    hubbard_u: np.ndarray
    occupation: np.ndarray
    spe: float
    mass: float
    h_tab: np.ndarray
    s_tab: np.ndarray
    poly_coeffs: np.ndarray
    poly_cutoff: float
    spline_xstart: np.ndarray
    spline_xend: np.ndarray
    spline_coeffs: np.ndarray
    spline_last: np.ndarray
    spline_exp: np.ndarray
    spline_cutoff: float

    def r_grid(self):
        """Radial abscissae of the tabulated rows, in Bohr."""

        return np.arange(1, self.n_grid + 1) * self.grid_dist

    def table_cutoff(self, dist_fudge=1.0):
        """Distance beyond which the interpolator returns exact zeros (Bohr)."""

        return self.n_grid * self.grid_dist + dist_fudge


def _read_spline_block(records, path):
    """Locate and parse the ``Spline`` repulsive block; returns None when absent."""

    start = None
    for i, line in enumerate(records):
        if line.replace("\t", " ").rstrip() == "Spline":
            start = i
            break
    if start is None:
        return None

    reader = _ListDirected(records, start + 1)
    # The cutoff on this record is discarded; the real one is the last interval end.
    n_interval = int(reader.read(2)[0])
    spline_exp = np.array(reader.read(3))
    xstart = np.zeros(n_interval)
    xend = np.zeros(n_interval)
    coeffs = np.zeros((max(n_interval - 1, 0), 4))
    for j in range(n_interval - 1):
        values = reader.read(6)
        xstart[j], xend[j] = values[0], values[1]
        coeffs[j, :] = values[2:6]
    values = reader.read(8)  # the last interval carries six coefficients
    xstart[-1], xend[-1] = values[0], values[1]
    spline_last = np.array(values[2:8])
    if np.any(np.abs(xend[:-1] - xstart[1:]) > 1.0e-8):
        raise ValueError(f"repulsive spline is not continuous in '{path}'")
    return xstart, xend, coeffs, spline_last, spline_exp, float(xend[-1])


def read_skf(path, homonuclear):
    """
    Parse one .skf file.

    Parameters
    ----------
    path : str
        Path of the ``<A>-<B>.skf`` file.
    homonuclear : bool
        True when ``A == B``. DFTB+ takes this from the species pair, not from the file
        (``parser.F90:3648``): a heteronuclear file read as homonuclear silently shifts
        every integral row by one grid point.

    Returns
    -------
    SkfData
        The parsed file, in Hartree atomic units.
    """

    with open(path, errors="replace") as handle:
        records = handle.read().split("\n")

    extended = records[0][:1] == "@"  # oldskdata.F90:134, the only discriminator
    n_shell = 4 if extended else 3
    n_column = 20 if extended else 10
    reader = _ListDirected(records, 1 if extended else 0)

    # Record 1: grid spacing and point count. Any further field is discarded, exactly as
    # the two-item Fortran read at oldskdata.F90:143 does.
    grid_dist, n_grid_header = reader.read(2)
    n_grid = int(n_grid_header) - 1  # oldskdata.F90:145

    e_onsite = np.zeros(4)
    hubbard_u = np.zeros(4)
    occupation = np.zeros(4)
    spe = 0.0
    mass = 0.0

    if homonuclear:
        # The atomic record runs DESCENDING in angular momentum on disk
        # (Ed Ep Es | SPE | Ud Up Us | fd fp fs), so it is reversed on the way in.
        values = reader.read(3 * n_shell + 1)
        for k in range(n_shell):
            e_onsite[n_shell - 1 - k] = values[k]
        spe = values[n_shell]
        for k in range(n_shell):
            hubbard_u[n_shell - 1 - k] = values[n_shell + 1 + k]
        for k in range(n_shell):
            occupation[n_shell - 1 - k] = values[2 * n_shell + 1 + k]
        values = reader.read(20)  # mass, c2..c9, rcut, then nine ignored fields
        mass = values[0] * AMU_TO_AU
    else:
        values = reader.read(20)  # dummy, c2..c9, rcut, then nine ignored fields
    poly_coeffs = np.array(values[1:9])
    poly_cutoff = values[9]

    h_tab = np.zeros((n_grid, 20))
    s_tab = np.zeros((n_grid, 20))
    for j in range(n_grid):
        row = np.array(reader.read(2 * n_column))
        if extended:
            h_tab[j, :] = row[:20]
            s_tab[j, :] = row[20:]
        else:  # 10 H integrals then 10 S integrals, scattered into the 20 slots
            h_tab[j, _ISK_INTER_OLD] = row[:10]
            s_tab[j, _ISK_INTER_OLD] = row[10:]

    spline = _read_spline_block(records, path)
    if spline is None:  # legal whenever the polynomial repulsive is selected instead
        spline = (
            np.zeros(0),
            np.zeros(0),
            np.zeros((0, 4)),
            np.zeros(6),
            np.zeros(3),
            0.0,
        )
    xstart, xend, coeffs, last, exp_head, cutoff = spline

    return SkfData(
        path=path,
        homonuclear=homonuclear,
        extended=extended,
        grid_dist=grid_dist,
        n_grid=n_grid,
        e_onsite=e_onsite,
        hubbard_u=hubbard_u,
        occupation=occupation,
        spe=spe,
        mass=mass,
        h_tab=h_tab,
        s_tab=s_tab,
        poly_coeffs=poly_coeffs,
        poly_cutoff=poly_cutoff,
        spline_xstart=xstart,
        spline_xend=xend,
        spline_coeffs=coeffs,
        spline_last=last,
        spline_exp=exp_head,
        spline_cutoff=cutoff,
    )


def repulsive_energy(skf, r):
    """Repulsive pair energy in Hartree from the spline block, splinerep.F90:158."""

    if r < 1.0e-2 or r >= skf.spline_cutoff:  # minNeighDist, accuracy.F90:78
        return 0.0
    if r < skf.spline_xstart[0]:
        a1, a2, a3 = skf.spline_exp
        return np.exp(-a1 * r + a2) + a3
    j = int(np.searchsorted(skf.spline_xstart, r, side="right")) - 1
    dr = r - skf.spline_xstart[j]
    if j < len(skf.spline_xstart) - 1:
        c0, c1, c2, c3 = skf.spline_coeffs[j]
        return c0 + dr * (c1 + dr * (c2 + dr * c3))
    c = skf.spline_last
    return c[0] + dr * (c[1] + dr * (c[2] + dr * (c[3] + dr * (c[4] + dr * c[5]))))


@kernel
def spline_repulsive(
    xstart, coeffs, last_coeffs, exp_coeffs, cutoff, n_interval, r, out
):
    """
    Value and first derivative of one repulsive spline, splinerep.F90:136-178.

    ``out`` receives ``(V, dV/dr)`` in Hartree and Hartree/Bohr. Below the first knot
    the spline is replaced by the exponential head; the last interval carries six
    coefficients instead of four.
    """

    out[0] = 0.0
    out[1] = 0.0
    if r < MIN_NEIGH_DIST or r >= cutoff:
        return
    if r < xstart[0]:
        head = math.exp(-exp_coeffs[0] * r + exp_coeffs[1])
        out[0] = head + exp_coeffs[2]
        out[1] = -exp_coeffs[0] * head
        return

    j = 0
    for i in range(n_interval):
        if xstart[i] <= r:
            j = i
    dr = r - xstart[j]
    if j < n_interval - 1:
        c0 = coeffs[j, 0]
        c1 = coeffs[j, 1]
        c2 = coeffs[j, 2]
        c3 = coeffs[j, 3]
        out[0] = c0 + dr * (c1 + dr * (c2 + dr * c3))
        out[1] = c1 + dr * (2.0 * c2 + dr * 3.0 * c3)
    else:
        c0 = last_coeffs[0]
        c1 = last_coeffs[1]
        c2 = last_coeffs[2]
        c3 = last_coeffs[3]
        c4 = last_coeffs[4]
        c5 = last_coeffs[5]
        out[0] = c0 + dr * (c1 + dr * (c2 + dr * (c3 + dr * (c4 + dr * c5))))
        out[1] = c1 + dr * (
            2.0 * c2 + dr * (3.0 * c3 + dr * (4.0 * c4 + dr * 5.0 * c5))
        )


@kernel
def repulsive_pair(sk, coords, atom_species, a, b, pair):
    """
    Repulsive energy of one unordered atom pair and its gradient on atom ``a``.

    Returns ``(energy, gx, gy, gz)``; the gradient on ``b`` is the negative. ``pair`` is
    the two-element ``(V, dV/dr)`` buffer of :func:`spline_repulsive`. The energy sum
    and the force use this one routine, so they cannot drift apart.
    """

    sp_a = atom_species[a]
    sp_b = atom_species[b]
    dx = coords[a, 0] - coords[b, 0]
    dy = coords[a, 1] - coords[b, 1]
    dz = coords[a, 2] - coords[b, 2]
    dist = math.sqrt(dx * dx + dy * dy + dz * dz)
    # twobodyrep.F90:261 indexes [neighbour, owner], which is the A-B table for the
    # half-list entry whose owner is the lower-numbered atom.
    p = sk.pair_index[sp_a, sp_b]
    spline_repulsive(
        sk.rep_xstart[p],
        sk.rep_coeffs[p],
        sk.rep_last[p],
        sk.rep_exp[p],
        sk.rep_cutoff[p],
        sk.rep_n_interval[p],
        dist,
        pair,
    )
    slope = pair[1] / dist
    return pair[0], slope * dx, slope * dy, slope * dz
