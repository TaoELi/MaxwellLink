# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Element data, basis layout, per-pair Slater-Koster tables and the shared constants.

:class:`SlaterKosterSet` is one parameter set restricted to the elements a calculation
uses: shell energies, Hubbard U, occupations, masses, and one interleaved radial table
per ordered species pair. :class:`DFTBSystem` is one geometry on top of it, with the
orbital offsets that define the dense matrix index, and :class:`ShellLayout` is the flat
shell list of that system (which atom owns each shell and orbital, and the reference
occupations). The parameter set hands its arrays to the compiled kernels as one
:data:`SKTables` pack.

The shells an element carries are set by an explicit maximum angular momentum, as in
DFTB+, and are laid out ascending in ``l``. The unit conversions here are DFTB+'s own and
must not be swapped for :mod:`maxwelllink.units`, which differs in the last digits.
"""

import os
from collections import namedtuple

import numpy as np

from .skfiles import SK_MAP, read_skf

# Unit conversions, taken from DFTB+ rather than from :mod:`maxwelllink.units`. The two
# disagree in the last digits (DFTB+ predates CODATA-2018), and every reference value the
# driver is tested against was produced with these, so they must not be swapped for the
# MaxwellLink ones.  constants.F90:28,31,49,55,67,73 and unitconversion.F90:185.
BOHR_TO_AA = 0.529177249
AA_TO_BOHR = 1.0 / BOHR_TO_AA  # DFTB+ builds the reciprocal once and multiplies by it
AU_TO_FS = 0.02418884326505
HARTREE_TO_J = 4.3597441775e-18
AU_TO_COULOMB = 1.60217653e-19
V_PER_M_TO_AU = 1.0e-10 / HARTREE_TO_J * AU_TO_COULOMB * BOHR_TO_AA
# A field given in V/Angstrom reaches DFTB+ as `1e10 * V_m__au` atomic field units; the
# delta kick then uses that number directly as an inverse length, which is dimensionally
# an impulse with an implicit 1 a.u. of time. Reproduce it literally.
V_PER_AA_TO_AU = 1.0e10 * V_PER_M_TO_AU

SHELL_NAMES = ("s", "p", "d", "f")
ANGULAR_MOMENTUM = {"s": 0, "p": 1, "d": 2, "f": 3}

MAX_ORB = 9  # s + p + d, the largest per-atom block
MAX_INTEGRAL = 14  # spd x spd shell pairs, the widest radial table

# Radial Slater-Koster grid, slakoeqgrid.F90:78,90,94 and accuracy.F90:44.
N_INTERPOLATION = 8
N_RIGHT = 4
DELTA_R = 1.0e-5
DIST_FUDGE = 1.0  # length of the polynomial tail in Bohr

# Maximum angular momentum per element, as the DFTB+ input blocks of the 3ob-3-1 and
# mio-1-1 sets declare it. Pass an explicit dictionary to override any of these.
MAX_ANGULAR_MOMENTUM = {
    "H": "s",
    "He": "s",
    "Li": "p",
    "Be": "p",
    "B": "p",
    "C": "p",
    "N": "p",
    "O": "p",
    "F": "p",
    "Ne": "p",
    "Na": "p",
    "Mg": "p",
    "Al": "p",
    "Si": "d",
    "P": "d",
    "S": "d",
    "Cl": "d",
    "K": "p",
    "Ca": "p",
    "Ga": "d",
    "As": "d",
    "Br": "d",
    "Ag": "d",
    "I": "d",
    "Au": "d",
    "Zn": "d",
}

#: Everything a compiled kernel needs from a :class:`SlaterKosterSet`, as plain arrays.
#: numba accepts a namedtuple of arrays on both targets -- and as a CUDA kernel argument
#: with device arrays in the fields -- which is what keeps the kernel signatures short.
SKTables = namedtuple(
    "SKTables",
    "n_orb_species shell_of_orbital on_site_energy hubbard_u occupation mass "
    "ang_shell n_shell pair_index tab_h tab_s tab_n_grid tab_n_integral "
    "tab_grid_dist tab_cutoff rep_xstart rep_coeffs rep_last rep_exp rep_cutoff "
    "rep_n_interval",
)


class SlaterKosterSet:
    """
    One Slater-Koster parameter set restricted to the species of a calculation.

    Parameters
    ----------
    path : str
        Directory holding the ``<A>-<B>.skf`` files.
    species : sequence of str
        Element symbols, in the order the calculation numbers them. The dense matrix
        layout depends on this order only through :class:`DFTBSystem`.
    max_angular_momentum : dict, optional
        Maximum angular momentum per element, as ``"s"``, ``"p"`` or ``"d"``. Missing
        entries fall back to :data:`MAX_ANGULAR_MOMENTUM`.
    separator, suffix : str
        File-name pieces, matching the DFTB+ ``Type2FileNames`` options.

    Attributes
    ----------
    n_shell : numpy.ndarray of int, shape (n_species,)
        Number of shells per species.
    ang_shell : numpy.ndarray of int, shape (n_species, max_shell)
        Angular momentum of each shell, ascending; ``-1`` pads unused slots.
    n_orb_species : numpy.ndarray of int, shape (n_species,)
        Orbitals per atom of that species, ``sum(2 * l + 1)``.
    shell_of_orbital : numpy.ndarray of int, shape (n_species, max_orb)
        Shell index owning each intra-atomic orbital; ``-1`` pads unused slots.
    on_site_energy : numpy.ndarray of float, shape (n_species, max_shell)
        Free-atom shell eigenvalue in Hartree; the H0 on-site diagonal.
    hubbard_u_shell : numpy.ndarray of float, shape (n_species, max_shell)
        Shell-resolved Hubbard U in Hartree, as the .skf file lists it.
    hubbard_u : numpy.ndarray of float, shape (n_species,)
        Single atomic Hubbard U, i.e. the value of the lowest shell. This is what
        non-shell-resolved DFTB (the DFTB+ default) uses.
    occupation : numpy.ndarray of float, shape (n_species, max_shell)
        Free-atom shell occupation in electrons.
    mass : numpy.ndarray of float, shape (n_species,)
        Atomic mass in electron masses.
    pair_index : numpy.ndarray of int, shape (n_species, n_species)
        ``pair_index[a, b]`` indexes the table arrays for the ordered pair ``(a, b)``.
    tab_h, tab_s : numpy.ndarray of float, shape (n_pair, max_grid, max_integral)
        Radial tables, zero-padded. Row ``k`` sits at ``r = (k + 1) * grid_dist``.
    tab_n_grid, tab_n_integral : numpy.ndarray of int, shape (n_pair,)
        Valid rows and valid columns of each pair table.
    tab_grid_dist : numpy.ndarray of float, shape (n_pair,)
        Radial grid spacing in Bohr.
    tab_cutoff : numpy.ndarray of float, shape (n_pair,)
        ``n_grid * grid_dist + 1.0`` Bohr; beyond it every integral is exactly zero.
    tables : SKTables
        The set as one pack of plain arrays, built once, for the compiled kernels.
    """

    #: Length of the polynomial tail DFTB+ appends to every radial table (Bohr).
    dist_fudge = DIST_FUDGE

    def __init__(
        self, path, species, max_angular_momentum=None, separator="-", suffix=".skf"
    ):
        self.path = path
        self.species = tuple(species)
        self.n_species = len(self.species)

        table = dict(MAX_ANGULAR_MOMENTUM)
        if max_angular_momentum is not None:
            table.update(max_angular_momentum)
        self.max_angular_momentum = {s: table[s] for s in self.species}

        # ---- shell layout: ascending l, as MaxAngularMomentum lays it out ----------
        shells = []
        for symbol in self.species:
            l_max = ANGULAR_MOMENTUM[self.max_angular_momentum[symbol]]
            if l_max > 2:
                raise NotImplementedError(
                    f"f shells are not implemented (species '{symbol}')"
                )
            shells.append(list(range(l_max + 1)))
        self.shells = shells

        max_shell = max(len(s) for s in shells)
        self.n_shell = np.array([len(s) for s in shells], dtype=np.int64)
        self.ang_shell = np.full((self.n_species, max_shell), -1, dtype=np.int64)
        for sp, shell_list in enumerate(shells):
            self.ang_shell[sp, : len(shell_list)] = shell_list
        self.n_orb_species = np.array(
            [sum(2 * ang + 1 for ang in s) for s in shells], dtype=np.int64
        )

        max_orb = int(self.n_orb_species.max())
        self.shell_of_orbital = np.full((self.n_species, max_orb), -1, dtype=np.int64)
        for sp, shell_list in enumerate(shells):
            i_orb = 0
            for i_shell, ang in enumerate(shell_list):
                for _ in range(2 * ang + 1):
                    self.shell_of_orbital[sp, i_orb] = i_shell
                    i_orb += 1

        # ---- read every ordered pair of .skf files ---------------------------------
        self._files = {}
        for a, sym_a in enumerate(self.species):
            for b, sym_b in enumerate(self.species):
                name = os.path.join(path, f"{sym_a}{separator}{sym_b}{suffix}")
                self._files[(a, b)] = read_skf(name, sym_a == sym_b)

        # ---- atomic data, taken from the homonuclear file only ---------------------
        self.on_site_energy = np.zeros((self.n_species, max_shell))
        self.hubbard_u_shell = np.zeros((self.n_species, max_shell))
        self.occupation = np.zeros((self.n_species, max_shell))
        self.mass = np.zeros(self.n_species)
        for sp, shell_list in enumerate(shells):
            skf = self._files[(sp, sp)]
            for i_shell, ang in enumerate(shell_list):
                self.on_site_energy[sp, i_shell] = skf.e_onsite[ang]
                self.hubbard_u_shell[sp, i_shell] = skf.hubbard_u[ang]
                self.occupation[sp, i_shell] = skf.occupation[ang]
            self.mass[sp] = skf.mass
        # Non-shell-resolved SCC: every shell inherits the U of the first (s) shell.
        self.hubbard_u = self.hubbard_u_shell[:, 0].copy()

        self._build_pair_tables()
        self._build_repulsive_tables()

        # one pack for the lifetime of the set, built here rather than per kernel call
        self.tables = SKTables(
            self.n_orb_species,
            self.shell_of_orbital,
            self.on_site_energy,
            self.hubbard_u,
            self.occupation,
            self.mass,
            self.ang_shell,
            self.n_shell,
            self.pair_index,
            self.tab_h,
            self.tab_s,
            self.tab_n_grid,
            self.tab_n_integral,
            self.tab_grid_dist,
            self.tab_cutoff,
            self.rep_xstart,
            self.rep_coeffs,
            self.rep_last,
            self.rep_exp,
            self.rep_cutoff,
            self.rep_n_interval,
        )

    def _build_pair_tables(self):
        """Interleave the .skf columns into one radial table per ordered pair."""

        n_pair = self.n_species * self.n_species
        self.pair_index = np.arange(n_pair, dtype=np.int64).reshape(
            self.n_species, self.n_species
        )

        columns = {}
        for a in range(self.n_species):
            for b in range(self.n_species):
                columns[(a, b)] = self._pair_columns(a, b)

        max_grid = max(f.n_grid for f in self._files.values())
        max_integral = max(len(c) for c in columns.values())

        self.tab_h = np.zeros((n_pair, max_grid, max_integral))
        self.tab_s = np.zeros((n_pair, max_grid, max_integral))
        self.tab_n_grid = np.zeros(n_pair, dtype=np.int64)
        self.tab_n_integral = np.zeros(n_pair, dtype=np.int64)
        self.tab_grid_dist = np.zeros(n_pair)
        self.tab_cutoff = np.zeros(n_pair)

        for a in range(self.n_species):
            for b in range(self.n_species):
                p = self.pair_index[a, b]
                # Grid spacing and length always come from the A-B file, for both
                # directions of the pair (parser.F90:3738-3747).
                head = self._files[(a, b)]
                n_grid, grid_dist = head.n_grid, head.grid_dist
                for i, (skf, column) in enumerate(columns[(a, b)]):
                    if skf.n_grid != n_grid or skf.grid_dist != grid_dist:
                        raise ValueError(
                            f"inconsistent SK grids for pair "
                            f"{self.species[a]}-{self.species[b]}"
                        )
                    self.tab_h[p, :n_grid, i] = skf.h_tab[:n_grid, column]
                    self.tab_s[p, :n_grid, i] = skf.s_tab[:n_grid, column]
                self.tab_n_grid[p] = n_grid
                self.tab_n_integral[p] = len(columns[(a, b)])
                self.tab_grid_dist[p] = grid_dist
                self.tab_cutoff[p] = n_grid * grid_dist + self.dist_fudge

    def _build_repulsive_tables(self):
        """Flatten every pair's repulsive spline into padded per-pair arrays."""

        n_pair = self.n_species * self.n_species
        splines = [
            self._files[(a, b)]
            for a in range(self.n_species)
            for b in range(self.n_species)
        ]
        max_interval = max(skf.spline_xstart.shape[0] for skf in splines)

        self.rep_xstart = np.zeros((n_pair, max_interval))
        self.rep_coeffs = np.zeros((n_pair, max(max_interval - 1, 1), 4))
        self.rep_last = np.zeros((n_pair, 6))
        self.rep_exp = np.zeros((n_pair, 3))
        self.rep_cutoff = np.zeros(n_pair)
        self.rep_n_interval = np.zeros(n_pair, dtype=np.int64)
        for p, skf in enumerate(splines):
            n_interval = skf.spline_xstart.shape[0]
            self.rep_xstart[p, :n_interval] = skf.spline_xstart
            self.rep_coeffs[p, : skf.spline_coeffs.shape[0]] = skf.spline_coeffs
            self.rep_last[p] = skf.spline_last
            self.rep_exp[p] = skf.spline_exp
            self.rep_cutoff[p] = skf.spline_cutoff
            self.rep_n_interval[p] = n_interval

    def _pair_columns(self, a, b):
        """List the (file, column) sources of the ordered pair table for (a, b)."""

        picks = []
        for l1 in self.shells[a]:  # outer: species a, the column atom
            for l2 in self.shells[b]:  # inner: species b, the row atom
                # The integral always lives in the file whose FIRST element carries the
                # lower angular momentum, so an A-B d-p block is read from B-A.
                if l1 <= l2:
                    skf, l_min, l_max = self._files[(a, b)], l1, l2
                else:
                    skf, l_min, l_max = self._files[(b, a)], l2, l1
                for m in range(min(l1, l2) + 1):
                    picks.append((skf, SK_MAP[m, l_max, l_min]))
        return picks

    def skf(self, a, b):
        """Raw parsed .skf file of the ordered species pair ``(a, b)``."""

        return self._files[(a, b)]


_SK_CACHE = {}


def load_sk_set(path, species, max_angular_momentum=None):
    """Read one parameter set, reusing it across every driver that shares it.

    Parsing a whole set is a few hundred file reads, so a batch of drivers on the same
    elements must not each pay for it.
    """

    key = (path, tuple(species), tuple(sorted((max_angular_momentum or {}).items())))
    if key not in _SK_CACHE:
        _SK_CACHE[key] = SlaterKosterSet(path, species, max_angular_momentum)
    return _SK_CACHE[key]


class DFTBSystem:
    """
    One geometry laid out on a parameter set: species map, orbital offsets, coordinates.

    Parameters
    ----------
    elements : sequence of str
        Element symbol of every atom, in input order.
    positions : array-like of float, shape (n_atom, 3)
        Atomic positions.
    sk_set : SlaterKosterSet
        Parameter set whose ``species`` tuple defines the species numbering.
    units : {"angstrom", "bohr"}
        Units of ``positions``. Angstrom values are converted with DFTB+'s own
        ``AA__Bohr = 1 / 0.529177249``.

    Attributes
    ----------
    coords : numpy.ndarray of float, shape (n_atom, 3)
        Positions in Bohr; the real-time drivers move them in place.
    atom_species : numpy.ndarray of int, shape (n_atom,)
        Species index of every atom.
    atom_offset : numpy.ndarray of int, shape (n_atom + 1,)
        First dense-matrix row/column of every atom; ``atom_offset[-1]`` is ``n_orb``.
    n_atom, n_orb : int
        Number of atoms and the dimension of the dense H0 and S matrices.
    masses : numpy.ndarray of float, shape (n_atom,)
        Atomic masses in electron masses.
    tables : SKTables
        The parameter set's kernel pack, ``sk_set.tables``.
    layout : ShellLayout
        The non-shell-resolved shell layout (the DFTB+ default), built on first use.
    """

    def __init__(self, elements, positions, sk_set, units="angstrom"):
        self.elements = tuple(elements)
        self.sk_set = sk_set
        self.tables = sk_set.tables

        coords = np.asarray(positions, dtype=float).reshape(len(self.elements), 3)
        if units == "angstrom":
            coords = coords * AA_TO_BOHR
        elif units != "bohr":
            raise ValueError("units must be 'angstrom' or 'bohr'")
        self.coords = np.ascontiguousarray(coords)

        lookup = {symbol: i for i, symbol in enumerate(sk_set.species)}
        self.atom_species = np.array(
            [lookup[symbol] for symbol in self.elements], dtype=np.int64
        )
        n_orb_atom = sk_set.n_orb_species[self.atom_species]
        self.atom_offset = np.zeros(len(self.elements) + 1, dtype=np.int64)
        self.atom_offset[1:] = np.cumsum(n_orb_atom)
        self.n_atom = len(self.elements)
        self.n_orb = int(self.atom_offset[-1])
        self.masses = np.ascontiguousarray(sk_set.mass[self.atom_species])
        self._layout = None

    @property
    def layout(self):
        """The :class:`ShellLayout` of this system with one Hubbard U per atom."""

        if self._layout is None:
            self._layout = ShellLayout(self)
        return self._layout

    def n_electrons(self):
        """Total number of valence electrons of the neutral system."""

        total = 0.0
        for sp in self.atom_species:
            total += self.sk_set.occupation[sp, : self.sk_set.n_shell[sp]].sum()
        return total


class ShellLayout:
    """
    Flat shell list of one system: which atom each shell belongs to, its U, its
    reference occupation, and the map from dense orbital index to shell index.

    Parameters
    ----------
    system : DFTBSystem
        Geometry and basis layout.
    shell_resolved : bool
        Whether every shell keeps its own Hubbard U. DFTB+ defaults to ``False``
        (``ShellResolvedSCC = No``, parser.F90:1375), in which case every shell of a
        species inherits the U of its s shell (parser.F90:3699). The difference is
        invisible in 3ob, whose files list one U three times, but Ag and H in mio-1-1
        really do carry three different values.

    Attributes
    ----------
    n_shell : int
        Total number of shells in the system.
    shell_atom : numpy.ndarray of int, shape (n_shell,)
        Atom index owning each shell.
    shell_u : numpy.ndarray of float, shape (n_shell,)
        Hubbard U of each shell in Hartree.
    orb_shell : numpy.ndarray of int, shape (n_orb,)
        Shell index owning each dense orbital.
    orb_atom : numpy.ndarray of int, shape (n_orb,)
        Atom index owning each dense orbital.
    q0_orb : numpy.ndarray of float, shape (n_orb,)
        Free-atom reference population of each orbital, the shell occupation spread
        evenly over its ``2 * l + 1`` members (sccinit.F90:132).
    """

    def __init__(self, system, shell_resolved=False):
        sk_set = system.sk_set
        shell_atom = []
        shell_u = []
        orb_shell = []
        orb_atom = []
        q0_orb = []
        for atom in range(system.n_atom):
            sp = system.atom_species[atom]
            for i_shell in range(sk_set.n_shell[sp]):
                ang = sk_set.ang_shell[sp, i_shell]
                shell_atom.append(atom)
                if shell_resolved:
                    shell_u.append(sk_set.hubbard_u_shell[sp, i_shell])
                else:
                    shell_u.append(sk_set.hubbard_u[sp])
                for _ in range(2 * ang + 1):
                    orb_shell.append(len(shell_atom) - 1)
                    orb_atom.append(atom)
                    q0_orb.append(sk_set.occupation[sp, i_shell] / (2 * ang + 1))
        self.n_shell = len(shell_atom)
        self.shell_atom = np.array(shell_atom, dtype=np.int64)
        self.shell_u = np.array(shell_u)
        self.orb_shell = np.array(orb_shell, dtype=np.int64)
        self.orb_atom = np.array(orb_atom, dtype=np.int64)
        self.q0_orb = np.array(q0_orb)
