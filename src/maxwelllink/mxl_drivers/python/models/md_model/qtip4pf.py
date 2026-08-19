# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
q-TIP4P/F flexible-water force field.

Implements the q-TIP4P/F water model of Habershon, Markland, and Manolopoulos
[J. Chem. Phys. 131, 024501 (2009)] as a :class:`DummyForceField` subclass.
"""

import os

import numpy as np
from maxwelllink.units import AMU_TO_AU, BOHR_PER_ANG

try:
    from .force_field import DummyForceField
except (ImportError, ValueError):  # allow running as a stand-alone script
    from force_field import DummyForceField

# Bundled equilibrated 216-molecule bulk-water geometry (already in atomic units).
# Used as the zero-config default so a fully default water force field is production-ready.
_WATER_216_XYZ = os.path.join(os.path.dirname(__file__), "data", "water_216.xyz")
_WATER_216_CACHE = None

# ---------------------------------------------------------#
# q-TIP4P/F force-field parameters (atomic units)          #
# ---------------------------------------------------------#

_ALPHA = 0.73612  # M-site mixing: r_M = alpha * r_O + alpha2 * (r_H1 + r_H2)
_ALPHA2 = 0.5 * (1.0 - _ALPHA)
_Q_O = -1.1128  # charge carried by the M-site (a.u.)
_Q_H = 0.5 * 1.1128  # charge on each hydrogen (a.u.)
_OO_SIG = 5.96946  # Lennard-Jones sigma for O-O (Bohr)
_OO_EPS = 2.95147e-4  # Lennard-Jones epsilon for O-O (Hartree)
_THETA0 = 107.4 * (np.pi / 180.0)  # equilibrium H-O-H angle (radians)
_REOH = 1.78  # equilibrium O-H length (Bohr)
_DE = 0.185  # Morse well depth prefactor (Hartree)
_DEB = 0.07  # harmonic bend force constant (Hartree / rad^2)
_ALP = 1.21  # Morse range parameter (1 / Bohr)
_RCUT_DEFAULT = 9.0 * BOHR_PER_ANG  # 9 Angstrom real-space cutoff

_M_O = 15.9994 * AMU_TO_AU
_M_H = 1.00794 * AMU_TO_AU

# quartic-Morse expansion coefficients (see reference intra_morse_harm)
_F1 = 7.0 / 12.0
_F2 = 7.0 / 3.0

# Abramowitz-Stegun 7.1.26 erfc polynomial coefficients.
_ERFC_P = 3.0525860
_ERFC_A = (
    0.254829592,
    -0.284496736,
    1.421413741,
    -1.453152027,
    1.061405429,
)


def _erfc(x):
    """
    Complementary error function via the Abramowitz-Stegun 7.1.26 approximation.

    Parameters
    ----------
    x : numpy.ndarray of float
        Non-negative argument(s).

    Returns
    -------
    numpy.ndarray of float
        Approximation to ``erfc(x)``, with absolute error below about ``1.5e-7``.
    """

    a1, a2, a3, a4, a5 = _ERFC_A
    t = _ERFC_P / (_ERFC_P + x)
    return np.exp(-x * x) * t * (a1 + t * (a2 + t * (a3 + t * (a4 + t * a5))))


def _load_water_216():
    """
    Load the bundled 216-molecule bulk-water geometry.

    The file ``water_216.xyz`` is copied from i-pi in atomic units (Bohr),
    ordered ``[O, H, H, ...]`` inside a cubic cell. The parsed arrays are
    cached, so repeated calls are cheap.

    Returns
    -------
    positions : numpy.ndarray of float, shape (648, 3)
        Atomic positions in atomic units (Bohr).
    box : numpy.ndarray of float, shape (3,)
        Cubic box lengths in atomic units (Bohr).
    """

    global _WATER_216_CACHE
    if _WATER_216_CACHE is None:
        with open(_WATER_216_XYZ) as handle:
            lines = handle.readlines()
        box_length = float(lines[1].split()[2])  # "# CELL(abcABC): a a a ..."
        coords = [
            [float(c) for c in line.split()[1:4]]
            for line in lines[2:]
            if len(line.split()) >= 4
        ]
        _WATER_216_CACHE = (
            np.array(coords, dtype=float),
            np.full(3, box_length, dtype=float),
        )
    positions, box = _WATER_216_CACHE
    return positions.copy(), box.copy()


class QTIP4PFForceField(DummyForceField):
    """
    q-TIP4P/F flexible-water force field.

    Implements the q-TIP4P/F water model of Habershon, Markland, and Manolopoulos
    [J. Chem. Phys. 131, 024501 (2009)].

    Atoms are stored in the canonical order ``[O, H, H, O, H, H, ...]``.

    Notes
    -----
    Electrostatics use a direct sum for an isolated molecule or cluster
    (``box is None``) and an Ewald sum for a periodic box. With no arguments the force
    field loads an equilibrated 216-molecule bulk-water box, so a fully
    default instance is ready for production runs.
    """

    name = "qtip4pf"

    def __init__(
        self, n_molecules=216, positions=None, box=None, rcut=None, ewald_wrcut=None
    ):
        """
        Initialize the q-TIP4P/F force field and build the initial geometry.

        Parameters
        ----------
        n_molecules : int, default: 216
            Number of water molecules. Ignored when ``positions`` is given. With the
            default value 216 and no ``positions``/``box``, the bundled
            equilibrated bulk-water box (``water_216.xyz``, atomic units) is loaded so
            that a fully default force field is production-ready.
        positions : array-like of float, shape (3 * n_molecules, 3), optional
            Initial atomic positions in Bohr, ordered ``[O, H, H, ...]``. If ``None`` a
            default geometry is built with a single equilibrium molecule for
            ``n_molecules == 1``, otherwise a cubic lattice inside ``box``.
        box : float or array-like of float, shape (3,), optional
            Periodic box lengths in Bohr. ``None`` treats the system as a finite
            cluster.
        rcut : float, optional
            Real-space cutoff in Bohr for the periodic LJ and Coulomb sums. Defaults to
            9 Angstrom.
        ewald_wrcut : float, optional
            Real-space cutoff in Bohr for the Ewald sum (also sets
            :math:`\\alpha = \\pi / \\mathrm{wrcut}` and
            ``kmax = int(alpha * max(box))``). Defaults to ``rshort * min(0.5, 1.2 * n_atoms**(-1/6))``.
        """

        # zero-config productive default: the equilibrated 216-molecule water box
        if positions is None and box is None and int(n_molecules) == 216:
            positions, box = _load_water_216()

        super().__init__(
            n_molecules=n_molecules,
            positions=positions,
            box=box,
            rcut=_RCUT_DEFAULT if rcut is None else rcut,
            ewald_wrcut=ewald_wrcut,
        )

        # the base has stored self.positions and self.na; water is triatomic [O, H, H]
        if self.na % 3 != 0:
            raise ValueError(
                "positions must have shape (3 * n_molecules, 3) in [O, H, H, ...]"
            )
        self.n_molecules = self.na // 3
        # per-atom masses, ordered [O, H, H, O, H, H, ...]
        self.masses = np.tile(np.array([[_M_O], [_M_H], [_M_H]]), (self.n_molecules, 1))

    # ------------------------------------- geometry --------------------------------
    @staticmethod
    def _single_molecule():
        """
        Build one equilibrium water molecule with the oxygen at the origin.

        Returns
        -------
        numpy.ndarray of float, shape (3, 3)
            Positions of ``[O, H, H]`` in atomic units (Bohr).
        """

        half = 0.5 * _THETA0
        o = np.array([0.0, 0.0, 0.0])
        h1 = _REOH * np.array([np.cos(half), np.sin(half), 0.0])
        h2 = _REOH * np.array([np.cos(half), -np.sin(half), 0.0])
        return np.vstack([o, h1, h2])

    def _build_default_geometry(self, n_molecules, box):
        """
        Build a default starting geometry for ``n_molecules`` water molecules.

        A single molecule is placed at the origin; several molecules are arranged on
        the smallest cubic lattice that holds them inside the periodic box.

        Parameters
        ----------
        n_molecules : int
            Number of water molecules to place.
        box : float or array-like of float, shape (3,), or None
            Periodic box lengths in Bohr. Required when ``n_molecules > 1``.

        Returns
        -------
        numpy.ndarray of float, shape (3 * n_molecules, 3)
            Atomic positions ordered ``[O, H, H, ...]`` in atomic units.

        Raises
        ------
        ValueError
            If more than one molecule is requested without a box.
        """

        mol = self._single_molecule()
        if n_molecules == 1:
            return mol
        if box is None:
            raise ValueError("A box is required to build more than one molecule.")
        length = float(np.broadcast_to(np.asarray(box, dtype=float), (3,))[0])
        # smallest cubic grid that holds all molecules
        n_side = int(np.ceil(n_molecules ** (1.0 / 3.0)))
        spacing = length / n_side
        positions = []
        count = 0
        for ix in range(n_side):
            for iy in range(n_side):
                for iz in range(n_side):
                    if count >= n_molecules:
                        break
                    shift = spacing * (np.array([ix, iy, iz]) + 0.5)
                    positions.append(mol + shift)
                    count += 1
        return np.vstack(positions)

    # ------------------------------ compiled kernels -------------------------------
    has_compiled_kernels = True

    #: one molecule's atoms, in the order the geometry lists them
    molecule_symbols = ("O", "H", "H")

    #: energy terms reported next to the potential: the two bond stretches and the bend
    term_names = ("stretch_au", "bend_au")

    def build_force_kernels(self, xp, threads_per_block=128):
        """
        Return this force field's compiled kernels from :mod:`kernels_qtip4pf`.

        Parameters
        ----------
        xp : module
            Array module: ``numpy`` for the CPU path, ``cupy`` for the GPU path.
        threads_per_block : int, default: 128
            CUDA block size, ignored on the CPU path.

        Returns
        -------
        kernels_qtip4pf.QTIP4PFForceKernels
            The compiled force evaluator.
        """

        from .kernels_qtip4pf import QTIP4PFForceKernels

        return QTIP4PFForceKernels(self, xp, threads_per_block)

    # ---------------------------------force evaluation ----------------------------
    def compute(self, x, efield, terms=None):
        """
        Evaluate the total q-TIP4P/F force and potential energy.

        The force combines the intramolecular stretch/bend, the oxygen-oxygen
        Lennard-Jones term, the intermolecular Coulomb term, and the external
        light-matter force :math:`q_i \\mathbf{E}` on each charge site; the virtual
        M-site force is redistributed onto O, H, H by the chain rule.

        Parameters
        ----------
        x : numpy.ndarray of float, shape (na, 3)
            Atomic positions in atomic units (Bohr).
        efield : numpy.ndarray of float, shape (3,)
            Effective electric field ``[E_x, E_y, E_z]`` in atomic units.
        terms : numpy.ndarray of float, shape (2,), optional
            Filled with the stretch and the bend energy when given.

        Returns
        -------
        forces : numpy.ndarray of float, shape (na, 3)
            Total force on each atom in atomic units.
        potential : float
            Mechanical potential energy in atomic units (Hartree).
        """

        x3 = x.reshape(self.n_molecules, 3, 3)  # [:, 0]=O, [:, 1]=H1, [:, 2]=H2
        forces = np.zeros((self.n_molecules, 3, 3))
        potential = 0.0

        # (1) intramolecular quartic-Morse stretch + harmonic bend
        f_intra, v_stretch, v_bend = self._intramolecular(x3)
        forces += f_intra
        potential += v_stretch + v_bend
        if terms is not None:
            terms[:] = (v_stretch, v_bend)

        # (2) Lennard-Jones between oxygens
        if self.n_molecules > 1:
            f_lj, v_lj = self._lennard_jones(x3[:, 0, :])
            forces[:, 0, :] += f_lj
            potential += v_lj

        # (3) Coulomb between charge sites (+ external field) on M-site and H atoms
        f_sites, v_coul = self._coulomb_site_forces(x3)
        potential += v_coul
        # external uniform field pushes each charge site: F = q * E
        f_sites[:, 0, :] += _Q_O * efield  # M-site
        f_sites[:, 1, :] += _Q_H * efield  # H1
        f_sites[:, 2, :] += _Q_H * efield  # H2
        # redistribute the virtual M-site force onto the real atoms (chain rule)
        forces[:, 0, :] += _ALPHA * f_sites[:, 0, :]
        forces[:, 1, :] += f_sites[:, 1, :] + _ALPHA2 * f_sites[:, 0, :]
        forces[:, 2, :] += f_sites[:, 2, :] + _ALPHA2 * f_sites[:, 0, :]

        return forces.reshape(self.na, 3), potential

    def _intramolecular(self, x3):
        """
        Evaluate the intramolecular quartic-Morse stretch and harmonic bend.

        Vectorized over all molecules.

        Parameters
        ----------
        x3 : numpy.ndarray of float, shape (n_molecules, 3, 3)
            Atomic positions grouped per molecule as ``[O, H1, H2]`` in Bohr.

        Returns
        -------
        forces : numpy.ndarray of float, shape (n_molecules, 3, 3)
            Intramolecular force on each atom in atomic units.
        stretch : float
            Total stretch energy in atomic units (Hartree).
        bend : float
            Total bend energy in atomic units (Hartree).
        """

        o = x3[:, 0, :]
        h1 = x3[:, 1, :]
        h2 = x3[:, 2, :]

        d1 = h1 - o  # O -> H1
        d2 = h2 - o  # O -> H2
        d3 = h2 - h1  # H1 -> H2
        r1 = np.linalg.norm(d1, axis=1)
        r2 = np.linalg.norm(d2, axis=1)
        r3 = np.linalg.norm(d3, axis=1)
        e1 = d1 / r1[:, None]
        e2 = d2 / r2[:, None]
        e3 = d3 / r3[:, None]

        # --- stretches (displacement from equilibrium O-H length) ---
        s1 = r1 - _REOH
        s2 = r2 - _REOH
        alp2 = _ALP * _ALP
        alp3 = alp2 * _ALP
        alp4 = alp3 * _ALP
        v_stretch = _DE * (
            alp2 * s1**2 - alp3 * s1**3 + _F1 * alp4 * s1**4
        ) + _DE * (alp2 * s2**2 - alp3 * s2**3 + _F1 * alp4 * s2**4)
        # dV/d(r) along each O-H bond
        a1 = _DE * (2.0 * alp2 * s1 - 3.0 * alp3 * s1**2 + _F2 * alp4 * s1**3)
        a2 = _DE * (2.0 * alp2 * s2 - 3.0 * alp3 * s2**2 + _F2 * alp4 * s2**3)

        # --- bend (angle from the law of cosines) ---
        u = r1**2 + r2**2 - r3**2
        vv = 2.0 * r1 * r2
        v2 = 1.0 / (vv * vv)
        arg = np.clip(u / vv, -1.0, 1.0)
        ang = np.arccos(arg)
        dang = ang - _THETA0
        darg = -1.0 / np.sqrt(1.0 - arg * arg)
        dtheta_dr1 = darg * ((2.0 * r1) * vv - (2.0 * r2) * u) * v2
        dtheta_dr2 = darg * ((2.0 * r2) * vv - (2.0 * r1) * u) * v2
        dtheta_dr3 = darg * ((-2.0 * r3) * vv) * v2
        v_bend = _DEB * dang**2
        a3 = 2.0 * _DEB * dang

        # total dV/d(bond length) for each of the three internal coordinates
        dvdr1 = a1 + a3 * dtheta_dr1
        dvdr2 = a2 + a3 * dtheta_dr2
        dvdr3 = a3 * dtheta_dr3
        g1 = dvdr1[:, None] * e1  # gradient contribution along O-H1
        g2 = dvdr2[:, None] * e2  # gradient contribution along O-H2
        g3 = dvdr3[:, None] * e3  # gradient contribution along H1-H2

        # forces = -gradient, distributed to the three atoms
        forces = np.zeros_like(x3)
        forces[:, 0, :] = g1 + g2  # on O
        forces[:, 1, :] = -g1 + g3  # on H1
        forces[:, 2, :] = -g2 - g3  # on H2

        return forces, float(np.sum(v_stretch)), float(np.sum(v_bend))

    def _lennard_jones(self, xo):
        """
        Evaluate the oxygen-oxygen Lennard-Jones energy and force.

        Uses the minimum-image convention with a cutoff clamped to half the shortest
        box side; the potential is shifted to zero at the cutoff so the energy is
        continuous.

        Parameters
        ----------
        xo : numpy.ndarray of float, shape (n_molecules, 3)
            Oxygen positions in atomic units (Bohr).

        Returns
        -------
        forces : numpy.ndarray of float, shape (n_molecules, 3)
            Lennard-Jones force on each oxygen in atomic units.
        potential : float
            Lennard-Jones potential energy in atomic units (Hartree).
        """

        # all pairwise oxygen displacements r_i - r_j
        dvec = xo[:, None, :] - xo[None, :, :]  # (nm, nm, 3)
        dvec = self._minimum_image(dvec)
        r2 = np.sum(dvec**2, axis=2)  # (nm, nm)
        np.fill_diagonal(r2, np.inf)  # exclude self-interaction
        v_shift = 0.0  # energy offset so V is continuous at the cutoff (periodic only)
        if self.box is not None:
            # keep the cutoff within the minimum-image limit (box / 2)
            cut = min(self.rcut, 0.5 * float(np.min(self.box)))
            r2 = np.where(r2 < cut**2, r2, np.inf)
            src6 = (_OO_SIG / cut) ** 6
            v_shift = src6 * (src6 - 1.0)  # LJ value at the cutoff (per pair)

        inv_r2 = 1.0 / r2
        sr2 = (_OO_SIG**2) * inv_r2
        sr6 = sr2**3
        # energy (shifted to zero at the cutoff); 0.5 removes the i,j double counting
        pair_energy = np.where(np.isfinite(r2), sr6 * (sr6 - 1.0) - v_shift, 0.0)
        potential = 0.5 * 4.0 * _OO_EPS * np.sum(pair_energy)
        # force magnitude / r^2 on i from j (positive => repulsive along +dvec)
        coeff = 48.0 * _OO_EPS * sr6 * (sr6 - 0.5) * inv_r2  # (nm, nm)
        forces = np.sum(coeff[:, :, None] * dvec, axis=1)  # (nm, 3)
        return forces, float(potential)

    def _coulomb_site_forces(self, x3):
        """
        Evaluate the intermolecular Coulomb energy and force on the charge sites.

        The charge sites are the virtual M-site and the two hydrogens of each molecule.
        Intramolecular pairs are excluded (they are folded into the bonded terms in
        q-TIP4P/F). A finite cluster (``box is None``) uses an exact direct sum; a
        periodic box uses an Ewald sum.

        Parameters
        ----------
        x3 : numpy.ndarray of float, shape (n_molecules, 3, 3)
            Atomic positions grouped per molecule as ``[O, H1, H2]`` in Bohr.

        Returns
        -------
        forces : numpy.ndarray of float, shape (n_molecules, 3, 3)
            Coulomb force on the three charge sites ``[M, H1, H2]`` of each molecule in
            atomic units.
        potential : float
            Intermolecular Coulomb potential energy in atomic units (Hartree).
        """

        # charge-site positions: M-site on the bisector, plus the two hydrogens
        m_site = _ALPHA * x3[:, 0, :] + _ALPHA2 * (x3[:, 1, :] + x3[:, 2, :])
        sites = np.stack([m_site, x3[:, 1, :], x3[:, 2, :]], axis=1)  # (nm, 3, 3)
        charges = np.array([_Q_O, _Q_H, _Q_H])

        if self.n_molecules < 2:
            # a single molecule has no intermolecular Coulomb (intramolecular excluded)
            return np.zeros_like(sites), 0.0

        sp = sites.reshape(self.na, 3)  # flatten sites, same layout as atoms
        q = np.tile(charges, self.n_molecules)
        mol_id = np.repeat(np.arange(self.n_molecules), 3)

        if self.box is None:
            forces, potential = self._coulomb_direct(sp, q, mol_id)
        else:
            forces, potential = self._coulomb_ewald(sp, q, mol_id)
        return forces.reshape(self.n_molecules, 3, 3), potential

    def _coulomb_direct(self, sp, q, mol_id):
        """
        Evaluate the exact ``1/r`` Coulomb sum over a finite cluster.

        Intramolecular pairs are excluded.

        Parameters
        ----------
        sp : numpy.ndarray of float, shape (ns, 3)
            Charge-site positions in atomic units (Bohr).
        q : numpy.ndarray of float, shape (ns,)
            Site charges in atomic units.
        mol_id : numpy.ndarray of int, shape (ns,)
            Index of the molecule each site belongs to.

        Returns
        -------
        forces : numpy.ndarray of float, shape (ns, 3)
            Coulomb force on each charge site in atomic units.
        potential : float
            Coulomb potential energy in atomic units (Hartree).
        """

        dvec = sp[:, None, :] - sp[None, :, :]  # (ns, ns, 3)
        r2 = np.sum(dvec**2, axis=2)
        np.fill_diagonal(r2, np.inf)
        same_mol = mol_id[:, None] == mol_id[None, :]
        r2 = np.where(same_mol, np.inf, r2)  # exclude intramolecular pairs

        inv_r = 1.0 / np.sqrt(r2)
        qq = q[:, None] * q[None, :]
        potential = 0.5 * np.sum(qq * inv_r)  # 0.5 for the double-counted pairs
        coeff = qq * inv_r / r2  # = q_i q_j / r^3
        forces = np.sum(coeff[:, :, None] * dvec, axis=1)  # (ns, 3)
        return forces, float(potential)

    def _coulomb_ewald(self, sp, q, mol_id):
        """
        Evaluate the Ewald Coulomb sum on the periodic charge sites.

        The real-space term uses ``erfc`` (the Abramowitz-Stegun polynomial) for
        pairs in different molecules and ``erfc - 1`` for pairs in the same molecule;
        the latter both removes the bare intramolecular Coulomb and supplies the
        reciprocal-space exclusion correction, exactly as in the reference code.

        Parameters
        ----------
        sp : numpy.ndarray of float, shape (ns, 3)
            Charge-site positions in atomic units (Bohr).
        q : numpy.ndarray of float, shape (ns,)
            Site charges in atomic units.
        mol_id : numpy.ndarray of int, shape (ns,)
            Index of the molecule each site belongs to.

        Returns
        -------
        forces : numpy.ndarray of float, shape (ns, 3)
            Coulomb force on each charge site in atomic units.
        potential : float
            Ewald Coulomb potential energy (real + reciprocal + self) in atomic units.
        """

        box = self.box
        volume = float(box[0] * box[1] * box[2])
        # Ewald parameters. The default real-space cutoff is rshort * min(0.5, 1.2 * n_atoms**(-1/6));
        # a caller-supplied value (e.g. to match another reference) overrides it.
        if self.ewald_wrcut is None:
            rshort = float(np.min(box))
            wrcut = rshort * min(0.5, 1.2 * self.na ** (-1.0 / 6.0))
        else:
            wrcut = self.ewald_wrcut
        alpha = np.pi / wrcut
        kmax = int(alpha * float(np.max(box)))
        rkmax2 = (2.0 * np.pi * alpha) ** 2

        # ---------------- real space (minimum image, cutoff wrcut) ----------------
        dvec = self._minimum_image(sp[:, None, :] - sp[None, :, :])  # (ns, ns, 3)
        r2 = np.sum(dvec**2, axis=2)
        np.fill_diagonal(r2, np.inf)
        r2 = np.where(r2 < wrcut**2, r2, np.inf)
        r = np.sqrt(r2)
        same_mol = mol_id[:, None] == mol_id[None, :]
        # g(r) = erfc(alpha r), minus 1 inside the same molecule (exclusion correction)
        g = _erfc(alpha * r)
        g = np.where(same_mol & np.isfinite(r), g - 1.0, g)
        qq = q[:, None] * q[None, :]
        v_real = 0.5 * np.sum(qq * g / r)
        # force prefactor: q_i q_j [ g/r + (2 alpha/sqrt(pi)) exp(-alpha^2 r^2) ] / r^2
        two_a_rootpi = 2.0 * alpha / np.sqrt(np.pi)
        coeff = qq * (g / r + two_a_rootpi * np.exp(-(alpha**2) * r2)) / r2
        f_real = np.sum(coeff[:, :, None] * dvec, axis=1)  # (ns, 3)

        # ---------------- reciprocal space (structure factor sum) ----------------
        krange = np.arange(-kmax, kmax + 1)
        kx, ky, kz = np.meshgrid(krange, krange, krange, indexing="ij")
        kvec = (2.0 * np.pi) * np.stack(
            [kx.ravel() / box[0], ky.ravel() / box[1], kz.ravel() / box[2]], axis=1
        )
        k2 = np.sum(kvec**2, axis=1)
        keep = (k2 > 0.0) & (k2 < rkmax2)
        kvec = kvec[keep]  # (nk, 3)
        k2 = k2[keep]
        a_k = (2.0 * np.pi / volume) * np.exp(-k2 / (4.0 * alpha**2)) / k2  # (nk,)

        phase = kvec @ sp.T  # (nk, ns)
        cos_kj = np.cos(phase)
        sin_kj = np.sin(phase)
        c_re = cos_kj @ q  # (nk,)  real part of the structure factor
        c_im = sin_kj @ q  # (nk,)  imaginary part
        v_recip = float(np.sum(a_k * (c_re**2 + c_im**2)))
        # force on site j: -2 q_j sum_k A_k k [ Im(S) cos - Re(S) sin ]
        w = a_k[:, None] * (c_im[:, None] * cos_kj - c_re[:, None] * sin_kj)  # (nk, ns)
        f_recip = -2.0 * q[:, None] * (w.T @ kvec)  # (ns, 3)

        # ---------------- self-energy correction ----------------
        v_self = -(alpha / np.sqrt(np.pi)) * float(np.sum(q**2))

        potential = float(v_real) + v_recip + v_self
        forces = f_real + f_recip
        return forces, potential

    # ------------------------------- dipole and its derivative ---------------------
    def dipole(self, x):
        """
        Compute the molecular dipole moment from the charge-site positions.

        Parameters
        ----------
        x : numpy.ndarray of float, shape (na, 3)
            Atomic positions in atomic units (Bohr).

        Returns
        -------
        numpy.ndarray of float, shape (3,)
            The total dipole :math:`\\boldsymbol{\\mu} = \\sum_i q_i \\mathbf{r}_i`
            over the M-site and hydrogen charges, in atomic units.
        """

        x3 = x.reshape(self.n_molecules, 3, 3)
        m_site = _ALPHA * x3[:, 0, :] + _ALPHA2 * (x3[:, 1, :] + x3[:, 2, :])
        return _Q_O * m_site.sum(axis=0) + _Q_H * (x3[:, 1, :] + x3[:, 2, :]).sum(
            axis=0
        )

    def dipole_velocity(self, v):
        """
        Compute the time derivative of the molecular dipole moment.

        Parameters
        ----------
        v : numpy.ndarray of float, shape (na, 3)
            Atomic velocities in atomic units.

        Returns
        -------
        numpy.ndarray of float, shape (3,)
            The dipole velocity :math:`\\mathrm{d}\\boldsymbol{\\mu}/\\mathrm{d}t =
            \\sum_i q_i \\mathbf{v}_i` over the charge sites, in atomic units.
        """

        v3 = v.reshape(self.n_molecules, 3, 3)
        v_m = _ALPHA * v3[:, 0, :] + _ALPHA2 * (v3[:, 1, :] + v3[:, 2, :])
        return _Q_O * v_m.sum(axis=0) + _Q_H * (v3[:, 1, :] + v3[:, 2, :]).sum(axis=0)
