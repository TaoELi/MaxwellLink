# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Flexible CH4 force field (ch4compass).

Implements the COMPASS class2 model of methane [H. Sun, J. Phys. Chem. B 102, 7338 (1998)].
"""

import os

import numpy as np
from maxwelllink.units import AMU_TO_AU, BOHR_PER_ANG, EV_TO_AU

try:
    from .force_field import DummyForceField
except (ImportError, ValueError):  # allow running as a stand-alone script
    from force_field import DummyForceField

# Bundled 100-molecule bulk-CH4 geometry (in atomic units)
_CH4_100_XYZ = os.path.join(os.path.dirname(__file__), "data", "ch4_100.xyz")
_CH4_100_CACHE = None

# ---------------------------------------------------------#
# ch4compass force-field parameters (atomic units)         #
# ---------------------------------------------------------#
# The LAMMPS reference is written in metal units (eV, Angstrom), which is converted to atomic units below.

_Q_C = -0.24  # charge on carbon (a.u.)
_Q_H = 0.06  # charge on each hydrogen (a.u.); a CH4 molecule is net neutral

# 9-6 Lennard-Jones pair parameters, V = eps [2 (r0/r)^9 - 3 (r0/r)^6]
_CC_EPS = 0.0026885751491437706 * EV_TO_AU  # C-C well depth (Hartree)
_CC_R0 = 3.8540 * BOHR_PER_ANG  # C-C minimum position r0 (Bohr)
_CH_EPS = 0.0016375337260730242 * EV_TO_AU  # C-H well depth (Hartree)
_CH_R0 = 3.366 * BOHR_PER_ANG  # C-H minimum position r0 (Bohr)
_HH_EPS = 0.0009973746521017215 * EV_TO_AU  # H-H well depth (Hartree)
_HH_R0 = 2.8780 * BOHR_PER_ANG  # H-H minimum position r0 (Bohr)
_RCUT_DEFAULT = 10.0 * BOHR_PER_ANG  # 10 Angstrom real-space cutoff (LAMMPS)

# class2 bond: V = K2 (r - r0)^2 + K3 (r - r0)^3 + K4 (r - r0)^4
_RCH = 1.101 * BOHR_PER_ANG  # equilibrium C-H length (Bohr)
_K2_BOND = 14.9606 * EV_TO_AU / BOHR_PER_ANG**2  # Hartree / Bohr^2
_K3_BOND = -30.0032 * EV_TO_AU / BOHR_PER_ANG**3  # Hartree / Bohr^3
_K4_BOND = 36.6253 * EV_TO_AU / BOHR_PER_ANG**4  # Hartree / Bohr^4

# class2 angle: V = K2 dth^2 + K3 dth^3 + K4 dth^4, dth = theta - theta0
_THETA0 = 107.6 * (np.pi / 180.0)  # equilibrium H-C-H angle (radians)
_K2_ANGLE = 1.71900 * EV_TO_AU  # Hartree / rad^2
_K3_ANGLE = -0.532989 * EV_TO_AU  # Hartree / rad^3
_K4_ANGLE = -0.105453 * EV_TO_AU  # Hartree / rad^4
# COMPASS bond-bond coupling: V = M (r1 - r_bb) (r2 - r_bb) on the two bonds of the angle
_M_BB = 0.231200 * EV_TO_AU / BOHR_PER_ANG**2  # Hartree / Bohr^2
_R_BB = _RCH
# COMPASS bond-angle coupling: V = N [(r1 - r_ba) + (r2 - r_ba)] dth
_N_BA = 0.785020 * EV_TO_AU / BOHR_PER_ANG  # Hartree / (Bohr rad)
_R_BA = _RCH

_M_C = 12.011 * AMU_TO_AU
_M_H = 1.008 * AMU_TO_AU

# Floor on sin(theta) in the angle gradient
_SIN_FLOOR = 1.0e-3

# per-kind tables (kind 0 = C, kind 1 = H) of the 9-6 Lennard-Jones parameters
_LJ_EPS = np.array([[_CC_EPS, _CH_EPS], [_CH_EPS, _HH_EPS]])
_LJ_R0 = np.array([[_CC_R0, _CH_R0], [_CH_R0, _HH_R0]])
# atom kinds of one molecule in the geometry order [C, H, H, H, H]
_KINDS = np.array([0, 1, 1, 1, 1])
# the six H-C-H angles of one molecule, as pairs of hydrogen slots (1..4)
_ANGLE_PAIRS = ((1, 2), (1, 3), (1, 4), (2, 3), (2, 4), (3, 4))

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


def _load_ch4_100():
    """
    Load the bundled 100-molecule bulk-CH4 geometry ready for production calculation.

    Returns
    -------
    positions : numpy.ndarray of float, shape (500, 3)
        Atomic positions in atomic units (Bohr).
    box : numpy.ndarray of float, shape (3,)
        Cubic box lengths in atomic units (Bohr).
    """

    global _CH4_100_CACHE
    if _CH4_100_CACHE is None:
        with open(_CH4_100_XYZ) as handle:
            lines = handle.readlines()
        box_length = float(lines[1].split()[2])  # "# CELL(abcABC): a a a ..."
        coords = [
            [float(c) for c in line.split()[1:4]]
            for line in lines[2:]
            if len(line.split()) >= 4
        ]
        _CH4_100_CACHE = (
            np.array(coords, dtype=float),
            np.full(3, box_length, dtype=float),
        )
    positions, box = _CH4_100_CACHE
    return positions.copy(), box.copy()


class CH4COMPASSForceField(DummyForceField):
    """
    Flexible CH4 force field (ch4compass).

    Implements the COMPASS class2 model of methane [H. Sun, J. Phys. Chem. B 102, 7338
    (1998)].

    Atoms are stored in the canonical order ``[C, H, H, H, H, C, H, H, H, H, ...]``.
    """

    name = "ch4compass"

    has_compiled_kernels = True

    # one molecule's atom order
    molecule_symbols = ("C", "H", "H", "H", "H")

    # energy terms reported next to the potential
    reported_term_names = ("stretch_au", "bend_au")

    def __init__(
        self,
        n_molecules=100,
        positions=None,
        box=None,
        replicate=None,
        rcut=None,
        ewald_wrcut=None,
    ):
        """
        Initialize the ch4compass force field and build the initial geometry.

        Parameters
        ----------
        n_molecules : int, default: 100
            Number of CH4 molecules. Ignored when ``positions`` is given. With the
            default value 100 and no ``positions``/``box``, the bundled bulk-CH4 box
            (``ch4_100.xyz``, atomic units, the LAMMPS reference starting geometry) is
            loaded so that a fully default force field is production-ready.
        positions : array-like of float, shape (5 * n_molecules, 3), optional
            Initial atomic positions in Bohr, ordered ``[C, H, H, H, H, ...]``. If
            ``None`` a default geometry is built with a single equilibrium molecule for
            ``n_molecules == 1``, otherwise a cubic lattice inside ``box``.
        box : float or array-like of float, shape (3,), optional
            Periodic box lengths in Bohr. ``None`` treats the system as a finite
            cluster.
        replicate : int or array-like of int, shape (3,), optional
            Replicate the periodic cell. The molecules and the box lengths grow together,
            so the density is unchanged.
        rcut : float, optional
            Real-space cutoff in Bohr for the periodic LJ and Coulomb sums. Defaults to
            10 Angstrom (the LAMMPS pair cutoff), also clamped to half the shortest box
            side (17.35 Bohr in the bundled 100-CH4 box).
        ewald_wrcut : float, optional
            Real-space cutoff in Bohr for the Ewald sum (also sets
            :math:`\\alpha = \\pi / \\mathrm{wrcut}` and
            ``kmax = int(alpha * max(box))``). Defaults to ``rshort * min(0.5, 1.2 * n_atoms**(-1/6))``.
        """

        # zero-config productive default corresponding to the 100-molecule CH4 box of the LAMMPS runs
        if positions is None and box is None and int(n_molecules) == 100:
            positions, box = _load_ch4_100()

        super().__init__(
            n_molecules=n_molecules,
            positions=positions,
            box=box,
            replicate=replicate,
            rcut=_RCUT_DEFAULT if rcut is None else rcut,
            ewald_wrcut=ewald_wrcut,
        )

        # the base has stored self.positions and self.n_atoms
        if self.n_atoms % 5 != 0:
            raise ValueError(
                "positions must have shape (5 * n_molecules, 3) in [C, H, H, H, H, ...]"
            )
        self.n_molecules = self.n_atoms // 5
        # per-atom masses, ordered as [C, H, H, H, H, C, H, H, H, H, ...]
        self.masses = np.tile(
            np.array([[_M_C], [_M_H], [_M_H], [_M_H], [_M_H]]), (self.n_molecules, 1)
        )

        # per-atom kind (0 = C, 1 = H) selecting the 9-6 Lennard-Jones pair tables
        self.kinds = np.tile(_KINDS, self.n_molecules)
        # molecule index of every atom
        self.mol_id = np.repeat(np.arange(self.n_molecules), 5)

    # ------------------------------------- geometry --------------------------------

    @staticmethod
    def _single_molecule():
        """
        Build one tetrahedral CH4 molecule with the carbon at the origin.

        Returns
        -------
        numpy.ndarray of float, shape (5, 3)
            Positions of ``[C, H, H, H, H]`` in atomic units (Bohr).
        """

        corners = np.array(
            [[1.0, 1.0, 1.0], [1.0, -1.0, -1.0], [-1.0, 1.0, -1.0], [-1.0, -1.0, 1.0]]
        )
        c = np.zeros((1, 3))
        h = _RCH * corners / np.sqrt(3.0)
        return np.vstack([c, h])

    def _build_default_geometry(self, n_molecules, box):
        """
        Build a default starting geometry for ``n_molecules`` CH4 molecules.

        A single molecule is placed at the origin; several molecules are arranged on
        the smallest cubic lattice that holds them inside the periodic box.

        Parameters
        ----------
        n_molecules : int
            Number of CH4 molecules to place.
        box : float or array-like of float, shape (3,), or None
            Periodic box lengths in Bohr. Required when ``n_molecules > 1``.

        Returns
        -------
        numpy.ndarray of float, shape (5 * n_molecules, 3)
            Atomic positions ordered ``[C, H, H, H, H, ...]`` in atomic units.

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

    def build_force_kernels(self, xp, threads_per_block=128):
        """
        Return this force field's compiled kernels from :mod:`kernels_ch4compass`.

        Parameters
        ----------
        xp : module
            Array module: ``numpy`` for the CPU path, ``cupy`` for the GPU path.
        threads_per_block : int, default: 128
            CUDA block size, ignored on the CPU path.

        Returns
        -------
        kernels_ch4compass.CH4COMPASSForceKernels
            The compiled force evaluator.
        """

        from ..kernels.kernels_ch4compass import CH4COMPASSForceKernels

        return CH4COMPASSForceKernels(self, xp, threads_per_block)

    # ---------------------------------force evaluation ----------------------------
    def compute(self, x, efield, terms=None):
        """
        Evaluate the total ch4compass force and potential energy.

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

        x5 = x.reshape(self.n_molecules, 5, 3)  # [:, 0]=C, [:, 1..4]=H
        forces = np.zeros((self.n_molecules, 5, 3))
        potential = 0.0

        # (1) intramolecular class2 bonds + class2 angles with their couplings
        f_intra, v_stretch, v_bend = self._intramolecular(x5)
        forces += f_intra
        potential += v_stretch + v_bend
        if terms is not None:
            terms[:] = (v_stretch, v_bend)

        # (2) 9-6 Lennard-Jones between all C and H atoms (intramolecular pairs excluded)
        if self.n_molecules > 1:
            f_lj, v_lj = self._lennard_jones(x5)
            forces += f_lj
            potential += v_lj

        # (3) Coulomb between the atomic charges (+ external field)
        f_sites, v_coul = self._coulomb_site_forces(x5)
        potential += v_coul
        # external uniform field pushes each charge: F = q * E
        f_sites[:, 0, :] += _Q_C * efield  # C
        f_sites[:, 1:, :] += _Q_H * efield  # the four hydrogens
        forces += f_sites  # charges are on the real atoms; no redistribution

        return forces.reshape(self.n_atoms, 3), potential

    def _intramolecular(self, x5):
        """
        Evaluate the class2 bonds and the class2 angles with their couplings.

        Parameters
        ----------
        x5 : numpy.ndarray of float, shape (n_molecules, 5, 3)
            Atomic positions grouped per molecule as ``[C, H1, H2, H3, H4]`` in Bohr.

        Returns
        -------
        forces : numpy.ndarray of float, shape (n_molecules, 5, 3)
            Intramolecular force on each atom in atomic units.
        stretch : float
            Total bond energy in atomic units (Hartree).
        bend : float
            Total angle energy (angle terms plus couplings) in atomic units.
        """

        c = x5[:, 0, :]
        d = x5[:, 1:, :] - c[:, None, :]  # C -> H_k, shape (nm, 4, 3)
        r = np.linalg.norm(d, axis=2)  # (nm, 4)
        # gradient of the energy with respect to each hydrogen; the carbon takes the
        # opposite of their sum (the bonded terms conserve momentum)
        grad = np.zeros_like(d)

        # --- class2 bonds (displacement from the equilibrium C-H length) ---
        s = r - _RCH
        v_stretch = np.sum(_K2_BOND * s**2 + _K3_BOND * s**3 + _K4_BOND * s**4)
        dvdr = 2.0 * _K2_BOND * s + 3.0 * _K3_BOND * s**2 + 4.0 * _K4_BOND * s**3
        grad += (dvdr / r)[:, :, None] * d

        # --- class2 angles: six H-C-H pairs per molecule ---
        v_bend = 0.0
        for j, k in _ANGLE_PAIRS:
            d1 = d[:, j - 1, :]
            d2 = d[:, k - 1, :]
            r1 = r[:, j - 1]
            r2 = r[:, k - 1]
            cos_t = np.clip(np.sum(d1 * d2, axis=1) / (r1 * r2), -1.0, 1.0)
            dang = np.arccos(cos_t) - _THETA0
            # d(theta)/d(cos theta) = -1 / sin(theta), floored like LAMMPS SMALL
            inv_sin = 1.0 / np.maximum(np.sqrt(1.0 - cos_t * cos_t), _SIN_FLOOR)
            b1 = r1 - _R_BB
            b2 = r2 - _R_BB
            v_bend += np.sum(
                _K2_ANGLE * dang**2
                + _K3_ANGLE * dang**3
                + _K4_ANGLE * dang**4
                + _M_BB * b1 * b2
                + _N_BA * ((r1 - _R_BA) + (r2 - _R_BA)) * dang
            )
            # partial derivatives of the angle energy with respect to theta, r1 and r2
            dv_dtheta = (
                2.0 * _K2_ANGLE * dang
                + 3.0 * _K3_ANGLE * dang**2
                + 4.0 * _K4_ANGLE * dang**3
                + _N_BA * ((r1 - _R_BA) + (r2 - _R_BA))
            )
            dv_dr1 = _M_BB * b2 + _N_BA * dang
            dv_dr2 = _M_BB * b1 + _N_BA * dang
            # d(theta)/d(d1) and d(theta)/d(d2) through the cosine
            inv_r1r2 = 1.0 / (r1 * r2)
            dtheta_dd1 = -inv_sin[:, None] * (
                d2 * inv_r1r2[:, None] - (cos_t / r1**2)[:, None] * d1
            )
            dtheta_dd2 = -inv_sin[:, None] * (
                d1 * inv_r1r2[:, None] - (cos_t / r2**2)[:, None] * d2
            )
            grad[:, j - 1, :] += (
                (dv_dr1 / r1)[:, None] * d1 + dv_dtheta[:, None] * dtheta_dd1
            )
            grad[:, k - 1, :] += (
                (dv_dr2 / r2)[:, None] * d2 + dv_dtheta[:, None] * dtheta_dd2
            )

        # forces = -gradient; the central carbon takes the opposite of the outer sum
        forces = np.zeros_like(x5)
        forces[:, 0, :] = grad.sum(axis=1)  # on C
        forces[:, 1:, :] = -grad  # on the hydrogens

        return forces, float(v_stretch), float(v_bend)

    def _lennard_jones(self, x5):
        """
        Evaluate the 9-6 Lennard-Jones energy and force over all C and H atoms.

        ``V = eps [2 (r0/r)^9 - 3 (r0/r)^6]`` with the explicit C-C, C-H and H-H
        parameters. Intramolecular pairs (the bonded 1-2 and 1-3 pairs) are excluded.
        In a periodic box the potential is shifted to zero at the cutoff so the
        energy is continuous.

        Parameters
        ----------
        x5 : numpy.ndarray of float, shape (n_molecules, 5, 3)
            Atomic positions grouped per molecule as ``[C, H1, H2, H3, H4]`` in Bohr.

        Returns
        -------
        forces : numpy.ndarray of float, shape (n_molecules, 5, 3)
            Lennard-Jones force on each atom in atomic units.
        potential : float
            Lennard-Jones potential energy in atomic units (Hartree).
        """

        xa = x5.reshape(self.n_atoms, 3)
        # all pairwise atom displacements r_i - r_j
        dvec = xa[:, None, :] - xa[None, :, :]  # (na, na, 3)
        dvec = self._minimum_image(dvec)
        r2 = np.sum(dvec**2, axis=2)  # (na, na)
        np.fill_diagonal(r2, np.inf)  # exclude self-interaction
        # exclude intramolecular pairs (folded into the bonded terms)
        same_mol = self.mol_id[:, None] == self.mol_id[None, :]
        r2 = np.where(same_mol, np.inf, r2)

        # per-pair parameters from the per-kind tables
        eps_ij = _LJ_EPS[self.kinds[:, None], self.kinds[None, :]]  # (na, na)
        r0_ij = _LJ_R0[self.kinds[:, None], self.kinds[None, :]]

        v_shift = 0.0  # energy offset so V is continuous at the cutoff (periodic only)
        if self.box is not None:
            # keep the cutoff within the minimum-image limit (box / 2)
            cut = min(self.rcut, 0.5 * float(np.min(self.box)))
            r2 = np.where(r2 < cut**2, r2, np.inf)
            src6 = (r0_ij / cut) ** 6
            v_shift = 2.0 * src6 * np.sqrt(src6) - 3.0 * src6  # V / eps at the cutoff

        inv_r2 = 1.0 / r2
        sr6 = (r0_ij**2 * inv_r2) ** 3  # (r0 / r)^6
        sr9 = sr6 * np.sqrt(sr6)  # (r0 / r)^9
        # energy (shifted to zero at the cutoff); 0.5 removes the i,j double counting
        pair_energy = np.where(
            np.isfinite(r2), eps_ij * (2.0 * sr9 - 3.0 * sr6 - v_shift), 0.0
        )
        potential = 0.5 * np.sum(pair_energy)
        # force magnitude / r^2 on i from j (positive => repulsive along +dvec)
        coeff = 18.0 * eps_ij * (sr9 - sr6) * inv_r2  # (na, na)
        coeff = np.where(np.isfinite(inv_r2), coeff, 0.0)
        forces = np.sum(coeff[:, :, None] * dvec, axis=1)  # (na, 3)
        return forces.reshape(self.n_molecules, 5, 3), float(potential)

    def _coulomb_site_forces(self, x5):
        """
        Evaluate the intermolecular Coulomb energy and force on the atomic charges.

        Parameters
        ----------
        x5 : numpy.ndarray of float, shape (n_molecules, 5, 3)
            Atomic positions grouped per molecule as ``[C, H1, H2, H3, H4]`` in Bohr.

        Returns
        -------
        forces : numpy.ndarray of float, shape (n_molecules, 5, 3)
            Coulomb force on the five atoms of each molecule in atomic units.
        potential : float
            Intermolecular Coulomb potential energy in atomic units (Hartree).
        """

        charges = np.array([_Q_C, _Q_H, _Q_H, _Q_H, _Q_H])

        if self.n_molecules < 2:
            # a single molecule has no intermolecular Coulomb (intramolecular excluded)
            return np.zeros_like(x5), 0.0

        sp = x5.reshape(self.n_atoms, 3)  # atom positions, same layout as charges
        q = np.tile(charges, self.n_molecules)

        if self.box is None:
            forces, potential = self._coulomb_direct(sp, q, self.mol_id)
        else:
            forces, potential = self._coulomb_ewald(sp, q, self.mol_id)
        return forces.reshape(self.n_molecules, 5, 3), potential

    def _coulomb_direct(self, sp, q, mol_id):
        """
        Evaluate the exact ``1/r`` Coulomb sum over a finite cluster.

        Intramolecular pairs are excluded.

        Parameters
        ----------
        sp : numpy.ndarray of float, shape (ns, 3)
            Charge positions in atomic units (Bohr).
        q : numpy.ndarray of float, shape (ns,)
            Charges in atomic units.
        mol_id : numpy.ndarray of int, shape (ns,)
            Index of the molecule each charge belongs to.

        Returns
        -------
        forces : numpy.ndarray of float, shape (ns, 3)
            Coulomb force on each charge in atomic units.
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
        Evaluate the Ewald Coulomb sum on the periodic charges.

        Parameters
        ----------
        sp : numpy.ndarray of float, shape (ns, 3)
            Charge positions in atomic units (Bohr).
        q : numpy.ndarray of float, shape (ns,)
            Charges in atomic units.
        mol_id : numpy.ndarray of int, shape (ns,)
            Index of the molecule each charge belongs to.

        Returns
        -------
        forces : numpy.ndarray of float, shape (ns, 3)
            Coulomb force on each charge in atomic units.
        potential : float
            Ewald Coulomb potential energy (real + reciprocal + self) in atomic units.
        """

        box = self.box
        volume = float(box[0] * box[1] * box[2])
        # Ewald parameters. The default real-space cutoff is rshort * min(0.5, 1.2 * n_atoms**(-1/6));
        # a caller-supplied value (e.g. to match another reference) overrides it.
        if self.ewald_wrcut is None:
            rshort = float(np.min(box))
            wrcut = rshort * min(0.5, 1.2 * self.n_atoms ** (-1.0 / 6.0))
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
        Compute the molecular dipole moment from the atomic charges.

        Parameters
        ----------
        x : numpy.ndarray of float, shape (na, 3)
            Atomic positions in atomic units (Bohr).

        Returns
        -------
        numpy.ndarray of float, shape (3,)
            The total dipole :math:`\\boldsymbol{\\mu} = \\sum_i q_i \\mathbf{r}_i`
            over the C and H charges, in atomic units.
        """

        x5 = x.reshape(self.n_molecules, 5, 3)
        return _Q_C * x5[:, 0, :].sum(axis=0) + _Q_H * x5[:, 1:, :].sum(axis=(0, 1))

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
            \\sum_i q_i \\mathbf{v}_i` over the charges, in atomic units.
        """

        v5 = v.reshape(self.n_molecules, 5, 3)
        return _Q_C * v5[:, 0, :].sum(axis=0) + _Q_H * v5[:, 1:, :].sum(axis=(0, 1))
