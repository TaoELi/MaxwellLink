# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# --------------------------------------------------------------------------------------#

"""
Flexible CO2 force field (co2jcp2021).

Implements the anharmonic flexible-CO2 model used by Li, Nitzan, and Subotnik
[J. Chem. Phys. 154, 094124 (2021)], itself a modification of the Cygan, Romanov, and
Myshakin flexible-CO2 model [J. Phys. Chem. C 116, 13079 (2012)].

Comapred to the Cygan force field, the harmonic C=O stretch is replaced by a quartic (Morse-like)
anharmonic stretch.
"""

import os

import numpy as np
from maxwelllink.units import AMU_TO_AU

try:
    from .force_field import DummyForceField
except (ImportError, ValueError):  # allow running as a stand-alone script
    from force_field import DummyForceField

# Bundled 36-molecule bulk-CO2 geometry (already in atomic units) at 300K.
_CO2_36_XYZ = os.path.join(os.path.dirname(__file__), "data", "co2_36.xyz")
_CO2_36_CACHE = None

# ---------------------------------------------------------#
# co2jcp2021 force-field parameters (atomic units)         #
# ---------------------------------------------------------#

_Q_C = 0.6512  # charge on carbon (a.u.)
_Q_O = -0.3256  # charge on each oxygen (a.u.); a CO2 molecule is net neutral

# Lennard-Jones per-atom parameters (like-pair values). Cross terms use the
# Lorentz-Berthelot rules sigma_ij = (sigma_i + sigma_j) / 2 and
# eps_ij = sqrt(eps_i eps_j), reproducing the explicit LAMMPS pair_coeffs:
#   C-C: eps 8.9126e-5, sigma 5.29123 ; C-O: 1.50620e-4, 5.50666 ; O-O: 2.54542e-4, 5.72209
_CC_SIG = 5.29123  # Lennard-Jones sigma for C-C (Bohr)
_CC_EPS = 8.9126e-5  # Lennard-Jones epsilon for C-C (Hartree)
_OO_SIG = 5.72209  # Lennard-Jones sigma for O-O (Bohr)
_OO_EPS = 2.54542e-4  # Lennard-Jones epsilon for O-O (Hartree)

_THETA0 = np.pi  # equilibrium O-C-O angle (radians); CO2 is linear
_RCO = 2.196  # equilibrium C-O length (Bohr)
_DE = 0.2026280851  # quartic-Morse well-depth prefactor D_r (Hartree)
_DEB = 0.0861  # harmonic bend force constant (Hartree / rad^2)
_ALP = 1.4919949794  # quartic-Morse range parameter alpha_r (1 / Bohr)
_RCUT_DEFAULT = 18.0  # 18 Bohr real-space cutoff (LAMMPS pair_style cutoff)

_M_C = 12.0107 * AMU_TO_AU
_M_O = 15.9994 * AMU_TO_AU

# quartic-Morse expansion coefficients (identical to q-TIP4P/F)
_F1 = 7.0 / 12.0
_F2 = 7.0 / 3.0

# Floor on sin(theta) so the arccos-gradient of the bend stays finite at the
# linear (theta = 180 degrees) equilibrium, where the true bend force vanishes.
_SIN_FLOOR = 1.0e-6

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


def _load_co2_36():
    """
    Load the bundled 36-molecule bulk-CO2 geometry ready for production calculation.

    Returns
    -------
    positions : numpy.ndarray of float, shape (108, 3)
        Atomic positions in atomic units (Bohr).
    box : numpy.ndarray of float, shape (3,)
        Cubic box lengths in atomic units (Bohr).
    """

    global _CO2_36_CACHE
    if _CO2_36_CACHE is None:
        with open(_CO2_36_XYZ) as handle:
            lines = handle.readlines()
        box_length = float(lines[1].split()[2])  # "# CELL(abcABC): a a a ..."
        coords = [
            [float(c) for c in line.split()[1:4]]
            for line in lines[2:]
            if len(line.split()) >= 4
        ]
        _CO2_36_CACHE = (
            np.array(coords, dtype=float),
            np.full(3, box_length, dtype=float),
        )
    positions, box = _CO2_36_CACHE
    return positions.copy(), box.copy()


class CO2JCP2021ForceField(DummyForceField):
    """
    Flexible CO2 force field (co2jcp2021).

    Implements the anharmonic flexible-CO2 model of Li, Nitzan, and Subotnik
    [J. Chem. Phys. 154, 094124 (2021)], a quartic-Morse-stretch modification of
    the Cygan, Romanov, and Myshakin flexible-CO2 model [J. Phys. Chem. C 116,
    13079 (2012)].

    Atoms are stored in the canonical order ``[C, O, O, C, O, O, ...]``.
    """

    name = "co2jcp2021"

    def __init__(
        self, n_molecules=36, positions=None, box=None, rcut=None, ewald_wrcut=None
    ):
        """
        Initialize the co2jcp2021 force field and build the initial geometry.

        Parameters
        ----------
        n_molecules : int, default: 36
            Number of CO2 molecules. Ignored when ``positions`` is given. With the
            default value 36 and no ``positions``/``box``, the bundled equilibrated
            bulk-CO2 box (``co2_36.xyz``, atomic units) is loaded so that a fully
            default force field is production-ready.
        positions : array-like of float, shape (3 * n_molecules, 3), optional
            Initial atomic positions in Bohr, ordered ``[C, O, O, ...]``. If ``None``
            a default geometry is built with a single equilibrium molecule for
            ``n_molecules == 1``, otherwise a cubic lattice inside ``box``.
        box : float or array-like of float, shape (3,), optional
            Periodic box lengths in Bohr. ``None`` treats the system as a finite
            cluster.
        rcut : float, optional
            Real-space cutoff in Bohr for the periodic LJ and Coulomb sums. Defaults to
            18 Bohr (the LAMMPS pair cutoff), clamped to half the shortest box side.
        ewald_wrcut : float, optional
            Real-space cutoff in Bohr for the Ewald sum (also sets
            :math:`\\alpha = \\pi / \\mathrm{wrcut}` and
            ``kmax = int(alpha * max(box))``). Defaults to ``rshort * min(0.5, 1.2 * n_atoms**(-1/6))``.
        """

        # zero-config productive default: the equilibrated 36-molecule CO2 box
        if positions is None and box is None and int(n_molecules) == 36:
            positions, box = _load_co2_36()

        super().__init__(
            n_molecules=n_molecules,
            positions=positions,
            box=box,
            rcut=_RCUT_DEFAULT if rcut is None else rcut,
            ewald_wrcut=ewald_wrcut,
        )

        # the base has stored self.positions and self.na; CO2 is triatomic [C, O, O]
        if self.na % 3 != 0:
            raise ValueError(
                "positions must have shape (3 * n_molecules, 3) in [C, O, O, ...]"
            )
        self.n_molecules = self.na // 3
        # per-atom masses, ordered [C, O, O, C, O, O, ...]
        self.masses = np.tile(np.array([[_M_C], [_M_O], [_M_O]]), (self.n_molecules, 1))

        # per-atom Lennard-Jones parameters (C, O, O per molecule); cross terms are
        # formed on the fly with Lorentz-Berthelot mixing in :meth:`_lennard_jones`.
        self.lj_sigma = np.tile([_CC_SIG, _OO_SIG, _OO_SIG], self.n_molecules)
        self.lj_eps = np.tile([_CC_EPS, _OO_EPS, _OO_EPS], self.n_molecules)
        # molecule index of every atom, used to exclude intramolecular pairs
        self.mol_id = np.repeat(np.arange(self.n_molecules), 3)

    # ------------------------------------- geometry --------------------------------
    @staticmethod
    def _single_molecule():
        """
        Build one equilibrium CO2 molecule with the carbon at the origin.

        The molecule is linear along the z axis: ``C`` at the origin and the two
        oxygens at :math:`\\pm r_{\\rm CO}\\,\\hat{z}``.

        Returns
        -------
        numpy.ndarray of float, shape (3, 3)
            Positions of ``[C, O, O]`` in atomic units (Bohr).
        """

        c = np.array([0.0, 0.0, 0.0])
        o1 = np.array([0.0, 0.0, _RCO])
        o2 = np.array([0.0, 0.0, -_RCO])
        return np.vstack([c, o1, o2])

    def _build_default_geometry(self, n_molecules, box):
        """
        Build a default starting geometry for ``n_molecules`` CO2 molecules.

        A single molecule is placed at the origin; several molecules are arranged on
        the smallest cubic lattice that holds them inside the periodic box.

        Parameters
        ----------
        n_molecules : int
            Number of CO2 molecules to place.
        box : float or array-like of float, shape (3,), or None
            Periodic box lengths in Bohr. Required when ``n_molecules > 1``.

        Returns
        -------
        numpy.ndarray of float, shape (3 * n_molecules, 3)
            Atomic positions ordered ``[C, O, O, ...]`` in atomic units.

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

    #: energy terms reported next to the potential: the two bond stretches and the bend
    term_names = ("stretch_au", "bend_au")

    def build_force_kernels(self, xp, threads_per_block=128):
        """
        Return this force field's compiled kernels from :mod:`kernels_co2`.

        Parameters
        ----------
        xp : module
            Array module: ``numpy`` for the CPU path, ``cupy`` for the GPU path.
        threads_per_block : int, default: 128
            CUDA block size, ignored on the CPU path.

        Returns
        -------
        kernels_co2.CO2ForceKernels
            The compiled force evaluator.
        """

        from .kernels_co2 import CO2ForceKernels

        return CO2ForceKernels(self, xp, threads_per_block)

    # ---------------------------------force evaluation ----------------------------
    def compute(self, x, efield, terms=None):
        """
        Evaluate the total co2jcp2021 force and potential energy.

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

        x3 = x.reshape(self.n_molecules, 3, 3)  # [:, 0]=C, [:, 1]=O1, [:, 2]=O2
        forces = np.zeros((self.n_molecules, 3, 3))
        potential = 0.0

        # (1) intramolecular quartic-Morse stretch + harmonic bend
        f_intra, v_stretch, v_bend = self._intramolecular(x3)
        forces += f_intra
        potential += v_stretch + v_bend
        if terms is not None:
            terms[:] = (v_stretch, v_bend)

        # (2) Lennard-Jones between all C and O atoms (intramolecular pairs excluded)
        if self.n_molecules > 1:
            f_lj, v_lj = self._lennard_jones(x3)
            forces += f_lj
            potential += v_lj

        # (3) Coulomb between the atomic charges (+ external field)
        f_sites, v_coul = self._coulomb_site_forces(x3)
        potential += v_coul
        # external uniform field pushes each charge: F = q * E
        f_sites[:, 0, :] += _Q_C * efield  # C
        f_sites[:, 1, :] += _Q_O * efield  # O1
        f_sites[:, 2, :] += _Q_O * efield  # O2
        forces += f_sites  # charges are on the real atoms; no redistribution

        return forces.reshape(self.na, 3), potential

    def _intramolecular(self, x3):
        """
        Evaluate the intramolecular quartic-Morse stretch and harmonic bend.

        Vectorized over all molecules. The carbon is the central atom, so the two
        internal bonds are C-O1 and C-O2 and the bend is the O1-C-O2 angle.

        Parameters
        ----------
        x3 : numpy.ndarray of float, shape (n_molecules, 3, 3)
            Atomic positions grouped per molecule as ``[C, O1, O2]`` in Bohr.

        Returns
        -------
        forces : numpy.ndarray of float, shape (n_molecules, 3, 3)
            Intramolecular force on each atom in atomic units.
        stretch : float
            Total stretch energy in atomic units (Hartree).
        bend : float
            Total bend energy in atomic units (Hartree).
        """

        c = x3[:, 0, :]
        o1 = x3[:, 1, :]
        o2 = x3[:, 2, :]

        d1 = o1 - c  # C -> O1
        d2 = o2 - c  # C -> O2
        d3 = o2 - o1  # O1 -> O2
        r1 = np.linalg.norm(d1, axis=1)
        r2 = np.linalg.norm(d2, axis=1)
        r3 = np.linalg.norm(d3, axis=1)
        e1 = d1 / r1[:, None]
        e2 = d2 / r2[:, None]
        e3 = d3 / r3[:, None]

        # --- stretches (displacement from equilibrium C-O length) ---
        s1 = r1 - _RCO
        s2 = r2 - _RCO
        alp2 = _ALP * _ALP
        alp3 = alp2 * _ALP
        alp4 = alp3 * _ALP
        v_stretch = _DE * (
            alp2 * s1**2 - alp3 * s1**3 + _F1 * alp4 * s1**4
        ) + _DE * (alp2 * s2**2 - alp3 * s2**3 + _F1 * alp4 * s2**4)
        # dV/d(r) along each C-O bond
        a1 = _DE * (2.0 * alp2 * s1 - 3.0 * alp3 * s1**2 + _F2 * alp4 * s1**3)
        a2 = _DE * (2.0 * alp2 * s2 - 3.0 * alp3 * s2**2 + _F2 * alp4 * s2**3)

        # --- bend (angle from the law of cosines) ---
        u = r1**2 + r2**2 - r3**2
        vv = 2.0 * r1 * r2
        v2 = 1.0 / (vv * vv)
        arg = np.clip(u / vv, -1.0, 1.0)
        ang = np.arccos(arg)
        dang = ang - _THETA0
        # sin(theta), floored so d(arccos)/d(arg) stays finite at the linear
        # equilibrium (theta = 180 deg), where dang -> 0 and the force vanishes
        sin_theta = np.sqrt(np.maximum(1.0 - arg * arg, _SIN_FLOOR**2))
        darg = -1.0 / sin_theta
        dtheta_dr1 = darg * ((2.0 * r1) * vv - (2.0 * r2) * u) * v2
        dtheta_dr2 = darg * ((2.0 * r2) * vv - (2.0 * r1) * u) * v2
        dtheta_dr3 = darg * ((-2.0 * r3) * vv) * v2
        v_bend = _DEB * dang**2
        a3 = 2.0 * _DEB * dang

        # total dV/d(bond length) for each of the three internal coordinates
        dvdr1 = a1 + a3 * dtheta_dr1
        dvdr2 = a2 + a3 * dtheta_dr2
        dvdr3 = a3 * dtheta_dr3
        g1 = dvdr1[:, None] * e1  # gradient contribution along C-O1
        g2 = dvdr2[:, None] * e2  # gradient contribution along C-O2
        g3 = dvdr3[:, None] * e3  # gradient contribution along O1-O2

        # forces = -gradient, distributed to the three atoms
        forces = np.zeros_like(x3)
        forces[:, 0, :] = g1 + g2  # on C
        forces[:, 1, :] = -g1 + g3  # on O1
        forces[:, 2, :] = -g2 - g3  # on O2

        return forces, float(np.sum(v_stretch)), float(np.sum(v_bend))

    def _lennard_jones(self, x3):
        """
        Evaluate the Lennard-Jones energy and force over all C and O atoms.

        Cross terms use Lorentz-Berthelot mixing of the per-atom parameters.
        Intramolecular pairs (the bonded 1-2 and 1-3 pairs) are excluded, exactly as
        in the LAMMPS reference. Uses the minimum-image convention with a cutoff
        clamped to half the shortest box side; the potential is shifted to zero at the
        cutoff so the energy is continuous.

        Parameters
        ----------
        x3 : numpy.ndarray of float, shape (n_molecules, 3, 3)
            Atomic positions grouped per molecule as ``[C, O1, O2]`` in Bohr.

        Returns
        -------
        forces : numpy.ndarray of float, shape (n_molecules, 3, 3)
            Lennard-Jones force on each atom in atomic units.
        potential : float
            Lennard-Jones potential energy in atomic units (Hartree).
        """

        xa = x3.reshape(self.na, 3)
        # all pairwise atom displacements r_i - r_j
        dvec = xa[:, None, :] - xa[None, :, :]  # (na, na, 3)
        dvec = self._minimum_image(dvec)
        r2 = np.sum(dvec**2, axis=2)  # (na, na)
        np.fill_diagonal(r2, np.inf)  # exclude self-interaction
        # exclude intramolecular pairs (folded into the bonded terms)
        same_mol = self.mol_id[:, None] == self.mol_id[None, :]
        r2 = np.where(same_mol, np.inf, r2)

        # Lorentz-Berthelot cross parameters for every pair
        sig_ij = 0.5 * (self.lj_sigma[:, None] + self.lj_sigma[None, :])  # (na, na)
        eps_ij = np.sqrt(self.lj_eps[:, None] * self.lj_eps[None, :])
        sig2 = sig_ij**2

        v_shift = 0.0  # energy offset so V is continuous at the cutoff (periodic only)
        if self.box is not None:
            # keep the cutoff within the minimum-image limit (box / 2)
            cut = min(self.rcut, 0.5 * float(np.min(self.box)))
            r2 = np.where(r2 < cut**2, r2, np.inf)
            src6 = (sig2 / cut**2) ** 3
            v_shift = src6 * (src6 - 1.0)  # LJ value at the cutoff (per pair)

        inv_r2 = 1.0 / r2
        sr6 = (sig2 * inv_r2) ** 3
        # energy (shifted to zero at the cutoff); 0.5 removes the i,j double counting
        pair_energy = np.where(
            np.isfinite(r2), 4.0 * eps_ij * (sr6 * (sr6 - 1.0) - v_shift), 0.0
        )
        potential = 0.5 * np.sum(pair_energy)
        # force magnitude / r^2 on i from j (positive => repulsive along +dvec)
        coeff = 48.0 * eps_ij * sr6 * (sr6 - 0.5) * inv_r2  # (na, na)
        coeff = np.where(np.isfinite(inv_r2), coeff, 0.0)
        forces = np.sum(coeff[:, :, None] * dvec, axis=1)  # (na, 3)
        return forces.reshape(self.n_molecules, 3, 3), float(potential)

    def _coulomb_site_forces(self, x3):
        """
        Evaluate the intermolecular Coulomb energy and force on the atomic charges.

        The charge sites are the real atoms ``[C, O1, O2]`` of each molecule.
        Intramolecular pairs are excluded (they are folded into the bonded terms). A
        finite cluster (``box is None``) uses an exact direct sum; a periodic box uses
        an Ewald sum.

        Parameters
        ----------
        x3 : numpy.ndarray of float, shape (n_molecules, 3, 3)
            Atomic positions grouped per molecule as ``[C, O1, O2]`` in Bohr.

        Returns
        -------
        forces : numpy.ndarray of float, shape (n_molecules, 3, 3)
            Coulomb force on the three atoms ``[C, O1, O2]`` of each molecule in
            atomic units.
        potential : float
            Intermolecular Coulomb potential energy in atomic units (Hartree).
        """

        charges = np.array([_Q_C, _Q_O, _Q_O])

        if self.n_molecules < 2:
            # a single molecule has no intermolecular Coulomb (intramolecular excluded)
            return np.zeros_like(x3), 0.0

        sp = x3.reshape(self.na, 3)  # atom positions, same layout as charges
        q = np.tile(charges, self.n_molecules)

        if self.box is None:
            forces, potential = self._coulomb_direct(sp, q, self.mol_id)
        else:
            forces, potential = self._coulomb_ewald(sp, q, self.mol_id)
        return forces.reshape(self.n_molecules, 3, 3), potential

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

        The real-space term uses ``erfc`` (the Abramowitz-Stegun polynomial) for pairs
        in different molecules and ``erfc - 1`` for pairs in the same molecule; the
        latter both removes the bare intramolecular Coulomb and supplies the
        reciprocal-space exclusion correction, exactly as in the water reference code.

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
        Compute the molecular dipole moment from the atomic charges.

        Parameters
        ----------
        x : numpy.ndarray of float, shape (na, 3)
            Atomic positions in atomic units (Bohr).

        Returns
        -------
        numpy.ndarray of float, shape (3,)
            The total dipole :math:`\\boldsymbol{\\mu} = \\sum_i q_i \\mathbf{r}_i`
            over the C and O charges, in atomic units.
        """

        x3 = x.reshape(self.n_molecules, 3, 3)
        return _Q_C * x3[:, 0, :].sum(axis=0) + _Q_O * (x3[:, 1, :] + x3[:, 2, :]).sum(
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
            \\sum_i q_i \\mathbf{v}_i` over the charges, in atomic units.
        """

        v3 = v.reshape(self.n_molecules, 3, 3)
        return _Q_C * v3[:, 0, :].sum(axis=0) + _Q_O * (v3[:, 1, :] + v3[:, 2, :]).sum(
            axis=0
        )
