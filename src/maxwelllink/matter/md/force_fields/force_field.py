# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Force-field base class for classical molecular-dynamics (MD) driver.

All quantities are in atomic units.
"""

import math

import numpy as np


def replicate_cell(positions, box, replicate):
    """
    Tile a periodic cell ``nx * ny * nz`` times.

    Parameters
    ----------
    positions : array-like of float, shape (..., na, 3)
        Atomic positions in atomic units (Bohr); leading axes (e.g. XYZ frames) are
        kept.
    box : float or array-like of float, shape (3,)
        Periodic box lengths in atomic units (Bohr) of the cell being tiled.
    replicate : int or array-like of int, shape (3,)
        Three integers are the images along x, y and z. One integer is the *total*
        number of images, laid out as a cube when it is a cubic number
        (``27 -> 3 x 3 x 3``), as a square in the xy plane when it is a square number
        (``4 -> 2 x 2 x 1``), and in a row along x otherwise (``6 -> 6 x 1 x 1``).

    Returns
    -------
    positions : numpy.ndarray of float, shape (..., na * nx * ny * nz, 3)
        The tiled positions; every image keeps the per-molecule atom order.
    box : numpy.ndarray of float, shape (3,)
        The enlarged box lengths.
    counts : numpy.ndarray of int, shape (3,)
        The image counts ``(nx, ny, nz)`` that were applied.

    Raises
    ------
    ValueError
        If ``replicate`` is not one or three positive integers.
    """

    # the image counts along x, y and z
    counts = np.asarray(replicate, dtype=float)
    if counts.shape not in ((), (3,)) or np.any(counts != np.round(counts)):
        raise ValueError("replicate must be one integer or three integers.")
    counts = counts.astype(int)
    if np.any(counts < 1):
        raise ValueError("replicate must be positive.")
    if counts.shape == ():
        total = int(counts)
        side = int(round(total ** (1.0 / 3.0)))
        if side**3 == total:  # a cubic number: nx = ny = nz
            counts = np.array([side, side, side])
        elif math.isqrt(total) ** 2 == total:  # a square number: nx = ny, nz = 1
            side = math.isqrt(total)
            counts = np.array([side, side, 1])
        else:  # a row of images along x
            counts = np.array([total, 1, 1])

    # every image is the whole cell shifted by a lattice vector of the unit box
    box = np.broadcast_to(np.asarray(box, dtype=float), (3,))
    positions = np.asarray(positions, dtype=float)
    images = []
    for ix in range(counts[0]):
        for iy in range(counts[1]):
            for iz in range(counts[2]):
                images.append(positions + box * np.array([ix, iy, iz]))
    return np.concatenate(images, axis=-2), box * counts, counts


class DummyForceField:
    """
    Abstract base class for a classical molecular-dynamics (MD) force field.
    """

    # externally used name by users
    name = "dummy"

    # whether this force field provides compiled (numba) kernels
    has_compiled_kernels = False

    # element symbols of one molecule in the order the force field expects them,
    # e.g. ``("C", "O", "O")``
    molecule_symbols = ()

    # names of the per-system energy terms the force field reports to
    # the MD driver, e.g. ``("stretch_au", "bend_au")``
    reported_term_names = ()

    def __init__(
        self,
        n_molecules=1,
        positions=None,
        box=None,
        replicate=None,
        rcut=None,
        ewald_wrcut=None,
    ):
        """
        Initialize the shared environment parameters of the force field.

        Parameters
        ----------
        n_molecules : int, optional
            Number of molecules in the system (before replication). This is used to
            determine the total number of atoms when the geometry is not provided.
            The default is 1.
        positions : array-like of float, shape (na, 3), optional
            Atomic positions in atomic units (Bohr). If ``None`` a default geometry is
            built from ``n_molecules`` via :meth:`_build_default_geometry`.
        box : float or array-like of float, shape (3,), optional
            Periodic box lengths in atomic units (Bohr). If ``None`` the system is a
            finite cluster and no periodic images are used.
        replicate : int or array-like of int, shape (3,), optional
            Replicate the periodic cell. The molecules and the box lengths grow together, 
            so the density is unchanged.
        rcut : float, optional
            Real-space cutoff in Bohr for the non-bonded sums when a box is present.
        ewald_wrcut : float, optional
            Real-space cutoff in Bohr for the Ewald sum (force fields with periodic
            electrostatics). ``None`` lets the subclass pick a default.
        """

        # geometry: use the caller-provided positions or build a default one
        if positions is not None:
            pos = np.array(positions, dtype=float)
            if pos.ndim != 2 or pos.shape[1] != 3:
                raise ValueError("positions must have shape (n_atoms, 3).")
            self.positions = pos
        else:
            self.positions = self._build_default_geometry(int(n_molecules), box)
        self.n_atoms = self.positions.shape[0]

        # periodic box and non-bonded cutoffs
        if box is None:
            self.box = None
        else:
            self.box = np.broadcast_to(np.asarray(box, dtype=float), (3,)).copy()
        self.rcut = None if rcut is None else float(rcut)
        self.ewald_wrcut = None if ewald_wrcut is None else float(ewald_wrcut)

        # replicate the periodic cell nx * ny * nz times
        self.replicate = np.ones(3, dtype=int)
        if replicate is not None:
            if self.box is None:
                raise ValueError("replicate requires a periodic box.")
            self.positions, self.box, self.replicate = replicate_cell(
                self.positions, self.box, replicate
            )
            self.n_atoms = self.positions.shape[0]

        # per-atom masses, overridden by subclasses to provide the real masses
        self.masses = np.ones((self.n_atoms, 1))

        # compiled kernels and their output buffer, which is built on the first compute_fast()
        self._force_kernels = None
        self._forces = None

    # ------------------------------------- helpers ---------------------------------
    def _minimum_image(self, dvec):
        """
        Wrap displacement vectors into the primary periodic cell.

        Parameters
        ----------
        dvec : numpy.ndarray of float, shape (..., 3)
            Displacement vectors in atomic units (Bohr).

        Returns
        -------
        numpy.ndarray of float, shape (..., 3)
            The minimum-image displacement vectors. Returned unchanged when no box is
            set (``self.box is None``).
        """

        if self.box is None:
            return dvec
        
        return dvec - self.box * np.round(dvec / self.box)

    # ----------------------------- to be overridden --------------------------------
    def _build_default_geometry(self, n_molecules, box):
        """
        Build a default geometry for the given number of molecules.

        Notes
        -----
        This method *must be* overridden by subclasses.

        Parameters
        ----------
        n_molecules : int
            Number of molecules in the system.
        box : float or array-like of float, shape (3,), optional
            Periodic box lengths in atomic units (Bohr). If ``None`` the system is a finite cluster and no periodic images are used.

        Returns
        -------
        numpy.ndarray of float, shape (na, 3)
            Atomic positions in atomic units (Bohr).
        """

        raise NotImplementedError(
            "Subclasses must implement _build_default_geometry()."
        )

    def build_force_kernels(self, xp, threads_per_block=128):
        """
        Return the compiled force evaluator of this force field.

        Notes
        -----
        This method *should be* overridden by subclasses that want the fast (numba)
        CPU path and the GPU batch backend. Without it only :meth:`compute` is
        available.

        Parameters
        ----------
        xp : module
            Array module: ``numpy`` for the CPU path, ``cupy`` for the GPU path.
        threads_per_block : int, default: 128
            CUDA block size, ignored on the CPU path.

        Returns
        -------
        object
            The force field's compiled kernels.
        """

        raise NotImplementedError(
            f"{type(self).__name__} provides no compiled kernels; only the NumPy "
            f"compute() path is available for it."
        )

    def compute_fast(self, x, efield, terms=None):
        """
        Evaluate the force and potential with the compiled (numba) kernels.

        Physically identical to :meth:`compute` but several times faster. The kernels
        and the output buffer are built once and reused.

        Parameters
        ----------
        x : numpy.ndarray of float, shape (na, 3)
            Atomic positions in atomic units (Bohr).
        efield : numpy.ndarray of float, shape (3,)
            Effective electric field ``[E_x, E_y, E_z]`` in atomic units.
        terms : numpy.ndarray of float, shape (len(reported_term_names),), optional
            Filled with the energy terms of :attr:`reported_term_names` when given.

        Returns
        -------
        forces : numpy.ndarray of float, shape (na, 3)
            Total force on each atom in atomic units.
        potential : float
            Mechanical potential energy in atomic units (Hartree).
        """

        if self._force_kernels is None:
            self._force_kernels = self.build_force_kernels(np)
            self._forces = np.zeros((self.n_atoms, 3))
        potential = self._force_kernels.forces_cpu(
            np.ascontiguousarray(x, dtype=float),
            self._forces,
            np.ascontiguousarray(efield, dtype=float).reshape(3),
            terms,
        )
        return self._forces, float(potential)

    def compute(self, x, efield, terms=None):
        """
        Evaluate the total force and potential energy for a geometry and field.

        The returned force must already include the external light-matter force
        :math:`q_i \\mathbf{E}` on each charge site (with any virtual-site forces
        redistributed onto the real atoms). The potential energy is the mechanical
        (field-free) one.

        Notes
        -----
        This method *must be* overridden by subclasses.

        Parameters
        ----------
        x : numpy.ndarray of float, shape (na, 3)
            Atomic positions in atomic units (Bohr).
        efield : numpy.ndarray of float, shape (3,)
            Effective electric field ``[E_x, E_y, E_z]`` in atomic units.
        terms : numpy.ndarray of float, shape (len(reported_term_names),), optional
            Filled with the energy terms of :attr:`reported_term_names` when given.

        Returns
        -------
        forces : numpy.ndarray of float, shape (na, 3)
            Total force on each atom in atomic units.
        potential : float
            Mechanical potential energy in atomic units (Hartree).
        """

        raise NotImplementedError("Subclasses must implement compute().")

    def dipole(self, x):
        """
        Return the molecular dipole moment for a given geometry.

        Notes
        -----
        This method *must be* overridden by subclasses.

        Parameters
        ----------
        x : numpy.ndarray of float, shape (na, 3)
            Atomic positions in atomic units (Bohr).

        Returns
        -------
        numpy.ndarray of float, shape (3,)
            The dipole moment :math:`\\boldsymbol{\\mu} = \\sum_i q_i \\mathbf{r}_i`
            in atomic units.
        """

        raise NotImplementedError("Subclasses must implement dipole().")

    def dipole_velocity(self, v):
        """
        Return the time derivative of the molecular dipole moment.

        Notes
        -----
        This method *must be* overridden by subclasses.

        Parameters
        ----------
        v : numpy.ndarray of float, shape (na, 3)
            Atomic velocities in atomic units.

        Returns
        -------
        numpy.ndarray of float, shape (3,)
            The dipole velocity :math:`\\mathrm{d}\\boldsymbol{\\mu}/\\mathrm{d}t =
            \\sum_i q_i \\mathbf{v}_i` in atomic units.
        """

        raise NotImplementedError("Subclasses must implement dipole_velocity().")
