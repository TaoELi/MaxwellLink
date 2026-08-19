# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Force-field base class for the conventional classical-MD driver.

All quantities are in atomic units.
"""

import numpy as np


class DummyForceField:
    """
    Abstract base class for a classical molecular-dynamics (MD) force field.
    """

    #: short identifier used by the force-field registry in ``md_model.py``
    name = "dummy"

    def __init__(
        self, n_molecules=1, positions=None, box=None, rcut=None, ewald_wrcut=None
    ):
        """
        Initialize the shared environment parameters of the force field.

        Notes
        -----
        This method *should be* extended by subclasses to build the geometry and the
        per-atom masses (``self.positions``, ``self.masses``, ``self.na``).

        Parameters
        ----------
        n_molecules : int, optional
            Number of molecules in the system. This is used to determine the total
            number of atoms when the geometry is not provided. The default is 1.
        positions : array-like of float, shape (na, 3), optional
            Atomic positions in atomic units (Bohr). If ``None`` a default geometry is
            built from ``n_molecules`` via :meth:`_build_default_geometry`.
        box : float or array-like of float, shape (3,), optional
            Periodic box lengths in atomic units (Bohr). If ``None`` the system is a
            finite cluster and no periodic images are used.
        rcut : float, optional
            Real-space cutoff in Bohr for the non-bonded sums when a box is present.
        ewald_wrcut : float, optional
            Real-space cutoff in Bohr for the Ewald sum (force fields with periodic
            electrostatics). ``None`` lets the subclass pick a default.
        """

        # geometry: use the caller-provided positions, or build a default one
        if positions is not None:
            pos = np.array(positions, dtype=float)
            if pos.ndim != 2 or pos.shape[1] != 3:
                raise ValueError("positions must have shape (n_atoms, 3).")
            self.positions = pos
        else:
            self.positions = self._build_default_geometry(int(n_molecules), box)
        self.na = self.positions.shape[0]

        # periodic box and non-bonded cutoffs
        if box is None:
            self.box = None
        else:
            self.box = np.broadcast_to(np.asarray(box, dtype=float), (3,)).copy()
        self.rcut = None if rcut is None else float(rcut)
        self.ewald_wrcut = None if ewald_wrcut is None else float(ewald_wrcut)

        # per-atom masses; subclasses overwrite this with the real masses
        self.masses = np.ones((self.na, 1))

        # compiled kernels and their output buffer, built on the first compute_fast()
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

    #: whether this force field provides compiled (numba) kernels
    has_compiled_kernels = False

    #: element symbols of one molecule in the order the force field expects them,
    #: e.g. ``("C", "O", "O")``; empty when the force field does not fix them
    molecule_symbols = ()

    #: names of the per-system energy terms the force field reports next to the
    #: potential, e.g. ``("stretch_au", "bend_au")``; empty when it reports none
    term_names = ()

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

        Physically identical to :meth:`compute` and agreeing with it to rounding, but
        several times faster and without the ``(na, na, 3)`` temporaries. The kernels
        and the output buffer are built once and reused.

        Parameters
        ----------
        x : numpy.ndarray of float, shape (na, 3)
            Atomic positions in atomic units (Bohr).
        efield : numpy.ndarray of float, shape (3,)
            Effective electric field ``[E_x, E_y, E_z]`` in atomic units.
        terms : numpy.ndarray of float, shape (len(term_names),), optional
            Filled with the energy terms of :attr:`term_names` when given.

        Returns
        -------
        forces : numpy.ndarray of float, shape (na, 3)
            Total force on each atom in atomic units.
        potential : float
            Mechanical potential energy in atomic units (Hartree).
        """

        if self._force_kernels is None:
            self._force_kernels = self.build_force_kernels(np)
            self._forces = np.zeros((self.na, 3))
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
        terms : numpy.ndarray of float, shape (len(term_names),), optional
            Filled with the energy terms of :attr:`term_names` when given.

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
