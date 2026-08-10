# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
Free space: an empty FDTD cell with absorbing (PML) boundaries.
"""

import meep as mp

from .dummy_cavity import DummyCavity, CYLINDRICAL


class Vacuum(DummyCavity):
    """
    An empty FDTD cell with absorbing (PML) boundaries, aka free space.

    Useful for spontaneous-emission and free-propagation tests.

    All placement and measurement methods are inherited from ``DummyCavity``
    unchanged.

    Notes
    -----
    With ``dimensions=mxl.CYLINDRICAL`` the cell is an (r, z) half plane: the
    axis sits at r = 0, the allowed region spans r in [0, size_r] and
    z in [-size_z/2, +size_z/2].

    Since the field of a z-polarized dipole on the axis has full rotational
    symmetry, an m = 0 cylindrical run (``make_simulation(m=0)``) reproduces
    3D free-space physics at 2D cost. An on-axis x- or y-polarized dipole can
    instead use one complex m = +1 or m = -1 sector.

    Examples
    --------
    >>> from maxwelllink.cavity import Vacuum
    >>> cav = Vacuum(size_nm=4000.0, omega_ref=2320.0, units="cm-1", dimensions=1)
    >>> mol = cav.place_molecule(driver="tls", driver_kwargs=dict(
    ...     omega=0.0106,  # driver parameters are in a.u.; ~2326 cm^-1
    ...     mu12=187.0, orientation=2, pe_initial=1e-4))
    >>> sim = cav.make_simulation(molecules=[mol])
    >>> sim.run(until=200)
    """

    def __init__(
        self,
        size_nm,
        omega_ref: float,
        units: str = "cm-1",
        dimensions: int = 1,
        resolution: float = None,
        pml_nm: float = None,
    ):
        """
        Initialize the parameters of an empty FDTD cell (free space).

        Parameters
        ----------
        size_nm : float or sequence of floats
            Interior size (nm) of the allowed region, *excluding* the PML
            that is added outside. A scalar gives an equal extent along every
            active axis; a sequence must have one entry per active axis such as
            (x,), (x, y), (x, y, z), or (r, z) for cylindrical cells.
        omega_ref : float
            Reference frequency (or wavelength) that sets the default
            resolution and PML thickness.
        units : str, default: "cm-1"
            Units of ``omega_ref``: "cm-1", "eV", "au", "nm", or "um".
        dimensions : int, default: 1
            1, 2, 3, or mxl.CYLINDRICAL.
        resolution : float or None, optional
            Meep resolution (pixels per Meep length unit). Default: 20 pixels
            per reference wavelength (the DummyCavity default).
        pml_nm : float or None, optional
            PML thickness in nm. Default: one reference wavelength.
        """

        # default attributes (units, grid, hotspot, ...), then resize below
        super().__init__(omega=omega_ref, units=units, dimensions=dimensions)
        lam = self.nm_to_meep(self.wavelength_nm)  # reference wavelength in um

        # one interior extent per active axis, in Meep units (um)
        axes = self._active_axes()  # "x", "xy", "xyz", or "xz" (cylindrical)
        if not hasattr(size_nm, "__len__"):  # a single number
            interior = [self.nm_to_meep(size_nm)] * len(axes)
        else:
            if len(size_nm) != len(axes):
                raise ValueError(
                    f"size_nm must be a scalar or a sequence of {len(axes)} "
                    f"entries for this cell (active axes: {', '.join(axes)})."
                )
            interior = [self.nm_to_meep(v) for v in size_nm]

        if resolution is not None:
            self.resolution = float(resolution)
        # the PML is added outside the requested interior on every boundary
        # (default thickness: one reference wavelength)
        self.pml_thickness = self.nm_to_meep(pml_nm) if pml_nm is not None else lam

        if self.dimensions == CYLINDRICAL:
            # the axis at r = 0 is not a boundary: the interior spans
            # r in [0, size_r], with PML at the outer r edge and both z ends
            self.cell_size = mp.Vector3(
                interior[0] + self.pml_thickness,
                0.0,
                interior[1] + 2.0 * self.pml_thickness,
            )
            self.allowed_bounds = {
                "x": (0.0, interior[0]),  # x plays the role of r
                "z": (-0.5 * interior[1], 0.5 * interior[1]),
            }
        else:
            cell = [0.0, 0.0, 0.0]
            for i in range(self.dimensions):
                cell[i] = interior[i] + 2.0 * self.pml_thickness
            self.cell_size = mp.Vector3(*cell)
            self.allowed_bounds = {
                axis: (-0.5 * interior[i], 0.5 * interior[i])
                for i, axis in enumerate(axes)
            }
        self.boundary_layers = [mp.PML(thickness=self.pml_thickness)]
        self.predicted = {
            "wavelength_ref_nm": self.wavelength_nm,
            "omega_ref_cminv": 1.0e7 / self.wavelength_nm,
        }
        self._warn_if_coarse()
