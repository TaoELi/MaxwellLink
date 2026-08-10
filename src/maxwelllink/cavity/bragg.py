# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
A quarter-wave Bragg (DBR) cavity builder for Meep.
"""

import warnings

import numpy as np
import meep as mp
from scipy.special import jn_zeros

from .dummy_cavity import DummyCavity, CYLINDRICAL

# an Er point source exactly on the axis of a cylindrical cell is numerically
# broken (https://github.com/NanoComp/meep/issues/2704), so near-axis dipoles
# are shifted off the axis by this many grid points (the Meep tutorial value)
OFF_AXIS_SHIFT_PX = 1.5


class BraggResonator(DummyCavity):
    """
    A quarter-wave Bragg (DBR) cavity in 1, 2, or 3 dimensions or
    in cylindrical coordinates.

    The mirrors are quarter-wave dielectric stacks: alternating layers of
    high (``n_hi``) and low (``n_lo``) refractive index, each one quarter of
    the design wavelength thick inside its medium.

    Examples
    --------
    >>> from maxwelllink.cavity import BraggResonator
    >>> cav = BraggResonator(omega=2320.0, units="cm-1", n_pairs=10,
    ...                      n_hi=2.0, n_lo=1.0, dimensions=1)
    >>> print(cav.summary())
    """

    def __init__(
        self,
        omega: float,
        units: str = "cm-1",
        n_pairs: int = 3,
        n_hi: float = 2.0,
        n_lo: float = 1.0,
        n_defect: float = 1.0,
        defect_order: int = 1,
        dimensions: int = 1,
        mirror_shape: str = "auto",
        lateral_size_nm: float = None,
        lateral_boundary: str = "pml",
        resolution: float = None,
        pml_nm: float = None,
    ):
        """
        Initialize the parameters of a quarter-wave Bragg (DBR) cavity.

        Parameters
        ----------
        omega : float
            Target cavity resonance in ``units``.
        units : str, default: "cm-1"
            Units of ``omega``: "cm-1", "eV", "au", "nm", or "um".
        n_pairs : int, default: 3
            Number of quarter-wave layer pairs per mirror (the Q dial).
        n_hi : float, default: 2.0
            High refractive index of the mirror stack (``n_hi > n_lo``).
        n_lo : float, default: 1.0
            Low refractive index of the mirror stack.
        n_defect : float, default: 1.0
            Refractive index of the defect gap between the mirrors.
        defect_order : int, default: 1
            The gap has an optical length of ``defect_order`` half
            wavelengths; for ring mirrors, the core boundary sits at the
            ``defect_order``-th zero of J0 instead.
        dimensions : int, default: 1
            1, 2, or 3 (layer stack along x), or ``mxl.CYLINDRICAL``
            (the (r, z) half plane; ``m = 0`` by default).
        mirror_shape : str, default: "auto"
            ``"planar"`` for flat mirror stacks (along x in Cartesian cells,
            disks along z in cylindrical ones) or ``"cylindrical"`` for
            concentric ring mirrors around the z axis (cylindrical cells
            only). ``"auto"`` resolves to ``"cylindrical"`` for
            ``dimensions=mxl.CYLINDRICAL`` and ``"planar"`` otherwise.
        lateral_size_nm : float or None, optional
            Extent (nm) of the allowed region along the directions parallel
            to the mirrors: y (and z) in 2D/3D, the cavity radius for
            cylindrical cells with planar (disk) mirrors, or the cell height
            along z for ring mirrors. Default: 5 cavity wavelengths. Must be
            omitted in 1D.
        lateral_boundary : str, default: "pml"
            ``"periodic"`` for an infinite planar cavity (Bloch-periodic
            boundaries) or ``"pml"`` for absorbing lateral boundaries
            (the only option for cylindrical cells).
        resolution : float or None, optional
            Meep resolution. Default: at least 20 pixels per wavelength in the
            densest medium and 8 pixels across the thinnest layer.
        pml_nm : float or None, optional
            PML thickness in nm. Default: one cavity wavelength.
        """

        # -------------- input checks --------------
        if n_hi <= n_lo:
            raise ValueError("n_hi must be larger than n_lo for a Bragg mirror.")
        if min(n_hi, n_lo, n_defect) <= 0:
            raise ValueError("Refractive indexes must be positive.")
        if int(n_pairs) < 1:
            raise ValueError("n_pairs must be at least 1.")
        if int(defect_order) < 1:
            raise ValueError("defect_order must be a positive integer.")
        if mirror_shape not in ("auto", "planar", "cylindrical"):
            raise ValueError("mirror_shape must be 'auto', 'planar', or 'cylindrical'.")
        if lateral_boundary not in ("periodic", "pml"):
            raise ValueError("lateral_boundary must be 'periodic' or 'pml'.")
        if int(dimensions) == 1 and lateral_size_nm is not None:
            warnings.warn("lateral_size_nm has no meaning in a 1D cavity.")
        if int(dimensions) == CYLINDRICAL and lateral_boundary == "periodic":
            raise ValueError(
                "A cylindrical cell has an absorbing side boundary; use "
                "lateral_boundary='pml'."
            )
        if mirror_shape == "cylindrical" and int(dimensions) != CYLINDRICAL:
            raise ValueError(
                "mirror_shape='cylindrical' requires dimensions=mxl.CYLINDRICAL."
            )
        if mirror_shape == "auto":
            mirror_shape = "cylindrical" if int(dimensions) == CYLINDRICAL else "planar"

        # default attributes (units, grid, hotspot, ...), overridden below
        super().__init__(omega=omega, units=units, dimensions=dimensions)
        lam = self.nm_to_meep(self.wavelength_nm)  # cavity wavelength in um

        self.n_pairs = int(n_pairs)
        self.n_hi = float(n_hi)
        self.n_lo = float(n_lo)
        self.n_defect = float(n_defect)
        self.defect_order = int(defect_order)
        self.mirror_shape = mirror_shape
        self.lateral_boundary = lateral_boundary

        # -------------- the quarter-wave layer stack (Meep units: um) --------------
        # quarter-wave mirror layers (n * t = lambda / 4) around a defect gap
        # of optical length defect_order half wavelengths
        t_hi = 0.25 * lam / self.n_hi
        t_lo = 0.25 * lam / self.n_lo
        t_gap = 0.5 * lam * self.defect_order / self.n_defect
        # default PML thickness: one design wavelength
        self.pml_thickness = self.nm_to_meep(pml_nm) if pml_nm is not None else lam
        pml = self.pml_thickness

        cylindrical = self.dimensions == CYLINDRICAL
        ring_mirrors = cylindrical and self.mirror_shape == "cylindrical"
        if ring_mirrors:
            # ring mirrors: a defect core surrounded by concentric
            # quarter-wave shells. The confined core field is Ez ~ J0(n k r)
            # with a node at the mirror surface, so the requested resonance
            # sits at the defect_order-th zero of J0; the quarter-wave shell
            # thicknesses are asymptotically (planar-wave) correct away from
            # the axis.
            zeros_j0 = jn_zeros(0, self.defect_order + 1)
            j0_defect = float(zeros_j0[self.defect_order - 1])
            r_core = j0_defect * lam / (2.0 * np.pi * self.n_defect)
            self.core_radius = r_core
            indexes = np.array([self.n_defect] + [self.n_hi, self.n_lo] * self.n_pairs)
            thicknesses = np.array([r_core] + [t_hi, t_lo] * self.n_pairs)
            # the mirror proper ends here; the outermost (low-index) shell
            # continues through one wavelength of radial clearance (hosting
            # the ring source and collection surface of the scattering
            # probe) and through the PML
            self.mirror_outer_radius = float(np.sum(thicknesses))
            thicknesses[-1] += lam + pml
            # the stack grows outward from the axis at r = 0
            centers = np.cumsum(thicknesses) - 0.5 * thicknesses
        else:
            indexes = np.array(
                [self.n_lo, self.n_hi] * self.n_pairs
                + [self.n_defect]
                + [self.n_hi, self.n_lo] * self.n_pairs
            )
            thicknesses = np.array(
                [t_lo, t_hi] * self.n_pairs + [t_gap] + [t_hi, t_lo] * self.n_pairs
            )
            # extend the outermost (low-index) layers through the PML
            thicknesses[0] += pml
            thicknesses[-1] += pml
            # center the stack so that the defect gap center sits at the origin
            length = float(np.sum(thicknesses))
            centers = np.cumsum(thicknesses) - 0.5 * thicknesses - 0.5 * length

        self.layer_indexes = indexes
        self.layer_thicknesses = thicknesses
        self.layer_centers = centers
        # one block per layer, spanning the full extent of the other axes; the
        # stack runs along x in Cartesian cells (and along r = x for ring
        # mirrors), and along z for planar disk mirrors in cylindrical cells
        if cylindrical and not ring_mirrors:
            self.geometry = [
                mp.Block(
                    size=mp.Vector3(mp.inf, mp.inf, float(t)),
                    center=mp.Vector3(0.0, 0.0, float(c)),
                    material=mp.Medium(index=float(n)),
                )
                for t, c, n in zip(thicknesses, centers, indexes)
            ]
        else:
            self.geometry = [
                mp.Block(
                    size=mp.Vector3(float(t), mp.inf, mp.inf),
                    center=mp.Vector3(float(c), 0.0, 0.0),
                    material=mp.Medium(index=float(n)),
                )
                for t, c, n in zip(thicknesses, centers, indexes)
            ]

        # -------------- cell size and boundaries --------------
        if ring_mirrors:
            # the (r, z) half plane: concentric shells around the axis at
            # r = 0, with PML at the outer radial edge; z is the open lateral
            # direction, terminated by PML at both ends
            z_size = (
                self.nm_to_meep(lateral_size_nm)
                if lateral_size_nm is not None
                else 5.0 * lam
            )
            r_total = float(np.sum(thicknesses))
            self.cell_size = mp.Vector3(r_total, 0.0, z_size + 2.0 * pml)
            self.boundary_layers = [
                mp.PML(thickness=pml, direction=mp.Z),
                mp.PML(thickness=pml, direction=mp.R, side=mp.High),
            ]
            self.k_point = None
            # the confined mode is azimuthally symmetric and z-polarized on
            # the axis, so on-axis molecules couple in the m = 0 sector
            self.m = 0
            self.allowed_bounds = {
                "x": (0.0, r_core),  # x plays the role of r
                "z": (-0.5 * z_size, 0.5 * z_size),
            }
        elif cylindrical:
            # the (r, z) half plane: mirrors are disks stacked along z, and
            # r spans [0, R] with the axis at r = 0 and PML at the outer edge
            r_size = (
                self.nm_to_meep(lateral_size_nm)
                if lateral_size_nm is not None
                else 5.0 * lam
            )
            self.cell_size = mp.Vector3(r_size + pml, 0.0, length)
            self.boundary_layers = [
                mp.PML(thickness=pml, direction=mp.Z),
                mp.PML(thickness=pml, direction=mp.R, side=mp.High),
            ]
            self.k_point = None
            # Use the azimuthally symmetric sector unless make_simulation()
            # receives an explicit m value.
            self.m = 0
            self.allowed_bounds = {
                "x": (0.0, r_size),  # x plays the role of r
                "z": (-0.5 * t_gap, 0.5 * t_gap),
            }
        else:
            self.boundary_layers = [mp.PML(thickness=pml, direction=mp.X)]
            self.k_point = None
            self.allowed_bounds = {"x": (-0.5 * t_gap, 0.5 * t_gap)}
            self.cell_size = mp.Vector3(length, 0.0, 0.0)
            if self.dimensions > 1:
                # lateral extent of the allowed region (default: five
                # wavelengths)
                t_size = (
                    self.nm_to_meep(lateral_size_nm)
                    if lateral_size_nm is not None
                    else 5.0 * lam
                )
                if lateral_boundary == "periodic":
                    cell_t = t_size
                    self.k_point = mp.Vector3()  # Bloch-periodic boundaries
                else:  # "pml": pad the cell and absorb in the lateral directions
                    cell_t = t_size + 2.0 * pml
                    self.boundary_layers.append(mp.PML(thickness=pml, direction=mp.Y))
                    if self.dimensions == 3:
                        self.boundary_layers.append(
                            mp.PML(thickness=pml, direction=mp.Z)
                        )
                self.allowed_bounds["y"] = (-0.5 * t_size, 0.5 * t_size)
                self.cell_size = mp.Vector3(length, cell_t, 0.0)
                if self.dimensions == 3:
                    self.allowed_bounds["z"] = (-0.5 * t_size, 0.5 * t_size)
                    self.cell_size = mp.Vector3(length, cell_t, cell_t)

        # -------------- grid resolution --------------
        # default: at least 20 px per wavelength in the densest medium and
        # 8 px across the thinnest layer
        t_min = min(t_hi, t_lo, r_core) if ring_mirrors else min(t_hi, t_lo, t_gap)
        n_max = max(self.n_hi, self.n_lo, self.n_defect)
        if resolution is not None:
            self.resolution = float(resolution)
        else:
            self.resolution = float(np.ceil(max(20.0 * n_max / lam, 8.0 / t_min)))

        # -------------- analytic estimates --------------
        # textbook thin-film estimates (Macleod, Thin-Film Optical Filters);
        # for ring mirrors they are asymptotic planar-wave approximations
        admittance = self.n_lo * (self.n_hi / self.n_lo) ** (2 * self.n_pairs)
        reflectance = ((self.n_defect - admittance) / (self.n_defect + admittance)) ** 2
        finesse = np.pi * np.sqrt(reflectance) / (1.0 - reflectance)
        omega_cminv = 1.0e7 / self.wavelength_nm
        if ring_mirrors:
            # the radial J0 standing wave holds j0/pi half cycles inside the
            # core, plus the mirror penetration; consecutive J0 zeros set the
            # radial free spectral range
            m_eff = j0_defect / np.pi + 1.0 / (self.n_hi - self.n_lo)
            j0_next = float(zeros_j0[self.defect_order])
            fsr_cminv = omega_cminv * (j0_next - j0_defect) / j0_defect
        else:
            # mirror penetration makes the effective gap hold m_eff (not
            # defect_order) half wavelengths; then Q = m_eff * finesse
            m_eff = self.defect_order + 1.0 / (self.n_hi - self.n_lo)
            fsr_cminv = omega_cminv / m_eff
        quality_factor = m_eff * finesse
        self.predicted = {
            "omega_cminv": omega_cminv,
            "wavelength_nm": self.wavelength_nm,
            "mirror_reflectance": float(reflectance),
            "quality_factor": float(quality_factor),
            "kappa_cminv": float(omega_cminv / quality_factor),
            "fsr_cminv": float(fsr_cminv),
        }
        if ring_mirrors:
            self.predicted["core_radius_nm"] = self.meep_to_nm(r_core)
        self._warn_if_coarse(n_max=n_max, t_min=t_min)

    # -------------- light-induced measurements --------------

    def optical_setup(self):
        """
        Optical setup of the Bragg cavity.

        Planar mirrors use the generic transmission planes of
        ``DummyCavity.optical_setup``.

        Cylindrical (ring) mirrors use the dark-field-type scattering probe
        (cf. ``NPoM.optical_setup``): an incoming cylindrical wave
        from a ring source in the radial clearance outside the mirrors
        drives the m = 0 mode.

        In both cases the reference structure is a homogeneous ``n_lo``
        medium (for the default ``n_lo = 1``: vacuum).
        """

        if self.mirror_shape == "cylindrical":
            lam = self.nm_to_meep(self.wavelength_nm)
            pml = self.pml_thickness
            margin = 2.0 / self.resolution  # two grid points off the PML
            z_box = 0.5 * self.cell_size.z - pml - margin
            # the collection box sits at the inner edge of the clearance and
            # the ring source between the box and the radial PML
            r_wall = self.mirror_outer_radius + 0.25 * lam
            r_source = self.mirror_outer_radius + 0.6 * lam
            surface = [
                mp.FluxRegion(  # the lid, the floor, and the outer wall
                    center=mp.Vector3(0.5 * r_wall, 0.0, z_box),
                    size=mp.Vector3(r_wall, 0.0, 0.0),
                    direction=mp.Z,
                    weight=+1.0,
                ),
                mp.FluxRegion(
                    center=mp.Vector3(0.5 * r_wall, 0.0, -z_box),
                    size=mp.Vector3(r_wall, 0.0, 0.0),
                    direction=mp.Z,
                    weight=-1.0,
                ),
                mp.FluxRegion(
                    center=mp.Vector3(r_wall, 0.0, 0.0),
                    size=mp.Vector3(0.0, 0.0, 2.0 * z_box),
                    direction=mp.R,
                    weight=+1.0,
                ),
            ]
            return {
                "probe": "scattering",
                "excitation": {
                    "center": mp.Vector3(r_source, 0.0, 0.0),
                    "size": mp.Vector3(0.0, 0.0, 2.0 * z_box),
                },
                "component": mp.Ez,
                "detectors": {
                    # the collection surface is already a closed box, so it
                    # doubles as the absorption box (separate monitors: only
                    # the scattered one is incident-subtracted)
                    "scattered": surface,
                    "absorption_box": list(surface),
                },
                # |E_inc|^2 over a short r-line at the core center (zero-size
                # DFT monitors are unreliable in cylindrical cells)
                "normalization": {
                    "center": self.hotspot_center,
                    "size": mp.Vector3(2.0 / self.resolution, 0.0, 0.0),
                },
                "reference_geometry": [
                    mp.Block(
                        size=mp.Vector3(mp.inf, mp.inf, mp.inf),
                        material=mp.Medium(index=self.n_lo),
                    )
                ],
                # watch the ringdown of the stored mode at the core center
                "decay_monitor": self.hotspot_center,
            }

        setup = super().optical_setup()
        setup["reference_geometry"] = [
            mp.Block(
                size=mp.Vector3(mp.inf, mp.inf, mp.inf),
                material=mp.Medium(index=self.n_lo),
            )
        ]
        return setup

    def emission_setup(self, offset_nm=(0.0, 0.0, 0.0), component=None):
        """
        Local-dipole (Purcell) probe of the Bragg cavity: a dipole in the
        defect gap, polarized parallel to the mirror planes, read out through
        one plane outside each mirror.

        The reference is the homogeneous defect medium (``n_defect``),
        so the LDOS ratio is exact. Same keys as ``DummyCavity.emission_setup``.

        Notes
        -----
        Cylindrical cells with the default ring mirrors use an on-axis
        z-polarized dipole (m = 0), which couples to the confined Ez mode
        and is regular on the axis.

        Cylindrical cells with planar (disk) mirrors default to an
        azimuthally symmetric (m = 0) ring of radial dipole; for the m = +-1
        near-axis dipole, pass ``component=mp.Er`` together with ``m=1``.

        Parameters
        ----------
        offset_nm : sequence of three floats, default: (0, 0, 0)
            Displacement (nm) of the dipole from the defect center.
        component : Meep field component or None, optional
            Dipole orientation. Default: ``mp.Ez`` (parallel to the mirrors)
            in Cartesian cells and for cylindrical ring mirrors, ``mp.Er``
            for cylindrical disk mirrors.
        """

        if self.dimensions == CYLINDRICAL:
            # a closed box just inside the PML: the two end disks (axial)
            # plus the outer side wall (lateral)
            box = self._emission_box_regions()
            center = self.hotspot_center + self._offset_to_meep(offset_nm)
            if component is None and self.mirror_shape == "cylindrical":
                # ring mirrors confine a z-polarized (m = 0) mode on the
                # axis, where an Ez point dipole is regular
                component = mp.Ez
            elif component is None:
                component = mp.Er
                if center.x == 0.0:
                    # the default m = 0 radial dipole is a ring one design
                    # wavelength off axis, clamped inside small-radius cells
                    lam = self.nm_to_meep(self.wavelength_nm)
                    center += mp.Vector3(
                        min(lam, 0.5 * self.allowed_bounds["x"][1]), 0.0, 0.0
                    )
            elif component == mp.Er and center.x == 0.0:
                # the near-axis transverse dipole of the m = +-1 sectors,
                # shifted off the singular axis (run it with m=1;
                # make_simulation rejects it at m=0)
                center += mp.Vector3(OFF_AXIS_SHIFT_PX / self.resolution, 0.0, 0.0)
            return {
                "excitation": {"center": center, "size": mp.Vector3()},
                "component": component,
                "detectors": {
                    "radiated": box,
                    "axial": box[:2],
                    "lateral": box[2:],
                },
                "reference_geometry": [
                    mp.Block(
                        size=mp.Vector3(mp.inf, mp.inf, mp.inf),
                        material=mp.Medium(index=self.n_defect),
                    )
                ],
                "reference_surface": box,
                # watch the ringdown at the lid, away from the dipole
                "decay_monitor": box[0].center,
            }

        # Cartesian cells: one flux plane outside each mirror, along x
        pml = self.pml_thickness
        x_left = -0.5 * self.cell_size.x + pml  # inner edge of the left PML
        x_right = 0.5 * self.cell_size.x - pml  # inner edge of the right PML
        # plane spacing: three grid points, capped for coarse grids (the same
        # convention as the transmission planes)
        spacing = min(3.0 / self.resolution, (x_right - x_left) / 8.0)
        transverse = mp.Vector3(0.0, self.cell_size.y, self.cell_size.z)
        planes = [
            mp.FluxRegion(  # outward normals: the left plane counts down
                center=mp.Vector3(x_right - spacing),
                size=transverse,
                direction=mp.X,
                weight=+1.0,
            ),
            mp.FluxRegion(
                center=mp.Vector3(x_left + spacing),
                size=transverse,
                direction=mp.X,
                weight=-1.0,
            ),
        ]
        return {
            "excitation": {
                "center": self.hotspot_center + self._offset_to_meep(offset_nm),
                "size": mp.Vector3(),
            },
            "component": component if component is not None else mp.Ez,
            "detectors": {"radiated": planes},
            "reference_geometry": [
                mp.Block(
                    size=mp.Vector3(mp.inf, mp.inf, mp.inf),
                    material=mp.Medium(index=self.n_defect),
                )
            ],
            "reference_surface": planes,
            # watch the ringdown at the radiated plane, away from the dipole
            "decay_monitor": mp.Vector3(x_right - spacing),
        }

    # -------------- simulation assembly --------------

    def make_simulation(
        self,
        molecules=None,
        hub=None,
        sources=None,
        extra_geometry=(),
        **meep_kwargs,
    ):
        """
        Build the simulation as ``DummyCavity.make_simulation`` does, after
        checking that a cylindrical run is consistent with its azimuthal
        sector ``m`` (see ``_check_cylindrical_sector``).

        Cartesian cells pass straight through.
        """

        if self.dimensions == CYLINDRICAL:
            m = meep_kwargs.get("m", self.m if self.m is not None else 0)
            self._check_cylindrical_sector(m, sources, extra_geometry)
        return super().make_simulation(
            molecules=molecules,
            hub=hub,
            sources=sources,
            extra_geometry=extra_geometry,
            **meep_kwargs,
        )

    def _check_cylindrical_sector(self, m, sources, extra_geometry):
        """
        Reject configurations inconsistent with the azimuthal sector m.

        An on-axis transverse (``Er``/``Ep``) point dipole exists only at
        m = +-1, and an on-axis ``Ez`` one only at m = 0.

        m != 0 forces complex fields, so molecular regions must be built with
        ``real_field_only=False``.
        """

        for source in sources or ():
            if source.size.norm() != 0.0:
                continue  # extended sources (e.g. the transmission plane)
            near_axis = source.center.x < (OFF_AXIS_SHIFT_PX + 1.0) / self.resolution
            if source.component in (mp.Er, mp.Ep) and near_axis and m == 0:
                raise ValueError(
                    "A near-axis transverse dipole belongs to the m = +-1 "
                    "sectors; run it with m=1, e.g. "
                    "purcell(..., component=mp.Er, m=1)."
                )
            if source.component == mp.Ez and source.center.x == 0.0 and m != 0:
                raise ValueError(
                    "An on-axis z dipole is azimuthally symmetric; run it at m=0."
                )
        if m != 0:
            for shape in list(extra_geometry):
                material = getattr(shape, "material", None)
                for sus in getattr(material, "E_susceptibilities", None) or []:
                    if getattr(sus, "real_field_only", False):
                        raise ValueError(
                            "m != 0 runs use complex fields; rebuild the "
                            "molecular region with "
                            "place_region(..., real_field_only=False)."
                        )
