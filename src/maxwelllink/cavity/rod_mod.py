# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
A periodic plasmonic rod-on-mirror cavity with a molecular annulus.
"""

import math
import warnings

import meep as mp

from .dummy_cavity import CYLINDRICAL, DummyCavity

# Geometry used by the plasmonic-water reference simulations. Lengths are nm.
RADIUS_NM = 280.0
LATTICE_GAP_NM = 500.0
ROD_HEIGHT_NM = 100.0
MIRROR_NM = 200.0
SUBSTRATE_NM = 40.0
ADHESION_NM = 4.0  # Cr adhesion layer off by default (4 nm in literature)
FILM_NM = 200.0
AIR_NM = 200.0
ANNULUS_WIDTH_NM = 50.0
EXTRA_HEIGHT_NM = 23.0
SECOND_ANNULUS_WIDTH_NM = 30.0
TOP_PML_NM = 800.0
BOTTOM_PML_NM = 200.0
RADIAL_PADDING_NM = 500.0
RADIAL_PML_NM = 800.0
RESOLUTION = 125.0
SOURCE_AMPLITUDE = 1.0e2


class PlasmonicRod(DummyCavity):
    """
    A gold-cylinder plasmonic cavity with a molecular annulus on the side and top.

    Geometries adapted from: Brawley et al. Nat. Chem. 17, 439–447 (2025).
    https://doi.org/10.1038/s41557-024-01723-6

    In 3D, one cylinder occupies a square, Bloch-periodic unit cell in the
    xy plane, with PML only along z. The cylindrical path is a rotationally
    symmetric, effective-radius approximation on the (r, z) half plane with
    radial and z-directed PML; it does not reproduce the square-periodic 3D
    boundary exactly. The transverse plasmon belongs to an ``m = +1`` or
    ``m = -1`` sector, rather than the z-polarized ``m = 0`` sector used by
    :class:`NPoM`.

    Molecular matter occupies an annulus around the metal cylinder. Repeated
    calls to :meth:`place_molecule` place localized molecules in this annulus
    in 3D. :meth:`place_region` creates a continuous socket-susceptibility
    annulus in either 3D or cylindrical coordinates.

    A localized off-axis molecule breaks rotational symmetry and cannot be
    represented by one cylindrical Fourier sector. Consequently, the
    cylindrical path supports the continuous annular region, while discrete
    annular molecules require the 3D path.

    Examples
    --------
    Continuous cylindrical molecular medium:

    >>> from maxwelllink.cavity import PlasmonicRod
    >>> cav = PlasmonicRod()
    >>> region = cav.place_region(hub=hub, real_field_only=False)
    >>> sim = cav.make_simulation(hub=hub, extra_geometry=[region])

    Multiple localized molecules in the 3D unit cell:

    >>> cav = PlasmonicRod(dimensions=3)
    >>> molecules = [
    ...     cav.place_molecule(hub=hub, hotspot="x_plus"),
    ...     cav.place_molecule(hub=hub, hotspot="x_minus"),
    ... ]
    >>> sim = cav.make_simulation(hub=hub, molecules=molecules)
    """

    def __init__(
        self,
        radius_nm: float = RADIUS_NM,
        lattice_gap_nm: float = LATTICE_GAP_NM,
        rod_height_nm: float = ROD_HEIGHT_NM,
        mirror_nm: float = MIRROR_NM,
        substrate_nm: float = SUBSTRATE_NM,
        adhesion_nm: float = ADHESION_NM,
        film_nm: float = FILM_NM,
        air_nm: float = AIR_NM,
        annulus_width_nm: float = ANNULUS_WIDTH_NM,
        second_annulus_width_nm: float = SECOND_ANNULUS_WIDTH_NM,
        extra_height_nm: float = EXTRA_HEIGHT_NM,
        background_index: float = 1.7,
        omega_ref: float = 3550.0,
        units: str = "cm-1",
        material=None,
        adhesion_material=None,
        substrate_material=None,
        dimensions: int = CYLINDRICAL,
        m: int = -1,
        polarization: str = "y",
        source_amplitude: float = SOURCE_AMPLITUDE,
        resolution: float = RESOLUTION,
        top_pml_nm: float = TOP_PML_NM,
        bottom_pml_nm: float = BOTTOM_PML_NM,
        radial_padding_nm: float = RADIAL_PADDING_NM,
        radial_pml_nm: float = RADIAL_PML_NM,
        cell_radius_mode: str = "equal-area",
        monitor_radius_nm: float = None,
    ):
        """
        Initialize the plasmonic cylinder-on-mirror cavity.

        Parameters
        ----------
        radius_nm : float, default: 280.0
            Radius of the gold cylinder.
        lattice_gap_nm : float, default: 500.0
            Edge-to-edge separation between neighboring cylinders. The 3D
            lattice period is ``2 * radius_nm + lattice_gap_nm``.
        rod_height_nm : float, default: 100.0
            Height of the gold cylinder.
        mirror_nm : float, default: 200.0
            Thickness of the bottom gold mirror.
        substrate_nm : float, default: 40.0
            Thickness of the Al2O3 layer above the mirror.
        adhesion_nm : float, default: 4.0
            Thickness of the Cr adhesion layer beneath the cylinder.
        film_nm : float, default: 200.0
            Height of the dielectric film containing the cylinder. It must be
            thicker than ``rod_height_nm + adhesion_nm``.
        air_nm : float, default: 200.0
            Vacuum height between the film and the top PML.
        annulus_width_nm : float, default: 50.0
            Default radial width of first molecular matter around the cylinder.
        second_annulus_width_nm : float, default: 30.0
            Default radial width of second molecular matter above the cylinder and besides the first molecular matter.
        extra_height_nm : float, default: 23.0
            Height difference between the top of the annulus and the top of the rod.
        background_index : float, default: 1.7
            Refractive index of the nonresonant film.
        omega_ref : float, default: 3550.0
            Reference frequency (or wavelength) in ``units``.
        units : str, default: "cm-1"
            Units of ``omega_ref``: "cm-1", "eV", "au", "nm", or "um".
        material : mp.Medium or None, optional
            Cylinder and mirror material. Default: ``meep.materials.Au``.
        adhesion_material : mp.Medium or None, optional
            Adhesion material. Default: ``meep.materials.Cr``.
        substrate_material : mp.Medium or None, optional
            Substrate material. Default: ``meep.materials.Al2O3_aniso``.
        dimensions : int, default: mxl.CYLINDRICAL
            ``mxl.CYLINDRICAL`` for the (r, z) reduction or 3 for the full
            periodic unit cell.
        m : int, default: -1
            Cylindrical azimuthal sector, ``+1`` or ``-1``. Ignored in 3D.
        polarization : {"x", "y"}, default: "y"
            Transverse incident polarization. The two choices are degenerate
            in the cylindrical geometry but select Ex or Ey in 3D.
        source_amplitude : float, default: 1e2
            Gaussian probe amplitude used by :meth:`linear_spectrum`. The
            large default follows the finite-temperature LAMMPS-water
            amplitude ladder and suppresses thermal-emission noise relative
            to the driven response. It can also be overridden for one
            measurement with ``linear_spectrum(..., source_amplitude=...)``.
        resolution : float, default: 125.0
            Meep pixels per micrometer.
        top_pml_nm, bottom_pml_nm : float
            Top and bottom z-directed PML thicknesses.
        radial_padding_nm : float, default: 500.0
            Cylindrical clearance between the effective unit-cell radius and
            the radial PML.
        radial_pml_nm : float, default: 800.0
            Cylindrical outer radial PML thickness.
        cell_radius_mode : {"equal-area", "half-period", "manual"}
            Effective unit-cell radius used by the cylindrical flux monitor.
            ``"equal-area"`` preserves the area of the square 3D unit cell.
        monitor_radius_nm : float or None, optional
            Effective cylindrical radius when ``cell_radius_mode="manual"``.
        """

        if dimensions not in (CYLINDRICAL, 3):
            raise ValueError("dimensions must be 3 or CYLINDRICAL.")
        if dimensions == CYLINDRICAL and (int(m) not in (-1, 1) or float(m) != int(m)):
            raise ValueError("A cylindrical PlasmonicRod requires m=+1 or m=-1.")
        polarization = str(polarization).lower()
        if polarization not in ("x", "y"):
            raise ValueError("polarization must be 'x' or 'y'.")

        lengths = {
            "radius_nm": radius_nm,
            "lattice_gap_nm": lattice_gap_nm,
            "rod_height_nm": rod_height_nm,
            "mirror_nm": mirror_nm,
            "substrate_nm": substrate_nm,
            "film_nm": film_nm,
            "air_nm": air_nm,
            "annulus_width_nm": annulus_width_nm,
            "extra_height_nm": extra_height_nm,
            "top_pml_nm": top_pml_nm,
            "bottom_pml_nm": bottom_pml_nm,
        }
        if any(float(value) <= 0.0 for value in lengths.values()):
            raise ValueError(f"All cavity lengths must be positive; got {lengths}.")
        # The Cr adhesion layer is optional, so it may be 0, but never negative.
        if float(adhesion_nm) < 0.0:
            raise ValueError("adhesion_nm must be >= 0 (use 0 to remove the Cr layer).")
        if float(film_nm) <= float(rod_height_nm) + float(adhesion_nm):
            raise ValueError(
                "film_nm must exceed rod_height_nm + adhesion_nm so the "
                "cylinder is embedded below the top of the film."
            )
        if float(extra_height_nm) > (float(film_nm) - float(rod_height_nm) - float(adhesion_nm)):
            raise ValueError(
                "extra_height_nm cannot exceed the available film height "
                "above the gold rod."
            )
        if float(background_index) < 1.0:
            raise ValueError("background_index must be at least 1.")
        if float(resolution) <= 0.0:
            raise ValueError("resolution must be positive.")
        source_amplitude = float(source_amplitude)
        if not math.isfinite(source_amplitude) or source_amplitude == 0.0:
            raise ValueError("source_amplitude must be finite and nonzero.")

        super().__init__(omega=omega_ref, units=units, dimensions=dimensions)

        self.radius_nm = float(radius_nm)
        self.lattice_gap_nm = float(lattice_gap_nm)
        self.period_nm = 2.0 * self.radius_nm + self.lattice_gap_nm
        self.rod_height_nm = float(rod_height_nm)
        self.mirror_nm = float(mirror_nm)
        self.substrate_nm = float(substrate_nm)
        self.adhesion_nm = float(adhesion_nm)
        self.film_nm = float(film_nm)
        self.air_nm = float(air_nm)
        self.annulus_width_nm = float(annulus_width_nm)
        self.second_annulus_width_nm = float(second_annulus_width_nm)
        self.extra_height_nm = float(extra_height_nm)
        self.background_index = float(background_index)
        self.polarization = polarization
        self.source_amplitude = source_amplitude
        self.resolution = float(resolution)

        from meep.materials import Al2O3_aniso, Au, Cr

        self.material = Au if material is None else material
        self.adhesion_material = Cr if adhesion_material is None else adhesion_material
        self.substrate_material = (
            Al2O3_aniso if substrate_material is None else substrate_material
        )
        self.background_material = mp.Medium(index=self.background_index)

        radius = self.nm_to_meep(self.radius_nm)
        period = self.nm_to_meep(self.period_nm)
        rod_height = self.nm_to_meep(self.rod_height_nm)
        mirror = self.nm_to_meep(self.mirror_nm)
        substrate = self.nm_to_meep(self.substrate_nm)
        adhesion = self.nm_to_meep(self.adhesion_nm)
        film = self.nm_to_meep(self.film_nm)
        air = self.nm_to_meep(self.air_nm)
        annulus_width = self.nm_to_meep(self.annulus_width_nm)
        second_annulus_width = self.nm_to_meep(self.second_annulus_width_nm)
        extra_height = self.nm_to_meep(self.extra_height_nm)
        top_pml = self.nm_to_meep(top_pml_nm)
        bottom_pml = self.nm_to_meep(bottom_pml_nm)

        self.top_pml = top_pml
        self.bottom_pml = bottom_pml
        self.pml_thickness = top_pml
        self.film_above_rod = film - rod_height - adhesion
        span_z = bottom_pml + mirror + substrate + film + air + top_pml

        film_top = 0.5 * span_z - top_pml - air
        film_bottom = film_top - film
        rod_bottom = film_bottom + adhesion
        rod_top = rod_bottom + rod_height

        # Extra molecular region above the top of the gold rod
        self.extra_height = extra_height

        # First annulus
        self.annulus_bottom_z = film_bottom
        self.annulus_top_z = rod_top + self.extra_height
        self.annulus_height = self.annulus_top_z - self.annulus_bottom_z
        self.annulus_center_z = 0.5 * (self.annulus_bottom_z + self.annulus_top_z)

        # Second annulus
        self.second_annulus_bottom_z = rod_top
        self.second_annulus_top_z = self.annulus_top_z
        self.second_annulus_height = self.extra_height
        self.second_annulus_center_z = 0.5 * (self.second_annulus_bottom_z + self.second_annulus_top_z)
        self.second_annulus_inner_radius = (radius - second_annulus_width)
        self.second_annulus_outer_radius = radius
        self.second_annulus_width = second_annulus_width #this is susceptible to modifications depending on the grid points
        if self.second_annulus_inner_radius < 0.0:
            raise ValueError(
                "second_annulus_width_nm cannot exceed radius_nm."
            )
        if float(second_annulus_width_nm) < 0.0:
            raise ValueError(
                "second_annulus_width_nm must be >= 0 "
                "(use 0 to remove the second annulus)."
            )

        z_film = film_top - 0.5 * film
        z_rod = rod_bottom + 0.5 * rod_height
        z_adhesion = film_bottom + 0.5 * adhesion
        z_substrate = film_bottom - 0.5 * substrate
        z_mirror = film_bottom - substrate - 0.5 * mirror

        def transverse_block(block_material, height, center_z):
            if self.dimensions == CYLINDRICAL:
                return mp.Block(
                    material=block_material,
                    size=mp.Vector3(self.cell_size.x, 0.0, height),
                    center=mp.Vector3(0.5 * self.cell_size.x, 0.0, center_z),
                )
            return mp.Block(
                material=block_material,
                size=mp.Vector3(mp.inf, mp.inf, height),
                center=mp.Vector3(0.0, 0.0, center_z),
            )

        def axial_cylinder(cylinder_material, cylinder_radius, height, center_z):
            if self.dimensions == CYLINDRICAL:
                # This is the radial_block convention of the reference input:
                # r spans [0, radius] on the cylindrical half plane.
                return mp.Block(
                    material=cylinder_material,
                    size=mp.Vector3(cylinder_radius, 0.0, height),
                    center=mp.Vector3(0.5 * cylinder_radius, 0.0, center_z),
                )
            return mp.Cylinder(
                material=cylinder_material,
                radius=cylinder_radius,
                height=height,
                center=mp.Vector3(0.0, 0.0, center_z),
            )

        if self.dimensions == CYLINDRICAL:
            modes = ("equal-area", "half-period", "manual")
            if cell_radius_mode not in modes:
                raise ValueError(f"cell_radius_mode must be one of {modes}.")
            if cell_radius_mode == "equal-area":
                monitor_radius = period / math.sqrt(math.pi)
            elif cell_radius_mode == "half-period":
                monitor_radius = 0.5 * period
            else:
                if monitor_radius_nm is None:
                    raise ValueError(
                        "monitor_radius_nm is required for cell_radius_mode='manual'."
                    )
                monitor_radius = self.nm_to_meep(monitor_radius_nm)

            radial_padding = self.nm_to_meep(radial_padding_nm)
            radial_pml = self.nm_to_meep(radial_pml_nm)
            if radial_padding <= 0.0 or radial_pml <= 0.0:
                raise ValueError(
                    "radial_padding_nm and radial_pml_nm must be positive."
                )
            self.monitor_radius = monitor_radius
            self.source_radius = monitor_radius + radial_padding
            self.radial_padding = radial_padding
            self.radial_pml = radial_pml
            self.cell_radius_mode = cell_radius_mode
            self.cell_size = mp.Vector3(
                self.source_radius + radial_pml,
                0.0,
                span_z,
            )
            self.boundary_layers = [
                mp.PML(thickness=top_pml, direction=mp.Z, side=mp.High),
                mp.PML(thickness=bottom_pml, direction=mp.Z, side=mp.Low),
                mp.PML(thickness=radial_pml, direction=mp.R, side=mp.High),
            ]
            self.m = int(m)
        else:
            self.monitor_radius = None
            self.source_radius = None
            self.radial_padding = None
            self.radial_pml = None
            self.cell_radius_mode = None
            self.cell_size = mp.Vector3(period, period, span_z)
            self.boundary_layers = [
                mp.PML(thickness=top_pml, direction=mp.Z, side=mp.High),
                mp.PML(thickness=bottom_pml, direction=mp.Z, side=mp.Low),
            ]
            # With no x/y boundary layer, k=(0,0,0) gives normal-incidence
            # Bloch-periodic boundaries in the transverse plane.
            self.k_point = mp.Vector3()

        self._validate_annulus_width(annulus_width)

        # The molecular annulus must be inserted after the film but before
        # the metal cylinder. The split is retained by sim_kwargs below.
        self._background_geometry = [
            transverse_block(self.background_material, film, z_film)
        ]
        # The gold cylinder rests on the substrate. A thin Cr adhesion layer
        # goes between them only when its thickness is nonzero.
        self._foreground_geometry = [
            axial_cylinder(self.material, radius, rod_height, z_rod),
        ]
        self._foreground_geometry += [
            axial_cylinder(self.background_material, self.second_annulus_inner_radius, self.second_annulus_height, self.second_annulus_center_z)
        ]
        if adhesion > 0.0:
            self._foreground_geometry.append(
                axial_cylinder(self.adhesion_material, radius, adhesion, z_adhesion)
            )
        self._foreground_geometry += [
            transverse_block(self.substrate_material, substrate, z_substrate),
            transverse_block(self.material, mirror, z_mirror),
        ]
        self.geometry = self._background_geometry + self._foreground_geometry

        hotspot_radius = radius + 0.5 * annulus_width
        self.hotspot_center = mp.Vector3(
            hotspot_radius,
            0.0,
            self.annulus_center_z,
        )
        if self.dimensions == CYLINDRICAL:
            self.hotspots = {"annulus": self.hotspot_center}
            self.allowed_bounds = {
                "x": (radius, radius + annulus_width),
                "z": (self.annulus_bottom_z, self.annulus_top_z),
            }
        else:
            self.hotspots = {
                "x_plus": self.hotspot_center,
                "x_minus": mp.Vector3(-hotspot_radius, 0.0, self.annulus_center_z),
                "y_plus": mp.Vector3(0.0, hotspot_radius, self.annulus_center_z),
                "y_minus": mp.Vector3(0.0, -hotspot_radius, self.annulus_center_z),
            }
            outer = radius + annulus_width
            self.allowed_bounds = {
                "x": (-outer, outer),
                "y": (-outer, outer),
                "z": (self.annulus_bottom_z, self.annulus_top_z),
            }

        second_inner_nm = self.meep_to_nm(self.second_annulus_inner_radius)
        second_outer_nm = self.meep_to_nm(self.second_annulus_outer_radius)
        second_width_nm = self.meep_to_nm(self.second_annulus_width)
        first_annulus_volume_nm3 = float(math.pi * ((self.radius_nm + self.annulus_width_nm) ** 2 - self.radius_nm**2) * self.meep_to_nm(self.annulus_height))
        second_annulus_volume_nm3 = float(math.pi * (second_outer_nm**2 - second_inner_nm**2) * self.meep_to_nm(self.second_annulus_height))
        self.predicted = {
            "period_nm": self.period_nm,
            # First annulus
            "annulus_inner_radius_nm": self.radius_nm,
            "annulus_outer_radius_nm": (self.radius_nm + self.annulus_width_nm),
            "annulus_height_nm": self.meep_to_nm(self.annulus_height),
            "annulus_volume_nm3": first_annulus_volume_nm3,
            # Extra height above rod
            "extra_height_nm": self.extra_height_nm,
            # Second annulus
            "second_annulus_inner_radius_nm": second_inner_nm,
            "second_annulus_outer_radius_nm": second_outer_nm,
            "second_annulus_width_nm": second_width_nm,
            "second_annulus_height_nm": self.meep_to_nm(self.second_annulus_height),
            "second_annulus_volume_nm3": second_annulus_volume_nm3,
            # Total molecular volume
            "total_annular_volume_nm3": (first_annulus_volume_nm3 + second_annulus_volume_nm3),
        }
        # adhesion can be zero and we should neglect it at this limit
        present_layers = [t for t in (adhesion, annulus_width, extra_height, second_annulus_width) if t > 0.0]
        self._warn_if_coarse(
            n_max=self.background_index,
            t_min=min(present_layers),
        )

    def _validate_annulus_width(self, width):
        """Validate an annulus width in Meep units against the active cell."""

        if width <= 0.0:
            raise ValueError("The annulus width must be positive.")
        outer = self.nm_to_meep(self.radius_nm) + width
        if self.dimensions == CYLINDRICAL:
            if outer >= self.monitor_radius:
                raise ValueError(
                    "The annulus outer radius must be smaller than the "
                    "cylindrical monitor radius. Reduce width_nm or enlarge "
                    "the effective cell radius."
                )
        elif outer >= 0.5 * self.cell_size.x:
            raise ValueError(
                "The annulus outer radius must be smaller than half the 3D "
                "lattice period so neighboring periodic annuli do not overlap."
            )

    def _annular_box_is_inside(self, center, size):
        """Return whether a Cartesian box lies fully in the 3D molecular region."""

        half_x = 0.5 * size.x
        half_y = 0.5 * size.y
        nearest_x = max(abs(center.x) - half_x, 0.0)
        nearest_y = max(abs(center.y) - half_y, 0.0)
        inner_extent = math.hypot(nearest_x, nearest_y)
        outer_extent = math.hypot(
            abs(center.x) + half_x,
            abs(center.y) + half_y,
        )
        inner = self.nm_to_meep(self.radius_nm)
        outer = inner + self.nm_to_meep(self.annulus_width_nm)
        second_inner = self.second_annulus_inner_radius
        low_z = center.z - 0.5 * size.z
        high_z = center.z + 0.5 * size.z
        tol = 1.0e-9
        if (
            outer_extent > outer + tol
            or low_z < self.annulus_bottom_z - tol
            or high_z > self.annulus_top_z + tol
        ):
            return False
        if low_z < self.second_annulus_bottom_z - tol:
            return inner_extent >= inner - tol

        return inner_extent >= second_inner - tol

    # -------------- molecule-level coupling --------------

    def place_molecule(
        self,
        hub=None,
        driver=None,
        offset_nm=(0.0, 0.0, 0.0),
        size_nm=None,
        sigma_nm=None,
        hotspot=None,
        **molecule_kwargs,
    ):
        """
        Create one localized molecule inside the 3D molecular annulus.

        Call this method repeatedly, selecting a named cardinal hotspot and/or
        supplying ``offset_nm``, to place multiple molecules. The defaults are
        one grid point for ``sigma`` and four grid points for the molecular
        box, which fit the reference 50 nm annulus at resolution 125.

        Cylindrical cells cannot use this route because an off-axis localized
        molecule is not representable in one azimuthal Fourier sector. Use
        :meth:`place_region` for cylindrical annular matter.
        """

        if self.dimensions == CYLINDRICAL:
            raise NotImplementedError(
                "A localized molecule in the annulus is off the cylindrical "
                "symmetry axis and cannot be represented by one m=+/-1 "
                "sector. Use PlasmonicRod(dimensions=3) for discrete "
                "Molecule objects, or place_region() for a cylindrical "
                "continuous MXLSocketSusceptibility medium."
            )

        if hotspot is None:
            base_point = self.hotspot_center
        elif hotspot in self.hotspots:
            base_point = self.hotspots[hotspot]
        else:
            raise ValueError(
                f"Unknown hotspot '{hotspot}'. Available: {sorted(self.hotspots)}."
            )

        sigma_nm = (
            self.meep_to_nm(1.0 / self.resolution)
            if sigma_nm is None
            else float(sigma_nm)
        )
        size_nm = (
            self.meep_to_nm(4.0 / self.resolution)
            if size_nm is None
            else float(size_nm)
        )
        if sigma_nm <= 0.0 or size_nm <= 0.0:
            raise ValueError("sigma_nm and size_nm must be positive.")

        center = base_point + self._offset_to_meep(offset_nm)
        extent = self.nm_to_meep(size_nm)
        size = mp.Vector3(extent, extent, extent)
        if not self._annular_box_is_inside(center, size):
            raise ValueError(
                "The Molecule box must lie fully inside the molecular region: "
                f"radial range {self.radius_nm:g}.."
                f"{self.radius_nm + self.annulus_width_nm:g} nm beside the rod, "
                f"and {self.meep_to_nm(self.second_annulus_inner_radius):g}.."
                f"{self.radius_nm + self.annulus_width_nm:g} nm above the rod. "
                f"Axial range "
                f"{self.meep_to_nm(self.annulus_bottom_z):.1f}.."
                f"{self.meep_to_nm(self.annulus_top_z):.1f} nm. Choose a "
                "different hotspot/offset or reduce size_nm."
            )

        molecule_kwargs.setdefault("polarization_type", "analytical")
        return super().place_molecule(
            hub=hub,
            driver=driver,
            offset_nm=offset_nm,
            size_nm=size_nm,
            sigma_nm=sigma_nm,
            hotspot=hotspot,
            **molecule_kwargs,
        )

    # -------------- grid-level coupling --------------

    def place_region(
        self,
        epsilon=None,
        hub=None,
        width_nm=None,
        rescaling_factor=1.0,
        **susceptibility_kwargs,
    ):
        """
        Create a continuous molecular region around and above the metal cylinder.

        The first annulus extends from ``radius_nm`` to
        ``radius_nm + width_nm`` and vertically from the bottom of the Cr layer
        to ``extra_height_nm`` above the cylinder. The additional upper region
        extends radially from ``radius_nm - second_annulus_width_nm`` to
        ``radius_nm`` over ``extra_height_nm``.

        Pass the returned object to :meth:`make_simulation` as
        ``extra_geometry=[region]``. The cavity inserts it before the foreground
        geometry, whose precedence carves out the metal cylinder and the central
        region above it.

        Parameters
        ----------
        epsilon : float or None, optional
            Background permittivity of the molecular medium. Default:
            ``background_index ** 2``.
        hub : :class:`~maxwelllink.sockets.susceptibility.SusceptibilitySocketHub` or None, optional
            Socket hub of the grid-level route.
        width_nm : float or None, optional
            Radial width of the first annulus. Default: ``annulus_width_nm`` from
            the constructor.
        rescaling_factor : float, default: 1.0
            Rescaling factor of ``mp.MXLSocketSusceptibility``.
        **susceptibility_kwargs
            Forwarded to ``mp.MXLSocketSusceptibility``, for example
            ``real_field_only=True``.

        Returns
        -------
        mp.Block or mp.Cylinder
            Molecular outer disk. The metal cylinder and upper background geometry
            carve out its center during simulation assembly.
        """

        width_nm = self.annulus_width_nm if width_nm is None else float(width_nm)
        width = self.nm_to_meep(width_nm)
        self._validate_annulus_width(width)

        inner = self.nm_to_meep(self.radius_nm)
        outer = inner + width

        if epsilon is None:
            epsilon = self.background_index**2

        medium = self._socket_medium(
            epsilon,
            hub,
            rescaling_factor,
            **susceptibility_kwargs,
        )

        center = mp.Vector3(0.0, 0.0, self.annulus_center_z)

        if self.dimensions == CYLINDRICAL:
            region = mp.Block(
                material=medium,
                size=mp.Vector3(outer, 0.0, self.annulus_height),
                center=mp.Vector3(0.5 * outer, 0.0, self.annulus_center_z),
            )
        else:
            region = mp.Cylinder(
                material=medium,
                radius=outer,
                height=self.annulus_height,
                center=center,
            )

        # sim_kwargs recognizes this marker and inserts the region before the
        # foreground geometry, which restores the inner regions by geometry precedence.
        region._maxwelllink_annular_cavity = self

        # First annulus.
        region._maxwelllink_inner_radius = inner
        region._maxwelllink_outer_radius = outer
        region._maxwelllink_height = self.annulus_height
        region._maxwelllink_annulus_width = width

        # Additional molecular region above the rod.
        region._maxwelllink_second_inner_radius = self.second_annulus_inner_radius
        region._maxwelllink_second_outer_radius = self.second_annulus_outer_radius
        region._maxwelllink_second_height = self.second_annulus_height
        region._maxwelllink_second_annulus_width = self.second_annulus_width

        self.placed_regions.append(
            {
                "center": center,
                "size": mp.Vector3(2.0 * outer, 2.0 * outer, self.annulus_height),
            }
        )

        return region

    def estimate_driver_count(self, region):
        """
        Estimate the number of FDTD grid points in an annular region.

        This geometric estimate is not the actual socket-driver count. The
        modified Meep susceptibility writes the authoritative count to its
        hub's ``driver_count_file`` after the material grid is initialized.
        """

        if getattr(region, "_maxwelllink_annular_cavity", None) is not self:
            raise ValueError("region was not created by this cavity's place_region().")
        # First annulus
        inner = region._maxwelllink_inner_radius
        outer = region._maxwelllink_outer_radius
        height = region._maxwelllink_height
        # Second annulus
        second_inner = region._maxwelllink_second_inner_radius
        second_outer = region._maxwelllink_second_outer_radius
        second_height = region._maxwelllink_second_height
        if self.dimensions == CYLINDRICAL:
            # First annulus
            n_r_1 = max(1, round((outer - inner) * self.resolution))
            n_z_1 = max(1, round(height * self.resolution))
            estimate_1 = n_r_1 * n_z_1
            # Second annulus
            if second_outer > second_inner and second_height > 0.0:
                n_r_2 = max(1, round((second_outer - second_inner) * self.resolution))
                n_z_2 = max(1, round(second_height * self.resolution))
                estimate_2 = n_r_2 * n_z_2
            else:
                estimate_2 = 0
            estimate = int(estimate_1 + estimate_2)
        else:
            # First annulus
            cross_section_1 = (math.pi * (outer**2 - inner**2) * self.resolution**2)
            n_z_1 = max(1, round(height * self.resolution))
            estimate_1 = (n_z_1 * max(1, round(cross_section_1)))
            # Second annulus
            if second_outer > second_inner and second_height > 0.0:
                cross_section_2 = (math.pi * (second_outer**2 - second_inner**2) * self.resolution**2)
                n_z_2 = max(1, round(second_height * self.resolution))
                estimate_2 = (n_z_2 * max(1, round(cross_section_2)))
            else:
                estimate_2 = 0
            estimate = int(estimate_1 + estimate_2)
        warnings.warn(
            f"Estimated socket-driver count = {estimate}. This is a geometric "
            "estimate, not the actual count. Use the susceptibility hub's "
            "driver_count_file after Meep initialization for the authoritative "
            "value.",
            UserWarning,
            stacklevel=2,
        )
        return estimate

    def sim_kwargs(self, extra_geometry=()):
        """
        Assemble geometry while preserving the molecular-annulus precedence.
        """

        annular_regions = []
        trailing_geometry = []
        for item in extra_geometry:
            if getattr(item, "_maxwelllink_annular_cavity", None) is self:
                annular_regions.append(item)
            else:
                trailing_geometry.append(item)

        kwargs = super().sim_kwargs()
        kwargs["geometry"] = (
            list(self._background_geometry)
            + annular_regions
            + list(self._foreground_geometry)
            + trailing_geometry
        )
        return kwargs

    # -------------- linear reflection spectrum --------------

    def optical_setup(self):
        """
        Normal-incidence reflection probe used by :meth:`linear_spectrum`.

        The reference is the empty cell. The full structure is backed by a
        thick gold mirror, so the returned loss/absorption spectrum is
        ``1 - reflection``, matching the reference simulations.
        """

        source_z = (
            0.5 * self.cell_size.z - self.top_pml - 0.2 * self.nm_to_meep(self.air_nm)
        )
        reflection_z = (
            0.5 * self.cell_size.z - self.top_pml - 0.5 * self.nm_to_meep(self.air_nm)
        )
        if self.dimensions == CYLINDRICAL:
            excitation = {
                "center": mp.Vector3(0.5 * self.source_radius, 0.0, source_z),
                "size": mp.Vector3(self.source_radius, 0.0, 0.0),
            }
            reflection = {
                "center": mp.Vector3(
                    0.5 * self.monitor_radius,
                    0.0,
                    reflection_z,
                ),
                "size": mp.Vector3(self.monitor_radius, 0.0, 0.0),
            }
            component = mp.Er
        else:
            excitation = {
                "center": mp.Vector3(0.0, 0.0, source_z),
                "size": mp.Vector3(self.cell_size.x, self.cell_size.y, 0.0),
            }
            reflection = {
                "center": mp.Vector3(0.0, 0.0, reflection_z),
                "size": mp.Vector3(self.cell_size.x, self.cell_size.y, 0.0),
            }
            component = mp.Ex if self.polarization == "x" else mp.Ey

        setup = {
            "probe": "reflection",
            "excitation": excitation,
            "detectors": {"reflection": reflection},
            "component": component,
            "source_amplitude": self.source_amplitude,
            "reference_geometry": [],
            "decay_monitor": reflection["center"],
        }
        if self.dimensions == CYLINDRICAL:
            phase = 1.0 if self.polarization == "x" else -1j * self.m
            setup["source_components"] = [
                {"component": mp.Er, "amplitude": phase},
                {"component": mp.Ep, "amplitude": phase * 1j * self.m},
            ]
            setup["source_is_integrated"] = True
        return setup

    # -------------- unsupported local-emission observables --------------

    def emission_setup(self, offset_nm=(0.0, 0.0, 0.0), component=None):
        """Reject the unvalidated local-dipole setup inherited from the base."""

        raise NotImplementedError(
            "PlasmonicRod does not yet define a validated local-dipole emission "
            "setup. Use optical_setup()/linear_spectrum() for reflection spectra."
        )

    def purcell(
        self,
        omega_min,
        omega_max,
        units="cm-1",
        offset_nm=(0.0, 0.0, 0.0),
        component=None,
        **kwargs,
    ):
        """Reject Purcell calculations until a rod-specific setup is available."""

        raise NotImplementedError(
            "Purcell calculations are disabled for PlasmonicRod because its "
            "off-axis m=+/-1 emission geometry has not been validated."
        )

    # -------------- rod-specific inspection --------------

    def _summary(self):
        """Return an accurate summary of the plasmonic reflection setup."""

        axes = self._active_axes()
        cell_nm = ", ".join(
            f"{self.meep_to_nm(getattr(self.cell_size, axis)):.1f}" for axis in axes
        )
        grid_points = 1
        for axis in axes:
            grid_points *= max(
                1, round(getattr(self.cell_size, axis) * self.resolution)
            )

        sigma_nm = self.meep_to_nm(1.0 / self.resolution)
        size_nm = self.meep_to_nm(4.0 / self.resolution)
        second_inner_nm = self.meep_to_nm(self.second_annulus_inner_radius)
        second_outer_nm = self.meep_to_nm(self.second_annulus_outer_radius)
        second_actual_width_nm = self.meep_to_nm(self.second_annulus_width)
        setup = self.optical_setup()
        source_z_nm = self.meep_to_nm(setup["excitation"]["center"].z)
        monitor_z_nm = self.meep_to_nm(setup["detectors"]["reflection"]["center"].z)

        if self.dimensions == CYLINDRICAL:
            label = f"cylindrical m={self.m}, {self.polarization}-polarized"
            boundary = (
                f"top/bottom z PML = {self.meep_to_nm(self.top_pml):.1f}/"
                f"{self.meep_to_nm(self.bottom_pml):.1f} nm; radial PML = "
                f"{self.meep_to_nm(self.radial_pml):.1f} nm"
            )
            reduction = (
                f"{self.cell_radius_mode} effective-radius approximation; "
                "not the square-periodic 3D cell"
            )
            placement = (
                "continuous place_region() supported; localized off-axis "
                "Molecule objects require dimensions=3"
            )
        else:
            label = f"3D, {self.polarization}-polarized"
            boundary = (
                "periodic xy at k=(0, 0, 0); top/bottom z PML = "
                f"{self.meep_to_nm(self.top_pml):.1f}/"
                f"{self.meep_to_nm(self.bottom_pml):.1f} nm"
            )
            reduction = "full square-periodic unit cell"
            placement = (
                "repeated place_molecule() and continuous place_region() supported"
            )

        lines = [
            f"{type(self).__name__} ({label})",
            f"  reference         : {1.0e7 / self.wavelength_nm:.2f} cm^-1 "
            f"({self.wavelength_nm:.2f} nm; Meep f={self.frequency_meep:.6g})",
            f"  resolution        : {self.resolution:g} px/um "
            f"(~{grid_points:,} active-coordinate grid points)",
            f"  cell size (nm)    : ({cell_nm})",
            f"  boundaries        : {boundary}",
            f"  cylindrical/3D    : {reduction}",
            f"  rod               : radius={self.radius_nm:.1f} nm, "
            f"height={self.rod_height_nm:.1f} nm, period={self.period_nm:.1f} nm",
            f"  first annulus     : r={self.radius_nm:.1f}.."
            f"{self.radius_nm + self.annulus_width_nm:.1f} nm, "
            f"height={self.meep_to_nm(self.annulus_height):.1f} nm",
            f"  extra top height  : {self.extra_height_nm:.1f} nm",
            f"  second annulus    : r={second_inner_nm:.1f}.."
            f"{second_outer_nm:.1f} nm, "
            f"actual width={second_actual_width_nm:.1f} nm, "
            f"height={self.meep_to_nm(self.second_annulus_height):.1f} nm",
            f"  placement         : {placement}",
            f"  molecule defaults : sigma={sigma_nm:.2f} nm (1 px), "
            f"size={size_nm:.2f} nm (4 px; 3D only)",
            f"  reflection probe  : source z={source_z_nm:.1f} nm, "
            f"monitor z={monitor_z_nm:.1f} nm, "
            f"source_amplitude={self.source_amplitude:g}",
            "  driver count      : estimate only; the hub driver_count_file is "
            "authoritative after Meep initialization",
            "  emission/Purcell  : disabled (not physically validated)",
        ]
        return "\n".join(lines)

    def summary(self):
        """Return an accurate summary of the plasmonic reflection setup (MPI safe)"""
        if mp.am_master():
            print(self._summary())
