# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
A plasmonic nanoparticle-on-mirror (NPoM) cavity built from the Meep materials
library: a gold nanosphere held a nanometer above a gold film.
"""

import warnings

import numpy as np
import meep as mp

from .dummy_cavity import DummyCavity, CYLINDRICAL

# the geometry of Chikkaraddy et al., Nature 535, 127 (2016)
RADIUS_NM = 20.0  # gold nanoparticle of 40 nm diameter
GAP_NM = 1.0  # cucurbit[7]uril spacer that hosts the molecule
SPACER_INDEX = 1.4  # refractive index of the cucurbit[7]uril monolayer
FILM_NM = 70.0  # evaporated gold mirror (~5 skin depths at 660 nm)

# gap plasmon reported for that geometry: dark-field scattering peak (nm),
# quality factor, and effective mode volume (nm^3) of the same paper
REPORTED = {
    "gap_mode_nm": 660.0,
    "quality_factor": 15.9,
    "mode_volume_nm3": 35.9,
}

# default grid: pixels across the spacer (0.15 nm for the 0.9 nm gap, finer
# than the 0.3 nm mesh at which the FDTD runs of the paper converged)
GAP_PIXELS = 6.0

# default free space around the particle and default boundary thickness, in
# units of the reference wavelength
PADDING_FRACTION = 0.15
BOUNDARY_FRACTION = 0.25

# Converged cell size per reddest measured wavelength, the sizing rule
# documented in __init__: a boundary layer inside the reactive near zone of
# the gap antenna loads the structure, which reddens and broadens the
# scattering resonance and makes the total LDOS rise with wavelength instead
# of decaying. emission_setup inverts the rule to size its own reference.
EMISSION_CLEARANCE_FRACTION = 0.75  # (radius + padding) per reddest wavelength
EMISSION_BOUNDARY_FRACTION = 0.9  # boundary thickness per reddest wavelength


class NPoM(DummyCavity):
    """
    A gold nanosphere above a gold mirror, separated by a molecular spacer.

    The geometry convention follows Chikkaraddy et al., Nature 535, 127
    (2016), doi:10.1038/nature17974.

    The gap center on the symmetry axis is the hotspot, and the allowed
    region is the spacer disk underneath the particle.

    The gap plasmon is a rotationally symmetric mode polarized along z, so a
    cylindrical ``m = 0`` run reproduces the full 3D physics at 2D cost.

    The paper's dark-field scattering spectrum comes from ``linear_spectrum``
    and its classical-emitter Purcell spectrum from ``purcell``. Both need a
    cell sized for the reddest measured wavelength; see the ``padding_nm``
    and ``pml_nm`` parameters of ``__init__``.

    Examples
    --------
    >>> from maxwelllink.cavity import NPoM
    >>> cav = NPoM(resolution=5000.0)
    >>> spectrum = cav.linear_spectrum(500.0, 900.0, units="nm", min_time=30.0)
    >>> lam, scattering = spectrum["wavelength_nm"], spectrum["scattering"]
    >>> enhancement = cav.purcell(500.0, 900.0, units="nm", min_time=30.0)
    >>> purcell_factor = enhancement["purcell"]
    """

    def __init__(
        self,
        radius_nm: float = RADIUS_NM,
        gap_nm: float = GAP_NM,
        spacer_index: float = SPACER_INDEX,
        film_nm: float = FILM_NM,
        omega_ref: float = REPORTED["gap_mode_nm"],
        units: str = "nm",
        material=None,
        dimensions: int = CYLINDRICAL,
        resolution: float = None,
        pml_nm: float = None,
        padding_nm: float = None,
    ):
        """
        Initialize the parameters of a gold nanoparticle-on-mirror cavity.

        Parameters
        ----------
        radius_nm : float, default: 20.0
            Radius (nm) of the gold nanosphere (40 nm diameter).
        gap_nm : float, default: 1.0
            Thickness (nm) of the spacer between the particle and the mirror.
        spacer_index : float, default: 1.4
            Refractive index of the spacer layer, which extends laterally
            across the whole cell as in the paper.
        film_nm : float, default: 70.0
            Thickness (nm) of the gold mirror.
        omega_ref : float, default: 660.0
            Reference frequency (or wavelength) in ``units``, i.e. roughly
            where the gap plasmon is expected. It sets no length of the
            structure, only the default grid, padding, and boundary thickness.
        units : str, default: "nm"
            Units of ``omega_ref``: "cm-1", "eV", "au", "nm", or "um".
        material : mp.Medium or None, optional
            Material of the particle and the mirror. Default: gold
            (``meep.materials.Au``).
        dimensions : int, default: mxl.CYLINDRICAL
            ``mxl.CYLINDRICAL`` for the (r, z) half plane, where the cavity
            sets ``m = 0`` (the sector holding the gap mode), or 3 for full
            3D.
        resolution : float or None, optional
            Meep resolution. Default: six pixels across the gap, and at
            least 20 pixels per reference wavelength in the spacer.
        pml_nm : float or None, optional
            Boundary thickness in nm. Default: a quarter of the reference
            wavelength, sized for a far-field probe at ``omega_ref``; a
            redder window needs more (see Notes).
        padding_nm : float or None, optional
            Free space (nm) between the particle and the boundary layers.
            Default: 0.15 reference wavelengths, sized for a far-field probe
            at ``omega_ref``; a redder window needs more (see Notes).

        Notes
        -----
        The default cell is not good enough for linear spectrum and purcell
        measurements over a wide wavelength range, especially for cylindrical cells.

        # TOY MODEL RUNNING IN LOCAL MACHINES:
        20-radius nm and 1 nm gap particle, 500-800 nm measurement window:
        ``NPoM(padding_nm=150.0, pml_nm=250.0)``, resolution 1000.

        # PRACTICAL CALCULATIONS FOR NATURE 2016 PAPER:
        20-radius nm and 1 nm gap particle, 500-800 nm measurement window:
        - Linear scattering spectrum: ``NPoM(padding_nm=300.0, pml_nm=300.0)``, resolution 4000.
        - Purcell factor: ``NPoM(padding_nm=500.0, pml_nm=700.0)``, resolution 4000.

        For converged spectra, size the cell from the *reddest* wavelength
        ``lam_max`` of the window to be measured:

            padding_nm >= 0.75 * lam_max - radius_nm
            pml_nm     >= 0.9  * lam_max

        Half of it is enough when ``linear_spectrum`` is only used for the
        resonance position and linewidth, while the full value is needed for
        ``purcell`` and for the long-wavelength tail of the scattering spectrum.

        With a cell that is too small, the scattering resonance comes out redshifted
        and too broad, and the total Purcell factor rises with wavelength instead of
        decaying.
        """

        # a dispersive metal and a rotationally symmetric mode: 1D/2D cells
        # cannot represent either the geometry or the vertical gap field
        if dimensions not in (CYLINDRICAL, 3):
            raise ValueError("dimensions must be 3 or CYLINDRICAL.")
        if min(float(radius_nm), float(gap_nm), float(film_nm)) <= 0.0:
            raise ValueError("radius_nm, gap_nm, and film_nm must be positive.")
        if float(spacer_index) < 1.0:
            raise ValueError("spacer_index must be at least 1.")

        # default attributes (units, grid, hotspot, ...), overridden below
        super().__init__(omega=omega_ref, units=units, dimensions=dimensions)
        lam = self.nm_to_meep(self.wavelength_nm)  # reference wavelength in um

        self.radius_nm = float(radius_nm)
        self.gap_nm = float(gap_nm)
        self.spacer_index = float(spacer_index)
        self.film_nm = float(film_nm)

        is_default_gold = material is None
        if is_default_gold:
            from meep.materials import Au

            material = Au
        self.material = material

        # -------------- the stack along z (Meep units: um) --------------
        radius = self.nm_to_meep(self.radius_nm)
        gap = self.nm_to_meep(self.gap_nm)
        film = self.nm_to_meep(self.film_nm)
        pad = (
            self.nm_to_meep(padding_nm)
            if padding_nm is not None
            else PADDING_FRACTION * lam
        )
        self.pml_thickness = (
            self.nm_to_meep(pml_nm) if pml_nm is not None else BOUNDARY_FRACTION * lam
        )
        boundary = self.pml_thickness
        self.padding = pad  # free space between the particle and the boundary

        # the cell is centered on the origin (the Meep convention), and the
        # mirror is backed by the bottom wall, so the stack sits below center:
        # film | spacer | particle | padding | PML, from the bottom up
        span_z = film + gap + 2.0 * radius + pad + boundary
        z_mirror = film - 0.5 * span_z  # top surface of the gold film
        z_hot = z_mirror + 0.5 * gap  # gap center, on the symmetry axis
        self.mirror_surface_z = z_mirror

        self.geometry = [
            mp.Block(  # the gold mirror, down to the bottom wall of the cell
                size=mp.Vector3(mp.inf, mp.inf, film),
                center=mp.Vector3(0.0, 0.0, z_mirror - 0.5 * film),
                material=material,
            ),
            mp.Block(  # the molecular spacer, an infinite flat sheet
                size=mp.Vector3(mp.inf, mp.inf, gap),
                center=mp.Vector3(0.0, 0.0, z_hot),
                material=mp.Medium(index=self.spacer_index),
            ),
            mp.Sphere(  # the gold nanoparticle, resting on the spacer
                radius=radius,
                center=mp.Vector3(0.0, 0.0, z_mirror + gap + radius),
                material=material,
            ),
        ]

        # -------------- cell size and boundaries --------------
        span_r = radius + pad + boundary
        if self.dimensions == CYLINDRICAL:
            self.cell_size = mp.Vector3(span_r, 0.0, span_z)
            sides = [mp.Absorber(thickness=boundary, direction=mp.R, side=mp.High)]
        else:
            self.cell_size = mp.Vector3(2.0 * span_r, 2.0 * span_r, span_z)
            sides = [
                mp.Absorber(thickness=boundary, direction=axis) for axis in (mp.X, mp.Y)
            ]
        self.boundary_layers = [
            mp.PML(thickness=boundary, direction=mp.Z, side=mp.High)
        ] + sides

        # the hotspot is the gap center; molecules stay inside the spacer disk
        # underneath the particle
        self.hotspot_center = mp.Vector3(0.0, 0.0, z_hot)
        # the gap plasmon is the rotationally symmetric mode, so cylindrical
        # runs default to that sector (override with m= at simulation time)
        if self.dimensions == CYLINDRICAL:
            self.m = 0
        self.allowed_bounds = {"z": (z_hot - 0.5 * gap, z_hot + 0.5 * gap)}
        if self.dimensions == CYLINDRICAL:
            self.allowed_bounds["x"] = (0.0, radius)  # x plays the role of r
        else:
            self.allowed_bounds["x"] = (-radius, radius)
            self.allowed_bounds["y"] = (-radius, radius)

        # -------------- grid resolution --------------
        # default: six pixels across the gap, and at least 20 px per
        # reference wavelength inside the spacer
        if resolution is not None:
            self.resolution = float(resolution)
        else:
            self.resolution = float(
                np.ceil(max(20.0 * self.spacer_index / lam, GAP_PIXELS / gap))
            )

        # -------------- analytic and reported estimates --------------
        # the gap mode spreads over a radius of about sqrt(R d), giving a mode
        # volume of about d^2 R (Chikkaraddy et al., Supplementary Sec. S6)
        self.predicted = {
            "wavelength_ref_nm": self.wavelength_nm,
            "omega_ref_cminv": 1.0e7 / self.wavelength_nm,
            "mode_radius_nm": float(np.sqrt(self.radius_nm * self.gap_nm)),
            "mode_volume_nm3": float(self.gap_nm**2 * self.radius_nm),
        }
        # the measured/simulated gap mode, when the structure is exactly the
        # one of the paper
        is_reported_geometry = (
            abs(self.radius_nm - RADIUS_NM) < 1.0e-9
            and abs(self.gap_nm - GAP_NM) < 1.0e-9
            and abs(self.spacer_index - SPACER_INDEX) < 1.0e-9
            and abs(self.film_nm - FILM_NM) < 1.0e-9
        )
        if is_default_gold and is_reported_geometry:
            self.predicted["gap_mode_nm_reported"] = REPORTED["gap_mode_nm"]
            self.predicted["quality_factor_reported"] = REPORTED["quality_factor"]
            self.predicted["mode_volume_nm3_reported"] = REPORTED["mode_volume_nm3"]
        try:
            # material permittivity at the reference frequency: the gap mode
            # needs a metal, i.e. a sizable negative real part
            eps = material.epsilon(self.frequency_meep)[0][0]
            self.predicted["eps_re_at_ref"] = float(np.real(eps))
            self.predicted["eps_im_at_ref"] = float(np.imag(eps))
        except Exception:
            pass  # non-dispersive or exotic media: skip the estimate

        self._warn_if_coarse(n_max=self.spacer_index, t_min=gap)

    # -------------- light-induced measurements --------------

    def _radiated_flux_regions(self, clearance_nm=None):
        """
        The surface through which the nanocavity radiates: a lid above the
        particle plus walls down to the mirror surface (which closes the box
        from below).

        Parameters
        ----------
        clearance_nm : float or None, optional
            Distance (nm) between the particle and the surface. Default: 60%
            of the way from the nanoparticle to the boundary layers.

        Returns
        -------
        list of mp.FluxRegion
            The lid first, then the walls.
        """

        # the particle occupies r <= radius and z <= top
        radius = self.nm_to_meep(self.radius_nm)
        top = self.mirror_surface_z + self.nm_to_meep(self.gap_nm) + 2.0 * radius
        clearance = (
            self.nm_to_meep(clearance_nm)
            if clearance_nm is not None
            else 0.6 * self.padding
        )
        if clearance >= self.padding:
            raise ValueError(
                f"A clearance of {self.meep_to_nm(clearance):.1f} nm would put "
                "the flux surface inside the boundary layers; the cavity has "
                f"only {self.meep_to_nm(self.padding):.1f} nm of padding."
            )
        if clearance < radius:
            warnings.warn(
                f"The flux surface clears the particle by only "
                f"{self.meep_to_nm(clearance):.1f} nm (radius "
                f"{self.radius_nm:.1f} nm), so it samples the reactive near "
                "field and the radiated spectrum will be distorted. Build "
                "the cavity with more padding_nm."
            )
        z_top = top + clearance
        z_bottom = self.mirror_surface_z  # the mirror closes the box
        height = z_top - z_bottom
        z_center = 0.5 * (z_bottom + z_top)

        if self.dimensions == CYLINDRICAL:
            wall_r = radius + clearance
            return [
                mp.FluxRegion(  # the lid above the particle
                    center=mp.Vector3(0.5 * wall_r, 0.0, z_top),
                    size=mp.Vector3(wall_r, 0.0, 0.0),
                    direction=mp.Z,
                ),
                mp.FluxRegion(  # the wall around it, down to the mirror
                    center=mp.Vector3(wall_r, 0.0, z_center),
                    size=mp.Vector3(0.0, 0.0, height),
                    direction=mp.R,
                ),
            ]

        half = radius + clearance
        regions = [
            mp.FluxRegion(  # the lid above the particle
                center=mp.Vector3(0.0, 0.0, z_top),
                size=mp.Vector3(2.0 * half, 2.0 * half, 0.0),
                direction=mp.Z,
            )
        ]
        for axis, direction in (("x", mp.X), ("y", mp.Y)):
            # each wall spans the other transverse axis and the box height
            other = "y" if axis == "x" else "x"
            size = [0.0, 0.0, height]
            size["xyz".index(other)] = 2.0 * half
            for sign in (+1.0, -1.0):
                center = [0.0, 0.0, z_center]
                center["xyz".index(axis)] = sign * half
                regions.append(
                    mp.FluxRegion(  # outward normals: the low faces count down
                        center=mp.Vector3(*center),
                        size=mp.Vector3(*size),
                        direction=direction,
                        weight=sign,
                    )
                )
        return regions

    def _box_bottom(self, lid, z_bottom):
        """
        The bottom face closing a collection box: the footprint of the given
        lid moved to ``z_bottom``, counting downward flux.
        """
        return mp.FluxRegion(
            center=mp.Vector3(lid.center.x, lid.center.y, z_bottom),
            size=lid.size,
            direction=mp.Z,
            weight=-1.0,
        )

    def optical_setup(self):
        """
        Far-field probe of the NPoM: the dark-field-type scattering
        measurement of Chikkaraddy et al., Nature 535, 127 (2016).

        A grazing sheet of vertical current drives the gap mode. The
        reference run (the film and spacer, without the particle) records the
        incident fields subtracted at the collection surface.

        All observables are normalized by the incident intensity at the
        hotspot. Same keys as ``DummyCavity.optical_setup``.
        """

        surface = self._radiated_flux_regions()  # the lid first, then the walls
        lid = surface[0]
        boundary = self.pml_thickness
        z_top = 0.5 * self.cell_size.z - boundary  # inner edge of the top PML
        z_mid = 0.5 * (self.mirror_surface_z + z_top)
        height = z_top - self.mirror_surface_z
        if self.dimensions == CYLINDRICAL:
            # the ring source sits between the collection wall (at
            # radius + clearance) and the absorber (at radius + padding)
            r_source = self.nm_to_meep(self.radius_nm) + 0.85 * self.padding
            excitation = {
                "center": mp.Vector3(r_source, 0.0, z_mid),
                "size": mp.Vector3(0.0, 0.0, height),
            }
        else:
            # 3D: a grazing sheet just inside the -x absorber. Declared for
            # completeness; resolving the gap in 3D is impractical.
            x_source = -0.5 * self.cell_size.x + boundary + 2.0 / self.resolution
            excitation = {
                "center": mp.Vector3(x_source, 0.0, z_mid),
                "size": mp.Vector3(0.0, self.cell_size.y - 2.0 * boundary, height),
            }

        return {
            "probe": "scattering",
            "excitation": excitation,
            "component": mp.Ez,
            "detectors": {
                # the closed box adds the mirror-surface floor to the
                # collection surface; net total-field flux through it gives
                # the power the particle absorbs
                "scattered": surface,
                "absorption_box": surface
                + [self._box_bottom(lid, self.mirror_surface_z)],
            },
            # |E_inc|^2 is recorded over a short r-line at the hotspot
            # (zero-size DFT monitors are unreliable in cylindrical cells)
            "normalization": {
                "center": self.hotspot_center,
                "size": mp.Vector3(2.0 / self.resolution, 0.0, 0.0),
            },
            # the film and the spacer, without the particle
            "reference_geometry": list(self.geometry[:2]),
            # watch the ringdown a mode radius off the axis, where the
            # stopping criterion is more robust than on the singular axis
            "decay_monitor": self.hotspot_center
            + mp.Vector3(self.nm_to_meep(self.predicted["mode_radius_nm"]), 0.0),
        }

    def emission_setup(self, offset_nm=(0.0, 0.0, 0.0), component=None):
        """
        Local-dipole (Purcell) probe of the NPoM: a z-polarized dipole at the
        gap hotspot (the classical-emitter method of Chikkaraddy et al.).

        The reference is the homogeneous spacer medium with its own closed
        collection box (the mirror is absent there, so an open-bottomed box
        would leak the downward radiation).

        Same keys as ``DummyCavity.emission_setup``.

        Parameters
        ----------
        offset_nm : sequence of three floats, default: (0, 0, 0)
            Displacement (nm) of the dipole from the gap hotspot.
        component : Meep field component or None, optional
            Dipole orientation. Default: ``mp.Ez``, along the gap field.
        """

        source_component = component if component is not None else mp.Ez
        excitation = {
            "center": self.hotspot_center + self._offset_to_meep(offset_nm),
            "size": mp.Vector3(),
        }
        surface = self._radiated_flux_regions()  # the lid first, then the walls

        # The homogeneous reference has its own inexpensive, symmetric cell.
        lambda_max_nm = max(
            self.wavelength_nm,
            (self.radius_nm + self.meep_to_nm(self.padding))
            / EMISSION_CLEARANCE_FRACTION,
            self.meep_to_nm(self.pml_thickness) / EMISSION_BOUNDARY_FRACTION,
        )
        reference_wavelength = self.nm_to_meep(lambda_max_nm)
        reference_resolution = min(self.resolution, 200.0)
        reference_pml = 0.5 * reference_wavelength
        reference_padding = 0.5 * reference_wavelength
        reference_half_extent = reference_padding + reference_pml
        monitor_half_extent = 0.8 * reference_padding
        reference_center = excitation["center"]
        reference_boundaries = [mp.PML(thickness=reference_pml)]

        if self.dimensions == CYLINDRICAL:
            reference_cell = mp.Vector3(
                reference_half_extent,
                0.0,
                2.0 * reference_half_extent,
            )
            z_bottom = reference_center.z - monitor_half_extent
            z_top = reference_center.z + monitor_half_extent
            lid = mp.FluxRegion(
                center=mp.Vector3(0.5 * monitor_half_extent, 0.0, z_top),
                size=mp.Vector3(monitor_half_extent, 0.0, 0.0),
                direction=mp.Z,
            )
            reference_surface = [
                lid,
                mp.FluxRegion(
                    center=mp.Vector3(monitor_half_extent, 0.0, reference_center.z),
                    size=mp.Vector3(0.0, 0.0, 2.0 * monitor_half_extent),
                    direction=mp.R,
                ),
                self._box_bottom(lid, z_bottom),
            ]
        else:
            reference_cell = mp.Vector3(
                2.0 * reference_half_extent,
                2.0 * reference_half_extent,
                2.0 * reference_half_extent,
            )
            reference_surface = []
            center = [reference_center.x, reference_center.y, reference_center.z]
            for index, direction in enumerate((mp.X, mp.Y, mp.Z)):
                size = [2.0 * monitor_half_extent] * 3
                size[index] = 0.0
                for sign in (+1.0, -1.0):
                    face_center = list(center)
                    face_center[index] += sign * monitor_half_extent
                    reference_surface.append(
                        mp.FluxRegion(
                            center=mp.Vector3(*face_center),
                            size=mp.Vector3(*size),
                            direction=direction,
                            weight=sign,
                        )
                    )

        reference_simulation_kwargs = {
            "cell_size": reference_cell,
            "geometry_center": reference_center,
            "resolution": reference_resolution,
        }

        # Meep's homogeneous-medium LDOS in the cavity-grid normalization.
        # The numerical cylindrical reference is converted to this same
        # resolution before the Purcell ratio is formed.
        def ldos_reference_analytical(freqs):
            freqs = np.asarray(freqs, dtype=float)
            if self.dimensions == CYLINDRICAL:
                return (
                    2.0 * np.pi * self.spacer_index * freqs**2 / (3.0 * self.resolution)
                )
            return 4.0 * self.spacer_index * freqs**2 / 3.0

        return {
            # a point dipole at the gap center, polarized along the gap field
            "excitation": excitation,
            # ``radiated`` is the complete radiating surface (lid + walls)
            "detectors": {
                "radiated": surface,
                "top": surface[:1],
                "lateral": surface[1:],
            },
            "component": source_component,
            "reference_geometry": [
                mp.Block(
                    size=mp.Vector3(mp.inf, mp.inf, mp.inf),
                    material=mp.Medium(index=self.spacer_index),
                )
            ],
            "reference_boundary_layers": reference_boundaries,
            "reference_simulation_kwargs": reference_simulation_kwargs,
            "reference_surface": reference_surface,
            # watch the ringdown a mode radius off the axis: on the dipole
            # itself the singular self-field collapses with the pulse and
            # would stop the run before the plasmon has rung down
            "decay_monitor": self.hotspot_center
            + mp.Vector3(self.nm_to_meep(self.predicted["mode_radius_nm"]), 0.0),
            "ldos_reference_analytical": ldos_reference_analytical,
        }

    # -------------- grid-level coupling --------------

    def place_region(
        self,
        epsilon=None,
        hub=None,
        offset_nm=(0.0, 0.0, 0.0),
        width_nm=None,
        rescaling_factor=1.0,
        **susceptibility_kwargs,
    ):
        """
        Create a disk of molecular medium inside the gap (grid-level coupling).

        The disk fills the spacer thickness and is centered at the hotspot
        plus ``offset_nm``.

        Pass it to ``make_simulation`` via ``extra_geometry=[region]``.

        Parameters
        ----------
        epsilon : float or None, optional
            Background permittivity of the molecular medium. Default: that of
            the spacer the disk replaces (``spacer_index ** 2``).
        hub : SusceptibilitySocketHub or None, optional
            Socket hub of the grid-level route.
        offset_nm : sequence of three floats, default: (0, 0, 0)
            Displacement (nm) of the disk center from the hotspot.
        width_nm : float or None, optional
            Diameter (nm) of the disk. Default: twice the lateral radius
            ``sqrt(radius * gap)`` of the gap mode.
        rescaling_factor : float, default: 1.0
            Rescaling factor of ``mp.MXLSocketSusceptibility``.
        **susceptibility_kwargs
            Forwarded to ``mp.MXLSocketSusceptibility`` (e.g.
            ``real_field_only``, ``timeout``).

        Returns
        -------
        mp.Cylinder
        """

        center = self.hotspot_center + self._offset_to_meep(offset_nm)
        gap = self.nm_to_meep(self.gap_nm)
        if width_nm is None:
            # default: the lateral extent of the gap mode itself
            radius = np.sqrt(self.nm_to_meep(self.radius_nm) * gap)
        else:
            radius = 0.5 * self.nm_to_meep(width_nm)
        size = mp.Vector3(2.0 * radius, 2.0 * radius, gap)
        self._check_bounds(center, size, "The molecular region", error=True)

        if epsilon is None:
            epsilon = self.spacer_index**2
        medium = self._socket_medium(
            epsilon, hub, rescaling_factor, **susceptibility_kwargs
        )
        # record it so that plot() can draw the region
        self.placed_regions.append({"center": center, "size": size})
        return mp.Cylinder(material=medium, center=center, radius=radius, height=gap)

    def estimate_driver_count(self, region):
        """
        Estimate how many socket molecules (drivers) the gap disk of
        ``place_region`` needs, equal to the number of FDTD grid points
        inside it.

        The disk is a rectangle of the (r, z) half plane in cylindrical
        cells, and a cylinder in 3D.

        Parameters
        ----------
        region : mp.Cylinder
            The region returned by ``place_region``.

        Returns
        -------
        int
            The estimated number of drivers (grid points inside the region).
        """

        n_z = max(1.0, round(region.height * self.resolution))
        n_r = max(1.0, round(region.radius * self.resolution))
        if self.dimensions == CYLINDRICAL:
            return int(n_z * n_r)
        return int(n_z * max(1.0, round(np.pi * n_r**2)))
