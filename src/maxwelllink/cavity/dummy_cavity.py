# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

import warnings

try:
    import meep as mp
except ImportError:
    raise ImportError(
        "The meep package is required for maxwelllink.cavity. Please install it: "
        "https://meep.readthedocs.io/en/latest/Installation/."
    )

from ..molecule import Molecule
from ..units import C_NM_PER_FS, wavelength_nm_from_omega

CYLINDRICAL = mp.CYLINDRICAL


class DummyCavity:
    """
    A dummy FDTD cavity for demonstration purposes.

    This class serves as a template for implementing specific cavity builders.
    By itself it describes a minimal empty cell enclosed by perfect metallic
    walls.

    Note that the Meep length unit is always fixed to 1 micrometer.
    """

    def __init__(self, omega=3000.0, units="cm-1", dimensions=1):
        """
        Initialize the necessary attributes for a minimal empty FDTD cavity.

        Notes
        -----
        This method *should be* overridden by subclasses to build the actual
        cavity.

        Call ``super().__init__(omega, units, dimensions)`` first, then
        overwrite the attributes the new cavity changes (``geometry``,
        ``cell_size``, ``allowed_bounds``, ...).

        Parameters
        ----------
        omega : float, default: 3000.0
            Design frequency (or wavelength) of the cavity in ``units``.
        units : str, default: "cm-1"
            Units of ``omega``: "cm-1", "eV", "au", "nm", or "um".
        dimensions : int, default: 1
            Dimensionality of the FDTD simulation: 1, 2, or 3 for Cartesian
            cells, or ``mxl.CYLINDRICAL`` for a cylindrical (r, z) cell.
        """

        self.dimensions = int(dimensions)
        if self.dimensions not in (1, 2, 3, CYLINDRICAL):
            raise ValueError("dimensions must be 1, 2, 3, or CYLINDRICAL.")

        # the Meep length unit is fixed to 1 um, matching the Meep materials
        # library; time_units_fs is the consistent Meep time unit used in mxl.Molecule
        self.length_units_nm = 1000.0
        self.time_units_fs = self.length_units_nm / C_NM_PER_FS
        # cavity frequency: vacuum wavelength in nm and in Meep frequency units
        self.wavelength_nm = wavelength_nm_from_omega(omega, units)
        self.frequency_meep = self.length_units_nm / self.wavelength_nm

        # a minimal empty cell: one design wavelength of vacuum per active axis,
        # enclosed by perfect metallic walls (Meep's default when no PML is set)
        lam = self.nm_to_meep(self.wavelength_nm)
        self.resolution = 20.0 / lam  # 20 px per design wavelength
        self.pml_thickness = None  # no PML; subclasses can set it optionally
        if self.dimensions == CYLINDRICAL:
            # r spans [0, R] with the axis at r = 0 and a metallic outer wall;
            # z is centered around the origin as in Cartesian cells
            self.cell_size = mp.Vector3(lam, 0.0, lam)
            self.allowed_bounds = {
                "x": (0.0, lam),  # x plays the role of the radial coordinate r
                "z": (-0.5 * lam, 0.5 * lam),
            }
        else:
            cell = [0.0, 0.0, 0.0]
            for i in range(self.dimensions):
                cell[i] = lam
            self.cell_size = mp.Vector3(*cell)
            self.allowed_bounds = {
                axis: (-0.5 * lam, 0.5 * lam) for axis in self._active_axes()
            }
        self.geometry = []
        self.boundary_layers = []  # empty: perfect metallic walls
        self.k_point = None
        # azimuthal number exp(i m phi) of a cylindrical cell
        self.m = None

        # molecule placement: the hotspot sits at the origin and molecules
        # may be placed anywhere in the vacuum interior
        self.hotspot_center = mp.Vector3()
        self.hotspots = {}  # optional named extra hotspots {"name": mp.Vector3}
        self.predicted = {}  # analytic estimates, filled by subclasses

        # geometry placed by place_molecule/place_region, kept so that plot()
        # can draw the molecules inside the cavity structure
        self.placed_molecules = []  # [{"center": mp.Vector3, "size": mp.Vector3}]
        self.placed_regions = []  # [{"center": mp.Vector3, "size": mp.Vector3}]

    # -------------- unit helpers (no need to override) --------------

    def nm_to_meep(self, value_nm):
        """
        Convert a length from nanometers to Meep units.
        """
        return float(value_nm) / self.length_units_nm

    def meep_to_nm(self, value_meep):
        """
        Convert a length from Meep units to nanometers.
        """
        return float(value_meep) * self.length_units_nm

    @property
    def allowed_bounds_nm(self):
        """
        Per-axis allowed bounds in nm, e.g. ``{"x": (-704.5, 704.5)}``.
        """
        return {
            axis: (self.meep_to_nm(lo), self.meep_to_nm(hi))
            for axis, (lo, hi) in self.allowed_bounds.items()
        }

    def _active_axes(self):
        """
        Return the active axis labels: "x", "xy", or "xyz" for Cartesian
        cells, and "xz" for cylindrical cells (x plays the role of r).
        """
        if self.dimensions == CYLINDRICAL:
            return "xz"
        return "xyz"[: self.dimensions]

    def _offset_to_meep(self, offset_nm):
        """
        Convert ``offset_nm=(dx, dy, dz)`` to a Meep-unit ``mp.Vector3``,
        rejecting components that do not exist in this dimensionality.
        """

        offset_nm = tuple(float(v) for v in offset_nm)
        if len(offset_nm) != 3:
            raise ValueError(
                "offset_nm must have three components (dx, dy, dz) in nm; "
                "use zeros for directions you do not want to shift."
            )
        label = (
            "cylindrical" if self.dimensions == CYLINDRICAL else f"{self.dimensions}D"
        )
        for axis, value in zip("xyz", offset_nm):
            if axis not in self._active_axes() and value != 0.0:
                raise ValueError(
                    f"offset_nm has a nonzero {axis}-component, but this cavity "
                    f"is {label}."
                )
        return mp.Vector3(*(self.nm_to_meep(v) for v in offset_nm))

    def _check_bounds(self, center, size, name, error=False):
        """
        Warn (``error=False``) or raise (``error=True``) when the given
        center and size leaves the allowed region along any axis.
        """

        for axis in self._active_axes():
            lo, hi = self.allowed_bounds[axis]
            c = getattr(center, axis)
            half = 0.5 * getattr(size, axis)
            low = c - half
            if self.dimensions == CYLINDRICAL and axis == "x":
                low = max(low, 0.0)  # r is radial
            if low < lo - 1e-9 or c + half > hi + 1e-9:
                message = (
                    f"{name} extends beyond the allowed region along {axis}: "
                    f"[{self.meep_to_nm(low):.1f}, "
                    f"{self.meep_to_nm(c + half):.1f}] nm vs allowed "
                    f"[{self.meep_to_nm(lo):.1f}, {self.meep_to_nm(hi):.1f}] nm."
                )
                if error:
                    raise ValueError(message + " Reduce the size or the offset.")
                warnings.warn(
                    message + " It may overlap the cavity structure or the PML."
                )

    # -------------- molecule-level coupling (no need to override) --------------

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
        Create an ``mxl.Molecule`` inside the cavity (molecule-level coupling).

        The molecule sits at the cavity hotspot by default and can be shifted
        with ``offset_nm``.

        Its ``center``, ``size``, ``sigma``, and ``dimensions`` are chosen
        consistently with the cavity grid, so the returned molecule can be
        passed directly to ``make_simulation``.

        Use ``place_region`` instead for grid-level coupling.

        Notes
        -----
        This method should *not* be overridden by subclasses.

        Parameters
        ----------
        hub : SocketHub or None, optional
            Socket hub for socket-mode molecules, exclusive with ``driver``.
        driver : str or None, optional
            Embedded driver name for non-socket molecules (e.g. ``"tls"``),
            exclusive with ``hub``.
        offset_nm : sequence of three floats, default: (0, 0, 0)
            Displacement (nm) from the hotspot; only components along active
            axes may be nonzero. Cylindrical molecule coupling supports only
            an on-axis molecule, so only ``(0, 0, dz)`` is accepted there.
        size_nm : float or None, optional
            Extent of the molecular polarization region along every active
            axis. Default: ten times ``sigma``.
        sigma_nm : float or None, optional
            Width of the regularized polarization kernel. Default: two grid
            points, which keeps the kernel resolvable at any resolution.
        hotspot : str or None, optional
            Name of an entry in ``self.hotspots`` to place the molecule at,
            for cavities with several field maxima. Default: ``hotspot_center``.
        **molecule_kwargs
            Forwarded verbatim to ``mxl.Molecule`` (e.g. ``driver_kwargs``,
            ``rescaling_factor``, ``polarization_type``). For the
            ``anisotropic`` polarization type, construct ``mxl.Molecule``
            directly instead (it needs a three-component sigma).

        Returns
        -------
        maxwelllink.Molecule
        """

        if hub is not None and driver is not None:
            raise ValueError(
                "hub and driver are mutually exclusive: pass one or the other."
            )

        # driver and driver_kwargs should be passed together
        if driver is not None and "driver_kwargs" not in molecule_kwargs:
            raise ValueError(
                "driver was provided but driver_kwargs was not. Pass both, or "
                "construct mxl.Molecule directly for full manual control."
            )

        for key in ("center", "size", "sigma", "dimensions", "resolution"):
            if key in molecule_kwargs:
                raise ValueError(
                    f"Do not pass '{key}' to place_molecule(); it is set by the "
                    "cavity. Use offset_nm / size_nm / sigma_nm instead, or "
                    "construct mxl.Molecule directly for full manual control."
                )

        if hotspot is None:
            base_point = self.hotspot_center
        elif hotspot in self.hotspots:
            base_point = self.hotspots[hotspot]
        else:
            raise ValueError(
                f"Unknown hotspot '{hotspot}'. Available: {sorted(self.hotspots)}."
            )

        center = base_point + self._offset_to_meep(offset_nm)
        sigma = 2.0 / self.resolution if sigma_nm is None else self.nm_to_meep(sigma_nm)
        extent = 10.0 * sigma if size_nm is None else self.nm_to_meep(size_nm)
        if self.dimensions == CYLINDRICAL:
            # Molecule uses Cartesian full extents even though the Meep grid is
            # cylindrical; the solver wrapper drops the inactive phi extent.
            size = mp.Vector3(extent, extent, extent)
        else:
            # the molecule extends along the active axes only (zero otherwise)
            size = [0.0, 0.0, 0.0]
            for axis in self._active_axes():
                size["xyz".index(axis)] = extent
            size = mp.Vector3(*size)
        self._check_bounds(center, size, "Molecule")

        molecule = Molecule(
            hub=hub,
            driver=driver,
            center=center,
            size=size,
            sigma=sigma,
            dimensions=self.dimensions,
            resolution=self.resolution,
            **molecule_kwargs,
        )
        # record it so that plot() can draw the molecules
        self.placed_molecules.append({"center": center, "size": size})
        return molecule

    # -------------- grid-level coupling (fdtdbath-meep build) --------------

    def _socket_medium(self, epsilon, hub, rescaling_factor, **susceptibility_kwargs):
        """
        Build the molecular medium for grid-level coupling: an ``mp.Medium``
        whose ``mp.MXLSocketSusceptibility`` turns every FDTD grid point
        inside a region into one socket molecule.

        ``place_region`` wraps the returned medium in a cavity-specific shape.
        """

        if not hasattr(mp, "MXLSocketSusceptibility"):
            raise RuntimeError(
                "mp.MXLSocketSusceptibility is unavailable: grid-level coupling "
                "requires the fdtdbath-meep build "
                "(https://github.com/TaoELi/fdtdbath-meep)."
            )
        susceptibility = mp.MXLSocketSusceptibility(
            rescaling_factor=rescaling_factor,
            time_units_fs=self.time_units_fs,
            hub=hub,
            **susceptibility_kwargs,
        )
        return mp.Medium(epsilon=epsilon, E_susceptibilities=[susceptibility])

    def place_region(
        self,
        epsilon=1.0,
        hub=None,
        offset_nm=(0.0, 0.0, 0.0),
        width_nm=None,
        rescaling_factor=1.0,
        **susceptibility_kwargs,
    ):
        """
        Create a region of molecular medium inside the cavity (grid-level
        coupling), where every FDTD grid point inside the region becomes one
        socket molecule (TCP sockets only).

        Pass the returned region to ``make_simulation`` via
        ``extra_geometry=[region]``. Use ``place_molecule`` instead for
        molecule-level coupling.

        The shape of the region is a convention of each cavity type. The
        default implemented here is a slab along x filling the allowed region
        (e.g. the defect gap of a Bragg resonator), with ``width_nm``
        shrinking the slab thickness along x.

        Notes
        -----
        This method can be *optionally* overridden by subclasses whose region
        is not a slab (see ``NPoM.place_region``).

        Parameters
        ----------
        epsilon : float, default: 1.0
            Background permittivity of the molecular medium.
        hub : SusceptibilitySocketHub or None, optional
            Socket hub of the grid-level route.
        offset_nm : sequence of three floats, default: (0, 0, 0)
            Displacement (nm) of the region center from the hotspot.
        width_nm : float or None, optional
            Size (nm) of the region under the cavity-specific convention
            (here: the slab thickness along x). Default: fill the natural
            region of the cavity.
        rescaling_factor : float, default: 1.0
            Rescaling factor of ``mp.MXLSocketSusceptibility``.
        **susceptibility_kwargs
            Forwarded to ``mp.MXLSocketSusceptibility`` (e.g.
            ``real_field_only``, ``timeout``).

        Returns
        -------
        mp.Block
            Pass it to ``make_simulation`` via ``extra_geometry=[region]``.
        """

        center = self.hotspot_center + self._offset_to_meep(offset_nm)
        # default: fill the allowed region along every active axis (mp.inf
        # on inactive axes, like the cavity layers); the axis label picks the
        # vector slot, so cylindrical cells fill (r, z) and skip phi
        size = [mp.inf, mp.inf, mp.inf]
        for axis in self._active_axes():
            lo, hi = self.allowed_bounds[axis]
            size["xyz".index(axis)] = hi - lo
        if width_nm is not None:
            size[0] = self.nm_to_meep(width_nm)
        size = mp.Vector3(*size)
        self._check_bounds(center, size, "The molecular region", error=True)

        medium = self._socket_medium(
            epsilon, hub, rescaling_factor, **susceptibility_kwargs
        )
        # record it so that plot() can draw the region
        self.placed_regions.append({"center": center, "size": size})
        return mp.Block(material=medium, center=center, size=size)

    def estimate_driver_count(self, region):
        """
        Estimate how many socket molecules (drivers) a region needs, equal to
        the number of FDTD grid points inside it.

        The authoritative number is written by the susceptibility hub to its
        ``driver_count_file`` once Meep connects.

        Notes
        -----
        The default implemented here counts the grid points of a box-shaped
        region (the slab of the default ``place_region``). This method can be
        *optionally* overridden by subclasses.

        Parameters
        ----------
        region : Meep geometric object
            The region returned by ``place_region``.

        Returns
        -------
        int
            The estimated number of drivers (grid points inside the region).
        """

        count = 1.0
        for axis in self._active_axes():
            extent = min(getattr(region.size, axis), getattr(self.cell_size, axis))
            count *= max(1.0, round(extent * self.resolution))
        return int(count)

    # -------------- measurement declarations (optionally overridden) --------------

    def optical_setup(self):
        """
        Return the far-field probe of this cavity as a plain dict, consumed by
        ``linear_spectrum`` and the measurement classes in
        ``maxwelllink.measurements``.

        Keys of the setup dict:

        - ``"probe"`` : ``"transmission"`` (partly transmitting cavities),
          ``"reflection"`` (opaque mirror-backed cavities), or
          ``"scattering"`` (localized plasmonic nanocavities);
        - ``"excitation"`` : ``{"center", "size"}`` of the source region;
        - ``"detectors"`` : dict of named detectors -- ``"transmission"`` and
          ``"reflection"`` planes (dicts with ``"center"`` and ``"size"``),
          or the ``"scattered"`` surface and closed ``"absorption_box"`` of a
          scattering probe (lists of ``mp.FluxRegion``);
        - ``"component"`` : the field component injected and detected;
        - ``"source_amplitude"`` : optional overall probe amplitude;
        - ``"source_components"`` : optional phased source components, each
          a dict with ``"component"`` and optional relative ``"amplitude"``;
        - ``"source_is_integrated"`` : optional Meep integrated-source flag;
        - ``"reference_geometry"`` : the structure of the normalization run;
        - ``"reference_boundary_layers"`` : optional boundary layers of the
          normalization run (transmission probe only);
        - ``"normalization"`` : region recording the incident intensity
          (scattering probe only);
        - ``"decay_monitor"`` : optional ``mp.Vector3`` watched by the
          stopping criterion.

        The default implemented here is a transmission probe along x (along z
        in cylindrical cells) with a vacuum reference.

        The local-dipole (Purcell) probe is declared separately by
        ``emission_setup``.

        Notes
        -----
        This method can be *optionally* overridden by subclasses.
        """

        pml = self.pml_thickness if self.pml_thickness is not None else 0.0
        if self.dimensions == CYLINDRICAL:
            # source and detector planes span r at fixed z, between the PMLs
            z_bottom = -0.5 * self.cell_size.z + pml
            z_top = 0.5 * self.cell_size.z - pml
            # plane spacing: three grid points, capped for coarse grids
            spacing = min(3.0 / self.resolution, (z_top - z_bottom) / 8.0)
            r_span = mp.Vector3(self.cell_size.x, 0.0, 0.0)
            r_mid = 0.5 * self.cell_size.x
            return {
                "probe": "transmission",
                "excitation": {
                    "center": mp.Vector3(r_mid, 0.0, z_top - spacing),
                    "size": r_span,
                },
                "detectors": {
                    "reflection": {
                        "center": mp.Vector3(r_mid, 0.0, z_top - 2.0 * spacing),
                        "size": r_span,
                    },
                    "transmission": {
                        "center": mp.Vector3(r_mid, 0.0, z_bottom + spacing),
                        "size": r_span,
                    },
                },
                "component": mp.Er,
                "reference_geometry": [],
            }

        x_left = -0.5 * self.cell_size.x + pml  # inner edge of the left PML
        x_right = 0.5 * self.cell_size.x - pml  # inner edge of the right PML
        # plane spacing: three grid points, capped for coarse grids
        spacing = min(3.0 / self.resolution, (x_right - x_left) / 8.0)
        transverse = mp.Vector3(0.0, self.cell_size.y, self.cell_size.z)
        return {
            "probe": "transmission",
            "excitation": {
                "center": mp.Vector3(x_left + spacing),
                "size": transverse,
            },
            "detectors": {
                "reflection": {
                    "center": mp.Vector3(x_left + 2.0 * spacing),
                    "size": transverse,
                },
                "transmission": {
                    "center": mp.Vector3(x_right - spacing),
                    "size": transverse,
                },
            },
            "component": mp.Ez,
            "reference_geometry": [],
        }

    def _emission_box_regions(self):
        """
        A closed, outward-oriented flux surface just inside the boundary
        layers (or the metallic walls when there is no PML), capturing
        everything the cavity radiates.
        """

        pml = self.pml_thickness if self.pml_thickness is not None else 0.0
        margin = pml + 2.0 / self.resolution  # two grid points inside the PML
        if self.dimensions == CYLINDRICAL:
            r_wall = self.cell_size.x - margin
            z_top = 0.5 * self.cell_size.z - margin
            return [
                mp.FluxRegion(  # the lid, the floor, and the outer wall
                    center=mp.Vector3(0.5 * r_wall, 0.0, z_top),
                    size=mp.Vector3(r_wall, 0.0, 0.0),
                    direction=mp.Z,
                    weight=+1.0,
                ),
                mp.FluxRegion(
                    center=mp.Vector3(0.5 * r_wall, 0.0, -z_top),
                    size=mp.Vector3(r_wall, 0.0, 0.0),
                    direction=mp.Z,
                    weight=-1.0,
                ),
                mp.FluxRegion(
                    center=mp.Vector3(r_wall, 0.0, 0.0),
                    size=mp.Vector3(0.0, 0.0, 2.0 * z_top),
                    direction=mp.R,
                    weight=+1.0,
                ),
            ]

        axes = self._active_axes()
        directions = {"x": mp.X, "y": mp.Y, "z": mp.Z}
        half = {axis: 0.5 * getattr(self.cell_size, axis) - margin for axis in axes}
        regions = []
        for axis in axes:
            # each face spans the full inner extent of the other active axes,
            # so that neighboring faces meet and the box closes
            size = [0.0, 0.0, 0.0]
            for other in axes:
                if other != axis:
                    size["xyz".index(other)] = 2.0 * half[other]
            for sign in (+1.0, -1.0):
                center = [0.0, 0.0, 0.0]
                center["xyz".index(axis)] = sign * half[axis]
                regions.append(
                    mp.FluxRegion(  # outward normals: the low faces count down
                        center=mp.Vector3(*center),
                        size=mp.Vector3(*size),
                        direction=directions[axis],
                        weight=sign,
                    )
                )
        return regions

    def emission_setup(self, offset_nm=(0.0, 0.0, 0.0), component=None):
        """
        Return the local-dipole (Purcell) probe of this cavity as a plain
        dict, consumed by ``purcell``.

        Keys of the setup dict:

        - ``"excitation"`` : the point dipole (``"center"``, zero ``"size"``),
          at the hotspot by default;
        - ``"component"`` : the dipole orientation;
        - ``"detectors"`` : dict of named flux surfaces (lists of
          ``mp.FluxRegion``); the required ``"radiated"`` entry is the full
          surface through which the cavity radiates;
        - ``"reference_geometry"`` : the homogeneous medium of the
          normalization run (same permittivity at the dipole as the cavity);
        - ``"reference_surface"`` : a closed surface of the (lossless)
          reference run, capturing the total emitted power;
        - ``"reference_boundary_layers"`` : optional boundary layers of the
          reference run;
        - ``"reference_simulation_kwargs"`` : optional reference-only Meep
          parameters such as ``cell_size``, ``geometry_center``, and
          ``resolution`` for reducing the reference computational cost;
        - ``"decay_monitor"`` : optional ``mp.Vector3`` watched by the
          stopping criterion, kept away from the singular dipole self-field.

        The default implemented here uses the cavity cell itself as the
        reference, so the Purcell factor of the template is identically 1.

        Notes
        -----
        This method can be *optionally* overridden by subclasses; see
        ``BraggResonator.emission_setup`` and ``NPoM.emission_setup``.

        Parameters
        ----------
        offset_nm : sequence of three floats, default: (0, 0, 0)
            Displacement (nm) of the dipole from the hotspot.
        component : Meep field component or None, optional
            Dipole orientation. Default: ``mp.Ez``.
        """

        box = self._emission_box_regions()
        return {
            "excitation": {
                "center": self.hotspot_center + self._offset_to_meep(offset_nm),
                "size": mp.Vector3(),
            },
            "component": component if component is not None else mp.Ez,
            "detectors": {"radiated": box},
            "reference_geometry": list(self.geometry),
            "reference_surface": box,
        }

    # -------------- measurement launchers (no need to override) --------------

    def linear_spectrum(self, omega_min, omega_max, units="cm-1", **kwargs):
        """
        Compute the far-field linear spectrum of the cavity: the probe
        declared by ``optical_setup()``.

        This gives transmission/reflection/absorption for mirror cavities and
        scattering/absorption/extinction for plasmonic nanocavities.

        For the local-dipole (Purcell) observables, use ``purcell`` instead.

        Parameters
        ----------
        omega_min, omega_max : float
            Frequency window in ``units``.
        units : str, default: "cm-1"
            Units of the window: "cm-1", "eV", "au", "nm", or "um".
        **kwargs
            Forwarded to ``MeepCavityMeasurement``: ``molecules``, ``hub``,
            ``extra_geometry``, ``nfreq``, ``decay_by``, ``steps``,
            ``max_time``, ``min_time``, ``source_amplitude``, and extra Meep
            keyword arguments.

        Returns
        -------
        dict
            Dictionary with arrays ``omega_cminv``, ``wavelength_nm``,
            ``frequency_meep``, and the observables of the declared probe.
        """

        from ..measurements import (
            MeepReflectionSpectroscopy,
            MeepScatteringSpectroscopy,
            MeepTransmissionSpectroscopy,
        )

        probe = self.optical_setup().get("probe")
        measurements = {
            "transmission": MeepTransmissionSpectroscopy,
            "reflection": MeepReflectionSpectroscopy,
            "scattering": MeepScatteringSpectroscopy,
        }
        if probe not in measurements:
            raise ValueError(
                f"optical_setup() declares the unknown probe {probe!r}; "
                "linear_spectrum supports 'transmission', 'reflection', and "
                "'scattering'. "
                "For the local-dipole (Purcell) measurement, call "
                "cavity.purcell() instead."
            )
        measurement = measurements[probe]
        return measurement(self, omega_min, omega_max, units=units, **kwargs).run()

    def purcell(
        self,
        omega_min,
        omega_max,
        units="cm-1",
        offset_nm=(0.0, 0.0, 0.0),
        component=None,
        **kwargs,
    ):
        """
        Compute the Purcell spectrum of the cavity.

        A point dipole at the hotspot drives the cavity in one run and the
        homogeneous reference structure of ``emission_setup()`` in another,
        and every observable is the ratio of the two runs.

        Parameters
        ----------
        omega_min, omega_max : float
            Frequency window in ``units``.
        units : str, default: "cm-1"
            Units of the window: "cm-1", "eV", "au", "nm", or "um".
        offset_nm : sequence of three floats, default: (0, 0, 0)
            Displacement (nm) of the dipole from the hotspot.
        component : Meep field component or None, optional
            Dipole orientation (e.g. ``mp.Er``). Default: the orientation
            chosen by the cavity (``mp.Ez``).
        **kwargs
            Forwarded to ``MeepCavityMeasurement``: ``molecules``, ``hub``,
            ``extra_geometry``, ``nfreq``, ``decay_by``, ``steps``,
            ``max_time``, ``min_time``, and extra Meep keyword arguments.

        Returns
        -------
        dict
            Dictionary with arrays ``omega_cminv``, ``wavelength_nm``,
            ``frequency_meep``, and the Purcell observables: ``purcell``
            (total decay-rate enhancement), ``purcell_radiative`` (far-field
            enhancement), ``radiative_efficiency`` (their ratio), plus the
            raw LDOS and flux arrays.
        """

        from ..measurements import MeepPurcellSpectroscopy

        return MeepPurcellSpectroscopy(
            self,
            omega_min,
            omega_max,
            units=units,
            offset_nm=offset_nm,
            component=component,
            **kwargs,
        ).run()

    # -------------- simulation assembly (no need to override) --------------

    def sim_kwargs(self, extra_geometry=()):
        """
        Return the generated Meep ingredients as a plain dict, for users who
        prefer to assemble ``mp.Simulation(**kwargs)`` themselves.

        Parameters
        ----------
        extra_geometry : sequence, optional
            Geometry appended after the cavity structure.

        Returns
        -------
        dict
            Keyword arguments for ``mp.Simulation``: ``cell_size``,
            ``geometry``, ``boundary_layers``, ``resolution``, and
            ``k_point`` when the cavity is periodic.
        """

        kwargs = dict(
            cell_size=self.cell_size,
            geometry=list(self.geometry) + list(extra_geometry),
            boundary_layers=list(self.boundary_layers),
            resolution=self.resolution,
        )
        if self.k_point is not None:
            kwargs["k_point"] = self.k_point
        # cylindrical coordinates cannot be inferred from the cell by Meep
        if self.dimensions == CYLINDRICAL:
            kwargs["dimensions"] = CYLINDRICAL
            if self.m is not None:
                kwargs["m"] = self.m
        return kwargs

    def make_simulation(
        self,
        molecules=None,
        hub=None,
        sources=None,
        extra_geometry=(),
        **meep_kwargs,
    ):
        """
        Build the ``mxl.MeepSimulation`` for this cavity.

        - Pass ``hub`` and ``molecules`` for molecule-level coupling via
          sockets;
        - Pass ``hub`` and ``extra_geometry`` for grid-level coupling via
          sockets;
        - Pass ``molecules`` alone for molecule-level coupling via embedded
          drivers (with ``driver`` and ``driver_kwargs`` in the molecules);
        - Pass nothing at all for a pure Meep simulation.

        Parameters
        ----------
        molecules : sequence of mxl.Molecule or None, optional
            Molecules to include in the simulation.
        hub : SocketHub or None, optional
            Socket hub shared by socket-mode molecules.
        sources : sequence or None, optional
            Additional native Meep sources (laser excitation etc.).
        extra_geometry : sequence, optional
            Geometry appended after the cavity structure, e.g. the region
            from ``place_region``.
        **meep_kwargs
            Extra keyword arguments forwarded to the simulation; they override
            the cavity defaults on conflicts.
        """

        kwargs = self.sim_kwargs(extra_geometry=extra_geometry)
        kwargs["sources"] = list(sources) if sources is not None else []
        kwargs.update(meep_kwargs)

        # imported here so that maxwelllink.cavity stays importable on its own
        from ..em_solvers.meep import MeepSimulation

        return MeepSimulation(
            hub=hub,
            molecules=list(molecules) if molecules is not None else [],
            length_units_nm=self.length_units_nm,
            **kwargs,
        )

    # -------------- inspection (no need to override) --------------

    def _warn_if_coarse(self, n_max=1.0, t_min=None):
        """
        Warn when the grid looks too coarse.

        Fewer than 10 px per wavelength in the densest medium (index ``n_max``)
        or fewer than 2 px across the thinnest layer (``t_min``, Meep units). Never fatal.
        """

        px_per_wavelength = (
            self.resolution * self.nm_to_meep(self.wavelength_nm) / n_max
        )
        if px_per_wavelength < 10.0:
            warnings.warn(
                f"resolution = {self.resolution:g} gives only "
                f"{px_per_wavelength:.1f} px per wavelength in the densest "
                "medium (recommended: >= 10). Results may be inaccurate."
            )
        if t_min is not None and t_min * self.resolution < 2.0:
            warnings.warn(
                f"The thinnest layer spans only {t_min * self.resolution:.1f} "
                "grid points (recommended: >= 8). Increase the resolution."
            )

    def _format_point_nm(self, point):
        """
        Format the active components of a point as nm, e.g. "0.0, -35.0".
        """
        return ", ".join(
            f"{self.meep_to_nm(getattr(point, axis)):.1f}"
            for axis in self._active_axes()
        )

    def summary(self):
        """
        Return a human-readable description of the generated setup.
        """

        label = (
            "cylindrical" if self.dimensions == CYLINDRICAL else f"{self.dimensions}D"
        )
        cell_nm = ", ".join(
            f"{self.meep_to_nm(getattr(self.cell_size, axis)):.1f}"
            for axis in self._active_axes()
        )
        sigma_nm = self.meep_to_nm(1.0 / self.resolution)
        grid_points = 1
        for axis in self._active_axes():
            grid_points *= max(
                1, round(getattr(self.cell_size, axis) * self.resolution)
            )

        lines = [
            f"{type(self).__name__} ({label})",
            f"  cavity wavelength : {self.wavelength_nm:.2f} nm "
            f"({1.0e7 / self.wavelength_nm:.2f} cm^-1)",
            f"  Meep units        : a = 1 um "
            f"(cavity frequency = {self.frequency_meep:.6g}, "
            f"1 time unit = {self.time_units_fs:.4f} fs)",
            f"  resolution        : {self.resolution:g} px per um "
            f"(~{grid_points:,} grid points)",
            f"  cell size (nm)    : ({cell_nm})",
        ]
        if self.pml_thickness is None:
            lines.append("  boundaries        : perfect metallic walls (no PML)")
        else:
            lines.append(
                f"  PML thickness     : {self.meep_to_nm(self.pml_thickness):.1f} nm "
                "(included in the cell size)"
            )
        lines.append(
            f"  hotspot (nm)      : ({self._format_point_nm(self.hotspot_center)})"
        )
        for name, point in self.hotspots.items():
            lines.append(f"    {name}: ({self._format_point_nm(point)})")
        for axis, (lo_nm, hi_nm) in self.allowed_bounds_nm.items():
            lines.append(f"  allowed region {axis}  : [{lo_nm:.1f}, {hi_nm:.1f}] nm")
        lines.append(
            f"  molecule defaults : sigma = {sigma_nm:.2f} nm (2 px), "
            f"size = {10.0 * sigma_nm:.2f} nm (10 sigma)"
        )

        try:
            setup = self.optical_setup()
        except NotImplementedError:
            setup = None
        if setup is None:
            lines.append("  optical setup     : not available (see optical_setup())")
        elif setup["probe"] == "scattering":
            # a grazing source sheet at fixed x (= r in cylindrical cells)
            source = self.meep_to_nm(setup["excitation"]["center"].x)
            detectors = ", ".join(
                f"{name} ({len(regions)} faces)"
                for name, regions in setup["detectors"].items()
            )
            lines.append(
                f"  optical setup     : scattering; source @ {source:.1f} "
                f"nm (x); {detectors}"
            )
        else:
            # transmission planes vary along x in Cartesian cells and along z
            # in cylindrical ones
            axis = "z" if self.dimensions == CYLINDRICAL else "x"
            source = self.meep_to_nm(getattr(setup["excitation"]["center"], axis))
            detectors = ", ".join(
                f"{name} @ {self.meep_to_nm(getattr(plane['center'], axis)):.1f} nm"
                for name, plane in setup["detectors"].items()
            )
            lines.append(
                f"  optical setup ({axis}) : excitation @ {source:.1f}"
                f" nm; {detectors}"
            )

        emission = self.emission_setup()
        detectors = ", ".join(
            f"{name} ({len(regions)} faces)"
            for name, regions in emission["detectors"].items()
        )
        lines.append(
            f"  emission setup    : dipole @ "
            f"({self._format_point_nm(emission['excitation']['center'])}) nm; "
            f"{detectors}"
        )

        if self.k_point is not None:
            lines.append("  lateral boundary  : periodic (k_point = (0, 0, 0))")
        if self.predicted:
            lines.append("  predicted (estimates):")
            for key, value in self.predicted.items():
                if isinstance(value, float):
                    lines.append(f"    {key}: {value:.6g}")
                else:
                    lines.append(f"    {key}: {value}")
        return "\n".join(lines)

    def plot(self, ax=None, **kwargs):
        """
        Visualize the cavity structure and its optical setup.

        Parameters
        ----------
        ax : matplotlib Axes or None, optional
            Axes to draw into. A new figure is created when None.
        **kwargs
            Forwarded to ``mp.Simulation.plot2D`` in 2D and 3D.

        Returns
        -------
        matplotlib Axes
            The axes containing the plot.
        """

        from ..tools.plotting import plot_cavity

        return plot_cavity(self, ax=ax, **kwargs)
