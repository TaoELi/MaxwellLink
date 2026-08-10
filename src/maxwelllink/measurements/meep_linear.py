# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
Meep cavity linear spectroscopy: two FDTD runs per measurement.
"""

import warnings

import numpy as np

try:
    import meep as mp
except ImportError:
    raise ImportError(
        "The meep package is required for maxwelllink.measurements. "
        "Please install it: https://meep.readthedocs.io/en/latest/Installation/."
    )

from .dummy_measurement import DummyMeasurement

# Meep time between checks of the field-decay stopping criterion
DECAY_CHECK_DT = 50


class MeepCavityMeasurement(DummyMeasurement):
    """
    Shared two-run machinery of every Meep cavity measurement.

    Each measurement excites the system with one broadband Gaussian pulse and
    performs two simulations that differ only in the structure:

    1. ``reference()``: the normalization run on the ``"reference_geometry"``
       of the cavity setup (no molecules, no ``extra_geometry``);
    2. ``signal_run()``: the full cavity, plus molecules and ``extra_geometry``.

    Subclasses fetch and validate their setup dict in ``_cavity_setup`` and
    implement the three ``DummyMeasurement`` steps on top of the helpers here.
    """

    decay_check_dt = DECAY_CHECK_DT

    def __init__(
        self,
        cavity,
        omega_min,
        omega_max,
        units="cm-1",
        nfreq=200,
        molecules=None,
        hub=None,
        extra_geometry=(),
        decay_by=1.0e-6,
        steps=None,
        max_time=1.0e4,
        min_time=0.0,
        source_amplitude=None,
        **meep_kwargs,
    ):
        """
        Initialize a two-run Meep measurement of an FDTD cavity.

        Parameters
        ----------
        cavity : DummyCavity subclass
            The cavity to probe; it must provide the setup dict that
            ``_cavity_setup`` fetches (``optical_setup()`` or
            ``emission_setup()``).
        omega_min, omega_max : float
            Frequency window in ``units``.
        units : str, default: "cm-1"
            Units of the window: "cm-1", "eV", "au", "nm", or "um".
        nfreq : int, default: 200
            Number of frequency points of the spectrum.
        molecules : sequence of mxl.Molecule or None, optional
            Molecules from ``place_molecule``, included in the signal run
            only (as in ``make_simulation``).
        hub : SocketHub or None, optional
            Socket hub shared by socket-mode molecules.
        extra_geometry : sequence, optional
            Geometry appended to the signal run only, e.g. the region from
            ``place_region`` or a nanoparticle.
        decay_by : float, default: 1e-6
            Stop each run once the monitored fields have decayed to this
            fraction of their peak.
        steps : int or None, optional
            Run each simulation for a fixed number of FDTD time steps
            instead of the decay criterion.
        max_time : float, default: 1e4
            Hard cap (Meep time units after the pulse) on the decay-based
            stopping, with a warning when it triggers; raise it for very
            high-Q cavities.
        min_time : float, default: 0.0
            Minimum Meep time to keep running after the pulse. A record of
            length T resolves quality factors only up to about
            ``frequency * T``, so raise this when a resonance comes out
            suspiciously broad.
        source_amplitude : float or None, optional
            Overall Gaussian-source amplitude. ``None`` uses the cavity
            setup's ``source_amplitude`` or 1.0 when the setup omits it.
            Per-component amplitudes in ``source_components`` are relative
            phase/polarization factors multiplied by this value.
        **meep_kwargs
            Extra keyword arguments forwarded to both simulations (e.g.
            ``m=``).
        """

        super().__init__(
            omega_min, omega_max, units=units, nfreq=nfreq, molecules=molecules
        )
        self.cavity = cavity
        self.setup = self._cavity_setup(cavity)  # fails fast on a wrong setup
        self.hub = hub
        self.extra_geometry = list(extra_geometry)
        self.decay_by = float(decay_by)
        self.steps = steps
        self.max_time = float(max_time)
        self.min_time = float(min_time)
        if source_amplitude is None:
            source_amplitude = self.setup.get("source_amplitude", 1.0)
        self.source_amplitude = float(source_amplitude)
        if not np.isfinite(self.source_amplitude) or self.source_amplitude == 0.0:
            raise ValueError("source_amplitude must be finite and nonzero.")
        self.meep_kwargs = dict(meep_kwargs)

        # incident fields at a detector, recorded by the reference run of the
        # linear measurements and subtracted in their signal run
        self._incident_flux_data = None

        # frequency window in Meep units (a = 1 um): f = omega_cminv * a / 1e7 nm
        f_lo = self.omega_min_cminv * cavity.length_units_nm * 1.0e-7
        f_hi = self.omega_max_cminv * cavity.length_units_nm * 1.0e-7
        self.fcen = 0.5 * (f_lo + f_hi)
        self.df = f_hi - f_lo
        self.freqs = np.linspace(f_lo, f_hi, self.nfreq)

    # -------------- Meep helpers shared by the two runs --------------

    def _cavity_setup(self, cavity):
        """
        Fetch and validate the setup dict of this measurement.

        Notes
        -----
        This method *must be* overridden by subclasses.
        """

        raise NotImplementedError("This method should be overridden by subclasses.")

    def _sources(self):
        """
        Broadband Gaussian pulse(s) through the setup's excitation region.

        Most cavities declare one ``component``. A cavity can instead declare
        ``source_components`` as dictionaries containing ``component`` and an
        optional complex relative ``amplitude``; this represents one physical
        source that requires several phased field components (for example an
        m=+/-1 cylindrical transverse plane wave). Every component is scaled
        by the measurement's overall ``source_amplitude``.
        """
        component_specs = self.setup.get(
            "source_components", ({"component": self.setup["component"]},)
        )
        pulse_kwargs = {"frequency": self.fcen, "fwidth": 2.7 * self.df}
        if self.setup.get("source_is_integrated", False):
            pulse_kwargs["is_integrated"] = True

        sources = []
        for spec in component_specs:
            source_kwargs = {
                "component": spec["component"],
                "center": self.setup["excitation"]["center"],
                "size": self.setup["excitation"]["size"],
                "amplitude": self.source_amplitude * spec.get("amplitude", 1.0),
            }
            sources.append(
                mp.Source(
                    # slightly wider than the window so the band edges keep power
                    mp.GaussianSource(**pulse_kwargs),
                    **source_kwargs,
                )
            )
        return sources

    def _reference_simulation(self):
        """
        Build the reference structure.
        """

        kwargs = self.cavity.sim_kwargs()
        kwargs.update(self.setup.get("reference_simulation_kwargs", {}))
        kwargs["geometry"] = list(self.setup["reference_geometry"])
        if "reference_boundary_layers" in self.setup:
            kwargs["boundary_layers"] = list(self.setup["reference_boundary_layers"])
        kwargs["sources"] = self._sources()
        kwargs.update(self.meep_kwargs)
        return mp.Simulation(**kwargs)

    def _signal_simulation(self):
        """
        The full cavity, plus molecules and ``extra_geometry``.
        """
        return self.cavity.make_simulation(
            molecules=self.molecules,
            hub=self.hub,
            sources=self._sources(),
            extra_geometry=self.extra_geometry,
            **self.meep_kwargs,
        )

    def _subtract_incident(self, sim, monitor):
        """
        Subtract the incident fields recorded by the reference run from a flux
        monitor of the signal run, so it records only the cavity response.
        """
        if self._incident_flux_data is None:
            raise RuntimeError(
                "Run reference() before signal_run(); run() does this automatically."
            )
        sim.load_minus_flux_data(monitor, self._incident_flux_data)

    def _run_until_done(self, sim, *step_functions, monitor_point=None):
        """
        Run a simulation (with optional Meep step functions) for ``steps``
        steps, or until the fields at ``monitor_point`` decay by ``decay_by``,
        capped at ``max_time`` after the pulse. By default, watch the setup's
        ``"decay_monitor"`` or the excitation center.
        """

        if self.steps is not None:
            sim.run(
                *step_functions,
                until=float(self.steps) * sim.Courant / sim.resolution,
            )
            return

        if monitor_point is None:
            monitor_point = self.setup.get(
                "decay_monitor", self.setup["excitation"]["center"]
            )
        decayed = mp.stop_when_fields_decayed(
            self.decay_check_dt,
            self.setup["component"],
            monitor_point,
            self.decay_by,
        )
        t_end = t_floor = None

        def decayed_or_timed_out(sim_):
            nonlocal t_end, t_floor
            if t_end is None:  # first check happens when the pulse ends
                t_end = sim_.meep_time() + self.max_time
                t_floor = sim_.meep_time() + self.min_time
            if sim_.meep_time() >= t_end:
                warnings.warn(
                    "The detector fields had not decayed below decay_by within "
                    f"max_time = {self.max_time:g} Meep time units (long-lived "
                    "modes, e.g. transverse guided modes of a Bloch-periodic "
                    "cell); the spectrum may be slightly under-resolved. "
                    "Increase max_time, or pass steps= for full control."
                )
                return True
            if sim_.meep_time() < t_floor:
                decayed(sim_)  # keep its running maximum up to date
                return False
            return decayed(sim_)

        sim.run(*step_functions, until_after_sources=decayed_or_timed_out)


class MeepTransmissionSpectroscopy(MeepCavityMeasurement):
    """
    Transmission/reflection spectroscopy of an FDTD cavity (two Meep runs).

    A plane-wave pulse crosses the structure; the reference run records the
    incident spectrum, and the signal run records what is transmitted and
    reflected by the full cavity.

    Examples
    --------
    >>> from maxwelllink.measurements import MeepTransmissionSpectroscopy
    >>> measurement = MeepTransmissionSpectroscopy(cavity, 2000.0, 2650.0, units="cm-1")
    >>> spectrum = measurement.run()
    """

    def _cavity_setup(self, cavity):
        """
        The plane-wave probe declared by ``cavity.optical_setup()``.
        """

        setup = cavity.optical_setup()
        if setup.get("probe") != "transmission":
            raise ValueError(
                "MeepTransmissionSpectroscopy needs an optical_setup() with "
                f"probe='transmission', but this cavity declares "
                f"{setup.get('probe')!r}. For the local-dipole (Purcell) "
                "measurement, call cavity.purcell() instead."
            )
        # the stopping criterion watches the transmitted fields by default
        setup.setdefault("decay_monitor", setup["detectors"]["transmission"]["center"])
        return setup

    def _add_monitors(self, sim):
        """
        Attach the reflection and transmission flux monitors.
        """
        refl = self.setup["detectors"]["reflection"]
        tran = self.setup["detectors"]["transmission"]
        return (
            sim.add_flux(
                self.freqs, mp.FluxRegion(center=refl["center"], size=refl["size"])
            ),
            sim.add_flux(
                self.freqs, mp.FluxRegion(center=tran["center"], size=tran["size"])
            ),
        )

    # -------------- the three measurement steps --------------

    def reference(self):
        """
        Normalization run: excite the reference structure (no molecules, no
        ``extra_geometry``) and record the incident spectrum.

        The incident fields at the reflection detector are stashed for the
        signal run.
        """

        sim = self._reference_simulation()
        refl, tran = self._add_monitors(sim)
        self._run_until_done(sim)

        self._incident_flux_data = sim.get_flux_data(refl)
        return {
            "frequency_meep": np.array(mp.get_flux_freqs(tran)),
            "incident": np.array(mp.get_fluxes(tran)),
        }

    def signal_run(self):
        """
        Scattering run: excite the full cavity (plus molecules and
        ``extra_geometry``).

        The incident wave is subtracted at the reflection detector, so it
        records only what returns.
        """

        sim = self._signal_simulation()
        refl, tran = self._add_monitors(sim)
        self._subtract_incident(sim, refl)
        self._run_until_done(sim)

        return {
            "transmitted": np.array(mp.get_fluxes(tran)),
            "reflected": np.array(mp.get_fluxes(refl)),
        }

    def postprocess(self, reference, signals):
        """
        Divide the fluxes into the T, R, and A = 1 - T - R spectra.
        """

        freqs = reference["frequency_meep"]
        transmission = signals["transmitted"] / reference["incident"]
        reflection = -signals["reflected"] / reference["incident"]
        return self._assemble_result(
            1.0e7 * freqs / self.cavity.length_units_nm,
            frequency_meep=freqs,
            transmission=transmission,
            reflection=reflection,
            absorption=1.0 - transmission - reflection,
        )


class MeepReflectionSpectroscopy(MeepCavityMeasurement):
    """
    Reflection spectroscopy of an opaque FDTD cavity (two Meep runs).

    The reference run records the incident spectrum. The signal run subtracts
    those incident fields at the same monitor and records the reflected power.
    For an opaque, mirror-backed structure, the unreflected fraction is the
    absorbed power, so this measurement returns ``absorption = 1 - reflection``.
    """

    def _cavity_setup(self, cavity):
        """The reflection probe declared by ``cavity.optical_setup()``."""

        setup = cavity.optical_setup()
        if setup.get("probe") != "reflection":
            raise ValueError(
                "MeepReflectionSpectroscopy needs an optical_setup() with "
                f"probe='reflection', but this cavity declares "
                f"{setup.get('probe')!r}."
            )
        setup.setdefault("decay_monitor", setup["detectors"]["reflection"]["center"])
        return setup

    def _add_monitor(self, sim):
        """Attach the reflection flux monitor."""

        detector = self.setup["detectors"]["reflection"]
        return sim.add_flux(
            self.freqs,
            mp.FluxRegion(center=detector["center"], size=detector["size"]),
        )

    def reference(self):
        """Record the incident flux and fields in the reference structure."""

        sim = self._reference_simulation()
        monitor = self._add_monitor(sim)
        self._run_until_done(sim)
        self._incident_flux_data = sim.get_flux_data(monitor)
        return {
            "frequency_meep": np.array(mp.get_flux_freqs(monitor)),
            "incident": np.array(mp.get_fluxes(monitor)),
        }

    def signal_run(self):
        """Record only the reflected flux from the full structure."""

        sim = self._signal_simulation()
        monitor = self._add_monitor(sim)
        self._subtract_incident(sim, monitor)
        self._run_until_done(sim)
        return {"reflected": np.array(mp.get_fluxes(monitor))}

    def postprocess(self, reference, signals):
        """Return the R and A = 1 - R spectra of an opaque structure."""

        freqs = reference["frequency_meep"]
        reflection = -signals["reflected"] / reference["incident"]
        return self._assemble_result(
            1.0e7 * freqs / self.cavity.length_units_nm,
            frequency_meep=freqs,
            reflection=reflection,
            absorption=1.0 - reflection,
        )


class MeepScatteringSpectroscopy(MeepCavityMeasurement):
    """
    Scattering spectroscopy of a localized scatterer (two Meep runs).

    The reference run excites the structure *without* the scatterer and
    records the incident fields at the collection surface.

    The signal run subtracts them, so the surface records scattered power only (the
    dark-field-type probe of Chikkaraddy et al., Nature 535, 127 (2016)).

    All observables are divided by the incident intensity at the cavity.

    Examples
    --------
    >>> from maxwelllink.cavity import NPoM
    >>> spectrum = NPoM().linear_spectrum(500.0, 900.0, units="nm")
    >>> spectrum["wavelength_nm"], spectrum["scattering"]
    """

    # a plasmonic mode rings down in a few Meep time units, far faster than
    # the high-Q mirror cavities
    decay_check_dt = 10.0

    def _cavity_setup(self, cavity):
        """
        The plane-wave scattering probe declared by ``optical_setup()``.
        """

        setup = cavity.optical_setup()
        if setup.get("probe") != "scattering":
            raise ValueError(
                "MeepScatteringSpectroscopy needs an optical_setup() with "
                f"probe='scattering', but this cavity declares "
                f"{setup.get('probe')!r}."
            )
        for key in ("scattered", "absorption_box"):
            if key not in setup["detectors"]:
                raise ValueError(f"The scattering setup lacks the '{key}' detector.")
        if "normalization" not in setup:
            raise ValueError(
                "The scattering setup lacks the 'normalization' region that "
                "records the incident intensity."
            )
        if "reference_boundary_layers" in setup:
            raise ValueError(
                "The scattering probe must keep the cavity boundary layers in "
                "the reference run (the incident-field subtraction needs "
                "identical cells); remove 'reference_boundary_layers'."
            )
        return setup

    def _add_monitors(self, sim):
        """
        One monitor over the collection surface, one over the closed box.
        """
        scattered = sim.add_flux(self.freqs, *self.setup["detectors"]["scattered"])
        box = sim.add_flux(self.freqs, *self.setup["detectors"]["absorption_box"])
        return scattered, box

    # -------------- the three measurement steps --------------

    def reference(self):
        """
        Normalization run: excite the structure without the scatterer.

        Records the incident spectrum ``|E_inc|^2`` at the hotspot, the
        incident fields at the collection surface (stashed for the signal
        run), and the net flux through the closed box.
        """

        sim = self._reference_simulation()
        scattered, box = self._add_monitors(sim)
        component = self.setup["component"]
        norm = self.setup["normalization"]
        # zero-size DFT monitors are unreliable in cylindrical cells, so the
        # normalization region is a small finite line whose samples we average
        probe = sim.add_dft_fields(
            [component], self.freqs, center=norm["center"], size=norm["size"]
        )
        self._run_until_done(sim)

        self._incident_flux_data = sim.get_flux_data(scattered)
        incident = np.array(
            [
                np.mean(np.abs(sim.get_dft_array(probe, component, i)) ** 2)
                for i in range(self.nfreq)
            ]
        )
        return {
            "frequency_meep": np.array(mp.get_flux_freqs(scattered)),
            "incident": incident,
            "box_flux": np.array(mp.get_fluxes(box)),
        }

    def signal_run(self):
        """
        Scattering run: excite the full structure.

        The stored incident fields are subtracted at the collection surface
        (scattered power only); the closed box keeps the total fields (for
        the absorbed power). The total intensity ``|E|^2`` at the hotspot is
        recorded for the field-enhancement spectrum.
        """

        sim = self._signal_simulation()
        scattered, box = self._add_monitors(sim)
        component = self.setup["component"]
        norm = self.setup["normalization"]
        probe = sim.add_dft_fields(
            [component], self.freqs, center=norm["center"], size=norm["size"]
        )
        self._subtract_incident(sim, scattered)
        self._run_until_done(sim)

        internal = np.array(
            [
                np.mean(np.abs(sim.get_dft_array(probe, component, i)) ** 2)
                for i in range(self.nfreq)
            ]
        )
        return {
            "scattered": np.array(mp.get_fluxes(scattered)),
            "box_flux": np.array(mp.get_fluxes(box)),
            "internal": internal,
        }

    def postprocess(self, reference, signals):
        """
        Combine the two runs into the scattering, absorption, and extinction
        spectra, all divided by the incident intensity at the hotspot, plus
        the field-enhancement spectrum ``enhancement = |E|^2 / |E_inc|^2`` at
        the hotspot (the sharpest signature of a high-Q resonance).
        """

        freqs = reference["frequency_meep"]
        incident = reference["incident"]
        scattering = signals["scattered"] / incident
        absorption = -(signals["box_flux"] - reference["box_flux"]) / incident
        return self._assemble_result(
            1.0e7 * freqs / self.cavity.length_units_nm,
            frequency_meep=freqs,
            incident=incident,
            scattering=scattering,
            absorption=absorption,
            extinction=scattering + absorption,
            enhancement=signals["internal"] / incident,
        )


class MeepPurcellSpectroscopy(MeepCavityMeasurement):
    """
    Purcell-factor spectroscopy of an FDTD cavity (two Meep runs).

    A point dipole drives the full cavity in one run and the homogeneous
    reference structure of ``emission_setup()`` in the other (the
    classical-emitter method), and the observables are ratios of the two runs:

    - ``purcell`` : total decay-rate enhancement, LDOS(cavity) / LDOS(reference);
    - ``purcell_radiative`` : far-field enhancement, the power crossing the
      ``"radiated"`` surface over the total power the reference dipole emits;
    - ``radiative_efficiency`` : their ratio, the fraction of the emitted
      power that reaches the far field.

    Examples
    --------
    >>> from maxwelllink.cavity import NPoM
    >>> spectrum = NPoM().purcell(500.0, 900.0, units="nm")
    >>> spectrum["wavelength_nm"], spectrum["purcell"]
    """

    # dipole-driven modes ring down in a few Meep time units, far faster than
    # the plane-wave runs of the high-Q mirror cavities
    decay_check_dt = 10.0

    def __init__(
        self,
        cavity,
        omega_min,
        omega_max,
        units="cm-1",
        offset_nm=(0.0, 0.0, 0.0),
        component=None,
        **kwargs,
    ):
        """
        Initialize the Purcell measurement of an FDTD cavity.

        Parameters
        ----------
        cavity : DummyCavity subclass
            The cavity to probe; it must implement ``emission_setup()``.
        omega_min, omega_max : float
            Frequency window in ``units``.
        units : str, default: "cm-1"
            Units of the window: "cm-1", "eV", "au", "nm", or "um".
        offset_nm : sequence of three floats, default: (0, 0, 0)
            Displacement (nm) of the dipole from the cavity hotspot.
        component : Meep field component or None, optional
            Dipole orientation (e.g. ``mp.Er``). Default: the orientation
            chosen by the cavity (``mp.Ez``).
        **kwargs
            Forwarded to ``MeepCavityMeasurement``: ``molecules``, ``hub``,
            ``extra_geometry``, ``nfreq``, ``decay_by``, ``steps``,
            ``max_time``, ``min_time``, and extra Meep keyword arguments.
        """

        # stored before super().__init__, whose _cavity_setup call needs it
        self._emission_kwargs = dict(offset_nm=offset_nm, component=component)
        super().__init__(cavity, omega_min, omega_max, units=units, **kwargs)

    def _cavity_setup(self, cavity):
        """
        The local-dipole setup declared by ``cavity.emission_setup()``.
        """

        setup = cavity.emission_setup(**self._emission_kwargs)
        if "radiated" not in setup.get("detectors", {}):
            raise ValueError(
                "emission_setup() must name a 'radiated' detector: the full "
                "surface through which the cavity radiates."
            )
        if "reference_surface" not in setup:
            raise ValueError(
                "emission_setup() must provide a closed 'reference_surface' "
                "that captures the total power the reference dipole radiates."
            )
        return setup

    # -------------- the three measurement steps --------------

    def reference(self):
        """
        Normalization run: drive the same dipole in the homogeneous reference
        structure.

        Records what the dipole emits (LDOS) and radiates through the closed
        ``reference_surface`` (= everything it emits, the reference being
        lossless).
        """

        sim = self._reference_simulation()
        monitor = sim.add_flux(self.freqs, *self.setup["reference_surface"])
        self._run_until_done(
            sim,
            mp.dft_ldos(self.freqs),
            monitor_point=self.setup["excitation"]["center"],
        )

        ldos_reference = np.array(sim.ldos_data)
        radiated_reference = np.array(mp.get_fluxes(monitor))
        if self.cavity.dimensions == mp.CYLINDRICAL:
            signal_resolution = float(
                self.meep_kwargs.get("resolution", self.cavity.resolution)
            )
            resolution_ratio = float(sim.resolution) / signal_resolution
            ldos_reference *= resolution_ratio
            radiated_reference *= resolution_ratio**2
        return {
            "frequency_meep": np.array(mp.get_flux_freqs(monitor)),
            "ldos": ldos_reference,
            "radiated": radiated_reference,
        }

    def signal_run(self):
        """
        Cavity run: drive the full cavity (plus molecules and
        ``extra_geometry``).

        Records the emitted power together with the power crossing each
        detector surface.
        """

        sim = self._signal_simulation()
        monitors = {
            name: sim.add_flux(self.freqs, *regions)
            for name, regions in self.setup["detectors"].items()
        }
        self._run_until_done(sim, mp.dft_ldos(self.freqs))

        return {
            "ldos": np.array(sim.ldos_data),
            "radiated": {
                name: np.array(mp.get_fluxes(monitor))
                for name, monitor in monitors.items()
            },
        }

    def postprocess(self, reference, signals):
        """
        Combine the two runs into the Purcell observables.

        Every spectrum is a ratio of the two runs, so all prefactors cancel.
        """

        freqs = reference["frequency_meep"]
        purcell = signals["ldos"] / reference["ldos"]
        purcell_radiative = signals["radiated"]["radiated"] / reference["radiated"]
        observables = {
            "frequency_meep": freqs,
            "purcell": purcell,
            "purcell_radiative": purcell_radiative,
            "radiative_efficiency": purcell_radiative / purcell,
            "ldos": signals["ldos"],
            "ldos_reference": reference["ldos"],
            "radiated_flux": signals["radiated"]["radiated"],
            "radiated_flux_reference": reference["radiated"],
        }
        # optionally add the analytical reference ldos if the setup contains one
        if "ldos_reference_analytical" in self.setup:
            # this is a callable function
            observables["ldos_reference_analytical"] = self.setup[
                "ldos_reference_analytical"
            ](freqs)

        # raw flux through any extra named detector (e.g. "top", "lateral")
        for name, flux in signals["radiated"].items():
            if name != "radiated":
                observables[f"{name}_flux"] = flux
        return self._assemble_result(
            1.0e7 * freqs / self.cavity.length_units_nm, **observables
        )
