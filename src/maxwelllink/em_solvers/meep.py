"""
Meep-specific EM units and coupling utilities for MaxwellLink.

Provides conversions between Meep units and atomic units, plus helpers for
source construction and field integrations.
"""

from __future__ import annotations
import json
import numpy as np
from math import exp
from typing import Optional, Dict, List
import atexit

from ..sockets import SocketHub
from .dummy_em import DummyEMUnits, MoleculeDummyWrapper
from ..molecule import Molecule, Vector3
from ..units import EV_TO_CM_INV, FS_TO_AU
from maxwelllink.tools import calc_transverse_components_3d

try:
    import meep as mp
except ImportError:
    raise ImportError(
        "The meep package is required for this module. Please install it: https://meep.readthedocs.io/en/latest/Installation/."
    )

# ----------------------------------------------------------------------------------
# Shared source registry (solver-native) keyed by polarization fingerprint (and comp)
# ----------------------------------------------------------------------------------
from collections import defaultdict

instantaneous_source_amplitudes = defaultdict(float)
_fingerprint_source: Dict = {}

# Helpers to reset this shared state between simulations/tests
__MXL__ATEXIT_REGISTERED = {"flag": False}


def _reset_module_state():
    instantaneous_source_amplitudes.clear()
    _fingerprint_source.clear()


def _register_sim_cleanup(sim):
    """
    Register an interpreter-exit cleanup that resets shared module state.

    Parameters
    ----------
    sim : meep.Simulation
        The active Meep simulation (unused; present for signature symmetry).
    """

    if not __MXL__ATEXIT_REGISTERED["flag"]:
        atexit.register(_reset_module_state)
        __MXL__ATEXIT_REGISTERED["flag"] = True


def _to_mp_v3(v):
    """
    Convert a 3-vector into ``mp.Vector3``.

    Parameters
    ----------
    v : Vector3 or tuple or list or mp.Vector3
        Vector-like input.

    Returns
    -------
    mp.Vector3
        The corresponding Meep vector.
    """

    if isinstance(v, Vector3):
        return mp.Vector3(v.x, v.y, v.z)
    # tolerate mp.Vector3 already, or (x,y,z)
    if hasattr(v, "x") and hasattr(v, "y") and hasattr(v, "z"):
        return v
    if isinstance(v, (tuple, list)) and len(v) == 3:
        return mp.Vector3(*v)
    return mp.Vector3()


def _make_custom_time_src(key):
    """
    Create a Meep ``CustomSource`` that streams time-dependent amplitudes from a
    shared accumulator keyed by ``key``.

    Parameters
    ----------
    key : hashable
        Lookup key for the instantaneous source amplitude.

    Returns
    -------
    mp.CustomSource
        A custom source object that queries the accumulator each time step.
    """

    # MEEP calls this each time step; stream from the global accumulator
    return mp.CustomSource(lambda t: instantaneous_source_amplitudes[key])


class MeepUnits(DummyEMUnits):

    def __init__(self, time_units_fs: float = 0.1):
        """
        Meep unit system with conversions to/from atomic units.

        Parameters
        ----------
        time_units_fs : float, default: 0.1
            The Meep time unit expressed in femtoseconds.
        """
        super().__init__()
        self.time_units_fs = time_units_fs

    def efield_em_to_au(self, Emu_vec3):
        """
        Convert the regularized electric-field integral from Meep units to atomic units.

        Notes
        -----
        This helper is used directly by the Meep backend.

        Parameters
        ----------
        Emu_vec3 : array-like of float, shape (3,)
            Regularized field integral in Meep units.

        Returns
        -------
        numpy.ndarray of float, shape (3,)
            Field integral in atomic units.
        """

        factor = 1.2929541569381223e-6 / (self.time_units_fs**2)
        return np.asarray(Emu_vec3, dtype=float) * factor

    def source_amp_au_to_em(self, amp_au_vec3):
        """
        Convert source amplitude (a.u.) to Meep units.

        Parameters
        ----------
        amp_au_vec3 : array-like of float, shape (3,)
            Source amplitude vector in atomic units.

        Returns
        -------
        numpy.ndarray of float, shape (3,)
            Source amplitude vector in Meep units.
        """

        factor = 0.002209799779149953
        return np.asarray(amp_au_vec3, dtype=float) * factor

    def time_em_to_au(self, time_em: float):
        """
        Convert Meep time to atomic units.

        Parameters
        ----------
        time_em : float
            Time in Meep units.

        Returns
        -------
        float
            Time in atomic units.
        """

        return float(time_em) * self.time_units_fs * FS_TO_AU

    def units_helper(self, dx, dt):
        """
        Explain the Meep unit system and its connection to atomic units.

        This prints a summary of conversions based on the current resolution and
        Courant factor.

        Parameters
        ----------
        dx : float
            Spatial grid spacing in Meep units.
        dt : float
            Time step size in Meep units.

        Raises
        ------
        RuntimeError
            If the Courant factor is not ``0.5``.
        """

        # calculate units conversion factors
        mu2efield_au = self.efield_em_to_au(1.0)
        mu2efield_si = mu2efield_au * 5.14220675112e11  # V/m
        resolution = int(1.0 / dx)
        courant = dt / dx
        if courant != 0.5:
            raise RuntimeError("MaxwellLink currently only supports Courant=0.5!")

        omega_mu_to_ev = 0.242 / self.time_units_fs * 27.211 * 0.1 * 2.0 * np.pi
        omega_mu_to_cminv = (
            0.242 / self.time_units_fs * 27.211 * 0.1 * EV_TO_CM_INV * 2.0 * np.pi
        )
        omega_mu_to_au = 0.242 / self.time_units_fs * 0.1 * 2.0 * np.pi

        # audipoledt2mu = atomic_to_meep_units_SourceAmp(1.0, self.time_units_fs)
        print(
            "\n\n ######### MaxwellLink Units Helper #########\n",
            "MEEP uses its own units system, which is based on the speed of light in vacuum (c=1), \n",
            "the permittivity of free space (epsilon_0=1), and the permeability of free space (mu_0=1). \n",
            "To couple MEEP with molecular dynamics, we set [c] = [epsilon_0] = [mu_0] = [hbar] = 1. \n",
            "By further defining the time unit as %.4E fs, we can fix the units system of MEEP (mu).\n\n"
            % self.time_units_fs,
            "Given the simulation resolution = %d,\n - FDTD dt = %.4E mu (0.5/resolution) = %.4E fs\n"
            % (resolution, dt, dt * self.time_units_fs),
            "- FDTD dx = %.4E mu (1.0/resolution) = %.4E nm\n"
            % (dx, dx * self.time_units_fs * 299.792458),
            "- Time [t]: 1 mu = %.4E fs = %.4E a.u.\n"
            % (self.time_units_fs, self.time_units_fs * FS_TO_AU),
            "- Length [x]: 1 mu = %.4E nm\n" % (299.792458 * self.time_units_fs),
            "- EM wavelength of 1 mu, angular frequency omega = 2pi mu = %.4E eV = %.4E cm-1 = %.4E a.u.\n"
            % (
                omega_mu_to_ev,
                omega_mu_to_cminv,
                omega_mu_to_au,
            ),
            "- Note that sources and dielectrics defined in MEEP use rotational frequency (f=omega/2pi), \n",
            "- so probabably we need covert 1 eV photon energy to rotational frequency f = %.4E mu\n"
            % (1.0 / omega_mu_to_ev),
            "- Electric field [E]: 1 mu = %.4E V/m = %.4E a.u.\n"
            % (mu2efield_si, mu2efield_au),
            "Hope this helps!\n",
            "############################################\n\n",
        )


class MoleculeMeepWrapper(MoleculeDummyWrapper):
    """
    Wrapper that adapts a ``Molecule`` to Meep, handling units, sources, and IO.
    """

    def __init__(
        self,
        time_units_fs: float = 0.1,
        dt: Optional[float] = None,
        molecule: Molecule = None,
    ):
        """
        Initialize the Meep molecule wrapper.

        Parameters
        ----------
        time_units_fs : float, default: 0.1
            The Meep time unit expressed in femtoseconds.
        dt : float or None, optional
            Time step in Meep units; if provided, propagated to the molecule.
        molecule : Molecule
            The molecule to wrap and couple to Meep.
        """
        super().__init__(molecule)
        # self.m = molecule
        self.m._refresh_time_units(time_units_fs)
        if dt is not None:
            self.m._refresh_time_step(dt)
        self.em_units = MeepUnits(time_units_fs=time_units_fs)

        # promote member variables of Molecule for convenience
        self.sigma = self.m.sigma
        self.dimensions = self.m.dimensions
        self.center = self.m.center
        self.size = self.m.size
        self.rescaling_factor = self.m.rescaling_factor
        self.polarization_fingerprint_hash = self.m.polarization_fingerprint_hash
        self.initial_payload = self.m.init_payload
        self.molecule_id = self.m.molecule_id
        self.additional_data_history = self.m.additional_data_history
        self.init_payload = self.m.init_payload
        self.sources = self.m.sources
        self.dt_au = self.m.dt_au
        self.polarization_type = self.m.polarization_type

        # if polarization_type is "numerical" or "transverse", self.m.resolution must be set to a positive number
        if self.polarization_type in ["numerical", "transverse", "point", "point-raw"]:
            if (
                not hasattr(self.m, "resolution")
                or self.m.resolution is None
                or self.m.resolution <= 0
            ):
                raise ValueError(
                    "For numerical, transverse, point, or point-raw polarization_type, the molecular resolution must be set to a positive number indicating the Meep simulation resolution."
                )
        if self.polarization_type == "anisotropic":
            if not isinstance(self.sigma, (list, tuple)) or len(self.sigma) != 3:
                raise ValueError(
                    "For anisotropic polarization_type, self.sigma should be a list or tuple of three floats indicating (sigma_x, sigma_y, sigma_z)."
                )
            self.sigma = np.array(self.sigma, dtype=float)

        if self.polarization_type == "point" or self.polarization_type == "point-raw":
            print(
                "###! Point dipole polarization_type yields very fluctuating spontaneous emission decay in 3D only !###",
                "###! Please consider using 'analytical', 'numerical' or 'transverse' polarization_type with a small sigma instead. !###",
            )

        self._polarization_prefactor_3d = (
            1.0 / (2.0 * np.pi) ** 1.5 / self.sigma**5 * self.rescaling_factor
        )
        self._polarization_prefactor_2d = (
            1.0 / (2.0 * np.pi) ** 1.0 / self.sigma**2 * self.rescaling_factor
        )
        self._polarization_prefactor_1d = (
            1.0 / (2.0 * np.pi) ** 0.5 / self.sigma * self.rescaling_factor
        )

        # reset the value of polarization prefactors in multidimeonsional case
        if self.polarization_type == "anisotropic":
            # note that in 3D case, now the polarization is simply exp^(- (x^2/2/sigma_x^2 + y^2/2/sigma_y^2 + z^2/2/sigma_z^2)) instead of the x/y/z^2 weighted form
            self.sigma_x, self.sigma_y, self.sigma_z = self.sigma
            self._polarization_prefactor_1d = (
                1.0 / (2.0 * np.pi) ** 0.5 / self.sigma_x * self.rescaling_factor
            )
            self._polarization_prefactor_2d = (
                1.0
                / (2.0 * np.pi) ** 1.0
                / (self.sigma_x * self.sigma_y)
                * self.rescaling_factor
            )
            self._polarization_prefactor_3d = (
                1.0
                / (2.0 * np.pi) ** 1.5
                / (self.sigma_x * self.sigma_y * self.sigma_z)
                * self.rescaling_factor
            )

    def _init_sources(self):
        """
        Construct Meep sources for the molecule's polarization kernel (1D/2D/3D).

        Notes
        -----
        Sources are memoized per polarization fingerprint and component to allow
        sharing across molecules with identical spatial kernels.
        """
        if self.polarization_type == "analytical":
            self._init_sources_gaussian_analytical()
        elif self.polarization_type == "numerical":
            self._init_sources_gaussian_numerical()
        elif self.polarization_type == "transverse":
            self._init_sources_transverse()
        elif self.polarization_type == "point" or self.polarization_type == "point-raw":
            self._init_sources_point()
        elif self.polarization_type == "anisotropic":
            self._init_sources_anisotropic_analytical()
        else:
            raise ValueError(
                f"Unsupported polarization_type '{self.polarization_type}' for Meep molecule."
            )

    def _init_sources_gaussian_analytical(self):
        """
        Construct Meep sources for the molecule's polarization kernel (1D/2D/3D)
        using analytical Gaussian functions.

        Notes
        -----
        Sources are memoized per polarization fingerprint and component to allow
        sharing across molecules with identical spatial kernels.
        """

        srcs = []
        center = _to_mp_v3(self.center)
        size = _to_mp_v3(self.size)

        def amp_func_3d_x(R):
            return (
                self._polarization_prefactor_3d
                * R.x**2
                * np.exp(-R.norm() ** 2 / (2.0 * self.sigma**2))
            )

        def amp_func_3d_y(R):
            return (
                self._polarization_prefactor_3d
                * R.y**2
                * np.exp(-R.norm() ** 2 / (2.0 * self.sigma**2))
            )

        def amp_func_3d_z(R):
            return (
                self._polarization_prefactor_3d
                * R.z**2
                * np.exp(-R.norm() ** 2 / (2.0 * self.sigma**2))
            )

        def amp_func_2d(R):
            return self._polarization_prefactor_2d * np.exp(
                -(R.x**2 + R.y**2) / (2.0 * self.sigma**2)
            )

        def amp_func_1d(R):
            return self._polarization_prefactor_1d * np.exp(
                -(R.x**2) / (2.0 * self.sigma**2)
            )

        if self.dimensions == 1:
            key = (self.polarization_fingerprint_hash, "Ez")
            if key not in _fingerprint_source:
                instantaneous_source_amplitudes[key] = 0.0
                _fingerprint_source[key] = mp.Source(
                    src=_make_custom_time_src(key),
                    component=mp.Ez,
                    center=center,
                    size=size,
                    amplitude=1.0,
                    amp_func=amp_func_1d,
                )
            srcs.append(_fingerprint_source[key])
        elif self.dimensions == 2:
            key = (self.polarization_fingerprint_hash, "Ez")
            if key not in _fingerprint_source:
                instantaneous_source_amplitudes[key] = 0.0
                _fingerprint_source[key] = mp.Source(
                    src=_make_custom_time_src(key),
                    component=mp.Ez,
                    center=center,
                    size=size,
                    amplitude=1.0,
                    amp_func=amp_func_2d,
                )
            srcs.append(_fingerprint_source[key])
        else:  # 3D
            for comp, tag, amp_func in (
                (mp.Ex, "Ex", amp_func_3d_x),
                (mp.Ey, "Ey", amp_func_3d_y),
                (mp.Ez, "Ez", amp_func_3d_z),
            ):
                key = (self.polarization_fingerprint_hash, tag)
                if key not in _fingerprint_source:
                    instantaneous_source_amplitudes[key] = 0.0
                    _fingerprint_source[key] = mp.Source(
                        src=_make_custom_time_src(key),
                        component=comp,
                        center=center,
                        size=size,
                        amplitude=1.0,
                        amp_func=amp_func,
                    )
                srcs.append(_fingerprint_source[key])

        self.sources = srcs

    def _init_sources_gaussian_numerical(self):
        """
        Construct Meep sources for the molecule's polarization kernel (1D/2D/3D)
        using numerical Gaussian functions ()

        Notes
        -----
        Sources are memoized per polarization fingerprint and component to allow
        sharing across molecules with identical spatial kernels.
        """

        srcs = []
        center = _to_mp_v3(self.center)
        size = _to_mp_v3(self.size)
        dx = 1.0 / self.m.resolution

        if self.dimensions == 1:
            key = (self.polarization_fingerprint_hash, "Ez")
            if key not in _fingerprint_source:

                # create polarization amp data numerically
                X = np.arange(-self.size.x / 2, self.size.x / 2, dx)
                amp_data = self._polarization_prefactor_1d * np.exp(
                    -(X**2) / (2.0 * self.sigma**2)
                )
                amp_data = amp_data.reshape((-1, 1, 1))
                amp_data = np.copy(amp_data.astype(np.complex128), order="C")

                instantaneous_source_amplitudes[key] = 0.0
                _fingerprint_source[key] = mp.Source(
                    src=_make_custom_time_src(key),
                    component=mp.Ez,
                    center=center,
                    size=size,
                    amplitude=1.0,
                    amp_data=amp_data,
                )
            srcs.append(_fingerprint_source[key])
        elif self.dimensions == 2:
            key = (self.polarization_fingerprint_hash, "Ez")
            if key not in _fingerprint_source:

                # create polarization amp data numerically
                X = np.arange(-self.size.x / 2, self.size.x / 2, dx)
                Y = np.arange(-self.size.y / 2, self.size.y / 2, dx)
                XX, YY = np.meshgrid(X, Y, indexing="ij")
                amp_data = self._polarization_prefactor_2d * np.exp(
                    -(XX**2 + YY**2) / (2.0 * self.sigma**2)
                )
                shape = amp_data.shape
                amp_data = amp_data.reshape((shape[0], shape[1], 1))
                amp_data = np.copy(amp_data.astype(np.complex128), order="C")

                instantaneous_source_amplitudes[key] = 0.0
                _fingerprint_source[key] = mp.Source(
                    src=_make_custom_time_src(key),
                    component=mp.Ez,
                    center=center,
                    size=size,
                    amplitude=1.0,
                    amp_data=amp_data,
                )
            srcs.append(_fingerprint_source[key])
        else:  # 3D
            for comp, tag in (
                (mp.Ex, "Ex"),
                (mp.Ey, "Ey"),
                (mp.Ez, "Ez"),
            ):
                key = (self.polarization_fingerprint_hash, tag)
                if key not in _fingerprint_source:
                    # create polarization amp data numerically
                    X = np.arange(-self.size.x / 2, self.size.x / 2, dx)
                    Y = np.arange(-self.size.y / 2, self.size.y / 2, dx)
                    Z = np.arange(-self.size.z / 2, self.size.z / 2, dx)

                    XX, YY, ZZ = np.meshgrid(X, Y, Z, indexing="ij")
                    if tag == "Ex":
                        amp_data = (
                            self._polarization_prefactor_3d
                            * XX**2
                            * np.exp(-(XX**2 + YY**2 + ZZ**2) / (2.0 * self.sigma**2))
                        )
                    elif tag == "Ey":
                        amp_data = (
                            self._polarization_prefactor_3d
                            * YY**2
                            * np.exp(-(XX**2 + YY**2 + ZZ**2) / (2.0 * self.sigma**2))
                        )
                    else:
                        amp_data = (
                            self._polarization_prefactor_3d
                            * ZZ**2
                            * np.exp(-(XX**2 + YY**2 + ZZ**2) / (2.0 * self.sigma**2))
                        )

                    amp_data = np.copy(amp_data.astype(np.complex128), order="C")

                    instantaneous_source_amplitudes[key] = 0.0
                    _fingerprint_source[key] = mp.Source(
                        src=_make_custom_time_src(key),
                        component=comp,
                        center=center,
                        size=size,
                        amplitude=1.0,
                        amp_data=amp_data,
                    )
                srcs.append(_fingerprint_source[key])

        self.sources = srcs

    def _init_sources_transverse(self):
        """
        Construct Meep sources for the molecule's polarization kernel (1D/2D/3D)
        using transverse numerical Gaussian functions evaluated by FFT.

        Notes
        -----
        Sources are memoized per polarization fingerprint and component to allow
        sharing across molecules with identical spatial kernels.
        """

        srcs = []
        center = _to_mp_v3(self.center)
        size = _to_mp_v3(self.size)
        dx = 1.0 / self.m.resolution

        if self.dimensions == 1 or self.dimensions == 2:
            raise ValueError(
                "Transverse polarization_type is currently only supported in 3D simulations."
            )

        if self.dimensions == 1:
            key = (self.polarization_fingerprint_hash, "Ez")
            if key not in _fingerprint_source:

                # create polarization amp data numerically
                X = np.arange(-self.size.x / 2, self.size.x / 2, dx)
                amp_data = self._polarization_prefactor_1d * np.exp(
                    -(X**2) / (2.0 * self.sigma**2)
                )
                amp_data = amp_data.reshape((-1, 1, 1))
                amp_data = np.copy(amp_data.astype(np.complex128), order="C")

                instantaneous_source_amplitudes[key] = 0.0
                _fingerprint_source[key] = mp.Source(
                    src=_make_custom_time_src(key),
                    component=mp.Ez,
                    center=center,
                    size=size,
                    amplitude=1.0,
                    amp_data=amp_data,
                )
            srcs.append(_fingerprint_source[key])
        elif self.dimensions == 2:
            key = (self.polarization_fingerprint_hash, "Ez")
            if key not in _fingerprint_source:

                # create polarization amp data numerically
                X = np.arange(-self.size.x / 2, self.size.x / 2, dx)
                Y = np.arange(-self.size.y / 2, self.size.y / 2, dx)
                XX, YY = np.meshgrid(X, Y, indexing="ij")
                amp_data = self._polarization_prefactor_2d * np.exp(
                    -(XX**2 + YY**2) / (2.0 * self.sigma**2)
                )
                shape = amp_data.shape
                amp_data = amp_data.reshape((shape[0], shape[1], 1))
                amp_data = np.copy(amp_data.astype(np.complex128), order="C")

                instantaneous_source_amplitudes[key] = 0.0
                _fingerprint_source[key] = mp.Source(
                    src=_make_custom_time_src(key),
                    component=mp.Ez,
                    center=center,
                    size=size,
                    amplitude=1.0,
                    amp_data=amp_data,
                )
            srcs.append(_fingerprint_source[key])
        else:  # 3D
            for comp, tag in (
                (mp.Ex, "Ex"),
                (mp.Ey, "Ey"),
                (mp.Ez, "Ez"),
            ):
                cutoff = 30
                key = (self.polarization_fingerprint_hash, tag)
                if key not in _fingerprint_source:
                    # create polarization amp data numerically
                    X = np.arange(-self.size.x / 2, self.size.x / 2, dx)
                    Y = np.arange(-self.size.y / 2, self.size.y / 2, dx)
                    Z = np.arange(-self.size.z / 2, self.size.z / 2, dx)

                    XX, YY, ZZ = np.meshgrid(X, Y, Z, indexing="ij")

                    if tag == "Ex":
                        _, px_t, py_t, pz_t = calc_transverse_components_3d(
                            size=(self.size.x, self.size.y, self.size.z),
                            dx=dx,
                            sigma=self.sigma,
                            mu12=self.rescaling_factor,
                            local_size=self.size.x * cutoff,
                            component="x",
                        )

                        amp_data = px_t
                    elif tag == "Ey":
                        _, px_t, py_t, pz_t = calc_transverse_components_3d(
                            size=(self.size.x, self.size.y, self.size.z),
                            dx=dx,
                            sigma=self.sigma,
                            mu12=self.rescaling_factor,
                            local_size=self.size.x * cutoff,
                            component="y",
                        )

                        amp_data = py_t
                    else:
                        _, px_t, py_t, pz_t = calc_transverse_components_3d(
                            size=(self.size.x, self.size.y, self.size.z),
                            dx=dx,
                            sigma=self.sigma,
                            mu12=self.rescaling_factor,
                            local_size=self.size.x * cutoff,
                            component="z",
                        )

                        amp_data = pz_t

                    amp_data = np.copy(amp_data.astype(np.complex128), order="C")

                    instantaneous_source_amplitudes[key] = 0.0
                    _fingerprint_source[key] = mp.Source(
                        src=_make_custom_time_src(key),
                        component=comp,
                        center=center,
                        size=size,
                        amplitude=1.0,
                        amp_data=amp_data,
                    )
                srcs.append(_fingerprint_source[key])

        self.sources = srcs

    def _init_sources_point(self):
        """
        Construct Meep sources for the molecule's polarization kernel (1D/2D/3D)
        using a point dipole approximation.

        Notes
        -----
        Sources are memoized per polarization fingerprint and component to allow
        sharing across molecules with identical spatial kernels.
        """

        srcs = []
        center = _to_mp_v3(self.center)
        size = _to_mp_v3(self.size)
        # we need to normalize the prefactor for point dipole source
        dx = 1.0 / self.m.resolution
        # overwrite the size to (0, 0, 0) as we are using point dipole approximation
        size = mp.Vector3(0.0, 0.0, 0.0)

        if self.dimensions == 1:
            key = (self.polarization_fingerprint_hash, "Ez")
            amplitude = self.rescaling_factor * 1.0 / dx**1
            if key not in _fingerprint_source:
                instantaneous_source_amplitudes[key] = 0.0
                _fingerprint_source[key] = mp.Source(
                    src=_make_custom_time_src(key),
                    component=mp.Ez,
                    center=center,
                    size=size,
                    amplitude=amplitude,
                )
            srcs.append(_fingerprint_source[key])
        elif self.dimensions == 2:
            key = (self.polarization_fingerprint_hash, "Ez")
            amplitude = self.rescaling_factor * 1.0 / dx**2
            if key not in _fingerprint_source:
                instantaneous_source_amplitudes[key] = 0.0
                _fingerprint_source[key] = mp.Source(
                    src=_make_custom_time_src(key),
                    component=mp.Ez,
                    center=center,
                    size=size,
                    amplitude=amplitude,
                )
            srcs.append(_fingerprint_source[key])
        else:  # 3D
            for comp, tag in (
                (mp.Ex, "Ex"),
                (mp.Ey, "Ey"),
                (mp.Ez, "Ez"),
            ):
                key = (self.polarization_fingerprint_hash, tag)
                amplitude = self.rescaling_factor * 1.0 / dx**3
                if key not in _fingerprint_source:
                    instantaneous_source_amplitudes[key] = 0.0
                    _fingerprint_source[key] = mp.Source(
                        src=_make_custom_time_src(key),
                        component=comp,
                        center=center,
                        size=size,
                        amplitude=amplitude,
                    )
                srcs.append(_fingerprint_source[key])

        self.sources = srcs

    def _init_sources_anisotropic_analytical(self):
        """
        Construct Meep sources for the molecule's polarization kernel (1D/2D/3D)
        using anisotropic analytical Gaussian functions.

        Notes
        -----
        Sources are memoized per polarization fingerprint and component to allow
        sharing across molecules with identical spatial kernels.
        """

        srcs = []
        center = _to_mp_v3(self.center)
        size = _to_mp_v3(self.size)

        def amp_func_3d(R):
            return self._polarization_prefactor_3d * np.exp(
                -R.x**2 / (2.0 * self.sigma_x**2)
                - R.y**2 / (2.0 * self.sigma_y**2)
                - R.z**2 / (2.0 * self.sigma_z**2)
            )

        def amp_func_2d(R):
            return self._polarization_prefactor_2d * np.exp(
                -(R.x**2 / (2.0 * self.sigma_x**2) + R.y**2 / (2.0 * self.sigma_y**2))
            )

        def amp_func_1d(R):
            return self._polarization_prefactor_1d * np.exp(
                -(R.x**2) / (2.0 * self.sigma_x**2)
            )

        if self.dimensions == 1:
            key = (self.polarization_fingerprint_hash, "Ez")
            if key not in _fingerprint_source:
                instantaneous_source_amplitudes[key] = 0.0
                _fingerprint_source[key] = mp.Source(
                    src=_make_custom_time_src(key),
                    component=mp.Ez,
                    center=center,
                    size=size,
                    amplitude=1.0,
                    amp_func=amp_func_1d,
                )
            srcs.append(_fingerprint_source[key])
        elif self.dimensions == 2:
            key = (self.polarization_fingerprint_hash, "Ez")
            if key not in _fingerprint_source:
                instantaneous_source_amplitudes[key] = 0.0
                _fingerprint_source[key] = mp.Source(
                    src=_make_custom_time_src(key),
                    component=mp.Ez,
                    center=center,
                    size=size,
                    amplitude=1.0,
                    amp_func=amp_func_2d,
                )
            srcs.append(_fingerprint_source[key])
        else:  # 3D
            for comp, tag in (
                (mp.Ex, "Ex"),
                (mp.Ey, "Ey"),
                (mp.Ez, "Ez"),
            ):
                key = (self.polarization_fingerprint_hash, tag)
                if key not in _fingerprint_source:
                    instantaneous_source_amplitudes[key] = 0.0
                    _fingerprint_source[key] = mp.Source(
                        src=_make_custom_time_src(key),
                        component=comp,
                        center=center,
                        size=size,
                        amplitude=1.0,
                        amp_func=amp_func_3d,
                    )
                srcs.append(_fingerprint_source[key])

        self.sources = srcs

    def _calculate_ep_integral(self, sim: mp.Simulation):
        """
        Compute the regularized E-field integral over the molecule's kernel.

        Parameters
        ----------
        sim : meep.Simulation
            Active Meep simulation.

        Returns
        -------
        list of float
            Regularized field integrals ``[I_x, I_y, I_z]`` in Meep units.
        """
        if self.polarization_type in ["analytical", "numerical", "transverse"]:
            return self._calculate_ep_integral_gaussian_analytical(sim)
        elif self.polarization_type == "point":
            return self._calculate_ep_integral_point(sim)
        elif self.polarization_type == "point-raw":
            return self._calculate_ep_integral_point_raw(sim)
        elif self.polarization_type == "anisotropic":
            return self._calculate_ep_integral_anisotropic_analytical(sim)
        else:
            raise ValueError(
                f"Unsupported polarization_type '{self.polarization_type}' for Meep molecule."
            )

    def _calculate_ep_integral_gaussian_analytical(self, sim: mp.Simulation):
        """
        Compute the regularized E-field integral over the molecule's kernel
        using analytical Gaussian functions.

        Parameters
        ----------
        sim : meep.Simulation
            Active Meep simulation.

        Returns
        -------
        list of float
            Regularized field integrals ``[I_x, I_y, I_z]`` in Meep units.
        """

        vol = mp.Volume(size=_to_mp_v3(self.size), center=_to_mp_v3(self.center))
        x = y = z = 0.0
        if self.dimensions == 1:
            z = sim.integrate_field_function(
                [mp.Ez],
                lambda R, ez: self._polarization_prefactor_1d
                * exp(
                    -((R.x - self.center.x) * (R.x - self.center.x))
                    / (2.0 * self.sigma**2)
                )
                * (ez),
                vol,
            )
        elif self.dimensions == 2:
            z = sim.integrate_field_function(
                [mp.Ez],
                lambda R, ez: self._polarization_prefactor_2d
                * exp(
                    -(
                        (R.x - self.center.x) * (R.x - self.center.x)
                        + (R.y - self.center.y) * (R.y - self.center.y)
                    )
                    / (2.0 * self.sigma**2)
                )
                * (ez),
                vol,
            )
        else:  # 3D
            z = sim.integrate_field_function(
                [mp.Ez],
                lambda R, ez: self._polarization_prefactor_3d
                * (R.z - self.center.z)
                * (R.z - self.center.z)
                * exp(
                    -(
                        (R.x - self.center.x) * (R.x - self.center.x)
                        + (R.y - self.center.y) * (R.y - self.center.y)
                        + (R.z - self.center.z) * (R.z - self.center.z)
                    )
                    / (2.0 * self.sigma**2)
                )
                * (ez),
                vol,
            )
            x = sim.integrate_field_function(
                [mp.Ex],
                lambda R, ex: self._polarization_prefactor_3d
                * (R.x - self.center.x)
                * (R.x - self.center.x)
                * exp(
                    -(
                        (R.x - self.center.x) * (R.x - self.center.x)
                        + (R.y - self.center.y) * (R.y - self.center.y)
                        + (R.z - self.center.z) * (R.z - self.center.z)
                    )
                    / (2.0 * self.sigma**2)
                )
                * (ex),
                vol,
            )
            y = sim.integrate_field_function(
                [mp.Ey],
                lambda R, ey: self._polarization_prefactor_3d
                * (R.y - self.center.y)
                * (R.y - self.center.y)
                * exp(
                    -(
                        (R.x - self.center.x) * (R.x - self.center.x)
                        + (R.y - self.center.y) * (R.y - self.center.y)
                        + (R.z - self.center.z) * (R.z - self.center.z)
                    )
                    / (2.0 * self.sigma**2)
                )
                * (ey),
                vol,
            )
        return [np.real(x), np.real(y), np.real(z)]

    def _calculate_ep_integral_point(self, sim: mp.Simulation):
        """
        Compute the regularized E-field integral over the molecule's kernel
        using point dipole approximation.

        Parameters
        ----------
        sim : meep.Simulation
            Active Meep simulation.

        Returns
        -------
        list of float
            Regularized field integrals ``[I_x, I_y, I_z]`` in Meep units.
        """

        dx = 1.0 / self.m.resolution

        vol = mp.Volume(size=_to_mp_v3(self.size), center=_to_mp_v3(self.center))
        x = y = z = 0.0
        if self.dimensions == 1:
            vol = mp.Volume(size=mp.Vector3(dx, 0, 0), center=_to_mp_v3(self.center))
            ez = sim.get_array(mp.Ez, vol).mean()
            z = self.rescaling_factor * ez * dx**1
        elif self.dimensions == 2:
            vol = mp.Volume(size=mp.Vector3(dx, dx, 0), center=_to_mp_v3(self.center))
            ez = sim.get_array(mp.Ez, vol).mean()
            z = self.rescaling_factor * ez * dx**2
        else:  # 3D
            # ex = sim.get_field_point(mp.Ex, self.center)
            # ey = sim.get_field_point(mp.Ey, self.center)
            # z = sim.get_field_point(mp.Ez, self.center)
            # directly using the raw value at the point yields very noisy results, so we multiply by the voxel volume here
            vol = mp.Volume(size=mp.Vector3(dx, dx, dx), center=_to_mp_v3(self.center))
            ex = sim.get_array(mp.Ex, vol).mean()
            ey = sim.get_array(mp.Ey, vol).mean()
            ez = sim.get_array(mp.Ez, vol).mean()
            x = self.rescaling_factor * ex * dx**3
            y = self.rescaling_factor * ey * dx**3
            z = self.rescaling_factor * ez * dx**3
        return [np.real(x), np.real(y), np.real(z)]

    def _calculate_ep_integral_point_raw(self, sim: mp.Simulation):
        """
        Compute the regularized E-field integral over the molecule's kernel
        using point dipole approximation with no sub-averaging over dx^3.

        Parameters
        ----------
        sim : meep.Simulation
            Active Meep simulation.

        Returns
        -------
        list of float
            Regularized field integrals ``[I_x, I_y, I_z]`` in Meep units.
        """

        dx = 1.0 / self.m.resolution

        x = y = z = 0.0
        if self.dimensions == 1:
            ez = sim.get_field_point(mp.Ez, self.center)
            z = self.rescaling_factor * ez * dx**1
        elif self.dimensions == 2:
            ez = sim.get_field_point(mp.Ez, self.center)
            z = self.rescaling_factor * ez * dx**2
        else:  # 3D
            ex = sim.get_field_point(mp.Ex, self.center)
            ey = sim.get_field_point(mp.Ey, self.center)
            ez = sim.get_field_point(mp.Ez, self.center)
            x = self.rescaling_factor * ex * dx**3
            y = self.rescaling_factor * ey * dx**3
            z = self.rescaling_factor * ez * dx**3
        return [np.real(x), np.real(y), np.real(z)]

    def _calculate_ep_integral_anisotropic_analytical(self, sim: mp.Simulation):
        """
        Compute the regularized E-field integral over the molecule's kernel
        using anisotropic analytical Gaussian functions.

        Parameters
        ----------
        sim : meep.Simulation
            Active Meep simulation.

        Returns
        -------
        list of float
            Regularized field integrals ``[I_x, I_y, I_z]`` in Meep units.
        """

        vol = mp.Volume(size=_to_mp_v3(self.size), center=_to_mp_v3(self.center))
        x = y = z = 0.0
        if self.dimensions == 1:
            z = sim.integrate_field_function(
                [mp.Ez],
                lambda R, ez: self._polarization_prefactor_1d
                * exp(
                    -((R.x - self.center.x) * (R.x - self.center.x))
                    / (2.0 * self.sigma_x**2)
                )
                * (ez),
                vol,
            )
        elif self.dimensions == 2:
            z = sim.integrate_field_function(
                [mp.Ez],
                lambda R, ez: self._polarization_prefactor_2d
                * exp(
                    -(
                        (R.x - self.center.x)
                        * (R.x - self.center.x)
                        / (2.0 * self.sigma_x**2)
                        + (R.y - self.center.y)
                        * (R.y - self.center.y)
                        / (2.0 * self.sigma_y**2)
                    )
                )
                * (ez),
                vol,
            )
        else:  # 3D
            z = sim.integrate_field_function(
                [mp.Ez],
                lambda R, ez: self._polarization_prefactor_3d
                * exp(
                    -(
                        (R.x - self.center.x)
                        * (R.x - self.center.x)
                        / (2.0 * self.sigma_x**2)
                        + (R.y - self.center.y)
                        * (R.y - self.center.y)
                        / (2.0 * self.sigma_y**2)
                        + (R.z - self.center.z)
                        * (R.z - self.center.z)
                        / (2.0 * self.sigma_z**2)
                    )
                )
                * (ez),
                vol,
            )
            x = sim.integrate_field_function(
                [mp.Ex],
                lambda R, ex: self._polarization_prefactor_3d
                * exp(
                    -(
                        (R.x - self.center.x)
                        * (R.x - self.center.x)
                        / (2.0 * self.sigma_x**2)
                        + (R.y - self.center.y)
                        * (R.y - self.center.y)
                        / (2.0 * self.sigma_y**2)
                        + (R.z - self.center.z)
                        * (R.z - self.center.z)
                        / (2.0 * self.sigma_z**2)
                    )
                )
                * (ex),
                vol,
            )
            y = sim.integrate_field_function(
                [mp.Ey],
                lambda R, ey: self._polarization_prefactor_3d
                * exp(
                    -(
                        (R.x - self.center.x)
                        * (R.x - self.center.x)
                        / (2.0 * self.sigma_x**2)
                        + (R.y - self.center.y)
                        * (R.y - self.center.y)
                        / (2.0 * self.sigma_y**2)
                        + (R.z - self.center.z)
                        * (R.z - self.center.z)
                        / (2.0 * self.sigma_z**2)
                    )
                )
                * (ey),
                vol,
            )
        return [np.real(x), np.real(y), np.real(z)]


# ---------- NON-SOCKET Step Function for MEEP ----------
def update_molecules_no_socket(
    molecules: List[MoleculeMeepWrapper], sources_non_molecule: List = None
):
    """
    Create a Meep step function (no sockets) that couples molecules to the EM grid.

    Parameters
    ----------
    molecules : list of MoleculeMeepWrapper
        Molecules to couple.
    sources_non_molecule : list or None, optional
        Additional Meep sources unrelated to molecules.

    Returns
    -------
    callable
        A step function compatible with ``meep.Simulation.run``.
    """

    if sources_non_molecule is None:
        sources_non_molecule = []

    started = {"flag": False}

    def __step_function__(sim: mp.Simulation):
        # First-time barrier: bind/init all drivers
        while not started["flag"]:

            _reset_module_state()

            for idx, m in enumerate(molecules):
                # initialize molecular drivers directly
                m.initialize_driver(assigned_id=idx)
                if not m.sources:
                    m._init_sources()
            unique_sources = list(
                {id(src): src for m in molecules for src in m.sources}.values()
            )
            sim.change_sources(list(sources_non_molecule) + unique_sources)
            _register_sim_cleanup(sim)
            started["flag"] = True

        # Build requests (E-field integrals cached by fingerprint)
        regularized_efield_integrals: Dict[int, List[float]] = {}
        for m in molecules:
            fp = m.polarization_fingerprint_hash
            if fp not in regularized_efield_integrals:
                int_ep_mu = m._calculate_ep_integral(sim)
                regularized_efield_integrals[fp] = int_ep_mu
            else:
                int_ep_mu = regularized_efield_integrals[fp]
            int_ep_au = m.em_units.efield_em_to_au(int_ep_mu)
            m.propagate(int_ep_au)

        # Aggregate amplitudes into per-(fingerprint,component) accumulators
        touched = set()
        for m in molecules:
            amp_au = np.asarray(m.calc_amp_vector(), dtype=float)
            amp_mu = m.em_units.source_amp_au_to_em(amp_au)

            if m.dimensions in (1, 2):
                key = (m.polarization_fingerprint_hash, "Ez")
                if key not in touched:
                    instantaneous_source_amplitudes[key] = 0.0
                    touched.add(key)
                instantaneous_source_amplitudes[key] += float(amp_mu[2])
            else:
                for tag, val in (
                    ("Ex", amp_mu[0]),
                    ("Ey", amp_mu[1]),
                    ("Ez", amp_mu[2]),
                ):
                    key = (m.polarization_fingerprint_hash, tag)
                    if key not in touched:
                        instantaneous_source_amplitudes[key] = 0.0
                        touched.add(key)
                    instantaneous_source_amplitudes[key] += float(val)

            extra_blob = m.d_f.append_additional_data()
            if extra_blob:
                try:
                    m.additional_data_history.append(extra_blob)
                except Exception:
                    pass

        # No change_sources(); CustomSource reads accumulators

    return __step_function__


# ---------- SOCKET: No-MPI Step Function for MEEP ----------
def update_molecules_no_mpi(
    hub, molecules: List[MoleculeMeepWrapper], sources_non_molecule: List = None
):
    """
    Create a Meep step function (sockets, no MPI) that couples molecules to the EM grid.

    Parameters
    ----------
    hub : :class:`~maxwelllink.sockets.sockets.SocketHub`
        Socket hub for driver communication.
    molecules : list of MoleculeMeepWrapper
        Molecules to couple.
    sources_non_molecule : list or None, optional
        Additional Meep sources unrelated to molecules.

    Returns
    -------
    callable
        A step function compatible with ``meep.Simulation.run``.
    """

    if sources_non_molecule is None:
        sources_non_molecule = []

    started = {"flag": False}
    paused = {"flag": False}

    def __step_function__(sim: mp.Simulation):
        # First-time barrier: bind/init all drivers
        while not started["flag"]:
            init_payloads = {
                m.molecule_id: {**m.init_payload, "molecule_id": m.molecule_id}
                for m in molecules
            }
            ok = hub.wait_until_bound(init_payloads, require_init=True, timeout=None)
            if not ok:
                raise RuntimeError("wait_until_bound timed out")
            # >>> deterministic per-simulation reset <<<
            _reset_module_state()

            # ensure meep sources exist & install once
            for m in molecules:
                if not m.sources:
                    m._init_sources()
            unique_sources = list(
                {id(src): src for m in molecules for src in m.sources}.values()
            )
            sim.change_sources(list(sources_non_molecule) + unique_sources)
            _register_sim_cleanup(sim)
            started["flag"] = True

        # Reconnect guard
        ids = [m.molecule_id for m in molecules]
        while not hub.all_bound(ids, require_init=True):
            if not paused["flag"]:
                print("[SocketHub] PAUSED: waiting for driver reconnection...")
                paused["flag"] = True
            init_payloads = {
                m.molecule_id: {**m.init_payload, "molecule_id": m.molecule_id}
                for m in molecules
            }
            hub.wait_until_bound(init_payloads, require_init=True, timeout=None)
            print("[SocketHub] RESUMED: all drivers reconnected.")
            paused["flag"] = False

        # Build requests (E-field integrals cached by fingerprint)
        requests = {}
        regularized_efield_integrals: Dict[int, List[float]] = {}
        for m in molecules:
            fp = m.polarization_fingerprint_hash
            if fp not in regularized_efield_integrals:
                int_ep_mu = m._calculate_ep_integral(sim)
                regularized_efield_integrals[fp] = int_ep_mu
            else:
                int_ep_mu = regularized_efield_integrals[fp]
            int_ep_au = m.em_units.efield_em_to_au(int_ep_mu)
            requests[m.molecule_id] = {
                "efield_au": int_ep_au,
                "meta": {"t": sim.meep_time()},
                "init": {**m.init_payload, "molecule_id": m.molecule_id},
            }

        # Hub round-trip with retry
        responses = hub.step_barrier(requests)
        while not responses:
            if not paused["flag"]:
                print("[SocketHub] PAUSED: waiting for driver reconnection...")
                paused["flag"] = True
            init_payloads = {
                m.molecule_id: {**m.init_payload, "molecule_id": m.molecule_id}
                for m in molecules
            }
            hub.wait_until_bound(init_payloads, require_init=True, timeout=None)
            print("[SocketHub] RESUMED: all drivers reconnected.")
            paused["flag"] = False
            responses = hub.step_barrier(requests)

        # Aggregate amplitudes into per-(fingerprint,component) accumulators
        touched = set()
        for m in molecules:
            if m.molecule_id not in responses:
                print(
                    f"Warning: no response for SocketMolecule {m.molecule_id} at t={sim.meep_time():.2f}."
                )
                continue
            amp_au = np.asarray(responses[m.molecule_id]["amp"], dtype=float)
            amp_mu = m.em_units.source_amp_au_to_em(amp_au)

            if m.dimensions in (1, 2):
                key = (m.polarization_fingerprint_hash, "Ez")
                if key not in touched:
                    instantaneous_source_amplitudes[key] = 0.0
                    touched.add(key)
                instantaneous_source_amplitudes[key] += float(amp_mu[2])
            else:
                for tag, val in (
                    ("Ex", amp_mu[0]),
                    ("Ey", amp_mu[1]),
                    ("Ez", amp_mu[2]),
                ):
                    key = (m.polarization_fingerprint_hash, tag)
                    if key not in touched:
                        instantaneous_source_amplitudes[key] = 0.0
                        touched.add(key)
                    instantaneous_source_amplitudes[key] += float(val)

            extra_blob = responses[m.molecule_id].get("extra", b"")
            if extra_blob:
                try:
                    m.additional_data_history.append(
                        json.loads(extra_blob.decode("utf-8"))
                    )
                except Exception:
                    pass

        # No change_sources(); CustomSource reads accumulators

    return __step_function__


# ---------- SOCKET: MPI Step Function for MEEP ----------
def update_molecules(
    hub, molecules: List[MoleculeMeepWrapper], sources_non_molecule: List = None
):
    """
    MPI-safe step function aligned with the no-MPI version and streaming sources.

    Parameters
    ----------
    hub : :class:`~maxwelllink.sockets.sockets.SocketHub`
        Socket hub for driver communication.
    molecules : list of MoleculeMeepWrapper
        Molecules to couple.
    sources_non_molecule : list or None, optional
        Additional Meep sources unrelated to molecules.

    Returns
    -------
    callable
        A step function compatible with ``meep.Simulation.run``.
    """

    import json as _json

    # detect MPI
    try:
        from mpi4py import MPI as _MPI

        _COMM = _MPI.COMM_WORLD
        _RANK = _COMM.Get_rank()
        _SIZE = _COMM.Get_size()
        _HAS_MPI = _SIZE > 1
    except Exception:
        _COMM = None
        _RANK = 0
        _SIZE = 1
        _HAS_MPI = False

    if sources_non_molecule is None:
        sources_non_molecule = []

    _is_master = (not _HAS_MPI) or (_RANK == 0 and mp.am_master())
    if _HAS_MPI and _RANK != 0:
        hub = None

    started = {"flag": False}
    paused = {"flag": False}

    def _bcast_bool(x: bool) -> bool:
        if _HAS_MPI:
            x = bool(_COMM.bcast(bool(x), root=0))
        return bool(x)

    def _bcast_array(arr, n):
        if not _HAS_MPI:
            return np.asarray(arr, dtype=float).reshape(n)
        if _is_master:
            buf = np.asarray(arr, dtype=float).reshape(n)
        else:
            buf = np.empty(n, dtype=float)
        _COMM.Bcast(buf, root=0)
        return buf

    def __step_function__(sim: mp.Simulation):
        # 1) FIRST-TIME BARRIER
        while not started["flag"]:
            ok = True
            if _is_master:
                init_payloads = {
                    m.molecule_id: {**m.init_payload, "molecule_id": m.molecule_id}
                    for m in molecules
                }
                ok = hub.wait_until_bound(
                    init_payloads, require_init=True, timeout=None
                )
            ok = _bcast_bool(ok)
            if not ok:
                raise RuntimeError("wait_until_bound timed out")

            # >>> deterministic per-simulation reset (all ranks must agree) <<<
            if _is_master:
                _reset_module_state()
            _ = _bcast_bool(True)  # simple sync point

            for m in molecules:
                if not m.sources:
                    m._init_sources()

            unique_sources = list(
                {id(src): src for m in molecules for src in m.sources}.values()
            )
            sim.change_sources(list(sources_non_molecule) + unique_sources)

            _register_sim_cleanup(sim)

            started["flag"] = True

        # 2) MID-RUN GUARD
        dropped = False
        if _is_master:
            ids = [m.molecule_id for m in molecules]
            dropped = not hub.all_bound(ids, require_init=True)
        dropped = _bcast_bool(dropped)
        while dropped:
            if not paused["flag"]:
                print("[SocketHub] PAUSED: waiting for driver reconnection...")
                paused["flag"] = True
            if _is_master:
                init_payloads = {
                    m.molecule_id: {**m.init_payload, "molecule_id": m.molecule_id}
                    for m in molecules
                }
                hub.wait_until_bound(init_payloads, require_init=True, timeout=None)
            _ = _bcast_bool(True)
            print("[SocketHub] RESUMED: all drivers reconnected.")
            paused["flag"] = False
            dropped = False
            if _is_master:
                ids = [m.molecule_id for m in molecules]
                dropped = not hub.all_bound(ids, require_init=True)
            dropped = _bcast_bool(dropped)

        # 3) BUILD REQUESTS
        requests = {}
        regularized_efield_integrals: Dict[int, List[float]] = {}
        for m in molecules:
            fp = m.polarization_fingerprint_hash
            if fp not in regularized_efield_integrals:
                int_ep_mu = m._calculate_ep_integral(sim)
                regularized_efield_integrals[fp] = int_ep_mu
            else:
                int_ep_mu = regularized_efield_integrals[fp]
            int_ep_au = m.em_units.efield_em_to_au(int_ep_mu)
            requests[m.molecule_id] = {
                "efield_au": int_ep_au,
                "meta": {"t": sim.meep_time()},
                "init": {**m.init_payload, "molecule_id": m.molecule_id},
            }

        # 4) MASTER EXCHANGE WITH DRIVERS
        nmol = len(molecules)
        amps_flat = np.zeros(3 * nmol, dtype=float)
        extras_by_id = {}
        had_responses = True

        if _is_master:
            responses = hub.step_barrier(requests)
            while not responses:
                if not paused["flag"]:
                    print("[SocketHub] PAUSED: waiting for driver reconnection...")
                    paused["flag"] = True
                init_payloads = {
                    m.molecule_id: {**m.init_payload, "molecule_id": m.molecule_id}
                    for m in molecules
                }
                hub.wait_until_bound(init_payloads, require_init=True, timeout=None)
                print("[SocketHub] RESUMED: all drivers reconnected.")
                paused["flag"] = False
                responses = hub.step_barrier(requests)

            off = 0
            for m in molecules:
                a = np.asarray(responses[m.molecule_id]["amp"], dtype=float).reshape(3)
                amps_flat[off : off + 3] = a
                extras_by_id[m.molecule_id] = responses[m.molecule_id].get("extra", b"")
                off += 3

        had_responses = _bcast_bool(had_responses and _is_master or (not _HAS_MPI))
        if not had_responses:
            return

        # 5) BROADCAST AMPLITUDES
        amps_flat = _bcast_array(amps_flat, 3 * nmol)

        # 6) UPDATE ACCUMULATORS (NO change_sources)
        touched = set()
        off = 0
        for m in molecules:
            amp_au = amps_flat[off : off + 3]
            off += 3
            amp_mu = m.em_units.source_amp_au_to_em(amp_au)

            if m.dimensions in (1, 2):
                key = (m.polarization_fingerprint_hash, "Ez")
                if key not in touched:
                    instantaneous_source_amplitudes[key] = 0.0
                    touched.add(key)
                instantaneous_source_amplitudes[key] += float(amp_mu[2])
            else:
                for tag, val in (
                    ("Ex", amp_mu[0]),
                    ("Ey", amp_mu[1]),
                    ("Ez", amp_mu[2]),
                ):
                    key = (m.polarization_fingerprint_hash, tag)
                    if key not in touched:
                        instantaneous_source_amplitudes[key] = 0.0
                        touched.add(key)
                    instantaneous_source_amplitudes[key] += float(val)

            if _is_master:
                extra_blob = extras_by_id.get(m.molecule_id, b"")
                if extra_blob:
                    try:
                        m.additional_data_history.append(
                            _json.loads(extra_blob.decode("utf-8"))
                        )
                    except Exception:
                        pass

    return __step_function__


class MeepSimulation(mp.Simulation):
    """
    A wrapper of ``meep.Simulation`` with MaxwellLink features.

    This extends Meep's simulation to couple with quantum models via MaxwellLink.

    Attributes
    ----------
    hub : :class:`~maxwelllink.sockets.sockets.SocketHub` or None
        Manager for socket communication with external drivers.
    molecules : list of MoleculeMeepWrapper
        Wrapped molecules coupled to the EM grid.
    time_units_fs : float
        Time unit in femtoseconds for unit conversions.
    """

    def __init__(
        self,
        hub: Optional[SocketHub] = None,
        molecules: Optional[List] = None,
        time_units_fs: float = 0.1,
        **kwargs,
    ):
        """
        Initialize the simulation and wrap molecules with Meep adapters.

        Parameters
        ----------
        hub : :class:`~maxwelllink.sockets.sockets.SocketHub` or None, optional
            Socket hub for driver communication.
        molecules : list or None, optional
            Molecules to couple; will be wrapped as ``MoleculeMeepWrapper``.
        time_units_fs : float, default: 0.1
            The Meep time unit expressed in femtoseconds.
        **kwargs
            Additional keyword arguments forwarded to ``meep.Simulation``.
        """

        super().__init__(**kwargs)
        if self.Courant != 0.5:
            raise RuntimeError("MaxwellLink currently only supports Courant=0.5!")

        self.socket_hub = hub
        self.molecules = molecules if molecules is not None else []
        self.time_units_fs = time_units_fs

        # we need to reassign the time_units_fs to each molecule
        self.dx = 1.0 / self.resolution
        self.dt = self.Courant * self.dx  # Courant factor = 0.5

        for idx in range(len(self.molecules)):
            # use meep wrapper for this molecule
            m = MoleculeMeepWrapper(
                time_units_fs=time_units_fs, dt=self.dt, molecule=self.molecules[idx]
            )
            self.molecules[idx] = m

        if len(self.molecules) > 0:
            self.molecules[0].em_units.units_helper(self.dx, self.dt)

    # overload mp.Simulation.run() function
    def run(self, *user_step_funcs, **kwargs):
        """
        Run the simulation with optional user step functions and stopping conditions.

        Notes
        -----
        If molecules are present, a MaxwellLink coupling step is automatically
        inserted (socket or non-socket variant as appropriate) before user-provided
        step functions.

        Parameters
        ----------
        *user_step_funcs
            One or more per-step callables.
        **kwargs
            Passed through to ``meep.Simulation.run`` (e.g., ``until=...``).
        """

        # if **kwargs contains "steps", we need to convert it to "until"
        if "steps" in kwargs:
            kwargs["until"] = float(kwargs.pop("steps") * self.dt)

        step_funcs = list(user_step_funcs)
        if self.molecules:
            # auto-insert our coupling step first
            if self.socket_hub is not None:
                step_funcs.insert(
                    0, update_molecules(self.socket_hub, self.molecules, self.sources)
                )
            else:
                step_funcs.insert(
                    0, update_molecules_no_socket(self.molecules, self.sources)
                )
        super().run(*step_funcs, **kwargs)

        # after run, stop and clean up the hub
        if self.socket_hub is not None:
            if mp.am_master():
                self.socket_hub.stop()
