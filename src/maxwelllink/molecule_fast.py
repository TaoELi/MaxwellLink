"""
This Python module defines realistic molecules coupled with the MEEP FDTD engine.

The important issue is that we plan to propagate molecules under atomic units, while MEEP uses its own unit system.
"""

from __future__ import annotations

import warnings
import numpy as np
from scipy.linalg import expm
from math import exp

from typing import Optional, Dict, List
from .sockets import SocketHub
import json
from collections import defaultdict
import atexit, weakref

try:
    import meep as mp
except ImportError:
    raise ImportError(
        "The meep package is required for this module. Please install it: https://meep.readthedocs.io/en/latest/Installation/."
    )

# We define the following two global dictionaries to hold unique state across multiple molecule instances.
# holds the instantaneous (per–time step) amplitude that Meep will sample
instantaneous_source_amplitudes = defaultdict(float)
# one Meep Source per unique polarization fingerprint (shared by molecules)
_fingerprint_source = {}


# The issue is if this file is called many times within the same python interpreter session,
# we need to ensure that the global state is reset between simulations.
def reset_module_state():
    """Clear shared state so a new Simulation starts from a clean slate."""
    instantaneous_source_amplitudes.clear()
    _fingerprint_source.clear()


# one-time registration guard for atexit
__MXL__ATEXIT_REGISTERED = {"flag": False}


def _register_sim_cleanup(sim):
    """Ensure we reset globals when this Simulation ends (GC) and at interpreter exit."""
    if not __MXL__ATEXIT_REGISTERED["flag"]:
        atexit.register(reset_module_state)
        __MXL__ATEXIT_REGISTERED["flag"] = True
    # When `sim` is garbage-collected, run the reset, too.
    weakref.finalize(sim, reset_module_state)


def make_custom_time_src(key):
    # Meep calls this each time step; just return the current amplitude for this fingerprint
    return mp.CustomSource(lambda t: instantaneous_source_amplitudes[key])


class DummyMolecule:
    """
    A dummy molecule class that serves as a parent class for realistic molecules.
    """

    def __init__(self, dt, dx, center=mp.Vector3(), size=mp.Vector3(1, 1, 1)):
        """
        Initialize the dummy molecule with default properties.
        This class is intended to be subclassed for specific molecular implementations.
        """
        self.dt = dt  # time step for the simulation used in MEEP
        self.dx = dx  # spatial resolution for the simulation used in MEEP
        self.center = center
        self.size = size
        # each molecule has a corresponding MEEP Source object and will be used in defining the MEEP simulation object
        self.sources = []

    def _propagate(self, int_ep):
        """
        Propagate the quantum molecular dynamics given the light-matter coupling int_ep.
        This method should be overridden by subclasses to implement specific propagation logic.
        """
        raise NotImplementedError("This method should be overridden by subclasses.")

    def _init_sources(self):
        """
        Initialize the sources for this molecule.
        This method should be overridden by subclasses to implement specific source initialization logic.
        """
        raise NotImplementedError("This method should be overridden by subclasses.")

    def _update_source_amplitude(self):
        """
        Update the source amplitude after propagating this molecule for one time step.
        This method should be overridden by subclasses to implement specific source update logic.
        """
        raise NotImplementedError("This method should be overridden by subclasses.")

    def _calculate_ep_integral(self, sim):
        """
        Calculate the integral of the electric field over the molecule's volume.
        This method should be overridden by subclasses to implement specific integral calculation logic.
        """
        raise NotImplementedError("This method should be overridden by subclasses.")

    def update_molecule(self, sources_non_molecule=[]):
        def __step_function__(sim):
            """
            A step function which is used in meep simulation.run() to account for the
            interaction between this molecule and the Maxwell field at each time step.

            - sources_non_molecule: A list of sources that are not part of this molecule.
            Typically any non-molecule sources such as Gaussian sources must be included here.
            """
            # 1. calculate the light-matter coupling: int_ep
            int_ep = self._calculate_ep_integral(sim)

            # 2. propagate the quantum molecular dynamics given int_ep
            self._propagate(int_ep)

            # 3. update the source amplitude after propagating this molecule for one time step
            self._update_source_amplitude()
            sim.change_sources(sources_non_molecule + self.sources)

        return __step_function__

    def units_helper(self, time_units_in_fs=0.1):
        """
        Helper function to explain the unit system used in MEEP and its connection to atomic units.
        """
        print(
            "MEEP uses its own unit system, which is based on the speed of light in vacuum (c), ",
            "the permittivity of free space (epsilon_0), and the permeability of free space (mu_0). ",
            "To couple MEEP with molecular dynamics, we set [c] = [epsilon_0] = [mu_0] = [hbar] = 1. ",
            "By further defining the time unit as %.2E fs, we can convert the MEEP unit (mu) to atomic units (au) as follows:"
            % time_units_in_fs,
            "- angular frequency [omega]: 1 mu = %.4E eV"
            % (41.357 * 0.1 / time_units_in_fs),
            "- length: 1 mu = %.3E nm" % (4.771 * time_units_in_fs / 0.1),
            "- dipole moment: 1 mu = %.4E Debye" % (1.8970 * time_units_in_fs / 0.1),
        )

    def print_dynamics(self):
        """
        Print the molecular dynamics properties.
        This method can be overridden by subclasses to provide specific molecular dynamics information.
        """
        raise NotImplementedError(
            "This method should be overridden by subclasses to print specific molecular dynamics properties."
        )


class TLSMolecule(DummyMolecule):
    """
    A simple two-level system (TLS) molecule class that inherits from DummyMolecule.
    This class represents a basic model of a molecule with two energy levels.
    """

    def __init__(
        self,
        resolution,
        center=mp.Vector3(),
        size=mp.Vector3(1, 1, 1),
        frequency=1.0,
        dipole_moment=0.1,
        sigma=0.1,
        orientation=mp.Ez,
        dimensions=2,
        rescaling_factor=1.0,
    ):
        """
        Initialize the TLS molecule with an energy gap and dipole moment.

        Parameters:
        - resolution: The resolution of the MEEP simulation, which determines the time step and spatial resolution.
        - center: The center of the molecule in the simulation space (default is mp.Vector3(0, 0, 0)).
        - size: The size of the molecule in the simulation space (default is mp.Vector3(1, 1, 1)).
        - frequency: The frequency of the molecule [omega = 2.0 * pi * frequency].
        - dipole_moment: The transient dipole moment of the molecule.
        - sigma: The width of the Gaussian profile for the TLS polarization density.
        - orientation: The polarization orientation of the molecule (default is mp.Ex).
        - dimensions: The dimensionality of the system (default is 3 for 3D, can be set to 2 for 2D).
        - rescaling_factor: An optional rescaling factor for the dipole moment and polarization density.
        """
        dt = 0.5 / resolution  # time step for the simulation
        dx = 1.0 / resolution  # spatial resolution for the simulation
        super().__init__(dt=dt, dx=dx, center=center, size=size)
        self.omega = (
            frequency  # * 2.0 * np.pi  # convert frequency to angular frequency
        )
        self.dipole_moment = dipole_moment
        self.sigma = sigma
        self.orientation = orientation
        self.dimensions = dimensions
        self.rescaling_factor = rescaling_factor
        self.t = 0.0  # current time
        # for 2D simulations, the TLS orientation is fixed to mp.Ez
        if self.dimensions == 2 and self.orientation != mp.Ez:
            warnings.warn(
                "In 2D simulations, the TLS orientation is fixed to mp.Ez. Setting orientation to mp.Ez."
            )
            self.orientation = mp.Ez
        if self.dimensions == 1:
            warnings.warn(
                "1D simulations are not supported for TLSMolecule. Please use 2D or 3D simulations."
            )
            raise ValueError(
                "1D simulations are not supported for TLSMolecule. Please use 2D or 3D simulations."
            )
        # the polarization prefactor in 3D
        self._polarization_prefactor_3d = (
            1.0
            / (2.0 * np.pi) ** (1.5)
            / self.sigma**5
            * self.dipole_moment
            * self.rescaling_factor
        )
        # the polarization prefactor in 2D
        self._polarization_prefactor_2d = (
            1.0
            / (2.0 * np.pi) ** (1.0)
            / self.sigma**2
            * self.dipole_moment
            * self.rescaling_factor
        )

        # the regularized E-field integral depends on the "fingerprint" of molecular polarization density distribution
        self.polarization_fingerprint = {
            "dimensions": self.dimensions,
            "sigma": self.sigma,
            "size": [self.size.x, self.size.y, self.size.z],
            "rescaling_factor": self.rescaling_factor,
            "center": [self.center.x, self.center.y, self.center.z],
        }
        # generate a hash value for the polarization fingerprint
        # this hash value is used to identify if two SocketMolecule instances have the same polarization distribution
        self.polarization_fingerprint_hash = hash(
            json.dumps(self.polarization_fingerprint)
        )
        print(
            f"TLSMolecule: polarization fingerprint hash = {self.polarization_fingerprint_hash}"
        )
        if self.polarization_fingerprint_hash not in instantaneous_source_amplitudes:
            instantaneous_source_amplitudes[self.polarization_fingerprint_hash] = (
                0.0  # initialize the instantaneous source amplitude for this fingerprint
            )

        # set the Hamiltonian and density matrix for the TLS molecule
        self.Hs = np.array([[0, 0], [0, self.omega]], dtype=np.complex128)
        self.SIGMAX = np.array([[0, 1], [1, 0]], dtype=np.complex128)
        self.expHs = expm(-1j * self.dt * self.Hs / 2.0)
        self.C = np.array([[1], [0]], dtype=np.complex128)
        self.rho = np.dot(self.C, self.C.conj().transpose())
        self.additional_data_history = []
        # initialize the sources for the TLS molecule
        self._init_sources()

    def reset_tls_population(self, excited_population=0.1):
        """
        Reset the TLS population to a specified excited state population.

        Parameters:
        - excited_population: The population of the excited state (default is 0.1).
        """
        if excited_population < 0 or excited_population > 1:
            raise ValueError("Excited population must be between 0 and 1.")
        self.C = np.array(
            [[(1 - excited_population) ** 0.5], [excited_population**0.5]],
            dtype=np.complex128,
        )
        self.rho = np.dot(self.C, self.C.conj().transpose())

    def _init_sources(self):
        """
        Initialize the sources for the TLS molecule.
        This method sets up the polarization profile based on the molecule's properties.
        """
        # set the source corresponding to the TLS molecule
        """
        import math
        local_size = 10.0
        if (
            self.size[0] > local_size
            or self.size[1] > local_size
            or self.size[2] > local_size
        ):
            raise ValueError(
                "The size of the molecule is too large. Please use a smaller size."
            )
        x = np.arange(-local_size / 2.0, local_size / 2.0, self.dx)
        y = np.arange(-local_size / 2.0, local_size / 2.0, self.dx)
        z = np.arange(-local_size / 2.0, local_size / 2.0, self.dx)
        if self.dimensions == 2:
            [X, Y] = np.meshgrid(x, y)
            R2 = X**2 + Y**2
            Pz_raw = self._polarization_prefactor_2d * np.exp(-R2 / 2.0 / self.sigma**2)
            start_idx_x, end_idx_x = int(
                (local_size - self.size[0]) / 2 / self.dx
            ), int((local_size + self.size[0]) / 2 / self.dx)
            start_idx_y, end_idx_y = int(
                (local_size - self.size[1]) / 2 / self.dx
            ), int((local_size + self.size[1]) / 2 / self.dx)
            Pz0 = Pz_raw[start_idx_x:end_idx_x, start_idx_y:end_idx_y]
            Pz0 = np.reshape(
                Pz0,
                (
                    math.ceil(self.size[0] / self.dx),
                    math.ceil(self.size[1] / self.dx),
                    1,
                ),
            )
            polarization_profile_2d = np.copy(Pz0.astype(np.complex128), order="C")

        elif self.dimensions == 3:
            [X, Y, Z] = np.meshgrid(x, y, z)
            R2 = X**2 + Y**2 + Z**2
            if self.orientation == mp.Ez:
                P3d_raw = (
                    self._polarization_prefactor_3d
                    * Z**2
                    * np.exp(-R2 / 2.0 / self.sigma**2)
                )
            elif self.orientation == mp.Ex:
                P3d_raw = (
                    self._polarization_prefactor_3d
                    * X**2
                    * np.exp(-R2 / 2.0 / self.sigma**2)
                )
            elif self.orientation == mp.Ey:
                P3d_raw = (
                    self._polarization_prefactor_3d
                    * Y**2
                    * np.exp(-R2 / 2.0 / self.sigma**2)
                )
            start_idx_x, end_idx_x = int(
                (local_size - self.size[0]) / 2 / self.dx
            ), int((local_size + self.size[0]) / 2 / self.dx)
            start_idx_y, end_idx_y = int(
                (local_size - self.size[1]) / 2 / self.dx
            ), int((local_size + self.size[1]) / 2 / self.dx)
            start_idx_z, end_idx_z = int(
                (local_size - self.size[2]) / 2 / self.dx
            ), int((local_size + self.size[2]) / 2 / self.dx)
            P3d0 = P3d_raw[
                start_idx_x:end_idx_x, start_idx_y:end_idx_y, start_idx_z:end_idx_z
            ]
            P3d0 = np.reshape(
                P3d0,
                (
                    math.ceil(self.size[0] / self.dx),
                    math.ceil(self.size[1] / self.dx),
                    math.ceil(self.size[2] / self.dx),
                ),
            )
            polarization_profile_3d = np.copy(P3d0.astype(np.complex128), order="C")

            # 1. number of primary cells in the molecule box
            Nx = int(np.ceil(self.size[0] / self.dx))
            Ny = int(np.ceil(self.size[1] / self.dx))
            Nz = int(np.ceil(self.size[2] / self.dx))

            # 2. half-cell offsets for the Yee lattice
            off = {
                mp.Ex: (0.0, 0.5, 0.5),
                mp.Ey: (0.5, 0.0, 0.5),
                mp.Ez: (0.5, 0.5, 0.0),
            }[self.orientation]

            # coordinate arrays on the **Nx×Ny×Nz** grid
            x = (np.arange(Nx) - (Nx - 1) / 2 + off[0]) * self.dx
            y = (np.arange(Ny) - (Ny - 1) / 2 + off[1]) * self.dx
            z = (np.arange(Nz) - (Nz - 1) / 2 + off[2]) * self.dx
            X, Y, Z = np.meshgrid(x, y, z, indexing="ij")
            R2 = X**2 + Y**2 + Z**2

            if self.orientation == mp.Ex:
                P = (
                    self._polarization_prefactor_3d
                    * X**2
                    * np.exp(-R2 / (2 * self.sigma**2))
                )
            elif self.orientation == mp.Ey:
                P = (
                    self._polarization_prefactor_3d
                    * Y**2
                    * np.exp(-R2 / (2 * self.sigma**2))
                )
            else:  # Ez
                P = (
                    self._polarization_prefactor_3d
                    * Z**2
                    * np.exp(-R2 / (2 * self.sigma**2))
                )

            polarization_profile_3d = np.ascontiguousarray(P.astype(np.complex128))
        """

        def amp_func_3d_x(R):
            return (
                self._polarization_prefactor_3d
                * R.x
                * R.x
                * np.exp(-(R.x * R.x + R.y * R.y + R.z * R.z) / 2.0 / self.sigma**2)
            )

        def amp_func_3d_y(R):
            return (
                self._polarization_prefactor_3d
                * R.y
                * R.y
                * np.exp(-(R.x * R.x + R.y * R.y + R.z * R.z) / 2.0 / self.sigma**2)
            )

        def amp_func_3d_z(R):
            return (
                self._polarization_prefactor_3d
                * R.z
                * R.z
                * np.exp(-(R.x * R.x + R.y * R.y + R.z * R.z) / 2.0 / self.sigma**2)
            )

        def amp_func_2d(R):
            return self._polarization_prefactor_2d * np.exp(
                -(R.x * R.x + R.y * R.y) / 2.0 / self.sigma**2
            )

        key = self.polarization_fingerprint_hash

        # ensure an accumulator exists for this fingerprint
        _ = instantaneous_source_amplitudes[key]  # initializes to 0.0 via defaultdict

        # note that using amp_data is x1.5 faster than using amp_func but for now we use amp_func for simplicity
        if key not in _fingerprint_source:
            _fingerprint_source[key] = mp.Source(
                src=make_custom_time_src(key),
                component=self.orientation,
                center=self.center,
                size=self.size,
                amplitude=1.0,
                # amp_data=polarization_profile_2d if self.dimensions == 2 else polarization_profile_3d
                amp_func=(
                    amp_func_2d
                    if self.dimensions == 2
                    else (
                        amp_func_3d_x
                        if self.orientation == mp.Ex
                        else (
                            amp_func_3d_y
                            if self.orientation == mp.Ey
                            else amp_func_3d_z
                        )
                    )
                ),
            )
        # Each TLSMolecule references the shared Source for its fingerprint
        self.sources = [_fingerprint_source[key]]

    def _propagate(self, int_ep):
        """
        TLS: Propagate the quantum molecular dynamics given the light-matter coupling int_ep.
        """
        # update the density matrix for one time step
        # the 2*pi factor in front of int_ep is to convert the energy to MEEP units,
        # note that the omega in expHs has already included the 2*pi factor
        U = np.dot(
            self.expHs, np.dot(expm((1j * self.dt * int_ep) * self.SIGMAX), self.expHs)
        )
        self.rho = np.dot(np.dot(U, self.rho), U.conj().transpose())
        self.t += self.dt  # update current time in a.u.
        self.additional_data_history.append(self.append_additional_data())

    def append_additional_data(self):
        """
        Append additional data to be sent back to MEEP, which can be retrieved by the user
        via the Python interface of MEEP: mp.TLSMolecule.additional_data_history.

        Returns:
        - A dictionary containing additional data.
        """
        data = {}
        data["time"] = self.t
        data["Pe"] = self.rho[1, 1].real
        data["Pg"] = self.rho[0, 0].real
        data["Pge_real"] = np.real(self.rho[0, 1])
        data["Pge_imag"] = np.imag(self.rho[0, 1])
        return data

    def _update_source_amplitude(self):
        """
        TLS: Update the source amplitude after propagating this molecule for one time step.
        """
        amp = -2.0 * self.omega * np.imag(self.rho[0, 1])
        return float(amp)

    def _calculate_ep_integral(self, sim):
        """
        TLS: Calculate the integral of the electric field over the molecule's volume.
        """
        int_ep = 0.0
        if self.dimensions == 2:
            int_ep = sim.integrate_field_function(
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
                mp.Volume(size=self.size, center=self.center),
            )
        elif self.dimensions == 3:
            if self.orientation == mp.Ez:
                int_ep = sim.integrate_field_function(
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
                    mp.Volume(size=self.size, center=self.center),
                )
            elif self.orientation == mp.Ex:
                int_ep = sim.integrate_field_function(
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
                    mp.Volume(size=self.size, center=self.center),
                )
            elif self.orientation == mp.Ey:
                int_ep = sim.integrate_field_function(
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
                    mp.Volume(size=self.size, center=self.center),
                )
        return int_ep

    def print_dynamics(self):
        for idx, rho in enumerate(self.rho_history):
            print(f"Time step {idx}: Excited-state population = {np.real(rho[1,1])}")


def update_molecules_no_socket(sources_non_molecule=None, molecules=None):
    if sources_non_molecule is None:
        sources_non_molecule = []
    if molecules is None:
        molecules = []

    started = {"flag": False}  # install sources once

    def __step_function__(sim):
        # 0) First call: install the union of unique fingerprint Sources + non-molecule sources
        if not started["flag"]:
            # ensure every molecule has created/registered its fingerprint Source
            for m in molecules:
                if not m.sources:  # safety
                    m._init_sources()
            unique_sources = list(
                {id(src): src for m in molecules for src in m.sources}.values()
            )
            sim.change_sources(list(sources_non_molecule) + unique_sources)
            # register cleanup to reset global state when sim ends or at exit
            _register_sim_cleanup(sim)
            started["flag"] = True

        # 1) (optional) precompute field integrals by fingerprint
        regularized_efield_integrals = {}

        # 2) ZERO the per-fingerprint accumulator for THIS STEP
        touched_keys = set()

        # 3) Propagate each molecule and ACCUMULATE its amplitude into the fingerprint bin
        for m in molecules:
            key = m.polarization_fingerprint_hash
            if key not in regularized_efield_integrals:
                int_ep = m._calculate_ep_integral(sim)
                regularized_efield_integrals[key] = int_ep
            else:
                int_ep = regularized_efield_integrals[key]

            m._propagate(int_ep)
            amp = m._update_source_amplitude()

            # first time we see this key this step, reset its accumulator
            if key not in touched_keys:
                instantaneous_source_amplitudes[key] = 0.0
                touched_keys.add(key)

            instantaneous_source_amplitudes[key] += amp

        # 4) DO NOT call sim.change_sources() here — CustomSource reads the updated accumulators automatically

    return __step_function__


fs_to_au = 41.341373335  # 1 fs = 41.341373335 a.u. from wikipedia::atomic_units


def meep_to_atomic_units_E(
    Emu_vec3: np.ndarray, time_units_fs: float = 0.1
) -> np.ndarray:
    """
    Conversion helper for the E-field integral from MEEP units to atomic units.
    MEEP uses the units system of epsion_0 = mu_0 = c = 1. Assuming SocketMolecule also uses the units of hbar=1,
    given \tau (time units) = time_units_fs, the conversion factor from MEEP units to atomic units can be constructed.
    """
    factor = 1.2929541569381223e-6 / (time_units_fs**2)
    return np.asarray(Emu_vec3, dtype=float) * factor


def atomic_to_meep_units_SourceAmp(
    amp_au_vec3: np.ndarray, time_units_fs: float = 0.1
) -> np.ndarray:
    """
    Conversion helper for the source amplitude from atomic units to MEEP units.

    source amplitude unit = dipole moment / time
    """
    factor = 0.002209799779149953
    return np.asarray(amp_au_vec3, dtype=float) * factor


class SocketMolecule(DummyMolecule):
    """
    A ScoketMolecule class uses the socket interface resembling to the i-pi socket interface to
    use a local driver code to propagate the quantum molecular dynamics.

    MEEP transfers [int_x, int_y, and int_z], where int_i = int dr E_i(r) * P_j(r) over the molecule volume, to the driver code.
    Here P_i(r) is the normalized spatial distribution of the molecular polarization density along the i-th direction (i = x, y, z),
    and this distribution is fixed during the simulation, excluding the magnitude of dipole moment (given by the driver code).

    SocketMolecule receives [amp_x, amp_y, and amp_z] from the driver code, where amp_i is the amplitude of the source along the i-th direction (i = x, y, z).
    The source amplitude is updated at each time step for a self-consistent propagation of the Maxwell-molecule coupled system.
    """

    def __init__(
        self,
        hub: SocketHub,
        molecule_id: int,
        resolution: int,
        center=mp.Vector3(),
        size=mp.Vector3(1, 1, 1),
        dimensions=2,
        sigma=0.1,
        init_payload: Optional[Dict] = None,
        time_units_fs: float = 0.1,
        rescaling_factor: float = 1.0,
    ):
        """
        Initialize the Socket molecule

        + **`hub` [ SocketHub ]** — Specifies the socket hub for communication.
        + **`molecule_id` [ int ]** — Specifies the unique ID for the molecule.
        + **`resolution` [ int ]** — Specifies the resolution of the simulation.
        + **`center` [ Vector3 ]** — Specifies the center position of the molecule.
        + **`size` [ Vector3 ]** — Specifies the size of the molecule for evaluating int dr E(r)*P(r).
        + **`dimensions` [ int ]** — Specifies the number of dimensions (2 or 3).
        + **`sigma` [ float ]** — Specifies the width of the molecular polarization density distribution.
        + **`init_payload` [ dict ]** — Specifies the initial payload for the molecule in the driver code.
        + **`time_units_fs` [ float ]** — Specifies the time units in femtoseconds.
          Given `time_units_fs`, SocketMolecule will tell the driver code to use a time step of
          `self.dt * time_units_fs * fs_to_au` atomic units, where `self.dt = 0.5 / resolution` is the time step in MEEP units.
        + **`rescaling_factor` [ float ]** — Specifies an optional rescaling factor for the dipole moment and polarization density.
        """
        super().__init__(
            dt=0.5 / resolution, dx=1.0 / resolution, center=center, size=size
        )
        self.dimensions = dimensions
        self.sigma = sigma
        self.rescaling_factor = rescaling_factor
        print(
            f"SocketMolecule: using a rescaling factor of {self.rescaling_factor} for the dipole moment and polarization density."
        )

        # MEEP by default uses the units system of epsion_0 = mu_0 = c = 1.
        # SocketMolecule communicates with the driver code using atomic units.
        # To connect the two unit systems, we define the time unit in fs.
        # The time step in MEEP is self.dt in MEEP units, which is self.dt * time_units_fs in fs.
        # So SocketMolecule will tell the driver code to use a time step of self.dt * time_units_fs * fs_to_au atomic units.
        self.time_units_fs = time_units_fs
        self.dt_au = self.dt * time_units_fs * fs_to_au  # time step in atomic units

        if dimensions not in [2, 3]:
            raise ValueError("SocketMolecule only supports 2D and 3D simulations.")
        if hub is None:
            raise ValueError("SocketHub must be provided for SocketMolecule.")
        if molecule_id is None:
            raise ValueError("molecule_id must be provided for SocketMolecule.")

        # the polarization prefactor in 3D excluding the transient dipole moment
        self._polarization_prefactor_3d = (
            1.0 / (2.0 * np.pi) ** (1.5) / self.sigma**5 * self.rescaling_factor
        )
        # the polarization prefactor in 2D excluding the transient dipole moment
        self._polarization_prefactor_2d = (
            1.0 / (2.0 * np.pi) ** (1.0) / self.sigma**2 * self.rescaling_factor
        )

        # the regularized E-field integral depends on the "fingerprint" of molecular polarization density distribution
        self.polarization_fingerprint = {
            "dimensions": self.dimensions,
            "sigma": self.sigma,
            "size": [self.size.x, self.size.y, self.size.z],
            "rescaling_factor": self.rescaling_factor,
            "center": [self.center.x, self.center.y, self.center.z],
        }
        # generate a hash value for the polarization fingerprint
        # this hash value is used to identify if two SocketMolecule instances have the same polarization distribution
        self.polarization_fingerprint_hash = hash(
            json.dumps(self.polarization_fingerprint)
        )
        print(
            f"SocketMolecule: polarization fingerprint hash = {self.polarization_fingerprint_hash}"
        )

        self._init_sources()

        # define the socket hub and molecule id
        self.hub = hub
        self.molecule_id = molecule_id
        self.init_payload = init_payload if init_payload is not None else {}
        # append the time step information to the init_payload
        self.init_payload.update({"dt_au": self.dt_au})

        # register this molecule with the hub so it knows to expect a client
        if mp.am_master():
            self.hub.register_molecule(self.molecule_id)

        # store the additional data the driver code transfers back to SocketMolecule. Each entry is a dict.
        self.additional_data_history = []

    def _init_sources(self):
        """
        Initialize the sources for the molecule. For general purposes, each molecule contains
        three polarization directions (Ex, Ey, Ez) in 3D or one (Ez) in 2D.
        """

        # Define the spatial shape of the polarization density
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

        # Build/reuse one Source per (fingerprint, component)
        srcs = []
        if self.dimensions == 2:
            key = (self.polarization_fingerprint_hash, "Ez")
            if key not in _fingerprint_source:
                instantaneous_source_amplitudes[key] = 0.0  # ensure accumulator exists
                _fingerprint_source[key] = mp.Source(
                    src=make_custom_time_src(key),  # <- reads accumulator each step
                    component=mp.Ez,
                    center=self.center,
                    size=self.size,
                    amplitude=1.0,  # fixed scalar; time dependence via CustomSource
                    amp_func=amp_func_2d,
                )
            srcs.append(_fingerprint_source[key])
        elif self.dimensions == 3:
            for comp, tag, amp_func in (
                (mp.Ex, "Ex", amp_func_3d_x),
                (mp.Ey, "Ey", amp_func_3d_y),
                (mp.Ez, "Ez", amp_func_3d_z),
            ):
                key = (self.polarization_fingerprint_hash, tag)
                if key not in _fingerprint_source:
                    instantaneous_source_amplitudes[key] = 0.0
                    _fingerprint_source[key] = mp.Source(
                        src=make_custom_time_src(key),
                        component=comp,
                        center=self.center,
                        size=self.size,
                        amplitude=1.0,
                        amp_func=amp_func,
                    )
                srcs.append(_fingerprint_source[key])
        else:
            raise ValueError("SocketMolecule only supports 2D or 3D.")

        # This molecule references the shared sources
        self.sources = srcs

    def _calculate_ep_integral(self, sim):
        """
        Calculate the integral of the electric field over the molecule's volume and return a vector (int_x, int_y, int_z).
        """
        vol = mp.Volume(size=self.size, center=self.center)
        int_ep_x, int_ep_y, int_ep_z = 0.0, 0.0, 0.0
        if self.dimensions == 2:
            int_ep_z = sim.integrate_field_function(
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
        elif self.dimensions == 3:
            int_ep_z = sim.integrate_field_function(
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
            int_ep_x = sim.integrate_field_function(
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
            int_ep_y = sim.integrate_field_function(
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
        # MEEP returns complex numbers, but the physical interaction is real
        return [np.real(int_ep_x), np.real(int_ep_y), np.real(int_ep_z)]

    def _update_source_amplitude(self, amp_vec3: np.ndarray):
        """Set amplitudes per component from driver-returned vector (atomic units)."""
        amp_mu = atomic_to_meep_units_SourceAmp(amp_vec3, self.time_units_fs)
        return np.asarray(amp_mu, dtype=float)


### Collective step function for many socket molecules ###


def update_molecules_no_mpi(
    hub: SocketHub, molecules: List[SocketMolecule], sources_non_molecule: List = None
):
    if sources_non_molecule is None:
        sources_non_molecule = []

    started = {"flag": False}
    paused = {"flag": False}

    def __step_function__(sim: mp.Simulation):
        # === First-time barrier ===
        while not started["flag"]:
            init_payloads = {
                m.molecule_id: {**m.init_payload, "molecule_id": m.molecule_id}
                for m in molecules
            }
            ok = hub.wait_until_bound(init_payloads, require_init=True, timeout=None)
            if not ok:
                raise RuntimeError("wait_until_bound timed out")
            # register cleanup to reset global state when sim ends or at exit
            _register_sim_cleanup(sim)
            started["flag"] = True

            # Install sources ONCE: union of non-molecule sources + unique shared sources
            unique_sources = list(
                {id(src): src for m in molecules for src in m.sources}.values()
            )
            sim.change_sources(list(sources_non_molecule) + unique_sources)

        # === Mid-run guard ===
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

        # === Build requests (cache integrals per fingerprint) ===
        requests = {}
        regularized_efield_integrals = {}
        for mol in molecules:
            key_fp = mol.polarization_fingerprint_hash
            if key_fp not in regularized_efield_integrals:
                int_ep_mu = mol._calculate_ep_integral(sim)
                regularized_efield_integrals[key_fp] = int_ep_mu
            else:
                int_ep_mu = regularized_efield_integrals[key_fp]
            int_ep_au = meep_to_atomic_units_E(int_ep_mu, mol.time_units_fs)
            requests[mol.molecule_id] = {
                "efield_au": int_ep_au,
                "meta": {"t": sim.meep_time()},
                "init": {**mol.init_payload, "molecule_id": mol.molecule_id},
            }

        # === Hub round-trip (with reconnection pause) ===
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

        # === Aggregate amplitudes into per-(fingerprint,component) accumulators ===
        # Zero only the keys we will touch this step (avoids clearing unrelated fingerprints)
        touched_keys = set()

        for mol in molecules:
            if mol.molecule_id not in responses:
                print(
                    f"Warning: no response for SocketMolecule {mol.molecule_id} at t={sim.meep_time():.2f}."
                )
                continue

            amp_au = np.asarray(responses[mol.molecule_id]["amp"], dtype=float)
            amp_mu = mol._update_source_amplitude(
                amp_au
            )  # returns [ax, ay, az] (or only z in 2D)

            # Map returned vector into component keys
            if mol.dimensions == 2:
                key = (mol.polarization_fingerprint_hash, "Ez")
                if key not in touched_keys:
                    instantaneous_source_amplitudes[key] = 0.0
                    touched_keys.add(key)
                instantaneous_source_amplitudes[key] += float(amp_mu[2])
            else:  # 3D
                for tag, val in (
                    ("Ex", amp_mu[0]),
                    ("Ey", amp_mu[1]),
                    ("Ez", amp_mu[2]),
                ):
                    key = (mol.polarization_fingerprint_hash, tag)
                    if key not in touched_keys:
                        instantaneous_source_amplitudes[key] = 0.0
                        touched_keys.add(key)
                    instantaneous_source_amplitudes[key] += float(val)

            # optional: stash any extra payload
            extra_blob = responses[mol.molecule_id].get("extra", b"")
            if extra_blob:
                try:
                    mol.additional_data_history.append(
                        json.loads(extra_blob.decode("utf-8"))
                    )
                except Exception:
                    pass

        # === Do NOT call sim.change_sources() here ===
        # Meep will sample the updated accumulators through CustomSource on the next field update.

    return __step_function__


def update_molecules(
    hub: SocketHub, molecules: List[SocketMolecule], sources_non_molecule: List = None
):
    """
    MPI-safe step function that installs sources once and then only streams
    amplitudes through CustomSource-backed accumulators. No per-step change_sources().
    """
    import numpy as _np
    import json as _json

    # --- detect MPI (optional) ---
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

    # Only the true master should touch the hub
    _is_master = (not _HAS_MPI) or (_RANK == 0 and mp.am_master())
    if _HAS_MPI and _RANK != 0:
        hub = None  # safety: non-master must never call hub methods

    started = {"flag": False}
    paused = {"flag": False}

    def _bcast_bool(x: bool) -> bool:
        if _HAS_MPI:
            x = bool(_COMM.bcast(bool(x), root=0))
        return bool(x)

    def _bcast_array(arr, n):
        """Broadcast a flat float64 array of length n from master."""
        if not _HAS_MPI:
            return _np.asarray(arr, dtype=float).reshape(n)
        if _is_master:
            buf = _np.asarray(arr, dtype=float).reshape(n)
        else:
            buf = _np.empty(n, dtype=float)
        _COMM.Bcast(buf, root=0)
        return buf

    def __step_function__(sim: mp.Simulation):
        # ============= 1) FIRST-TIME BARRIER (ALWAYS BLOCK UNTIL BOUND) =============
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

            # Ensure each molecule has created/registered its shared sources
            for m in molecules:
                if not m.sources:
                    m._init_sources()

            # Install sources identically on all ranks ONCE
            unique_sources = list(
                {id(src): src for m in molecules for src in m.sources}.values()
            )
            sim.change_sources(list(sources_non_molecule) + unique_sources)
            # register cleanup to reset global state when sim ends or at exit
            _register_sim_cleanup(sim)
            started["flag"] = True

        # ============= 2) MID-RUN GUARD (BLOCK UNTIL EVERYONE IS STILL BOUND) =============
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
            _ = _bcast_bool(True)  # sync point

            print("[SocketHub] RESUMED: all drivers reconnected.")
            paused["flag"] = False

            dropped = False
            if _is_master:
                ids = [m.molecule_id for m in molecules]
                dropped = not hub.all_bound(ids, require_init=True)
            dropped = _bcast_bool(dropped)

        # ============= 3) BUILD REQUESTS (E-FIELD INTEGRALS) =============
        requests = {}
        regularized_efield_integrals = {}
        for mol in molecules:
            key_fp = mol.polarization_fingerprint_hash
            if key_fp not in regularized_efield_integrals:
                int_ep_mu = mol._calculate_ep_integral(sim)
                regularized_efield_integrals[key_fp] = int_ep_mu
            else:
                int_ep_mu = regularized_efield_integrals[key_fp]
            int_ep_au = meep_to_atomic_units_E(int_ep_mu, mol.time_units_fs)
            requests[mol.molecule_id] = {
                "efield_au": int_ep_au,
                "meta": {"t": sim.meep_time()},
                "init": {**mol.init_payload, "molecule_id": mol.molecule_id},
            }

        # ============= 4) HUB ROUND-TRIP ON MASTER, WITH RETRY =============
        nmol = len(molecules)
        amps_flat = _np.zeros(3 * nmol, dtype=float)  # [ax,ay,az] per molecule
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

            # Pack responses in molecule order (atomic units)
            off = 0
            for mol in molecules:
                a = _np.asarray(responses[mol.molecule_id]["amp"], dtype=float).reshape(
                    3
                )
                amps_flat[off : off + 3] = a
                extras_by_id[mol.molecule_id] = responses[mol.molecule_id].get(
                    "extra", b""
                )
                off += 3

        had_responses = _bcast_bool(had_responses and _is_master or (not _HAS_MPI))
        if not had_responses:
            return  # should not happen with the retry loop

        # ============= 5) BROADCAST AMPLITUDES (a.u.) TO ALL RANKS =============
        amps_flat = _bcast_array(amps_flat, 3 * nmol)

        # ============= 6) UPDATE ACCUMULATORS (NO change_sources) =============
        # Zero only the accumulators we touch this step
        touched_keys = set()

        off = 0
        for mol in molecules:
            amp_au = amps_flat[off : off + 3]
            off += 3

            # Convert to Meep units using the molecule's own conversion (handles time_units_fs)
            amp_mu = mol._update_source_amplitude(
                amp_au
            )  # returns np.array([ax, ay, az])

            # Sum into shared accumulators by (fingerprint, component)
            if mol.dimensions == 2:
                key = (mol.polarization_fingerprint_hash, "Ez")
                if key not in touched_keys:
                    instantaneous_source_amplitudes[key] = 0.0
                    touched_keys.add(key)
                instantaneous_source_amplitudes[key] += float(amp_mu[2])
            else:
                for tag, val in (
                    ("Ex", amp_mu[0]),
                    ("Ey", amp_mu[1]),
                    ("Ez", amp_mu[2]),
                ):
                    key = (mol.polarization_fingerprint_hash, tag)
                    if key not in touched_keys:
                        instantaneous_source_amplitudes[key] = 0.0
                        touched_keys.add(key)
                    instantaneous_source_amplitudes[key] += float(val)

            # Master keeps optional extras to avoid extra comms
            if _is_master:
                extra_blob = extras_by_id.get(mol.molecule_id, b"")
                if extra_blob:
                    try:
                        mol.additional_data_history.append(
                            _json.loads(extra_blob.decode("utf-8"))
                        )
                    except Exception:
                        pass

        # ============= 7) DONE — CustomSource will read new accumulators next tick =============
        # NO sim.change_sources() here.

    return __step_function__
