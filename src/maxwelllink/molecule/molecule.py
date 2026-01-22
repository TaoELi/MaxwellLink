"""
A molecule class capable of operating in both socket and non-socket modes.

In socket mode, the molecule communicates with an external process (e.g., a
quantum driver) via a socket connection through :class:`~maxwelllink.sockets.sockets.SocketHub`.
In non-socket mode, it directly instantiates a molecular dynamics driver defined
in ``__drivers__`` (e.g., ``tlsmodel``, ``qutipmodel``).

EM-specific source creation and field-integral computations are provided by
backend modules.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Dict, Optional

from maxwelllink.sockets import SocketHub, am_master, mpi_bcast_from_master
from maxwelllink.units import FS_TO_AU
from maxwelllink.mxl_drivers.python.models import __drivers__

from collections import deque


@dataclass
class Vector3:
    """
    Minimal 3D vector container used to avoid depending on ``mp.Vector3`` in the
    abstract layer.
    """

    x: float = 0.0
    y: float = 0.0
    z: float = 0.0


class Molecule:
    """
    A molecule class which can support both socket and non-socket modes.

    EM-specific source creation and field-integral computation are provided by
    EM backends.
    """

    def __init__(
        self,
        hub: Optional[SocketHub] = None,
        driver: Optional[str] = None,
        center: Optional[Vector3] = None,
        size: Optional[Vector3] = None,
        dimensions: Optional[int] = None,
        sigma: Optional[float] = None,
        resolution: Optional[int] = None,
        init_payload: Optional[Dict] = None,
        driver_kwargs: Optional[Dict] = None,
        rescaling_factor: float = 1.0,
        store_additional_data: bool = True,
        polarization_type: Optional[str] = None,
    ):
        """
        Parameters
        ----------
        hub : :class:`~maxwelllink.sockets.sockets.SocketHub` or None, optional
            Socket hub for socket mode. If provided, ``driver`` must be ``None``.
        driver : str or None, optional
            Driver name for non-socket mode. If provided, ``hub`` must be ``None``.
        center : Vector3 or None, optional
            Molecule center position.
        size : Vector3 or None, optional
            Molecule size (extent).
        dimensions : int or None, optional
            Simulation dimensionality; one of ``1``, ``2``, or ``3``.
        sigma : float or list or None, optional
            Spatial polarization kernel width.
        resolution : int or None, optional
            Optional real-space resolution for building FDTD sources.
        init_payload : dict or None, optional
            Optional initialization payload for socket communication with molecular drivers.
        driver_kwargs : dict or None, optional
            Keyword arguments passed to the selected driver in non-socket mode.
        rescaling_factor : float, default: 1.0
            Rescaling factor for polarization.
        store_additional_data : bool, default: True
            Whether to store additional data history as a growing list (if True) or only keep the latest five frames (if False).
        polarization_type : str or None, optional
            Type of polarization to use in EM FDTD propagation. Five options: ``analytical``, ``numerical``, ``transverse``, ``point``, ``anisotropic``.
            Default is ``analytical``. ``analytical`` uses analytical Gaussian polarization profile;
            ``numerical`` uses numerical Gaussian polarization profile;
            ``transverse`` uses approximate transverse components of numerical Gaussian polarization profile from FFT;
            ``point`` uses point dipole approximation, which might not be accurate for evaluating self-interaction;
            ``anisotropic`` uses anisotropic and analytical Gaussian polarization profile [self.sigma should be a list of three floats for 
            (sigma_x, sigma_y, sigma_z) instead of a single float].

        Raises
        ------
        ValueError
            If both ``hub`` and ``driver`` are provided, or neither is provided, or
            if ``dimensions`` is not in ``{1, 2, 3}``, or if an unsupported driver is
            requested in non-socket mode.
        ImportError
            If the requested driver cannot be imported.
        Exception
            If driver setup fails for other reasons (the driver docstring is printed).
        """

        self.hub = hub
        self.driver = driver
        self.center = center
        self.size = size
        self.dimensions = dimensions
        self.sigma = sigma
        # optional setting resolution for building real-space sources in FDTD code
        self.resolution = resolution
        # optional setting init_payload for socket communication with molecular drivers
        self.init_payload = init_payload if init_payload is not None else {}
        self.rescaling_factor = rescaling_factor

        self.molecule_id = -1  # Default ID for non-socket mode
        self.time_units_fs = 0.0  # Optional time unit when connecting to FDTD code
        self.init_payload["dt_au"] = 0.0  # Placeholder for dt_au in init_payload

        # if resolution is provided, we also compute dx and dt
        self.dx = 1.0 / resolution if resolution is not None else 0.0
        self.dt = 0.5 / resolution if resolution is not None else 0.0
        self.polarization_type = (
            polarization_type.lower() if polarization_type else "analytical"
        )
        # reserve for sources and additional data history
        self.sources = []
        self.additional_data_history = []
        if not store_additional_data:
            # use a deque to limit memory usage: if thousands of molecules are attached,
            # perhaps we don't want to store too much history)
            self.additional_data_history = deque(maxlen=5)

        if self.dimensions not in (1, 2, 3) and self.dimensions is not None:
            raise ValueError("Molecule only supports 1D, 2D and 3D simulations.")

        # identify the spatial polarization kernel function
        if self.size is None:
            self.size = Vector3(0.0, 0.0, 0.0)
        if self.center is None:
            self.center = Vector3(0.0, 0.0, 0.0)

        self.polarization_fingerprint = {
            "dimensions": self.dimensions,
            "sigma": self.sigma,
            "size": [self.size.x, self.size.y, self.size.z],
            "rescaling_factor": self.rescaling_factor,
            "center": [self.center.x, self.center.y, self.center.z],
            "polarization_type": self.polarization_type,
        }
        self.polarization_fingerprint_hash = hash(
            json.dumps(self.polarization_fingerprint)
        )

        self.mode = "none"
        if hub is not None and driver is None:
            self.mode = "socket"
        elif hub is None and driver is not None:
            self.mode = "non-socket"
        else:
            raise ValueError("Either hub or driver must be provided, but not both.")

        if self.mode == "socket":
            # register with the hub on rank 0
            if am_master():
                self.molecule_id = self.hub.register_molecule_return_id()
                print(
                    f"[Init Molecule] Under socket mode, registered molecule with ID {self.molecule_id}"
                )
            # if using mpi, we also need to broadcast the molecule_id to other ranks
            self.molecule_id = mpi_bcast_from_master(self.molecule_id)
        elif self.mode == "non-socket":
            self.driver = str(driver).lower()
            if self.driver not in list(__drivers__.keys()):
                raise ValueError(
                    f"[Init Molecule] Unsupported driver: {self.driver}, only supports {list(__drivers__.keys())}"
                )
            print(
                f"[Init Molecule] Operating in non-socket mode, using driver: {self.driver}"
            )
            # now we initialize the driver
            try:
                self.d_f = __drivers__[self.driver](**driver_kwargs)
            except ImportError:
                # specific errors have already been triggered
                raise
            except Exception as err:
                print(
                    f"Error setting up molecular dynamics model {self.driver} with args {driver_kwargs}"
                )
                print(__drivers__[self.driver].__doc__)
                print("Error trace: ")
                raise err

    def _refresh_time_units(self, time_units_fs):
        """
        Refresh the external time-unit scale (in femtoseconds) used to convert EM time
        steps to atomic units and propagate it into the initialization payload.

        Parameters
        ----------
        time_units_fs : float
            Time unit in femtoseconds used by the EM solver.

        Notes
        -----
        Sets ``self.dt_au = self.dt * time_units_fs * FS_TO_AU`` and updates
        ``init_payload['dt_au']`` accordingly.
        """

        self.time_units_fs = time_units_fs
        self.dt_au = self.dt * time_units_fs * FS_TO_AU
        # also refresh in init_payload for drivers
        self.init_payload["dt_au"] = self.dt_au

    def _refresh_time_step(self, dt_em):
        """
        Refresh the EM time step (dimensionless, in EM code units) and the derived
        atomic-unit time step, then propagate it into the initialization payload.

        Parameters
        ----------
        dt_em : float
            EM solver time step.

        Notes
        -----
        Sets ``self.dt = dt_em`` and
        ``self.dt_au = self.dt * self.time_units_fs * FS_TO_AU``, and updates
        ``init_payload['dt_au']`` accordingly.
        """

        self.dt = dt_em
        self.dt_au = self.dt * self.time_units_fs * FS_TO_AU
        # also refresh in init_payload for drivers
        self.init_payload["dt_au"] = self.dt_au

    def initialize_driver(self, dt_au, molecule_id):
        """
        Initialize the non-socket driver with the given time step (a.u.) and molecule ID.

        Parameters
        ----------
        dt_au : float
            Time step in atomic units passed to the driver.
        molecule_id : int
            Molecule identifier.

        Raises
        ------
        NotImplementedError
            If called in socket mode (not implemented yet).
        RuntimeError
            If the molecule is not properly initialized in socket or non-socket mode.
        """

        if self.mode == "socket":
            raise NotImplementedError(
                "Socket mode driver initialization is not implemented yet."
            )
        elif self.mode == "non-socket":
            # use the driver to initialize
            self.dt_au = float(dt_au)
            self.molecule_id = int(molecule_id)
            self.d_f.initialize(self.dt_au, self.molecule_id)
        else:
            raise RuntimeError(
                "Molecule is not properly initialized in socket or non-socket mode."
            )

    def propagate(self, efield_vec3):
        """
        Propagate the molecule by one step under the given effective electric field.

        Parameters
        ----------
        efield_vec3 : array-like of float, shape (3,)
            Effective electric field vector ``[E_x, E_y, E_z]``.

        Raises
        ------
        NotImplementedError
            If called in socket mode (not implemented yet).
        RuntimeError
            If the molecule is not properly initialized in socket or non-socket mode.
        """

        if self.mode == "socket":
            raise NotImplementedError("Socket mode propagation is not implemented yet.")
        elif self.mode == "non-socket":
            # use the driver to propagate
            self.d_f.propagate(efield_vec3)
        else:
            raise RuntimeError(
                "Molecule is not properly initialized in socket or non-socket mode."
            )

    def calc_amp_vector(self):
        """
        Compute and return the source amplitude vector for the current step.

        Returns
        -------
        numpy.ndarray of float, shape (3,)
            Amplitude vector, typically
            :math:`[\\mathrm{d}\\mu_x/\\mathrm{d}t,\\ \\mathrm{d}\\mu_y/\\mathrm{d}t,\\ \\mathrm{d}\\mu_z/\\mathrm{d}t]`.

        Raises
        ------
        NotImplementedError
            If called in socket mode (not implemented yet).
        RuntimeError
            If the molecule is not properly initialized in socket or non-socket mode.
        """

        if self.mode == "socket":
            raise NotImplementedError(
                "Socket mode amplitude calculation is not implemented yet."
            )
        elif self.mode == "non-socket":
            # use the driver to calculate amplitude vector
            return self.d_f.calc_amp_vector()
        else:
            raise RuntimeError(
                "Molecule is not properly initialized in socket or non-socket mode."
            )
