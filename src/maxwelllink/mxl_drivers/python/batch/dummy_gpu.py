# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                        #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink   #
# If you use this code, always credit and cite arXiv:2512.06173.                        #
# See AGENTS.md and README.md for details.                                              #
# --------------------------------------------------------------------------------------#

"""
Dummy vectorized batch model: the template for GPU-batched drivers.

Here, a vectorized batch model advances ``num`` sub-systems together using contiguous arrays.
"""

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True, eq=False)
class BatchStepResult:
    """
    Columnar physics outputs for one FDTD step (contiguous host arrays).

    All arrays are host (NumPy) arrays in atomic units for a batch of ``num``
    sub-systems.

    Attributes
    ----------
    amplitude_au : numpy.ndarray, shape (num, 3)
        Source amplitude ``dmu/dt`` consumed by the EM solver.
    dipole_half_au : numpy.ndarray, shape (num, 3)
        Dipole half a step after the force time (cavity coupling).
    dipole_force_au : numpy.ndarray, shape (num, 3)
        Dipole at the force time (velocity-Verlet drift / dipole self-energy).
    energy_au : numpy.ndarray, shape (num,)
        Per-sub-system energy (diagnostic).
    """

    amplitude_au: np.ndarray
    dipole_half_au: np.ndarray
    dipole_force_au: np.ndarray
    energy_au: np.ndarray


class DummyBatchModel:
    """
    A dummy vectorized batch model for demonstration purposes.

    This class serves as a template for implementing GPU-batched moleular drivers.  It
    advances ``num`` sub-systems together with contiguous arrays and provides
    the interface consumed by :class:`~maxwelllink.sockets.aggregated.GPUBatchBridge`.
    """

    # -------------- heavy-load initialization (at AGGINIT) --------------

    def initialize(self, dt_au, molecule_ids):
        """
        Set the shared time step and molecule IDs for this batch model.

        This function is called by the GPU bridge once the group membership is
        known (the AGGINIT stage of aggregate socket communication).

        Notes
        -----
        This method *should be* overridden by subclasses to allocate their
        contiguous state. The base implementation only records the common
        bookkeeping shared by every batch model.

        Parameters
        ----------
        dt_au : float
            The time step in atomic units (a.u.), shared by every sub-system.
        molecule_ids : array-like of int
            Molecule IDs assigned by the hub, one per sub-system.
        """

        self.dt = float(dt_au)  # shared time step in a.u.
        self.molecule_ids = tuple(int(mid) for mid in molecule_ids)
        self.num = len(self.molecule_ids)  # number of sub-systems in the batch
        self.t = 0.0  # current time in a.u.

    # -------------- one FDTD step under E-field --------------

    def step(self, efield_au):
        """
        Advance every sub-system by one FDTD step given the effective E-field.

        This method should be overridden by subclasses to implement specific
        vectorized propagation logic.

        Notes
        -----
        This method *must be* overridden by subclasses.

        Parameters
        ----------
        efield_au : numpy.ndarray of float, shape (num, 3)
            Contiguous host array of effective electric field vectors in a.u.,
            one ``[E_x, E_y, E_z]`` row per sub-system.

        Returns
        -------
        BatchStepResult
            Columnar physics outputs (amplitude, dipoles, energy) for this step.
        """

        raise NotImplementedError("This method should be overridden by subclasses.")

    # ------------ optional operation --------------

    def append_additional_data(self):
        """
        Append additional data for each sub-system to send back to MaxwellLink.

        Notes
        -----
        This method can be *optionally* overridden by subclasses to send
        additional data to MaxwellLink. We recommend including 
        "time_au", "energy_au", 
        dipole components at half-step time: "mux_au", "muy_au", "muz_au" 
        and the force-time dipole (``mux_m_au``/``muy_m_au``/``muz_m_au``)
        in each dictionary, matching the scalar-driver format.

        Returns
        -------
        list of dict
            One additional-data dictionary per sub-system (empty by default).
        """

        return []

    def close(self):
        """
        Release any device resources held by this batch model.

        Notes
        -----
        This method can be *optionally* overridden by subclasses to free device
        arrays (e.g. CuPy buffers). The default implementation does nothing.
        """

        pass
