# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

import numpy as np
from ..molecule import Molecule
from ..sockets import SocketHub
from typing import Optional, List


class DummyEMUnits:
    """
    Dummy EM units with 1:1 conversions to atomic units.
    """

    def __init__(self):
        pass

    def efield_em_to_au(self, Emu_vec3):
        """
        Convert the electric field vector from EM units to atomic units (a.u.).

        Parameters
        ----------
        Emu_vec3 : array-like of float, shape (3,)
            Electric field vector in EM units.

        Returns
        -------
        numpy.ndarray of float, shape (3,)
            Electric field vector in atomic units.
        """

        return np.asarray(Emu_vec3, dtype=float)

    def source_amp_au_to_em(self, amp_au_vec3):
        """
        Convert a source amplitude vector from atomic units (a.u.) to EM units.

        Parameters
        ----------
        amp_au_vec3 : array-like of float, shape (3,)
            Source amplitude vector in atomic units.

        Returns
        -------
        numpy.ndarray of float, shape (3,)
            Source amplitude vector in EM units.
        """

        return np.asarray(amp_au_vec3, dtype=float)

    def time_em_to_au(self, time_em: float):
        """
        Convert time from EM units to atomic units.

        Parameters
        ----------
        time_em : float
            Time in EM units.

        Returns
        -------
        float
            Time in atomic units.
        """

        return float(time_em)

    def units_helper(self) -> str:
        """
        Return a human-readable description of these EM units.

        Returns
        -------
        str
            Description of the dummy EM unit system (1:1 conversion).
        """

        return "Dummy EM units (1:1 conversion)"


class MoleculeDummyWrapper:
    def __init__(self, molecule: Molecule):
        """
        Lightweight wrapper that associates a Molecule with dummy EM units.

        Parameters
        ----------
        molecule : Molecule
            The molecule to wrap.
        """

        self.m = molecule
        self.em_units = DummyEMUnits()

    def initialize_driver(self, assigned_id: int):
        """
        Initialize the wrapped molecule's driver (non-socket mode).

        Notes
        -----
        Uses the molecule's cached ``dt_au`` but changes its ``molecule_id`` from ``assigned_id``.
        """
        self.molecule_id = assigned_id if assigned_id is not None else self.molecule_id
        self.m.molecule_id = self.molecule_id
        self.m.initialize_driver(self.dt_au, self.molecule_id)
        self.d_f = self.m.d_f

    def propagate(self, efield_vec3):
        """
        Propagate the wrapped molecule for one EM step.

        Parameters
        ----------
        efield_vec3 : array-like of float, shape (3,)
            Effective electric field vector in atomic units.
        """

        self.m.propagate(efield_vec3)

    def calc_amp_vector(self):
        """
        Compute and return the current source amplitude vector from the molecule.

        Returns
        -------
        numpy.ndarray of float, shape (3,)
            Source amplitudes in atomic units.
        """

        return self.m.calc_amp_vector()


class DummyEMSimulation:
    """
    Minimal dummy EM simulation container.
    """

    def __init__(
        self, hub: Optional[SocketHub] = None, molecules: Optional[List] = None
    ):
        self.hub = hub
        self.molecules = molecules if molecules is not None else []

    def run(self, until: float):
        pass
