import numpy as np
from ..molecule import Molecule


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

        self.molecule = molecule
        self.em_units = DummyEMUnits()


class DummyEMSimulation:
    """
    Minimal dummy EM simulation container.
    """

    def __init__(self):
        pass

    def run(self, until: float):
        pass
