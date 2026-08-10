# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

"""
User-friendly FDTD cavity builders for Meep FDTD coupled with MXL Molecules.

Example
-------
>>> from maxwelllink.cavity import BraggResonator
>>> cav = BraggResonator(omega=2320.0, units="cm-1", n_pairs=10, dimensions=1)
>>> mol = cav.place_molecule(driver="tls", driver_kwargs=dict(
...     omega=0.0106,  # driver parameters are in a.u.; ~2326 cm^-1
...     mu12=187.0, orientation=2, pe_initial=1e-4))
>>> sim = cav.make_simulation(molecules=[mol])
>>> sim.run(until=200)
"""

from .dummy_cavity import DummyCavity, CYLINDRICAL
from .vacuum import Vacuum
from .bragg import BraggResonator
from .npom import NPoM
from .rod import PlasmonicRod

__all__ = [
    "DummyCavity",
    "Vacuum",
    "BraggResonator",
    "NPoM",
    "PlasmonicRod",
    "CYLINDRICAL",
]
