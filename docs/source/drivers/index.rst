Drivers
=======

**MaxwellLink** ships with several Python molecular drivers that can be accessed either
through the ``mxl_driver`` console script (socket mode) or directly via
:class:`~maxwelllink.molecule.molecule.Molecule` in non-socket mode. These Python drivers inherit from
:class:`~maxwelllink.mxl_drivers.python.models.dummy_model.DummyModel` and provide the unified driver API.


Python-based Drivers for Testing and Debugging
------------------------------------------------


The light-weight two-level system (TLS) driver and the simple harmonic oscillator (SHO) driver are designed for testing and debugging the simulation of electronic or nuclear dynamics, respectively.


.. toctree::
   :maxdepth: 1

   tls
   sho

Python-based Drivers for Productive Runs
------------------------------------------------

.. toctree::
   :maxdepth: 1

   qutip
   rttddft
   rtehrenfest
   ase

Socket Connection to Third-Party Drivers
------------------------------------------------

Additionally, **MaxwellLink** provides direct connections to third-party C++ or Fortran molecular packages, such as LAMMPS and DFTB+.

.. toctree::
   :maxdepth: 1

   lammps
   dftbplus
