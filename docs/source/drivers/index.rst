Drivers
=======

**MaxwellLink** ships with several Python molecular drivers that can be accessed either
through the ``mxl_driver`` console script (socket mode) or directly via
:class:`~maxwelllink.molecule.molecule.Molecule` in non-socket mode. These Python drivers inherit from
:class:`~maxwelllink.mxl_drivers.python.models.dummy_model.DummyModel` and provide the unified driver API.

.. toctree::
   :maxdepth: 1

   tls
   qutip
   rttddft
   rtehrenfest
   ase

Additionally, **MaxwellLink** provides direct connections to third-party C++ or Fortran molecular packages, such as LAMMPS and DFTB+.

.. toctree::
   :maxdepth: 1

   lammps
   dftbplus
