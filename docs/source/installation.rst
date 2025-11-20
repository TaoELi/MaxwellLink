Installation
============

The recommended way to install **MaxwellLink** is through a conda environment that
provides third-party EM solvers and molecular drivers alongside Python 3.9+.

Prerequisites
-------------

- A recent Python (``>=3.9``). **MaxwellLink** is tested with CPython.
- A working MPI stack (e.g. MPICH or OpenMPI) whenever you plan to run Meep or
  **MaxwellLink** under ``mpirun``.
- C/Fortran toolchains supplied by your platform if you plan to build drivers
  such as LAMMPS from source.

Create a conda environment
--------------------------

.. code-block:: bash

   # Create a fresh conda environment for MaxwellLink
   CONDA_ENV="mxl"
   conda create -n "$CONDA_ENV" python=3.11
   conda activate "$CONDA_ENV"

Install **MaxwellLink** from source
-----------------------------------

.. code-block:: bash

   git clone https://github.com/TaoELi/MaxwellLink.git
   cd MaxwellLink
   pip install .

This installs the Python package together with the ``mxl_driver`` console entry
point used to launch molecular drivers.

Optional EM solvers
-----------------------

Currently, **MaxwellLink** ships with `Meep <https://meep.readthedocs.io/en/latest/>`_ and a simple single-mode cavity solver. While the built-in
single-mode cavity is mainly for prototyping and debugging, `Meep <https://meep.readthedocs.io/en/latest/>`_ is a full-featured FDTD package
suitable for production simulations. One can install `Meep <https://meep.readthedocs.io/en/latest/>`_ with MPI support via conda:

.. code-block:: bash

   conda install -c conda-forge pymeep="*=mpi_mpich_*"

.. note::
   Note that it appears that `Meep <https://meep.readthedocs.io/en/latest/>`_  available in conda may not have the optimal MPI performance. Installing `Meep <https://meep.readthedocs.io/en/latest/>`_  from source following the
   `official instructions <https://meep.readthedocs.io/en/latest/Installation/>`_ may yield better MPI performance for large-scale simulations.

Optional driver dependencies
----------------------------

A two-level system (TLS) model ships with **MaxwellLink** and does not require extra packages.
Beyond this lightweight driver, **MaxwellLink** supports several molecular drivers that depend on
third-party packages. Install any molecular drivers below that you want to use from the command line. Each molecular driver can
be pulled into the same conda environment.

.. code-block:: bash

   # Model Hamiltonians via QuTiP
   conda install -c conda-forge qutip

   # Psi4 for RT-TDDFT and Ehrenfest drivers
   conda install -c conda-forge psi4

   # Atomic Simulation Environment (ASE) for classical/MM drivers
   conda install -c conda-forge ase

   # LAMMPS driver with fix mxl (installs a custom binary lmp_mxl)
   mxl_install_lammps

The `LAMMPS <https://www.lammps.org/>`_ helper downloads, patches, and builds a `LAMMPS <https://www.lammps.org/>`_ executable that contains
``fix mxl``; alternatively copy the provided ``fix_maxwelllink.*`` sources into
your existing `LAMMPS <https://www.lammps.org/>`_ build and recompile.

Verify the installation
-----------------------

After installing the desired drivers, run the core regression tests to confirm
that the coupling between EM solvers and the molecular drivers works in your environment.

.. code-block:: bash

   # Run the fast tests (TLS under socket/non-socket modes)
   pytest -m core -v

   # Run the optional Psi4 and QuTiP tests when the dependencies are available
   pytest -m "optional or slow" -v

If the optional tests are skipped, confirm that the corresponding packages were
installed or adjust your ``PYTHONPATH`` so that the drivers can import them.
