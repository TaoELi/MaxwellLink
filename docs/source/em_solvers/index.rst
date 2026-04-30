EM Solvers
==========

**MaxwellLink** ships with four electromagnetic backends. 

- The `Meep <https://meep.readthedocs.io/en/latest/>`_ interface runs a full finite-difference time-domain (FDTD) grid and streams polarization sources
  from molecules.

- The single-mode cavity emulator integrates a single harmonic oscillator entirely in atomic units. 

- The multimode cavity represents the electromagnetic environment with multiple resonant modes and supports 2D Fabry-Perot geometries.

- The laser-driven solver applies a user-defined time-dependent electric field directly to the molecular dipoles with no molecular response back to the field.

Use the pages below for each EM solver.

.. toctree::
   :maxdepth: 1

   meep
   single_mode_cavity
   multimode_cavity
   laser_driven
