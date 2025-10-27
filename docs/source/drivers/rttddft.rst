RT-TDDFT driver
===============

The RT-TDDFT driver couples **MaxwellLink** to the `Psi4 <https://psicode.org/>`_ quantum chemistry package
for real-time time-dependent density functional theory propagation. It is
implemented by :class:`maxwelllink.mxl_drivers.python.models.RTTDDFTModel`,
which supports delta-kick excitations, automatic sub-stepping, and optional
checkpoint/restart workflows.

.. note::

  Electronic dynamics are propagated through

  .. math::

     \frac{d}{dt} \mathbf{P}^{\mathrm{e}}_{\mathrm{o}}(t) = -\frac{i}{\hbar}\Bigl[\mathbf{F}^{\mathrm{e}}_{\mathrm{o}}(t) - \widetilde{\mathbf{E}}(t)\cdot \vec{\boldsymbol{\mu}}^{\mathrm{e}}_{\mathrm{o}},\, \mathbf{P}^{\mathrm{e}}_{\mathrm{o}}(t)\Bigr],

  where the Kohn--Sham matrix :math:`\mathbf{F}^{\mathrm{e}}(t)` is rebuilt self-consistently from the evolving density and transformed between orthogonal and non-orthogonal bases each step. After every integration the driver reports the dipole current through

  .. math::

     \frac{d}{dt}\langle \vec{\boldsymbol{\mu}}^{\mathrm{e}} \rangle = \mathrm{Tr}\!\left(\frac{d}{dt}\mathbf{P}^{\mathrm{e}}(t)\,\vec{\boldsymbol{\mu}}^{\mathrm{e}}\right),

  providing the light--matter source term for the Maxwell solver. The nuclei are assumed fixed during the simulation.

Requirements
------------

- ``psi4`` available in the driver environment (`Psi4 <https://psicode.org/>`_ with Python bindings).
- The molecular geometry provided via an XYZ file whose second line specifies
  charge and multiplicity (e.g. ``0 1``).

Usage
-----

Socket mode
^^^^^^^^^^^

.. code-block:: bash

   mxl_driver --model rttddft --port 31415 \
     --param "molecule_xyz=${PWD}/tests/data/hcn.xyz, functional=PBE0, \
              basis=cc-pVDZ, dt_rttddft_au=0.04, delta_kick_au=1e-2, \
              delta_kick_direction=xyz, electron_propagation=pc, \
              threshold_pc=1e-6, memory=8GB, num_threads=4, \
              checkpoint=false, restart=false"

Non-socket mode
^^^^^^^^^^^^^^^

.. code-block:: python

   mxl.Molecule(
       driver="rttddft",
       driver_kwargs={
           "molecule_xyz": "tests/data/hcn.xyz",
           "functional": "PBE0",
           "basis": "cc-pVDZ",
           "dt_rttddft_au": 0.04,
           "delta_kick_au": 1e-2,
           "delta_kick_direction": "xyz",
           "electron_propagation": "pc",
           "memory": "8GB",
           "num_threads": 4,
       },
       # ...
   )


Parameters
----------

.. list-table::
   :header-rows: 1

   * - Name
     - Description
   * - ``engine``
     - Computational backend. Currently only ``psi4`` is supported. Default:
       ``psi4``.
   * - ``molecule_xyz``
     - Path to the geometry file. Relative paths are resolved on the driver
       side. Required.
   * - ``functional``
     - `Psi4 <https://psicode.org/>`_ functional label (``SCF``, ``PBE0``, ``B3LYP`` …). Default: ``SCF``.
   * - ``basis``
     - `Psi4 <https://psicode.org/>`_ basis set label (``sto-3g``, ``6-31g``, ``cc-pVDZ`` …). Default:
       ``sto-3g``.
   * - ``dt_rttddft_au``
     - Electronic time step in atomic units. Sub-stepping is applied when this
       is smaller than the FDTD step. Default: ``0.04``.
   * - ``delta_kick_au``
     - Magnitude of the initial delta-kick applied along the selected axes
       (a.u.). Default: ``0.0``.
   * - ``delta_kick_direction``
     - Axes for the delta-kick (``x``, ``y``, ``z``, ``xy``, ``xz``, ``yz``,
       ``xyz``). Default: ``xyz``.
   * - ``memory``
     - `Psi4 <https://psicode.org/>`_ memory allocation string (e.g. ``8GB``). Default: ``8GB``.
   * - ``num_threads``
     - Number of threads assigned to `Psi4 <https://psicode.org/>`_. Default: ``1``.
   * - ``electron_propagation``
     - Electronic propagator: ``etrs`` (enforced time-reversal symmetry) or
       ``pc`` (predictor–corrector). Default: ``etrs``.
   * - ``threshold_pc``
     - Convergence threshold for the predictor–corrector scheme. Default:
       ``1e-6``.
   * - ``remove_permanent_dipole``
     - When ``True`` subtract the permanent dipole from the light–matter
       coupling. Default: ``False``.
   * - ``dft_grid_name``
     - `Psi4 <https://psicode.org/>`_ quadrature grid label (``SG0``, ``SG1`` …). Default: ``SG0``.
   * - ``dft_radial_points``
     - Number of radial grid points (negative values select `Psi4 <https://psicode.org/>`_ defaults).
       Default: ``-1``.
   * - ``dft_spherical_points``
     - Number of angular grid points (negative values select `Psi4 <https://psicode.org/>`_ defaults).
       Default: ``-1``.
   * - ``checkpoint``
     - When ``True`` write ``rttddft_checkpoint_id_<n>.npy`` snapshots for
       restart. Default: ``False``.
   * - ``restart``
     - When ``True`` attempt to resume from the last checkpoint. Default:
       ``False``.
   * - ``verbose``
     - When ``True`` print propagation diagnostics and dipole traces.
       Default: ``False``.

Returned data
-------------

- ``time_au`` – Simulation clock in atomic units.
- ``energy_au`` – Total electronic energy from `Psi4 <https://psicode.org/>`_.
- ``mux_au``, ``muy_au``, ``muz_au`` – Time-dependent dipole components in atomic units.

Notes
-----

- Ensure the `Psi4 <https://psicode.org/>`_ executable or Python module is available on ``PATH``/
  ``PYTHONPATH`` for the driver process.
- Additional `Psi4 <https://psicode.org/>`_ keywords can be supplied via environment variables or by
  editing the `Psi4 <https://psicode.org/>`_ input template prior to running the driver.
