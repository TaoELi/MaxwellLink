RT-TDDFT-Ehrenfest driver
=========================

The RT-TDDFT-Ehrenfest driver extends :doc:`rttddft <rttddft>` with classical
nuclear propagation using Ehrenfest or Born–Oppenheimer forces. It is provided
by :class:`maxwelllink.mxl_drivers.python.models.RTEhrenfestModel`, which
computes electronic dynamics with RT-TDDFT while integrating nuclear motion under
mean-field electronic potential energy surfaces. This Python driver couples to
EM solvers in both electronic and nuclear degrees of freedom.

.. note::

  Electronic propagation follows the RT-TDDFT equation of motion

  .. math::

     \frac{d}{dt} \mathbf{P}^{\mathrm{e}}_{\mathrm{o}}(t) = -\frac{i}{\hbar}\Bigl[\mathbf{F}^{\mathrm{e}}_{\mathrm{o}}(t) - \widetilde{\mathbf{E}}(t)\cdot \vec{\boldsymbol{\mu}}^{\mathrm{e}}_{\mathrm{o}},\, \mathbf{P}^{\mathrm{e}}_{\mathrm{o}}(t)\Bigr],

  while the nuclei are propagated through

  .. math::

     M_n \ddot{\mathbf{R}}_n = -\nabla_{\mathbf{R}_n} E_{\mathrm{mf}}(\{\mathbf{R}_m\}, t) + Z_n \widetilde{\mathbf{E}}(t),

  where :math:`E_{\mathrm{mf}}` denotes the Ehrenfest mean-field or Born--Oppenheimer potential specified by ``force_type``, and :math:`Z_n` is the nuclear 
  charge of atom :math:`n`. The emitted dipole current combines both nuclear and electronic contributions,

  .. math::

     \dot{\boldsymbol{\mu}} = \sum_n Z_n \mathbf{v}_n + \mathrm{Tr}\!\left(\frac{d}{dt}\mathbf{P}^{\mathrm{e}}(t)\,\vec{\boldsymbol{\mu}}^{\mathrm{e}}\right),

  and is sent back to the Maxwell solver each time step.

Requirements
------------

- ``psi4`` available in the driver environment (`Psi4 <https://psicode.org/>`_ with Python bindings).
- Same structure requirements as the RT-TDDFT driver (XYZ file with charge and
  multiplicity on the second line).

Usage
-----

Socket mode
^^^^^^^^^^^

.. code-block:: bash

   mxl_driver --model rtehrenfest --port 31415 \
     --param "molecule_xyz=${PWD}/tests/data/hcn.xyz, functional=B3LYP, \
              basis=sto-3g, dt_rttddft_au=0.04, force_type=ehrenfest, \
              n_fock_per_nuc=10, n_elec_per_fock=10, \
              checkpoint=false, restart=false"

Non-socket mode
^^^^^^^^^^^^^^^

.. code-block:: python

   mxl.Molecule(
       driver="rtehrenfest",
       driver_kwargs={
           "molecule_xyz": "tests/data/hcn.xyz",
           "functional": "B3LYP",
           "basis": "sto-3g",
           "dt_rttddft_au": 0.04,
           "force_type": "bo",
           "n_fock_per_nuc": 10,
           "n_elec_per_fock": 10,
           "partial_charges": [1.0, -1.0, 0.0],
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
     - Path to the geometry file (charge/multiplicity on the second line).
       Required.
   * - ``functional``
     - `Psi4 <https://psicode.org/>`_ functional label. Default: ``SCF``.
   * - ``basis``
     - `Psi4 <https://psicode.org/>`_ basis set label. Default: ``sto-3g``.
   * - ``dt_rttddft_au``
     - Electronic time step in atomic units. Default: ``0.04``.
   * - ``delta_kick_au``
     - Initial delta-kick amplitude (a.u.). Default: ``0.0``.
   * - ``delta_kick_direction``
     - Axes for the delta-kick (``x``, ``y``, ``z``, ``xy``, ``xz``, ``yz``,
       ``xyz``). Default: ``xyz``.
   * - ``memory``
     - `Psi4 <https://psicode.org/>`_ memory allocation string. Default: ``8GB``.
   * - ``num_threads``
     - CPU threads assigned to `Psi4 <https://psicode.org/>`_. Default: ``1``.
   * - ``electron_propagation``
     - Electronic propagator: ``etrs`` or ``pc``. Default: ``etrs``.
   * - ``threshold_pc``
     - Predictor–corrector convergence threshold (used when
       ``electron_propagation=pc``). Default: ``1e-6``.
   * - ``remove_permanent_dipole``
     - When ``True`` subtract the permanent dipole from the coupling term.
       Default: ``False``.
   * - ``dft_grid_name``
     - `Psi4 <https://psicode.org/>`_ quadrature grid label. Default: ``SG0``.
   * - ``dft_radial_points`` / ``dft_spherical_points``
     - Grid sizes (negative values fall back to `Psi4 <https://psicode.org/>`_ defaults). Defaults:
       ``-1``.
   * - ``force_type``
     - ``ehrenfest`` for mean-field forces or ``bo`` for Born–Oppenheimer
       gradients. Default: ``ehrenfest``.
   * - ``n_fock_per_nuc``
     - Number of Fock builds per nuclear update. Default: ``10``.
   * - ``n_elec_per_fock``
     - Number of electronic steps per Fock build. Default: ``10``.
   * - ``mass_amu``
     - Optional per-atom masses (amu). Default: ``None`` (Psi4 values).
   * - ``friction_gamma_au``
     - Langevin friction coefficient in atomic units. Default: ``0.0``.
   * - ``temperature_K``
     - Target temperature for Langevin dynamics. Default: ``None`` (disabled).
   * - ``rng_seed``
     - Random seed for the Langevin thermostat. Default: ``1234``.
   * - ``partial_charges``
     - Optional per-atom charges for coupling to external fields. Default:
       ``None``.
   * - ``homo_to_lumo``
     - When ``True`` promotes one alpha electron from HOMO to LUMO at
       initialization. Default: ``False``.
   * - ``fix_nuclei_indices``
     - Indices of nuclei to freeze during propagation. Default: ``None``.
   * - ``checkpoint``
     - When ``True`` write ``rttddft_checkpoint_id_<n>.npy`` snapshots for later
       recovery. Default: ``False``.
   * - ``restart``
     - When ``True`` attempt to resume from the latest checkpoint. Default:
       ``False``.
   * - ``verbose``
     - When ``True`` print propagation diagnostics and dipole traces.
       Default: ``False``.

.. note::

   ``partial_charges`` should only be provided when ``force_type="bo"``. When defining 
   this parameter in the socket mode (command line), please always use ``"[1.0 -1.0 0.0]"`` 
   (no comma separating values) to ensure correct parsing. When ``force_type="ehrenfest"``, the driver
   computes forces from the Ehrenfest mean-field potential and ignores ``partial_charges``.

Returned data
-------------

- ``time_au`` – Simulation clock in atomic units.
- ``energy_au`` – Total energy including nuclear and electronic contributions.
- ``mux_au``, ``muy_au``, ``muz_au`` – Time-dependent dipole components in atomic units.

Notes
-----

- The driver combines electronic and nuclear contributions when returning
  :math:`\mathrm{d}\boldsymbol{\mu}/\mathrm{d}t` to **MaxwellLink**.
- Regression test
  ``tests/test_ase/test_ase_psi4_bomd.py`` benchmarks both ``force_type="bo"``
  and ``force_type="ehrenfest"`` against `ASE <https://wiki.fysik.dtu.dk/ase/>`_/`Psi4 <https://psicode.org/>`_ BOMD runs.
