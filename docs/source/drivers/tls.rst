TLS driver
==========

The TLS driver implements a minimal closed two-level system with a single
transition frequency and dipole moment. It is provided by
:class:`maxwelllink.mxl_drivers.python.models.TLSModel` and ships with
**MaxwellLink** for lightweight regression tests and benchmark scenarios.

.. note::

  The TLS driver advances the two-level density matrix through

  .. math::

     \frac{d}{dt}\hat{\rho}(t) = -\frac{i}{\hbar}\Bigl[\hat{H}_0 - \widetilde{\mathbf{E}}(t)\cdot \hat{\boldsymbol{\mu}},\, \hat{\rho}(t)\Bigr],

  where :math:`\hat{H}_0 = \mathrm{diag}(0, \hbar\omega_0)` and :math:`\hat{\boldsymbol{\mu}} = \mu_{12}\,\mathbf{e}_{i}\,\hat{\sigma}_x`. :math:`\mathbf{e}_i` denotes
  the orientation unit vector of the dipole. The emitted dipole current is evaluated analytically as

  .. math::

     \frac{d}{dt}\langle \hat{\boldsymbol{\mu}} \rangle = -2\,\omega_0\,\mu_{12}\,\operatorname{Im}\!\bigl(\rho_{12}\bigr)\,\mathbf{e}_{i},

  which is passed directly to Maxwell's equations.

Requirements
------------

- No additional packages are required beyond **MaxwellLink**'s dependencies.
- ``scipy`` (already bundled) is used for the short-time propagator.

Usage
-----

Socket mode
^^^^^^^^^^^

.. code-block:: bash

   mxl_driver --model tls --port 31415 \
     --param "omega=0.242, mu12=187, orientation=2, pe_initial=1e-4, \
              checkpoint=false, restart=false"

Non-socket mode
^^^^^^^^^^^^^^^

.. code-block:: python

   mxl.Molecule(
       driver="tls",
       driver_kwargs={
           "omega": 0.242,
           "mu12": 187.0,
           "orientation": 2,
           "pe_initial": 1e-4,
       },
       # ...
   )


Parameters
----------

.. list-table::
   :header-rows: 1

   * - Name
     - Description
   * - ``omega``
     - Transition frequency in atomic units. Default: ``2.4188843e-1``, corresponding to ``1.0`` in
       `Meep <https://meep.readthedocs.io/en/latest/>`_ units when ``time_units_fs=0.1``.
   * - ``mu12``
     - Transition dipole moment in atomic units; scaling with the emitted source
       amplitude. Default: ``1.870819866e2``, corresponding to ``0.1`` in
       `Meep <https://meep.readthedocs.io/en/latest/>`_ units when ``time_units_fs=0.1``.
   * - ``orientation``
     - Dipole orientation: ``0`` couples to ``E_x``, ``1`` to ``E_y``, ``2`` to
       ``E_z``. Default: ``2``.
   * - ``pe_initial``
     - Initial excited-state population. Default: ``0.0``.
   * - ``checkpoint``
     - When ``True`` write ``tls_checkpoint_id_<n>.npz`` after each step. Default:
       ``False``.
   * - ``restart``
     - When ``True`` resume from the latest checkpoint if present. Default:
       ``False``.
   * - ``verbose``
     - When ``True`` print field values and density-matrix diagnostics each step.
       Default: ``False``.

Returned data
-------------

- ``time_au`` – Simulation time in atomic units.
- ``energy_au`` – Instantaneous TLS energy.
- ``mux_au``, ``muy_au``, ``muz_au`` – Dipole vector components (non-zero along the selected orientation) in atomic units.
- ``Pe`` / ``Pg`` – Excited- and ground-state populations.
- ``Pge_real`` / ``Pge_imag`` – Real and imaginary parts of the coherence.

Notes
-----

- The driver exposes a deterministic analytic evolution; see
  ``tests/test_tls/test_meep_2d_socket_tls1_relaxation.py`` for reference
  output.
- The `QuTiP <https://qutip.org/>`_ driver replicates this model when ``preset=tls``; use the TLS driver
  for the lightest-weight option.
