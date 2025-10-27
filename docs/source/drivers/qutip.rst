QuTiP driver
============

The QuTiP driver embeds **MaxwellLink** in the rich modelling ecosystem provided by
`QuTiP <https://qutip.org/>`_. It can run preset TLS models or load user-defined
Hamiltonians, collapse operators, and initial states from an external Python
module. This Python driver is implemented in :class:`maxwelllink.mxl_drivers.python.models.QuTiPModel`.

.. note::

  The QuTiP driver advances the density matrix according to

  .. math::

     \frac{d}{dt}\hat{\rho}(t) = -\frac{i}{\hbar}\left[\hat{H} - \widetilde{\mathbf{E}}(t)\cdot \hat{\boldsymbol{\mu}}, \hat{\rho}(t)\right] - \mathcal{L}\bigl[\hat{\rho}(t)\bigr],

  where the Hamiltonian, dipole operators, and optional Lindblad super-operators are supplied by the user. The emitted dipole current is returned via

  .. math::

     \frac{d}{dt}\langle \hat{\boldsymbol{\mu}} \rangle = \mathrm{Tr}\!\left(\frac{d}{dt}\hat{\rho}(t)\,\hat{\boldsymbol{\mu}}\right),

  providing a consistent source term for Maxwell's equations.

Requirements
------------

- ``qutip`` (install via ``conda install -c conda-forge qutip``).
- Any custom module referenced through ``module=...`` must be importable on the
  driver side.

Usage
-----

Socket mode
^^^^^^^^^^^

.. code-block:: bash

   mxl_driver --model qutip --port 31415 \
     --param "preset=tls, fd_dmudt=false, \
              preset_kwargs=omega=0.242,mu12=187,orientation=2,pe_initial=1e-3, \
              gamma_relax=0.0, gamma_dephase=0.0"

Non-socket mode
^^^^^^^^^^^^^^^

.. code-block:: python

   mxl.Molecule(
       driver="qutip",
       driver_kwargs={
           "preset": "tls",
           "fd_dmudt": False,
           "preset_kwargs": 
               "omega=0.242,mu12=187,orientation=2,pe_initial=1e-3,\
                gamma_relax=0.0,gamma_dephase=0.0",
           "verbose": True,
       },
       # ...
   )


Parameters
----------

.. list-table::
   :header-rows: 1

   * - Name
     - Description
   * - ``preset``
     - ``tls`` for the built-in two-level system or ``custom`` to load a user
       module. Default: ``tls``.
   * - ``preset_kwargs``
     - Comma-separated overrides for the TLS preset (same parameters as the TLS
       driver, plus Lindblad rates ``gamma_relax`` and ``gamma_dephase``). Default:
       ``""``.
   * - ``module``
     - (``preset=custom``) Path to a Python file that defines
       ``build_model(**kwargs)`` returning a dict with ``H0``, ``mu_ops``,
       optional ``c_ops``, and an initial density matrix ``rho0``. Default: ``None``.
   * - ``kwargs``
     - Parameters forwarded to ``build_model`` when using the custom preset. Default:
       ``""``.
   * - ``fd_dmudt``
     - When ``True`` the driver evaluates :math:`\mathrm{d}\mu/\mathrm{d}t`
       through finite differences instead of analytical evaluation. Default: ``True``.
   * - ``verbose``
     - When ``True`` the driver prints initialization details and time-step diagnostics.
       Default: ``False``.
   * - ``checkpoint``
     - When ``True`` the driver saves intermediate results to disk for
       later analysis. Default: ``False``.
   * - ``restart``
     - When ``True`` the driver attempts to load checkpointed data on startup. Default: ``False``.

Reference custom module
^^^^^^^^^^^^^^^^^^^^^^^

The TLS regression test
``tests/test_qutip/test_meep_2d_socket_qutip_tls_relaxation.py`` launches the
driver in ``preset=custom`` mode. The helper ``tests/test_qutip/build_tls.py``
defines:

.. code-block:: python

   def build_model(**kwargs):
       return {
           "H0": qutip.Qobj(...),            # bare Hamiltonian
           "mu_ops": {"x": mux, "y": muy, "z": muz},
           "c_ops": [ ... ],                 # optional collapse operators
           "rho0": qutip.Qobj(...),          # initial density matrix or ket
       }

Any unused dipole components can be set to ``None``. Collapse operators are optional.

Returned data
-------------

The driver pushes several parameters back to the EM process and can be
retrieved from ``Molecule.additional_data_history`` (see
``tests/test_qutip/test_meep_2d_socket_qutip_tls_relaxation.py``):

- ``time_au`` – Simulation time in atomic units.
- ``energy_au`` – Instantaneous expectation value of the system Hamiltonian.
- ``mux_au``, ``muy_au``, ``muz_au`` – Dipole expectation values in atomic units.
- ``rho_diag`` – Density matrix diagonal elements (populations) as a list.
- ``Pg`` / ``Pe`` – Ground- and excited-state populations when the Hilbert space dimension is two.
- ``Pge_real`` / ``Pge_imag`` – Real and imaginary parts of the TLS coherence when the Hilbert space dimension is two.
