Lorentz bath driver
===================

.. warning::
  
  This is a beta feature and may change in future versions. 

The Lorentz bath driver implements a classical bright harmonic oscillator coupled
to a bath of classical dark oscillators. It is provided by
:class:`maxwelllink.mxl_drivers.python.models.LorentzBathModel` and ships with
**MaxwellLink** for reduced polaritonic-chemistry models where a collective
molecular bright mode exchanges energy with dark molecular modes.

.. note::

  The Lorentz bath driver advances the bright coordinate :math:`q_B`, its
  conjugate momentum :math:`p_B`, and bath coordinates :math:`q_j, p_j` generated
  by the Hamiltonian

  .. math::

     H = \frac{1}{2}\left(p_B - \sum_j k_j q_j\right)^2
         + \frac{1}{2}\omega_0^2 q_B^2
         - \mu_0 q_B\,\bigl(\widetilde{\mathbf{E}}(t)\cdot\mathbf{e}_i\bigr)
         + \sum_j\left(\frac{1}{2}\omega_j^2 q_j^2 + \frac{1}{2}p_j^2\right).

  Here :math:`\mathbf{e}_i` denotes the selected dipole orientation, :math:`k_j`
  are bright-dark coupling coefficients, and :math:`\omega_j` are bath-mode
  frequencies. The emitted dipole current returned to Maxwell's equations is
  evaluated by:

  .. math::

     \frac{d}{dt}\langle\boldsymbol{\mu}\rangle
       = \mu_0\left(p_B - \sum_j k_j q_j\right)\mathbf{e}_i.

Requirements
------------

- No additional packages are required beyond **MaxwellLink**'s dependencies.

Usage
-----

Socket mode
^^^^^^^^^^^

.. code-block:: bash

   mxl_driver --model lorentz_bath --port 31415 \
     --param "omega=0.242, mu0=187, orientation=2, num_bath=101, \
              bath_width=0.05, bath_form=lorentzian, bath_dephasing=0.002, \
              checkpoint=false, restart=false"

Non-socket mode
^^^^^^^^^^^^^^^

.. code-block:: python

   mxl.Molecule(
       driver="lorentz_bath",
       driver_kwargs={
           "omega": 0.242,
           "mu0": 187.0,
           "orientation": 2,
           "num_bath": 101,
           "bath_width": 0.05,
           "bath_form": "lorentzian",
           "bath_dephasing": 0.002,
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
     - Bright oscillator frequency :math:`\omega_0` in atomic units. Default:
       ``2.4188843e-1``, corresponding to ``1.0`` in
       `Meep <https://meep.readthedocs.io/en/latest/>`_ units when
       ``time_units_fs=0.1``.
   * - ``mu0``
     - Dipole-coordinate coupling prefactor :math:`\mu_0` in atomic units, so
       that the instantaneous bright-mode dipole is :math:`\mu(t)=\mu_0 q_B(t)`;
       scales the emitted source amplitude. Default: ``1.870819866e2``,
       corresponding to ``0.1`` in `Meep <https://meep.readthedocs.io/en/latest/>`_
       units when ``time_units_fs=0.1``.
   * - ``orientation``
     - Dipole orientation: ``0`` couples to ``E_x``, ``1`` to ``E_y``, ``2`` to
       ``E_z``. Default: ``2``.
   * - ``num_bath``
     - Number of bath oscillators used by the convenient bath builder. Must be
       greater than ``1``. Used only when ``omega_bath`` and ``k_bath`` are not
       both provided.
   * - ``bath_width``
     - Frequency width of the convenient bath distribution in atomic units. The
       bath frequencies are placed uniformly from ``omega - 0.5*bath_width`` to
       ``omega + 0.5*bath_width``.
   * - ``bath_form``
     - Coupling-envelope form for the convenient bath builder. Supported values:
       ``uniform``, ``gaussian``, and ``lorentzian``.
   * - ``bath_dephasing``
     - Bright-to-dark dephasing scale in atomic units used by the convenient bath
       builder to set the coupling coefficients :math:`k_j`. Default: ``0.0``.
   * - ``bath_relaxation``
     - Direct bath-momentum relaxation rate in atomic units. When positive,
       bath momenta are damped by ``exp(-bath_relaxation*dt)`` each time step.
       Default: ``0.0``.
   * - ``omega_bath``
     - Direct list of bath oscillator frequencies in atomic units. When supplied
       together with ``k_bath``, this direct definition has priority over
       ``num_bath``/``bath_width``/``bath_form``.
   * - ``k_bath``
     - Direct list of bright-dark coupling coefficients in atomic units. Must
       have the same length as ``omega_bath``.
   * - ``p_initial``
     - Initial bright-mode momentum :math:`p_B` in atomic units. Default:
       ``0.0``.
   * - ``q_initial``
     - Initial bright-mode position :math:`q_B` in atomic units. Default:
       ``0.0``.
   * - ``p_bath_initial``
     - Initial bath momenta as a list with the same length as the bath. Defaults
       to zeros.
   * - ``q_bath_initial``
     - Initial bath positions as a list with the same length as the bath.
       Defaults to zeros.
   * - ``langevin_tau_au``
     - Optional Langevin thermostat damping time in atomic units. The thermostat
       is enabled during initialization when this value is provided and
       ``temperature_au > 0``. Default: ``None``.
   * - ``initializer``
     - Optional initializer. Supported value: ``maxwell_boltzmann``, which samples
       bright and bath coordinates and momenta using ``temperature_au``.
   * - ``temperature_au``
     - Temperature in atomic units used by the Langevin thermostat and
       ``maxwell_boltzmann`` initializer. Default: ``0.0``.
   * - ``random_seed``
     - Random seed used by the initializer and thermostat. Default: ``114514``.
   * - ``checkpoint``
     - When ``True`` write ``lorentz_bath_checkpoint_id_<n>.npz`` after each step.
       Default: ``False``.
   * - ``restart``
     - When ``True`` resume from the latest checkpoint if present. Default:
       ``False``.
   * - ``verbose``
     - When ``True`` print field values and oscillator diagnostics each step.
       Default: ``False``.

Returned data
-------------

- ``time_au`` - Simulation time in atomic units.
- ``energy_au`` - Total half-step Hamiltonian energy in atomic units.
- ``energy_lorentz_au`` - Bright oscillator contribution, including the
  bright-bath momentum-shift terms, in atomic units.
- ``energy_bath_au`` - Bath oscillator energy in atomic units.
- ``mux_au``, ``muy_au``, ``muz_au`` - Half-step dipole vector components
  (non-zero along the selected orientation) in atomic units.
- ``mux_m_au``, ``muy_m_au``, ``muz_m_au`` - Full-step bright-mode dipole vector
  components in atomic units.
- ``p_au`` - Full-step bright-mode momentum :math:`p_B` in atomic units.
- ``q_au`` - Full-step bright-mode position :math:`q_B` in atomic units.

Notes
-----

- A bath definition is required. Provide either direct ``omega_bath`` and
  ``k_bath`` lists, or the convenient builder parameters ``num_bath``,
  ``bath_width``, and ``bath_form``.
- Direct ``omega_bath``/``k_bath`` settings have priority over the convenient
  bath builder.
- The convenient bath builder currently places bath frequencies on an evenly
  spaced grid and supports uniform, Gaussian, and Lorentzian coupling envelopes.
  Use direct bath arrays for custom spectral densities.
- This driver is a reduced classical model intended for studying bright-mode
  dephasing into dark molecular modes. For atomistic molecular dynamics, prefer
  the **LAMMPS**, **DFTB+**, or **ASE** drivers.
