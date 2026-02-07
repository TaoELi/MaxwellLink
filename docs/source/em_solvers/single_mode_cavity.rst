Single-Mode Cavity
==================

The :mod:`maxwelllink.em_solvers.single_mode_cavity` module provides a
lightweight electromagnetic solver that replaces a full FDTD grid with a single
damped harmonic oscillator. It evolves entirely in atomic units while
supporting the same :class:`~maxwelllink.molecule.molecule.Molecule` abstraction used by the Meep
backend, making it well suited for rapid prototyping, regression tests, and
workflows focused on one classical cavity mode. Multiple molecular dipole
components can be coupled simultaneously by supplying composite axes such as
``"xy"``.

.. note::

  The cavity replaces the full EM grid with canonical coordinate :math:`\mathbf{q}_c` and momentum :math:`\mathbf{p}_c = \dot{\mathbf{q}}_c` obeying the component-wise equation

  .. math::

     \ddot{\mathbf{q}}_c = -\omega_c^{2} \mathbf{q}_c - \varepsilon \sum_{m} \boldsymbol{\mu}_{m} - \kappa \dot{\mathbf{q}}_c + D(t),

  where :math:`\omega_c` is ``frequency_au``, :math:`\kappa` is ``damping_au``, :math:`\varepsilon = 1/\sqrt{\epsilon_0 V}` is ``coupling_strength``, and :math:`D(t)` is the optional external drive. The sum runs over the dipole components selected by ``coupling_axis`` for each coupled molecule. The effective electric field returned to the drivers is

  .. math::

     \mathbf{E}(t) = -\varepsilon\, \mathbf{q}_c(t) - \delta_{\mathrm{DSE}} \frac{\varepsilon^{2}}{\omega_c^{2}}\, \boldsymbol{\mu}(t),

  with :math:`\boldsymbol{\mu}(t)` the summed molecular dipole restricted to the requested axes and :math:`\delta_{\mathrm{DSE}} = 1` only when ``include_dse=True``.

Requirements
------------

- No additional dependencies beyond the **MaxwellLink** Python stack are required.

Usage
-----

Socket mode
^^^^^^^^^^^

.. code-block:: python

   import maxwelllink as mxl

   hub = mxl.SocketHub(host="127.0.0.1", port=31415, timeout=10.0, latency=1e-5)
   molecule = mxl.Molecule(hub=hub)

   sim = mxl.SingleModeSimulation(
       dt_au=0.5,
       frequency_au=0.242,
       damping_au=0.0,
       molecules=[molecule],
       coupling_strength=1e-4,
       qc_initial=[0, 0, 1e-5],
       coupling_axis="z",
       hub=hub,
       record_history=True,
   )

   sim.run(steps=4000)

Launch :command:`mxl_driver --model tls --port 31415 ...` (or another driver)
after running this script.

Non-socket mode
^^^^^^^^^^^^^^^

.. code-block:: python

   import maxwelllink as mxl

   tls = mxl.Molecule(
       driver="tls",
       driver_kwargs=dict(
           omega=0.242,
           mu12=187.0,
           orientation=2,
           pe_initial=1e-4,
       ),
   )

   sim = mxl.SingleModeSimulation(
       dt_au=0.5,
       frequency_au=0.242,
       damping_au=0.0,
       molecules=[tls],
       coupling_strength=1e-4,
       qc_initial=[0, 0, 1e-5],
       coupling_axis="z",
       record_history=True,
   )

   sim.run(steps=4000)

Parameters
----------

.. list-table::
   :header-rows: 1

   * - Name
     - Description
   * - ``dt_au``
     - Integration time step in atomic units. Must be positive.
   * - ``frequency_au``
     - Cavity angular frequency :math:`\omega_c` (a.u.).
   * - ``damping_au``
     - Linear damping coefficient :math:`\kappa` applied to the cavity momentum (a.u.).
   * - ``molecules``
     - Iterable of :class:`~maxwelllink.molecule.molecule.Molecule` instances. Socket and non-socket
       molecules can be mixed in the same simulation.
   * - ``drive``
     - Constant float or callable ``drive(time_au)`` returning the external drive term.
       Defaults to ``0.0``.
   * - ``coupling_strength``
     - Scalar prefactor :math:`g` for the molecular polarization feedback. Default: ``1.0``.
   * - ``coupling_axis``
     - One or more dipole components to couple (case-insensitive union of ``"x"``, ``"y"``, ``"z"``, e.g. ``"xy"``). Default: ``"xy"``.
   * - ``hub``
     - Optional :class:`~maxwelllink.sockets.sockets.SocketHub` shared by all socket-mode molecules.
       The simulation infers the hub from the first socket molecule when omitted.
   * - ``qc_initial``
     - Initial cavity coordinate vector (sequence of three floats or scalar applied to each coupled axis). Default: ``[0.0, 0.0, 0.0]``.
   * - ``pc_initial``
     - Initial cavity momentum vector (a.u.). Default: ``[0.0, 0.0, 0.0]``.
   * - ``mu_initial``
     - Initial total molecular dipole vector prior to axis masking (a.u.). Default: ``[0.0, 0.0, 0.0]``.
   * - ``dmudt_initial``
     - Initial time derivative of the total molecular dipole vector (a.u.). Default: ``[0.0, 0.0, 0.0]``.
   * - ``shift_dipole_baseline``
     - When ``True`` subtract the initial dipole so the simulation starts from a zero baseline (helps with large permanent dipoles). Default: ``False``.
   * - ``molecule_half_step``
     - When ``True`` extrapolate molecular responses from half-step data (use ``False`` for full-step drivers). Default: ``False``.
   * - ``record_history``
     - When ``True`` store histories for time, field, momentum, drive, and net molecular response.
       Default: ``True``.
   * - ``include_dse``
     - When ``True`` add the dipole self-energy correction to the field returned to the molecules and account for it in the cavity energy. Default: ``False``.

Returned data
-------------

Calling :class:`~maxwelllink.em_solvers.single_mode_cavity.SingleModeSimulation` with ``record_history=True``
populates:

- :attr:`SingleModeSimulation.time_history` – time stamps in atomic units.
- :attr:`SingleModeSimulation.qc_history` – cavity coordinate :math:`q_c(t)`.
- :attr:`SingleModeSimulation.pc_history` – cavity momentum :math:`\dot{q}_c(t)`.
- :attr:`SingleModeSimulation.drive_history` – external drive values.
- :attr:`SingleModeSimulation.molecule_response_history` – summed molecular response along ``coupling_axis``.
- :attr:`SingleModeSimulation.energy_history` – total energy of the cavity and coupled molecules (requires ``record_history=True``).

Each :class:`~maxwelllink.molecule.molecule.Molecule` keeps
:attr:`~maxwelllink.molecule.molecule.Molecule.additional_data_history`, which records driver
data (e.g., TLS populations, energies, timestamps). Socket drivers may
append extra JSON payloads through the hub; embedded drivers populate the same
history via :meth:`append_additional_data`.

Notes
-----

- Provide either ``steps`` or ``until`` to :meth:`SingleModeSimulation.run`, not both.
- Socket-mode molecules must all bind to the same :class:`~maxwelllink.sockets.sockets.SocketHub`;
  the simulation waits until every driver acknowledges initialization.
- Setting ``record_history=False`` avoids list allocations for throughput-critical runs.
