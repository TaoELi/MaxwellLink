Laser-Driven Simulation
=======================

The :mod:`maxwelllink.em_solvers.laser_driven` module applies a user-defined
electric field directly to **MaxwellLink** molecules. It trades spatial grids
for a prescribed laser profile that can excite any combination of ``x``,
``y``, and ``z`` dipole components while keeping the rest of the simulation in
atomic units. Both socket-connected and embedded (non-socket) drivers are
supported through lightweight wrappers that reuse the standard
:class:`~maxwelllink.Molecule` interface.

.. note::

  The solver injects the same scalar drive into every selected axis:

  .. math::

     \mathbf{E}(t) = f(t)\,\hat{\mathbf{n}},

  where :math:`f(t)` is supplied via ``drive`` and :math:`\hat{\mathbf{n}}`
  masks the requested dipole axes. Molecular responses are collected as both
  dipoles and time derivatives, enabling downstream analyses or feedback
  without additional unit conversions.


Requirements
------------

- No external electromagnetic backend is required; all quantities remain in
  atomic units within the **MaxwellLink** Python stack.


Usage
-----

Socket mode
^^^^^^^^^^^

.. code-block:: python

   import maxwelllink as mxl
   from maxwelllink.em_solvers.laser_driven import LaserDrivenSimulation
   from maxwelllink.tools import gaussian_enveloped_cosine

   host, port = mxl.get_available_host_port()
   hub = mxl.SocketHub(host=host, port=port, timeout=10.0, latency=1e-5)

   molecule = mxl.Molecule(hub=hub)
   drive = gaussian_enveloped_cosine(
       amplitude_au=5e-4,
       omega_au=0.15,
       sigma_au=40.0,
   )

   sim = LaserDrivenSimulation(
       dt_au=0.5,
       molecules=[molecule],
       drive=drive,
       coupling_axis="z",
       hub=hub,
       record_history=True,
   )

   # Launch the external driver, e.g.:
   # mxl_driver --model tls --port <port> ...
   sim.run(steps=4000)

Non-socket mode
^^^^^^^^^^^^^^^

.. code-block:: python

   import maxwelllink as mxl
   from maxwelllink.em_solvers.laser_driven import LaserDrivenSimulation
   from maxwelllink.tools import gaussian_pulse

   tls = mxl.Molecule(
       driver="tls",
       driver_kwargs=dict(
           omega=0.242,
           mu12=187.0,
           orientation=2,
           pe_initial=1e-4,
       ),
   )

   sim = LaserDrivenSimulation(
       dt_au=0.5,
       molecules=[tls],
       drive=gaussian_pulse(amplitude_au=1e-4, sigma_au=30.0),
       coupling_axis="xy",
       record_history=False,
   )

   sim.run(until=500.0)


Parameters
----------

.. list-table::
   :header-rows: 1

   * - Name
     - Description
   * - ``dt_au``
     - Positive integration time step in atomic units. Propagates each molecule with the same cadence; values ``<= 0`` raise :class:`ValueError`.
   * - ``molecules``
     - Iterable of :class:`~maxwelllink.Molecule` instances. Socket and embedded drivers can be mixed; wrappers refresh their time-step metadata automatically.
   * - ``drive``
     - Constant float or callable ``drive(time_au)`` that returns the electric-field amplitude in atomic units. Helper generators such as :func:`~maxwelllink.tools.gaussian_pulse` live in :mod:`maxwelllink.tools`.
   * - ``coupling_axis``
     - Case-insensitive combination of ``"x"``, ``"y"``, ``"z"`` (e.g. ``"xz"``). At least one axis must be supplied; unchecked axes receive zero field and contribute nothing to the feedback.
   * - ``hub``
     - Optional :class:`~maxwelllink.SocketHub`. Required when any molecule operates in socket mode; all socket molecules must share the same hub.
   * - ``record_history``
     - When ``True`` record time stamps, evaluated drive, and molecular response snapshots for each step.


Returned data
-------------

- ``sim.time_history`` – list of time stamps (a.u.) when ``record_history=True``.
- ``sim.drive_history`` – sampled values of ``drive(time_au)`` at each recorded step.
- ``sim.molecule_response_history`` – per-step total :math:`d\boldsymbol{\mu}/dt` masked to the chosen axes.
- ``molecule.additional_data_history`` – driver diagnostics appended automatically; socket drivers may return extra JSON payloads via ``extra``.


Notes
-----

- This solver does not propagate electromagnetic fields as an independent entity; the user-defined ``drive`` is applied uniformly to all molecules at each time step.

- This solver might appear advantageous for exploring how quantum molecular dynamics might be controlled by altering the shape and phase of the applied laser field.