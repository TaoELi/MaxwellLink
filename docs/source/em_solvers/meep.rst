Meep FDTD Solver
================

The :mod:`maxwelllink.em_solvers.meep` backend couples **MaxwellLink** molecules to
`Meep <https://meep.readthedocs.io/>`_ (``pymeep``) simulations. It wraps
`meep.Simulation <https://meep.readthedocs.io/en/master/Python_User_Interface/#simulation>`_, converts between Meep units and atomic units, creates
regularized polarization sources, and shuttles electric-field integrals and
driver responses each time step.

.. note::

  The Meep backend advances the classical Maxwell equations

  .. math::

     \partial_t \mathbf{D}(\mathbf{r}, t) = \nabla \times \mathbf{H}(\mathbf{r}, t) - \mathbf{J}_{\mathrm{mol}}(\mathbf{r}, t), \qquad
     \partial_t \mathbf{B}(\mathbf{r}, t) = - \nabla \times \mathbf{E}(\mathbf{r}, t),

  with constitutive relations :math:`\mathbf{D} = \varepsilon_0 \varepsilon^{\ast}_{\mathrm{r}}(\mathbf{r}, \omega)\, \mathbf{E}` and :math:`\mathbf{B} = \mu\, \mathbf{H}` (typically :math:`\mu = \mu_0`). Molecular sources enter through

  .. math::

     \mathbf{J}_{\mathrm{mol}}(\mathbf{r}, t) = \sum_{m} \partial_t \mathbf{P}_{\mathrm{mol}}^{m}(\mathbf{r}, t), \qquad
     \mathbf{P}_{\mathrm{mol}}^{m}(\mathbf{r}, t) = \sum_{i=x,y,z} \gamma\, \mu_{m}^{i}(t)\, \boldsymbol{\kappa}_{m}^{i}(\mathbf{r}),

  where each spatial kernel :math:`\boldsymbol{\kappa}_{m}^{i}` is normalized over the molecular volume :math:`\Omega_m`. The solver supplies every coupled molecule with the regularized electric field

  .. math::

     \widetilde{E}_{m}^{i}(t) = \int_{\Omega_m} d\mathbf{r}\, \mathbf{E}(\mathbf{r}, t) \cdot \boldsymbol{\kappa}_{m}^{i}(\mathbf{r}),

  and expects the returned dipole time derivative :math:`d\mu_{m}^{i}/dt` to reconstruct :math:`\mathbf{J}_{\mathrm{mol}}` self-consistently on the FDTD grid.

Requirements
------------

- `pymeep` (imported as :mod:`meep`) must be installed. Import failures raise a
  clear :class:`ImportError`.
- Optional `mpi4py` support is detected automatically. When present, only the
  Meep master rank communicates with the molecular drivers while amplitudes are
  broadcast to worker ranks.


Usage
-----

Socket mode
^^^^^^^^^^^

.. code-block:: python

   import meep as mp
   import maxwelllink as mxl
   from maxwelllink import sockets as mxs

   host, port = mxs.get_available_host_port()
   hub = mxl.SocketHub(host=host, port=port, timeout=10.0, latency=1e-5)

   molecule = mxl.Molecule(
       hub=hub,
       center=mp.Vector3(0, 0, 0),
       size=mp.Vector3(1, 1, 1),
       sigma=0.1,
       dimensions=2,
   )

   sim = mxl.MeepSimulation(
       hub=hub,
       molecules=[molecule],
       time_units_fs=0.1,
       cell_size=mp.Vector3(8, 8, 0),
       geometry=[],
       sources=[],
       boundary_layers=[mp.PML(3.0)],
       resolution=10,
   )

   # Launch the driver separately (e.g. mxl_driver --model tls --port <port> ...)
   sim.run(until=90)

Non-socket mode
^^^^^^^^^^^^^^^

.. code-block:: python

   import meep as mp
   import maxwelllink as mxl

   molecule = mxl.Molecule(
       driver="tls",
       driver_kwargs=dict(
           omega=0.242,
           mu12=187.0,
           orientation=2,
           pe_initial=1e-4,
       ),
       center=mp.Vector3(),
       size=mp.Vector3(1, 1, 1),
       sigma=0.1,
       dimensions=2,
   )

   sim = mxl.MeepSimulation(
       molecules=[molecule],
       time_units_fs=0.1,
       cell_size=mp.Vector3(8, 8, 0),
       geometry=[],
       sources=[],
       boundary_layers=[mp.PML(3.0)],
       resolution=10,
   )

   sim.run(until=90)

**MaxwellLink** inserts the appropriate coupling step automatically: sockets when
``hub`` is provided, or embedded drivers otherwise.

Parameters
----------

.. list-table::
   :header-rows: 1

   * - Name
     - Description
   * - ``hub``
     - Optional :class:`~maxwelllink.sockets.sockets.SocketHub`. Required for socket-connected molecules; ignored for embedded drivers.
   * - ``molecules``
     - Iterable of :class:`~maxwelllink.molecule.molecule.Molecule` instances. They are wrapped into :class:`~maxwelllink.em_solvers.meep.MoleculeMeepWrapper` and may mix socket and non-socket modes.
   * - ``time_units_fs``
     - Meep time unit expressed in femtoseconds (default ``0.1``). Used to refresh molecular time steps and unit conversions.
   * - ``geometry``
     - Sequence of Meep geometry objects forwarded to :class:`meep.Simulation`.
   * - ``sources``
     - List of native Meep sources. Molecular sources are added automatically; this list is for additional excitations.
   * - ``cell_size``
     - Simulation domain size passed to :class:`meep.Simulation`.
   * - ``boundary_layers``
     - List of boundary conditions (for example ``[mp.PML(thickness)]``).
   * - ``resolution``
     - Spatial resolution (pixels per distance unit). Used to derive ``dx`` and enforce ``Courant = 0.5``.
   * - ``**kwargs``
     - Remaining keyword arguments are forwarded verbatim to :class:`meep.Simulation` (e.g. ``default_material``, ``k_point``, ``symmetries``).

Returned data
-------------

- ``sim.molecules`` – list of :class:`MoleculeMeepWrapper` instances. Each wrapper exposes the underlying :class:`~maxwelllink.molecule.molecule.Molecule`.
- ``molecule.additional_data_history`` – diagnostics produced by the driver (time stamps, populations, energies, custom JSON payloads).
- Standard Meep data channels remain available (e.g. ``sim.fields``, flux regions, near-to-far field monitors).
- For debugging, ``maxwelllink.em_solvers.meep.instantaneous_source_amplitudes`` stores the most recent source amplitudes per polarization fingerprint.

Notes
-----

- :class:`MeepSimulation` enforces ``Courant = 0.5``. Provide ``resolution`` and other grid parameters accordingly.
- MPI runs automatically confine socket communication to rank 0 while broadcasting amplitudes to all ranks; disconnections pause the solver until the hub reports reconnection.
- The ``dimensions=...`` input can only be assigned in :class:`Molecule` not :class:`MeepSimulation`. For example, assigning ``dimensions=1`` in both :class:`Molecule` and :class:`MeepSimulation` will lead to runtime error: ``meep: cannot require a ez component in a 1D grid``.
- In 1D and 2D simulations, :class:`Molecule` (or molecular drivers) should have a transition dipole moment along z-direction, because :class:`MeepSimulation` sets the :class:`Molecule` to emit only along z-direction in 1D and 2D.