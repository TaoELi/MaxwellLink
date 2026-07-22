Meep Socket-Susceptibility Solver
=================================

.. warning::

   **Experimental feature — not yet production-ready.** This grid-level
   coupling is under active development and requires a **locally modified Meep
   build**, `fdtdbath-meep <https://github.com/TaoELi/fdtdbath-meep>`_, rather
   than a stock ``pymeep`` installation. For production simulations, refer to the standard Meep
   coupling described in :doc:`meep`.

The socket-susceptibility backend couples **plain** `Meep <https://meep.readthedocs.io/>`_
simulations to **MaxwellLink** molecular drivers directly at the C level of
the FDTD time-stepping loop. A ``mp.MXLSocketSusceptibility`` object (provided
by the modified `fdtdbath-meep <https://github.com/TaoELi/fdtdbath-meep>`_
build) is attached to an ordinary ``mp.Medium``, whereupon **every FDTD grid
point** covered by that medium becomes a *socket molecule* served by its own
molecular driver through a
:class:`~maxwelllink.sockets.susceptibility.SusceptibilitySocketHub`.

This constitutes the second of the two Meep couplings shipped with
**MaxwellLink**, complementary to :doc:`meep`:

- The :doc:`meep` route (``mxl.Molecule`` + ``mxl.MeepSimulation``) provides
  **molecule-level** coupling: each molecule acts as a point-dipole emitter whose
  polarization density spans multiple FDTD grid points. Best suited to scenarios involving a limited number of emitters,
  such as spontaneous emission and resonance energy transfer.

- The **socket-susceptibility** route (described on this page) provides
  **grid-level** coupling: the molecular response is incorporated directly into
  Meep's material update, assigning one driver per active grid point with no
  ``mxl.Molecule`` objects involved. This approach targets **collective strong
  coupling of macroscopic molecular ensembles at extremely large scale**.

.. note::

  For a medium carrying ``MXLSocketSusceptibility``, Meep delegates the
  polarization update at every active grid point :math:`\mathbf{r}_g` to a
  molecular driver. At each FDTD time step, the C client transmits the
  rescaled local electric field (converted to atomic units)

  .. math::

     \widetilde{\mathbf{E}}_{g}(t) = \gamma\, \mathbf{E}(\mathbf{r}_g, t)

  to socket molecule :math:`g`. The driver then propagates its molecular model
  for one FDTD time step and returns :math:`d\boldsymbol{\mu}_{g}/dt`, which
  Meep deposits as the polarization-current density of the corresponding grid
  cell (volume :math:`\Delta V`):

  .. math::

     \partial_t \mathbf{P}(\mathbf{r}_g, t) = \frac{\gamma}{\Delta V}\, \frac{d\boldsymbol{\mu}_{g}(t)}{dt}.

  All Meep :math:`\leftrightarrow` atomic-unit conversions are handled
  internally through ``time_units_fs``. The rescaling factor :math:`\gamma`
  (``rescaling_factor``) is applied *symmetrically* to the outgoing field and
  the returned response. 
  
  Thus, a driver simulating :math:`n_{\mathrm{sim}}`
  molecules faithfully represents the :math:`N_{\mathrm{phys}}` physical
  molecules residing in its grid cell when

  .. math::

     \gamma = \sqrt{N_{\mathrm{phys}} / n_{\mathrm{sim}}},

  which preserves the collective (bright-state) light–matter coupling in the
  linear-response limit.

Comparison with the ``MeepSimulation`` route
--------------------------------------------

.. list-table::
   :header-rows: 1

   * -
     - :doc:`meep` (molecule-level)
     - Socket susceptibility (grid-level)
   * - Meep entry point
     - ``mxl.MeepSimulation`` wrapper around ``meep.Simulation``
     - Plain ``mp.Simulation`` (``fdtdbath-meep`` build)
   * - Molecule placement
     - Each ``mxl.Molecule`` carries ``center``/``size``/``sigma`` and spans
       multiple grid points via a regularized kernel
     - Every active grid point of the socket medium constitutes one socket
       molecule; placement follows standard Meep geometry and material
       assignment
   * - Coupling level
     - Python step function inserted between Meep time steps; molecules enter
       as **current sources** :math:`\mathbf{J}_{\mathrm{mol}}`
     - C-level susceptibility within Meep's material update; molecules enter
       as **material polarization** :math:`\mathbf{P}`
   * - Driver connection
     - Socket (``SocketHub``/``AggregatedSocketHub``) or embedded drivers
     - Socket only (``SusceptibilitySocketHub`` or
       ``AggregatedSusceptibilitySocketHub``)
   * - Typical use cases
     - Small numbers of emitters: spontaneous emission, resonance energy
       transfer, superradiance
     - Macroscopic ensembles: collective (vibrational) strong coupling with
       realistic molecular matter filling a cavity or nanostructure

Requirements
------------

- The modified Meep build `fdtdbath-meep
  <https://github.com/TaoELi/fdtdbath-meep>`_ must be installed from source.
  Because the socket client resides inside Meep's C++ FDTD core, a stock
  ``pymeep`` package (e.g. from conda-forge) does **not** provide it. To
  verify a correct installation:

  .. code-block:: python

     import meep as mp
     assert hasattr(mp, "MXLSocketSusceptibility")

- No additional dependency is required on the **MaxwellLink** side. The hub
  automatically launches its server in a separate child process, a design
  necessitated by the fact that Meep's C time-stepping loop holds the Python
  GIL while awaiting socket responses, preventing the server from running
  within the same process.
- Only TCP connections are supported: the Meep C client connects by host and
  port, so UNIX domain sockets cannot be used between Meep and the hub.

Usage
-----

EM side: ``SusceptibilitySocketHub``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

:class:`~maxwelllink.sockets.susceptibility.SusceptibilitySocketHub` is the
recommended hub for most applications. A single TCP listener serves both the
Meep ranks and all molecular drivers directly.

.. code-block:: python

   import meep as mp
   import maxwelllink as mxl

   # Publish the endpoint for the drivers (two lines: host, then port).
   host, port = mxl.get_available_host_port(
       localhost=False, save_to_file="tcp_host_port_info.txt"
   )
   hub = mxl.SusceptibilitySocketHub(
       host=host,
       port=port,
       timeout=7200.0,                           # generous for HPC queueing
       latency=0.01,
       driver_count_file="num_socket_molecule",  # written once Meep connects
   )

   # Attach the socket susceptibility to an ordinary Meep medium.
   socket_susc = mp.MXLSocketSusceptibility(
       rescaling_factor=8.89,   # sqrt(N_phys / n_sim); see the note above
       time_units_fs=3.33564,   # 1 Meep time unit in fs (a = 1 um here)
       hub=hub,
       real_field_only=True,
   )
   molecular_medium = mp.Medium(
       epsilon=1.7**2,
       E_susceptibilities=[socket_susc],
   )

   # Every grid point inside this block becomes one socket molecule:
   # a 0.08 x 0.08 um^2 patch at resolution 125 -> 10 x 10 = 100 drivers.
   geometry = [
       mp.Block(
           material=molecular_medium,
           size=mp.Vector3(0.08, 0.08, 0),
           center=mp.Vector3(),
       ),
   ]

   sim = mp.Simulation(          # plain Meep simulation
       cell_size=mp.Vector3(2, 2, 0),
       geometry=geometry,
       sources=[],               # any native Meep sources
       boundary_layers=[mp.PML(0.4)],
       resolution=125,
   )

   try:
       sim.run(until=150)
   finally:
       hub.stop()

All native Meep capabilities remain fully available.

Driver side
^^^^^^^^^^^

Any socket-mode **MaxwellLink** driver can serve a socket molecule, including
the Python ``mxl_driver`` models (:doc:`../drivers/index`), the LAMMPS
``fix mxl`` client (:doc:`../drivers/lammps`), and others. Drivers connect to
the hub's TCP endpoint in the same manner as in the ``SocketHub`` workflows
(:doc:`../usage`); the only additional consideration is *how many* drivers to
launch. The hub writes the total number of socket molecules requested by Meep
to ``driver_count_file`` (by default, ``num_socket_molecule``), which a SLURM
job array can read to size itself accordingly:

.. code-block:: bash

   #!/bin/bash
   #SBATCH --array=0-255          # >= expected number of socket molecules

   # Wait until the EM job publishes its endpoint and driver count.
   until [[ -s tcp_host_port_info.txt && -s num_socket_molecule ]]; do sleep 1; done
   HOST=$(sed -n '1p' tcp_host_port_info.txt)
   PORT=$(sed -n '2p' tcp_host_port_info.txt)
   N=$(tr -d '[:space:]' < num_socket_molecule)

   # Array tasks beyond the required driver count simply exit.
   (( SLURM_ARRAY_TASK_ID >= N )) && exit 0

   mxl_driver --model sho --address ${HOST} --port ${PORT} \
     --param "omega=0.0906,mu0=187.0819866,orientation=2"

For molecular-dynamics ensembles, replace the final command with one LAMMPS
client per array task (each in its own working directory), where the LAMMPS
input couples through ``fix mxl``::

   fix 1 all mxl HOST PORT reset_dipole
   fix 2 all nve

As with any multi-job workflow, submit the EM job first and the driver array as
a dependent job (see :doc:`../usage`).

Validating against a classical Lorentzian medium
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

:meth:`~maxwelllink.sockets.susceptibility.SusceptibilitySocketHub.lorentzian_conversion`
provides a convenient route for validation by mapping a classical
``mp.LorentzianSusceptibility(frequency=..., sigma=...)`` onto equivalent SHO
socket drivers. The method returns both the ``rescaling_factor`` to pass to
``MXLSocketSusceptibility`` and a ready-to-run ``mxl_driver --model sho``
command. This allows the socket medium to be validated against the known
classical dispersion before committing to computationally expensive molecular
drivers:

.. code-block:: python

   conv = hub.lorentzian_conversion(
       frequency=0.355,       # Lorentzian resonance (Meep units)
       sigma=0.01,            # Lorentzian oscillator strength
       resolution=125,
       dimensions=2,
       time_units_fs=3.33564,
       orientation=2,
   )
   socket_susc = mp.MXLSocketSusceptibility(
       rescaling_factor=conv["rescaling_factor"],
       time_units_fs=3.33564,
       hub=hub,
   )
   print(conv["driver_command"])   # launch this once per socket molecule


Parameters
----------

``mp.MXLSocketSusceptibility`` (Meep side):

.. list-table::
   :header-rows: 1

   * - Name
     - Description
   * - ``rescaling_factor``
     - Symmetric bright-state coupling scale :math:`\lambda`, applied to both
       the electric field transmitted to the driver and the returned
       :math:`d\boldsymbol{\mu}/dt`. To represent :math:`N` physical molecules
       per grid cell using a single driver simulating :math:`n` molecules, set
       this to :math:`\sqrt{N/n}`. Default: ``1.0``.
   * - ``time_units_fs``
     - Number of femtoseconds corresponding to one Meep time unit (default:
       ``0.1``). This value must be consistent with any unit conversions
       applied on the driver side.
   * - ``hub``
     - Required keyword argument: a
       :class:`~maxwelllink.sockets.susceptibility.SusceptibilitySocketHub` or
       :class:`~maxwelllink.sockets.aggregated_susceptibility.AggregatedSusceptibilitySocketHub`
       instance that supplies the ``host``, ``port``, and ``timeout``
       parameters for Meep's internal socket connection.
   * - ``label``
     - Optional Meep-side label used in molecule-map output and material
       equivalence checks; never transmitted to **MaxwellLink**. Default:
       ``""``.
   * - ``real_field_only``
     - When ``True`` (default), only the real part of the electric field
       drives the socket molecules. When ``False``, complex-field Meep runs
       (e.g. cylindrical simulations with ``m != 0``, runs with a
       ``k_point``, or those using ``force_complex_fields=True``) create
       **independent socket molecules for the real and imaginary field
       components**, thereby doubling the driver count.

:class:`~maxwelllink.sockets.susceptibility.SusceptibilitySocketHub` (MaxwellLink side):

.. list-table::
   :header-rows: 1

   * - Name
     - Description
   * - ``host`` / ``port``
     - TCP bind endpoint (default port: ``31415``; setting ``port=0`` requests
       an OS-assigned ephemeral port). Use
       :func:`~maxwelllink.sockets.sockets.get_available_host_port` with
       ``localhost=False`` for multi-node deployments.
   * - ``timeout``
     - Socket timeout in seconds applied to bound clients (default:
       ``60000.0``). This should be set longer than the maximum expected wait
       time for all drivers to come online, accounting for factors such as
       SLURM queueing delays.
   * - ``latency``
     - Polling interval in seconds for the accept/bind loops (default:
       ``0.05``).
   * - ``driver_count_file``
     - Path to a file that will receive the total socket-molecule count
       requested by Meep, written once as a single integer (default:
       ``"num_socket_molecule"``). SLURM driver arrays typically read this
       file to determine the appropriate array size. Set to ``None`` to
       disable.


Returned data
-------------

- Standard Meep data channels remain unaffected.
- Molecular observables are stored by each individual driver process (e.g.
  LAMMPS trajectory files and log output in each driver's working directory).
  Because no ``mxl.Molecule`` objects exist in the Meep process, there is no
  ``additional_data_history`` available on the EM side.

Notes
-----

- The hub must be constructed **before** ``mp.Simulation`` begins execution
  and should always be shut down afterwards via ``hub.stop()`` (use
  ``try/finally`` as shown above). The hub's child server process does not
  terminate on its own.
- The total number of drivers equals the number of grid points within the
  socket medium (doubled in complex-field runs when ``real_field_only=False``).
  This count can be estimated as the medium volume multiplied by
  ``resolution**dimensions`` when initially sizing the driver job array; the
  exact value should then be read from ``driver_count_file`` at runtime.
- Under MPI parallelism, each Meep rank opens its own connection to the hub
  and requests drivers only for the grid points it owns; the hub aggregates
  the totals across ranks. MPI-launcher environment variables are stripped
  automatically when spawning the hub's child process, so
  ``mpirun``/``srun``-launched Meep operates without additional configuration.
- If a driver disconnects, the corresponding FDTD step pauses until the driver
  reconnects, following the same behavior as in the ``SocketHub`` workflows.
  Checkpointed drivers (``checkpoint=true`` / ``restart=true``) can resume
  transparently after interruptions.
- Physically, each socket molecule represents *the molecular ensemble residing
  within a single FDTD grid cell*. For example, coupling bulk water
  (33.4 molecules/nm\ :sup:`3`) at an 8 nm grid spacing yields approximately
  17,000 physical water molecules per cell. Simulating 216 water molecules per
  LAMMPS driver then requires ``rescaling_factor = sqrt(17000/216) ≈ 8.9``.

.. seealso::

   - :doc:`meep` — molecule-level Meep coupling
     (``mxl.Molecule`` + ``mxl.MeepSimulation``).
   - :doc:`../drivers/index` and :doc:`../drivers/lammps` for
     molecular drivers capable of serving socket molecules.
   - :mod:`maxwelllink.sockets.susceptibility` and
