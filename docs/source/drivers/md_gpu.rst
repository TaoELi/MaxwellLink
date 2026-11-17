:orphan:

GPU MD driver
=============

The GPU MD driver advances many independent classical molecular dynamics (MD)
cells together on one GPU. It is provided by
:class:`~maxwelllink.mxl_drivers.python.batch.md_gpu.MDGPUBatchModel` and uses
the built-in MD engine in ``maxwelllink.matter.md``. Each cell contains an
atomistic molecular system and exchanges its electric field and dipole current
with the EM solver through :class:`~maxwelllink.sockets.aggregated.AggregatedSocketHub`.


**All MD cells on one GPU can share a single bridge connection.** The bridge
receives the fields for the entire MD batch and returns their
responses in one batched manner per EM step. This will significantly reduce 
the communication cost when the number of MD cells is large (at the million scale).

.. note::

  Within each MD cell, the driver adds the electric-field force to the molecular
  force field and returns the dipole current:

  .. math::

     \mathbf{F}^{\mathrm{EM}}_{is} = Q_i\widetilde{\mathbf{E}}_s,
     \qquad
     \frac{\mathrm{d}\boldsymbol{\mu}_s}{\mathrm{d}t}
       = \sum_i Q_i\mathbf{v}_{is}.

  Here :math:`s` labels an MD cell, and :math:`i` labels its charge sites.

Requirements
------------

- An NVIDIA GPU with a working CUDA environment and sufficient device memory
  for the batch.
- ``cupy`` for GPU arrays and compiled CUDA kernels. For a CUDA 12 installation, install
  the GPU extra with ``pip install "maxwelllink[gpu-cuda12]"``.
- The built-in force fields are ``qtip4pf`` (flexible water), ``co2jcp2021``
  (flexible CO2), ``cs2compass`` (COMPASS CS2), and ``ch4compass`` (COMPASS CH4).
  More empricial force fields will be added in the future.

Usage
-----

EM-side setup
^^^^^^^^^^^^^

Create one ``Molecule`` object per MD cell and assign every cell to the same
bridge. The following ``em_run.py`` example couples 144 cells to a one-dimensional
Fabry-Perot cavity. Each cell will contain 36 CO2 molecules when the bridge is
started with the parameters below.

.. code-block:: python

   import maxwelllink as mxl
   from maxwelllink.em_solvers.multimode_cavity import (
       FabryPerotCavity, MultiModeSimulation,
   )
   from maxwelllink.sockets import AggregatedSocketHub, get_available_host_port
   from maxwelllink.units import unit

   cavity = FabryPerotCavity(
       frequency_au=655.0 * unit("cm_inv"),
       coupling_strength=1e-6,
       coupling_axis="xy",
       n_grid_x=144,
       y_grid_1d=[0.0],
       delta_omega_x_au=5.0 * unit("cm_inv"),
       delta_omega_y_au=0.0,
       n_mode_x=144,
       n_mode_y=1,
   )

   host, port = get_available_host_port(localhost=True)
   hub = AggregatedSocketHub(host=host, port=port, timeout=6000.0, latency=1e-3)

   # One socket-side Molecule represents one complete MD cell
   molecules = [
       mxl.Molecule(hub=hub, rescaling_factor=1.0, store_additional_data=False)
       for _ in range(cavity.n_grid)
   ]

   # Put every cell in one group served by one GPU bridge
   hub.init_remote_bridges(
       molecules,
       molecules_per_bridge=len(molecules),
       unix_prefix="md_gpu_",
       save_file="aggregation.json",
   )

   sim = MultiModeSimulation(
       hub=hub,
       molecules=molecules,
       dt_au=0.5 * unit("fs"),
       include_dse=True,
       cavity_geometry=cavity,
   )
   try:
       sim.run(steps=2000, record_history=False)
   finally:
       hub.stop()

GPU bridge
^^^^^^^^^^

Start ``python em_run.py`` first. Once it has written ``aggregation.json``, run
the following command in a second terminal in the same working directory:

.. code-block:: bash

   mxl_bridge --info aggregation.json --idx 0 \
     --backend gpu --model md \
     --param "ff=co2jcp2021, thermostat=nvt, friction_fs=5000.0, \
              temperature_K=300.0, pre_nvt=True, pre_nvt_duration_ps=10.0, \
              reset_dipole=True, seed=114514"

``--backend gpu --model md`` selects the GPU batch MD driver for all cells. Different from the other drivers, here no additional ``mxl_driver`` processes are needed.

The EM solver supplies the common MD time step through ``dt_au``. In this
example it is 0.5 fs, and the EM calculation runs on the CPU while the molecular
dynamics run on the GPU. ``store_additional_data=False`` avoids retaining a
separate diagnostic history for every ``Molecule`` to reduce the CPU side memory growth.

Parameters
----------

The following parameters are passed through ``mxl_bridge --param`` to the
shared MD engine. They apply to every cell in the batch.

.. list-table::
   :header-rows: 1

   * - Name
     - Description
   * - ``ff``
     - Built-in force-field name: ``qtip4pf``, ``co2jcp2021``, ``cs2compass``, or
       ``ch4compass``. Default: ``qtip4pf``.
   * - ``n_molecules``
     - Number of physical molecules per cell before replication. Default:
       ``None``, which loads the force field's bundled periodic box: 32 waters,
       36 CO2, 36 CS2, or 100 CH4 molecules.
   * - ``xyz``
     - Optional starting geometry in XYZ format, with coordinates in Angstrom.
       Atom order must be O-H-H for water, C-O-O for CO2, C-S-S for CS2, or
       C-H-H-H-H for CH4, repeated for each molecule. Default: ``None``.
   * - ``batch_xyz``
     - Optional multi-frame XYZ file providing a different starting geometry
       for each cell. A cell uses the frame indexed by its hub-assigned
       molecule ID. All frames must have the same atoms and ordering, and the
       file must contain more frames than the largest ID. Default: ``None``.
   * - ``box``
     - Periodic box length in Bohr, or three lengths when supplied through the
       Python API. Bundled bulk geometries supply their own box. For custom
       coordinates, ``None`` means a finite cluster. Default: ``None``.
   * - ``replicate``
     - Replicate each periodic cell at fixed density. An integer specifies the
       total number of images; for example, ``8`` gives a 2 x 2 x 2 replication.
       A three-component replication can be supplied through the Python API.
       Default: ``None``.
   * - ``rcut``, ``ewald_wrcut``
     - Optional non-bonded pair cutoff and Ewald real-space cutoff in Bohr.
       Defaults are set by the selected force field.
   * - ``thermostat``
     - ``nve`` for velocity Verlet, or ``nvt`` for Langevin dynamics.
       Default: ``nve``.
   * - ``temperature_K``
     - Temperature in Kelvin for initial Maxwell-Boltzmann velocities and
       the Langevin thermostat. Default: ``300.0``.
   * - ``friction_fs``
     - Production Langevin relaxation time in femtoseconds, used when
       ``thermostat=nvt``. Default: ``100.0``.
   * - ``init_velocities``
     - When ``True`` sample initial velocities at ``temperature_K``. When
       ``False`` start at rest. Default: ``True``.
   * - ``pre_nvt``
     - When ``True`` equilibrate each cell in zero field before coupled
       dynamics. Default: ``False``.
   * - ``pre_nvt_duration_ps``
     - Duration of the initial NVT equilibration in picoseconds.
       Default: ``20.0``.
   * - ``pre_nvt_friction_fs``
     - Langevin relaxation time during equilibration in femtoseconds,
       independent of the production ``friction_fs``. Default: ``100.0``.
   * - ``pre_nvt_timestep_fs``
     - Optional equilibration time step in femtoseconds. Default: ``None``,
       which uses the coupling time step. The coupling time step is restored
       after equilibration.
   * - ``reset_dipole``
     - Report dipoles relative to each cell's initial dipole, captured after
       equilibration. Default: ``True``.
   * - ``seed``
     - Base random seed. Each cell uses ``seed + molecule_id`` to generate
       independent initial velocities and thermostat noise. Default: ``0``.
   * - ``property_filename``
     - Optional file for per-cell MD properties. HDF5 output is streamed when
       ``h5py`` is installed; a ``.npz`` suffix or missing ``h5py`` selects NPZ
       output, buffered until the driver closes. Default: ``None``.
   * - ``property_list``
     - Properties to record. ``base`` records ``temperature_K``, ``energy_au``,
       ``stretch_au``, and ``bend_au``. ``all`` adds kinetic, potential,
       translational, rotational, and vibrational energy diagnostics. A
       semicolon-separated list selects individual properties.
       Default: ``base``.
   * - ``traj_filename``
     - Optional extended-XYZ trajectory with one frame per cell at each
       recording step. Position coordinates are in Angstrom. Default: ``None``.
   * - ``traj_content``
     - Trajectory content: ``positions``, ``velocities``,
       ``angular_velocity_body``, or ``all``. Velocities are in atomic units;
       angular output includes principal moments of inertia.
       Default: ``positions``.
   * - ``record_every_steps``
     - Recording interval in production MD steps for property and trajectory
       files. Default: ``10``.
   * - ``record_max_steps``
     - Maximum number of recorded frames, rather than MD integration steps.
       Default: ``None`` for no limit.
   * - ``checkpoint``
     - When ``True`` save the whole batch to
       ``md_batch_checkpoint_id_<n>.npz`` on driver shutdown, where ``<n>`` is
       the first molecule ID in the group. Default: ``False``.
   * - ``restart``
     - When ``True`` and ``checkpoint=True``, restore the batch checkpoint if
       present and skip pre-equilibration. Default: ``False``.

Returned data
-------------

The driver returns a dipole current :math:`\mathrm{d}\boldsymbol{\mu}/\mathrm{d}t`
for each cell in atomic units. Its additional-data fields are:

- ``time_au`` – Simulation time in atomic units.
- ``energy_au`` – Total molecular kinetic and potential energy in atomic units.
- ``potential_au`` – Molecular force-field potential energy in atomic units.
- ``mux_au``, ``muy_au``, ``muz_au`` – Cell dipole components in atomic units
  half a time step after the force evaluation time.
- ``mux_m_au``, ``muy_m_au``, ``muz_m_au`` – Cell dipole components in atomic
  units at the force evaluation time.

Set ``store_additional_data=True`` on the EM-side ``Molecule`` objects when
their additional-data histories are needed. For MD properties such as
temperature or trajectory output, use ``property_filename`` or
``traj_filename`` on the bridge. These files contain the entire batch and have
``_id_<n>`` inserted before the extension, using the group's first molecule ID.
For example, ``property_filename=md_properties.h5`` produces
``md_properties_id_<n>.h5`` when ``h5py`` is available.

Notes
-----

- Enable ``pre_nvt=True`` when starting from the bundled bulk geometries for
  production simulations. Allow enough hub timeout for CUDA compilation and
  equilibration before the first coupled step.
- The GPU batch driver is most useful for many cells. Its compiled integrator
  assigns one CUDA block to each cell, advancing the batch together. The shared
  engine is also available as ``maxwelllink.matter.MD`` for standalone MD.
- For multiple GPUs, partition the cells into smaller bridge groups and launch
  one bridge per GPU with the corresponding ``--idx`` and
  ``CUDA_VISIBLE_DEVICES`` setting. Communication then scales with the number
  of bridges. ``verbose=True`` is unsupported by the MD batch driver.
- See :doc:`../em_solvers/multimode_cavity` for the cavity solver and
  ``tutorials/12_multimode_gpu_md_transport_condensation.ipynb`` for examples of
  driven polariton transport and condensation with this driver.
