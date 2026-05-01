Multimode Cavity
================

The :mod:`maxwelllink.em_solvers.multimode_cavity` module implements a Fabry-Pérot resonator described by an arbitrary
collection of damped harmonic oscillators (photon modes) coupled to a
spatially distributed molecular grid points, each representing a distinct molecular simulation cell.

See the following reference for the underlying theory:

.. admonition:: Reference
   
   TE Li. Mesoscale Molecular Simulations of Fabry–Pérot Vibrational Strong Coupling. `J. Chem. Theory Comput. 20, 7016–7031 (2024) <https://doi.org/10.1021/acs.jctc.4c00349>`_.



.. note::

  Under the dipole gauge, the total light-matter Hamiltonian is

  .. math::

     H = H_{\rm mol} + \frac{1}{2}\, \mathbf{p}_{k\lambda}^{2}
       + \frac{1}{2} \sum_{k\lambda} \omega_{k}^{2}
         \left( q_{k\lambda}
              + \frac{\varepsilon_{k\lambda}}{\omega_{k}^{2}}
                \sum_{i} \boldsymbol{\mu}_{i}\cdot \mathbf{f}_{k\lambda}(\mathbf{r}_{i})
         \right)^{2}.

  Each photon mode (a classical harmonic oscillator) obeys the equation

  .. math::

     \ddot{q}_{k\lambda} = -\omega_{k}^{2}\, q_{k\lambda}
       - \varepsilon_{k\lambda} \sum_{i} \boldsymbol{\mu}_{i}\cdot \mathbf{f}_{k\lambda}(\mathbf{r}_{i})
       - \gamma_{k\lambda}\, p_{k\lambda}
       + D_{k\lambda}(t),

  and the effective electric field returned to the molecular drivers is

  .. math::

     \mathbf{E}(t) = -\sum_{k\lambda} \varepsilon_{k\lambda}\, q_{k\lambda}(t)\, \mathbf{f}_{k\lambda}(\mathbf{r})
       - \delta_{\mathrm{DSE}}\, \sum_{k\lambda} \frac{\varepsilon_{k\lambda}^{2}}{\omega_{k}^{2}}\,
         \mathbf{f}_{k\lambda}(\mathbf{r}) \left(\sum_{i} \boldsymbol{\mu}_{i}(t)\cdot \mathbf{f}_{k\lambda}(\mathbf{r}_{i})\right).

  For parameters, 
  
  - :math:`\omega_{k}` denotes the angular frequency of the cavity mode :math:`(k,\lambda)`, 
  - :math:`\gamma_{k\lambda}` is ``damping_au``,
  - :math:`\varepsilon_{k\lambda}` represents the light-matter coupling strength ``coupling_strength``, 
  - :math:`D_{k\lambda}(t)` is the optional external drive,
  - :math:`\delta_{\mathrm{DSE}}` is the dipole self-energy term (1 if included, 0 otherwise), and 
  - :math:`\mathbf{f}_{k\lambda}(\mathbf{r})` is the photonic mode function evaluated at the molecular grid point :math:`\mathbf{r}`. 
  
  The mode frequencies follow a planar Fabry-Pérot dispersion
  :math:`\omega_{k} = \sqrt{\omega_{\rm c}^{2} + (l_{x}\Delta\omega_{x})^{2} + (l_{y}\Delta\omega_{y})^{2}}`
  controlled by ``frequency_au``, ``delta_omega_x_au``, ``delta_omega_y_au``,
  ``n_mode_x``, and ``n_mode_y``. 
  
  The dipole self-energy term (:math:`\delta_{\mathrm{DSE}} = 1`) is included only when ``include_dse=True``.

Requirements
------------

- No additional dependencies beyond the **MaxwellLink** Python stack are required.

Usage
-----

The multimode solver is configured in two pieces:

- :class:`~maxwelllink.em_solvers.multimode_cavity.FabryPerotCavity` describing the cavity geometry (such as photonic mode functions), and 
- :class:`~maxwelllink.em_solvers.multimode_cavity.MultiModeSimulation` describing the time integrator and thermostatting.

The support of additional cavity geometries is planned for future versions.

Socket mode
^^^^^^^^^^^

.. code-block:: python

   import maxwelllink as mxl

   hub = mxl.SocketHub(host="127.0.0.1", port=31415, timeout=10.0, latency=1e-5)

   # 4 x 4 grid of molecules attached to the cavity, all in socket mode
   molecules = [mxl.Molecule(hub=hub) for _ in range(16)]

   cavity = mxl.FabryPerotCavity(
       frequency_au=0.242,
       coupling_strength=1e-4,
       coupling_axis="y",
       n_grid_x=4,
       n_grid_y=4,
       delta_omega_x_au=0.05,
       delta_omega_y_au=0.05,
       n_mode_x=4,
       n_mode_y=4,
   )

   sim = mxl.MultiModeSimulation(
       dt_au=0.5,
       damping_au=0.0,
       molecules=molecules,
       cavity_geometry=cavity,
       hub=hub,
       include_dse=True,
       record_history=True,
   )

   sim.run(steps=4000)

Launch :command:`mxl_driver --model tls --port 31415 ...` (or another driver)
once for every molecule after running this script.

Non-socket mode
^^^^^^^^^^^^^^^

.. code-block:: python

   import maxwelllink as mxl

   # 4 x 4 grid of embedded TLS molecules
   molecules = [
       mxl.Molecule(
           driver="tls",
           driver_kwargs=dict(
               omega=0.242,
               mu12=187.0,
               orientation=2,
               pe_initial=1e-4 if i == 0 else 0.0,
           ),
       )
       for i in range(16)
   ]

   cavity = mxl.FabryPerotCavity(
       frequency_au=0.242,
       coupling_strength=1e-4,
       coupling_axis="y",
       n_grid_x=4,
       n_grid_y=4,
       delta_omega_x_au=0.05,
       delta_omega_y_au=0.05,
       n_mode_x=4,
       n_mode_y=4,
   )

   sim = mxl.MultiModeSimulation(
       dt_au=0.5,
       damping_au=0.0,
       molecules=molecules,
       cavity_geometry=cavity,
       include_dse=False,
       record_history=True,
   )

   sim.run(steps=4000)

Cavity geometry parameters
--------------------------

These arguments are passed to
:class:`~maxwelllink.em_solvers.multimode_cavity.FabryPerotCavity` and define
the photon mode dispersion together with the spatial grid of molecular sites.

.. list-table::
   :header-rows: 1

   * - Name
     - Description
   * - ``frequency_au``
     - Reference cavity angular frequency :math:`\omega_{\rm c}` (a.u.) corresponding to the :math:`k_{\parallel}=0` cavity mode. Required.
   * - ``coupling_strength``
     - Light-matter coupling strength :math:`\varepsilon` for the fundamental cavity mode. Per-mode couplings are rescaled as
       :math:`\varepsilon_{k\lambda} = \varepsilon\, \omega_{k\lambda}/\min_{k\lambda}\omega_{k\lambda}`.
       Default: ``1.0``.
   * - ``coupling_axis``
     - One or more dipole components to couple (case-insensitive union of
       ``"x"``, ``"y"``, ``"z"``, e.g. ``"xy"``). Default: ``"xy"``.
   * - ``x_grid_1d``
     - Explicit list of molecular grid coordinates along :math:`x`, in units of
       the cavity length :math:`L_x`. Ignored if ``n_grid_x`` is provided.
   * - ``y_grid_1d``
     - Explicit list of molecular grid coordinates along :math:`y`, in units of
       the cavity length :math:`L_y`. Ignored if ``n_grid_y`` is provided.
   * - ``n_grid_x``
     - Number of uniformly spaced molecular sites along :math:`x`. Overrides
       ``x_grid_1d`` when set.
   * - ``n_grid_y``
     - Number of uniformly spaced molecular sites along :math:`y`. Overrides
       ``y_grid_1d`` when set.
   * - ``n_repeat_x``
     - Number of times each :math:`x` grid coordinate is repeated (useful for
       stacking multiple molecules per spatial site). Default: ``1``.
   * - ``n_repeat_y``
     - Number of times each :math:`y` grid coordinate is repeated. Default: ``1``.
   * - ``delta_omega_x_au``
     - Cavity frequency spacing :math:`\Delta\omega_{x}` along :math:`x` (a.u.) used
       in the planar dispersion relation. Required.
   * - ``delta_omega_y_au``
     - Cavity frequency spacing :math:`\Delta\omega_{y}` along :math:`y` (a.u.). Required.
   * - ``n_mode_x``
     - Number of cavity modes along :math:`x`. Default: ``1``.
   * - ``n_mode_y``
     - Number of cavity modes along :math:`y`. Default: ``1``.
   * - ``abc_cutoff``
     - Absorbing boundary condition cutoff applied to the molecular grid (in
       fractional grid units). ``0.0`` disables the ABC; positive values
       smoothly damp grid points within ``abc_cutoff`` of either boundary to
       suppress unphysical reflections. Default: ``0.0``.

Simulation parameters
---------------------

These arguments are passed to
:class:`~maxwelllink.em_solvers.multimode_cavity.MultiModeSimulation`.

.. list-table::
   :header-rows: 1

   * - Name
     - Description
   * - ``dt_au``
     - Integration time step in atomic units. Must be positive.
   * - ``damping_au``
     - Linear damping coefficient :math:`\kappa` applied to all cavity-mode
       momenta (a.u.).
   * - ``molecules``
     - Iterable of :class:`~maxwelllink.molecule.molecule.Molecule` instances.
       Socket and non-socket molecules can be mixed in the same simulation.
       The number of molecules should match the spatial grid defined on the
       cavity.
   * - ``hub``
     - Optional :class:`~maxwelllink.sockets.sockets.SocketHub` shared by all
       socket-mode molecules. The simulation infers the hub from the first
       socket molecule when omitted.
   * - ``cavity_geometry``
     - :class:`~maxwelllink.em_solvers.multimode_cavity.FabryPerotCavity`
       instance describing the photon dispersion and the molecular grid. The
       simulation transparently exposes the cavity attributes (``n_mode``,
       ``n_grid``, ``omega_k``, ``ftilde_k``, ...) through attribute lookup.
   * - ``qc_initial``
     - Initial cavity-mode coordinates with shape ``(n_mode, 3)``. Defaults to
       zeros (or to a Maxwell-Boltzmann sample when ``T_initial_au`` is set).
   * - ``pc_initial``
     - Initial cavity-mode momenta with shape ``(n_mode, 3)``. Defaults to
       zeros (or to a Maxwell-Boltzmann sample when ``T_initial_au`` is set).
   * - ``T_initial_au``
     - Initial temperature (a.u.) used to draw a Maxwell-Boltzmann distribution
       for ``qc_initial`` and/or ``pc_initial`` when they are not supplied.
   * - ``thermostat_seed``
     - Random seed for the Maxwell-Boltzmann initialization and the Langevin
       thermostat. Use to obtain reproducible trajectories.
   * - ``mu_initial``
     - Initial total molecular dipole vector with shape ``(n_grid, 3)`` prior
       to axis masking (a.u.). Default: zeros.
   * - ``dmudt_initial``
     - Initial time derivative of the total molecular dipole vector with shape
       ``(n_grid, 3)`` (a.u.). Default: zeros.
   * - ``NVT_T_au``
     - Target temperature (a.u.) for the Langevin thermostat applied to the
       cavity modes. ``None`` corresponds to NVE evolution.
   * - ``langevin_tau_au``
     - Characteristic relaxation time (a.u.) for the Langevin thermostat.
       Required when ``NVT_T_au`` is provided.
   * - ``include_dse``
     - When ``True`` add the dipole self-energy correction to the field
       returned to the molecules and account for it in the cavity energy.
       Default: ``True``.
   * - ``molecule_half_step``
     - When ``True`` extrapolate molecular responses from half-step data (use
       ``False`` for full-step drivers). Default: ``False``.
   * - ``shift_dipole_baseline``
     - When ``True`` subtract the initial dipole so the simulation starts from
       a zero baseline (helps with large permanent dipoles). Default: ``False``.
   * - ``gauge``
     - Gauge choice for the light-matter coupling. Currently only ``"dipole"``
       is supported.
   * - ``excited_mode_list``
     - List of cavity-mode indices that receive the photon-side pulse defined
       by ``photon_pulse_drive``. Default: ``[]`` (no photon pulse applied).
   * - ``photon_pulse_drive``
     - Constant float or callable ``photon_pulse_drive(time_au)`` returning the
       drive applied to the cavity modes listed in ``excited_mode_list``.
       Defaults to ``0.0``.
   * - ``photon_pulse_axis``
     - One or more axes (``"x"``, ``"y"``, ``"z"``) along which the photon
       pulse acts. Default: ``"y"``.
   * - ``excited_grid_list``
     - List of molecular grid indices that receive the molecule-side pulse
       defined by ``molecule_pulse_drive``. Default: ``[]`` (no molecule
       pulse applied).
   * - ``molecule_pulse_drive``
     - Constant float or callable ``molecule_pulse_drive(time_au)`` returning
       the strength of the pulse applied to ``excited_grid_list``. Defaults to
       ``0.0``.
   * - ``molecule_pulse_axis``
     - One or more axes (``"x"``, ``"y"``, ``"z"``) along which the molecule
       pulse acts. Default: ``"y"``.

Returned data
-------------

Calling `simu`=:class:`~maxwelllink.em_solvers.multimode_cavity.MultiModeSimulation`
with ``record_history=True`` populates the following attributes:

- :attr:`simu.time_history` – time stamps in atomic units.
- :attr:`simu.qc_history` – cavity-mode coordinates with shape ``(n_record, n_mode, 3)``.
- :attr:`simu.pc_history` – cavity-mode momenta with shape ``(n_record, n_mode, 3)``.
- :attr:`simu.photon_drive_history` – photon-side pulse values applied to ``excited_mode_list``.
- :attr:`simu.molecule_pulse_history` – molecule-side pulse values applied to ``excited_grid_list``.
- :attr:`simu.energy_history` – total energy of the cavity and coupled molecules.
- :attr:`simu.effective_efield_history` – effective electric field at each molecular grid point with shape ``(n_record, n_grid, 3)``.
- :attr:`simu.molecule_response_history` – :math:`\partial_t\boldsymbol{\mu}` summed along ``coupling_axis`` for every grid point.
- :attr:`simu.molecule_dipole_history` – total molecular dipole at every grid point with shape ``(n_record, n_grid, 3)``.

Each :class:`~maxwelllink.molecule.molecule.Molecule` keeps
:attr:`~maxwelllink.molecule.molecule.Molecule.additional_data_history`, which
records driver data (e.g., TLS populations, energies, timestamps).

Notes
-----

- Provide either ``steps`` or ``until`` to :meth:`MultiModeSimulation.run`, not both.
- Socket-mode molecules must all bind to the same :class:`~maxwelllink.sockets.sockets.SocketHub`;
  the simulation waits until every driver acknowledges initialization.
- The number of molecules supplied to ``molecules`` should equal ``n_grid`` (= ``n_mode_x`` * ``n_mode_y``) defined on
  the :class:`~maxwelllink.em_solvers.multimode_cavity.FabryPerotCavity`. Each molecule
  is bound to a distinct grid point through its ``molecule_id``.
- Using ``abc_cutoff > 0`` is recommended for large planar grids to suppress
  spurious reflections of the photon field at the boundaries; the cutoff is
  expressed as a fraction of the cavity length.
- ``T_initial_au`` together with ``thermostat_seed`` provides a thermalized
  starting condition for the photon bath; combine with ``NVT_T_au`` and
  ``langevin_tau_au`` to maintain that temperature throughout the run.
- Setting ``record_history=False`` (or tuning ``record_every_steps`` when
  recording to disk) avoids large memory allocations for throughput-critical runs.
