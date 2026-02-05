Contributing
============

This page highlights the key extension points in **MaxwellLink** and the patterns we
recommend when adding new molecular drivers or electromagnetics (EM) solvers. The
codebase follows a “thin core, pluggable backends” design: the socket protocol,
shared unit helpers, and :class:`~maxwelllink.molecule.molecule.Molecule` abstraction hide most of the
coupling logic so new components only need to implement domain-specific details.

Source Layout
-------------

- ``src/maxwelllink/mxl_drivers/``: Python molecular drivers (TLS,
  QuTiP, RTTDDFT, ASE, ...) and LAMMPS code for the C++ molecular driver.
- ``src/maxwelllink/em_solvers``: EM backends such as the Meep wrapper and the
  single-mode cavity solver. Each solver ships its own unit system and molecule
  wrapper.
- ``tests/``: Pytest suites that exercise both socket and embedded modes.
- ``docs/source/``: User and developer documentation. Please document any new public
  feature here.
- ``skills/``: Agent Skills for AI integration (experimental).


.. admonition:: Numerical considerations for implementing a molecular driver

  Before going to technical details, one should note that by default, **MaxwellLink** sends the regularized E-field vector at step :math:`n` to the molecular driver and
  expects the molecular dipole time derivative at step :math:`n+1/2` in return. This requirement is particularly important for
  **energy conservation** in the FDTD EM solvers, which use E-field and electric current densities in staggered time grids.

  This requirement is automatically satisfied if the molecular driver propagates electronic dynamics when building the Hamiltonian
  at mid-point time steps (such as RT-TDDFT or using model Hamiltonians):

  .. math::

     \mathbf{P}^{\mathrm{e}}(t+\Delta t/2) = e^{-i \mathbf{H}^{\mathrm{e}}(t) \Delta t / \hbar} \mathbf{P}^{\mathrm{e}}(t-\Delta t/2) e^{i \mathbf{H}^{\mathrm{e}}(t) \Delta t / \hbar} .

  Molecular information calculated using the new density matrix :math:`\mathbf{P}^{\mathrm{e}}(t+\Delta t/2)` (such as dipole moment and its time derivative) will then correspond to step :math:`n+1/2`.

  However, if the molecular drivers use a velocity-verlet algorithm to propagate nuclear motion (such as classical MD drivers), special care is needed to ensure the dipole time derivative is evaluated at step :math:`n+1/2`. 
  This is because in a standard velocity-verlet scheme, the electric field alters nuclear forces at step :math:`n`, 
  while nuclear velocities (and thus dipole time derivatives) and positions (and thus dipole vectors) are also updated to step :math:`n` at the end of one velocity-verlet cycle. In other words, both the sent E-field
  and the returned dipole time derivative (or dipole vector) correspond to the same step, violating the staggered time grid requirement in **MaxwellLink**.

  To resolve this issue, developers can (i) return the extrapolated dipole time derivative (and dipole vector) at step :math:`n+1/2` using the computed values at steps :math:`n` 
  and previously returned values at step :math:`n-1/2`.

  .. math::

     \frac{d\mu}{dt}\Big|_{t+(n+1/2)\Delta t} \approx 2 \frac{d\mu}{dt}\Big|_{t+n\Delta t} - \frac{d\mu}{dt}\Big|_{t+(n-1/2)\Delta t} .

  Of course, developers may also (ii) further propagate nuclear velocities to step :math:`n+1/2` internally to return the correct dipole time derivative, but this would cause more difficulties in implementation and can be more expensive, especially if
  the users wish to maintain compatibility with existing MD codes. For example, in LAMMPS, this would require an additional MPI for loop over all atoms, which can be costly for large systems.

  Users should be aware of these numerical considerations when developing new molecular drivers that involve nuclear motion.


Adding a Python Molecular Driver
--------------------------------

Python drivers encapsulate the quantum or classical model that responds to the EM
field. They are loaded either via the ``mxl_driver`` CLI (socket mode) or
instantiated directly through :class:`~maxwelllink.molecule.molecule.Molecule` (embedded mode).

Driver skeleton
~~~~~~~~~~~~~~~

1. Create ``src/maxwelllink/mxl_drivers/python/models/<name>_model.py``.
2. Subclass :class:`~maxwelllink.mxl_drivers.python.models.dummy_model.DummyModel`.

   - Override :meth:`initialize` if extra setup is required once the hub assigns a time step and molecule id.

   - Override :meth:`propagate` to advance the molecular state under the effective
     electric field expressed in **atomic units**.

   - Override :meth:`calc_amp_vector` to return ``dμ/dt`` in **atomic units**. The base
     class handles the stage/commit protocol used by the socket driver.

   - Override :meth:`append_additional_data` if you wish to stream diagnostics back
     to the EM solver (they appear in ``Molecule.additional_data_history`` and also in ``Molecule.extra``).

   - Implement ``_dump_to_checkpoint`` / ``_reset_from_checkpoint`` when
     checkpoint/restart is desirable.

3. Expose the driver by updating ``__drivers__`` in
   ``src/maxwelllink/mxl_drivers/python/models/__init__.py``. The key becomes the
   ``--model`` value accepted by ``mxl_driver``.

4. If the driver should be importable from ``maxwelllink`` (e.g., ``mxl.QuTiPModel``),
   add a lazy import branch in ``src/maxwelllink/__init__.py``.

5. Add or update docs under ``docs/source/drivers`` describing user-facing
   parameters.

Example template:

.. code-block:: python

   import numpy as np
   from .dummy_model import DummyModel

   class AwesomeModel(DummyModel):
       def __init__(self, omega, damping, verbose=False, **kwargs):
           super().__init__(verbose=verbose, **kwargs)
           self.omega = float(omega)
           self.damping = float(damping)
           self._current_amp = np.zeros(3)

       def initialize(self, dt_new, molecule_id):
           super().initialize(dt_new, molecule_id)
           # Pre-compute any propagators or allocate arrays here.

       def propagate(self, effective_efield_vec):
           # Update internal state using self.dt (atomic units) and the field.
           self._current_amp = self._solve_heisenberg(effective_efield_vec)

       def calc_amp_vector(self):
           return self._current_amp

Ensure every new parameter is documented and exposed through ``__init__`` so the
driver can be constructed from ``--param`` strings (see
:func:`maxwelllink.mxl_drivers.python.mxl_driver.read_args_kwargs`).

Testing tips
~~~~~~~~~~~~

- Add unit tests in ``tests/`` that run the driver in embedded mode (instantiate
  :class:`~maxwelllink.molecule.molecule.Molecule` with ``driver="<name>"``) and, if possible, through
  the socket communication using ``SocketHub``.

- Run ``pytest tests/<area>`` before opening a pull request. 



Connecting to External C++/Fortran Drivers
------------------------------------------

External MD or quantum codes written in C++/Fortran can communicate with MaxwellLink through
the socket protocol. The LAMMPS driver (``fix_maxwelllink.cpp``) serves as a
production-ready reference for implementation. Experienced developers
can modify the LAMMPS driver to connect production-level codes to MaxwellLink.



Implementing a New EM Solver
--------------------------------
  
EM solvers orchestrate the Maxwell-time stepping, query molecules for their source
amplitudes, and convert between native units and atomic units. Existing backends
(``meep.py`` and ``single_mode_cavity.py``) demonstrate both a grid-based FDTD EM solver and
a single-mode cavity toy model.

Core building blocks
~~~~~~~~~~~~~~~~~~~~

- :class:`~maxwelllink.em_solvers.dummy_em.DummyEMUnits` stores conversion routines.
  Subclass it to translate native electric fields, source amplitudes, and time steps
  into atomic units.

- :class:`~maxwelllink.em_solvers.dummy_em.MoleculeDummyWrapper` wraps
  :class:`~maxwelllink.molecule.molecule.Molecule` instances so the unique molecular setting for one
  EM solver can be specified.

- :class:`~maxwelllink.em_solvers.dummy_em.DummyEMSimulation` is the main simulation container exposed to users; 
  real solvers typically extend it with solver-specific operations in a :meth:`run` loop.

- :class:`~maxwelllink.sockets.sockets.SocketHub` handles the socket protocol for molecules
  operating in socket mode.

Solver skeleton
~~~~~~~~~~~~~~~

1. Create ``src/maxwelllink/em_solvers/<name>.py``.
2. Implement a units helper:

   .. code-block:: python

      from .dummy_em import DummyEMUnits

      class AwesomeUnits(DummyEMUnits):
          def __init__(self, length_unit_nm, time_unit_fs):
              super().__init__()
              self.length_unit_nm = length_unit_nm
              self.time_unit_fs = time_unit_fs

          def efield_em_to_au(self, field_vec3):
              return np.asarray(field_vec3) * self._field_scale

          def source_amp_au_to_em(self, amp_vec3):
              return np.asarray(amp_vec3) / self._field_scale

          def time_em_to_au(self, time_em):
              return time_em * self.time_unit_fs * FS_TO_AU

3. Author a molecule wrapper that derives from
   :class:`~maxwelllink.em_solvers.dummy_em.MoleculeDummyWrapper`. Stamp solver
   metadata onto the molecule (e.g., spatial kernels, native units) and decide how
   to build EM sources in native data structures. Reuse the molecule’s
   ``init_payload`` so socket drivers receive any solver-specific hints.

4. Implement ``AwesomeSimulation`` that wires everything together. Common steps:

   - Define the time step, and :class:`Molecule` wrappers.

   - Split molecules by mode (socket vs. non-socket) and call
     ``m.initialize_driver`` for embedded drivers.

   - During each time step:

     * Gather fields at molecular sites, convert them to atomic units with
       ``AwesomeUnits.efield_em_to_au``, and call ``propagate`` on each wrapper.
     * After the solver advances its state, request source amplitudes (socket mode
       via :class:`~maxwelllink.sockets.sockets.SocketHub`, non-socket via ``calc_amp_vector``) and inject them
       back into the EM update.
     * Push any additional per-molecule diagnostics into
       ``Molecule.additional_data_history``.

5. Export the solver by adding a lazy import branch in ``src/maxwelllink/__init__.py``
   so users can instantiate ``mxl.AwesomeSimulation``. Update
   ``docs/source/em_solvers/index.rst`` with a new page describing runtime knobs.


Testing and Documentation
-------------------------

- Extend ``tests/`` with regression coverage for new solvers/drivers. For socket
  clients, prefer lightweight smoke tests that execute within half a minute.

- Update Sphinx docs so users can discover the feature (driver guide, EM solver
  page, release notes if applicable).

- Run ``make doc html`` locally to ensure the documentation builds cleanly.
  Address any warnings related to the new content before submitting a pull request.
