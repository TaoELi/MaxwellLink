LAMMPS driver
=============

**MaxwellLink** ships a C++ ``fix mxl`` for `LAMMPS <https://www.lammps.org/>`_ so classical molecular dynamics
jobs can exchange electric fields and dipole currents with the EM solver. The
sources live in :mod:`maxwelllink.mxl_drivers.lammps` alongside a convenience
installer.

.. note::

  During the run the ``fix mxl`` connects to the ``SocketHub``, receives the electric field,
  applies

  .. math::

    \mathbf{F}_i = Q_i \widetilde{\mathbf{E}}, 

  and returns

  .. math::

    \mathrm{d}\boldsymbol{\mu}/\mathrm{d}t = \sum_i Q_i \mathbf{v}_i, 

  where :math:`Q_i` and :math:`\mathbf{v}_i` are the partial charge and velocity of atom :math:`i`.

Requirements
------------

- POSIX-like environment (the fix uses BSD sockets).
- A `LAMMPS <https://www.lammps.org/>`_ build with atom IDs enabled and non-LJ units (such as ``units real`` or
  ``metal``).

Usage
-----

Socket preparation
^^^^^^^^^^^^^^^^^^

On the **MaxwellLink** side create a socket hub, for example in `Meep <https://meep.readthedocs.io/en/latest/>`_:

.. code-block:: python

   hub = mxl.SocketHub(host="0.0.0.0", port=31415, timeout=60.0)

LAMMPS build helper
^^^^^^^^^^^^^^^^^^^

Use the bundled script to build a `LAMMPS <https://www.lammps.org/>`_ executable that contains the fix:

.. code-block:: bash

   mxl_install_lammps

The script clones `LAMMPS <https://www.lammps.org/>`_, copies ``fix_maxwelllink.cpp``/``.h`` into ``src/MISC``,
and compiles an ``lmp_mxl`` binary placed on ``PATH``. Advanced users may copy
the fix into an existing source tree and rebuild manually.

LAMMPS input
^^^^^^^^^^^^

Add the fix to the group of atoms to be coupled:

.. code-block:: none

   fix 1 all mxl localhost 31415


Parameters
----------

.. list-table::
   :header-rows: 1

   * - Name
     - Description
   * - ``host``
     - Hostname or IP address of the **MaxwellLink** process (third argument in the
       ``fix`` command). Required.
   * - ``port``
     - TCP port exposed by :class:`maxwelllink.SocketHub`. Must lie in ``(1024,
       65536]`` for TCP mode. Required.
   * - ``unix``
     - Optional flag (append ``unix`` to the ``fix`` command) to connect via a
       Unix-domain socket at ``/tmp/socketmxl_<host>``.

Returned data
-------------


- ``time_au`` – Simulation time in atomic units.
- ``mux_au``, ``muy_au``, ``muz_au`` – Molecular dipole components in atomic units at half a time step after the force evaluation time.
- ``mux_m_au``, ``muy_m_au``, ``muz_m_au`` – Molecular dipole components in atomic units at the force evaluation time.
- ``energy_au`` – Sum of kinetic and potential energy in atomic units.
- ``temp_K`` – Instantaneous temperature in Kelvin.
- ``pe_au`` – Potential energy contribution in atomic units (currently zero with pairwise-only setups).
- ``ke_au`` – Kinetic energy contribution in atomic units.

Notes
-----

- Ensure every atom has a charge (``Q``) defined; otherwise the external force is
  zero.
- The fix forces neighbor-list rebuilds every step, matching the requirement of
  exchanging fields at the FDTD cadence.
- See :mod:`maxwelllink.mxl_drivers.lammps.install` for scripting helpers that
  manage download and compilation.
