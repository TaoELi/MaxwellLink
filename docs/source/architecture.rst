Architecture
============

Architecture overview
---------------------

**MaxwellLink** separates electromagnetic propagation from molecular dynamics by
placing them in different processes (or even different nodes) and letting them
communicate through a socket protocol, inspired by the `i-PI <https://docs.ipi-code.org/>`_ project:

1. The EM solver (such as `Meep <https://meep.readthedocs.io/en/latest/>`_ FDTD or single-mode cavity) advances Maxwell's equations.
2. After each time step, **MaxwellLink** measures the regularized electric field at
   every coupled molecule and converts it to atomic units.
3. Those field vectors are sent to the driver processes through a ``SocketHub``
   barrier call.
4. Each driver propagates its molecular model for one EM step (possibly using
   sub-steps) and returns the time-derivative of the dipole moment and optional
   metadata.
5. The returned amplitudes are converted back to EM units and injected to EM solvers 
   before the next time step begins.

.. image:: ../../media/workflow.png
   :alt: MaxwellLink workflow diagram
   :align: center
   :scale: 25

SocketHub
---------

``SocketHub`` (:mod:`maxwelllink.sockets.sockets`) manages the inter-code communication:

- Supports both TCP sockets (``host``/``port``) and UNIX domain sockets
  (``unixsocket``).
- Generates molecule IDs on demand.
- Implements the ``NEEDINIT -> INIT -> READY/HAVEDATA`` handshake for each client.
- Detects dropped connections during sends or receives and pauses the EM solver
  until all expected drivers reconnect.
- Exposes helpers such as :func:`maxwelllink.sockets.sockets.get_available_host_port` for easy
  use.

Abstract Molecule
-----------------

``Molecule`` (:mod:`maxwelllink.molecule.molecule`) provides a unified interface for constructing molecular 
drivers for both socket communications and non-socket (single-process) runs. Pass
``hub=SocketHub(...)`` to connect to an external driver, or ``driver="..."`` (and
``driver_kwargs``) to instantiate the model locally. 

Every molecule records time-resolved data in ``Molecule.additional_data_history`` (a Python list of dicts ordered in the simulation time).

For faciliating obtaining molecular time-resolved data, ``Molecule.extra`` is a dict that stores the post-processed data in the form of Numpy arrays. 
The content of ``extra`` is determined by the driver implementation; for example, the TLS driver stores the population trajectory in ``extra["Pe"]`` and the time points in ``extra["time_au"]``.  

In ``Molecule``, each molecule only stores the information necessary for
EM simulations, such as the center / size of the molecule in the EM grid and the
Gaussian width (``sigma``) for molecular polarization density distribution. The detailed 
molecular parameters and dynamics are handled by each driver implementation.

EM solvers
---------------------

Currently, three EM solvers are available in **MaxwellLink**: 

- The **Meep FDTD** engine: ``MeepSimulation`` (:mod:`maxwelllink.em_solvers.meep`)

- The **single-mode cavity** solver:  ``SingleModeSimulation`` (:mod:`maxwelllink.em_solvers.single_mode_cavity`).

- The **laser-driven dynamics** solver:  ``LaserDrivenSimulation`` (:mod:`maxwelllink.em_solvers.laser_driven`).

``MeepSimulation`` derives from `meep.Simulation <https://meep.readthedocs.io/en/master/Python_User_Interface/#simulation>`_ and automatically
inserts the appropriate step function for updating molecules when ``MeepSimulation.run()`` is called.
When using ``MeepSimulation``, three additional parameters should be specified compared to a regular
`meep.Simulation <https://meep.readthedocs.io/en/master/Python_User_Interface/#simulation>`_:

- ``molecules``: a list of :class:`~maxwelllink.molecule.molecule.Molecule` objects to couple to the EM solver.
- ``time_units_fs``: the mapping between Meep time units and real time in femtoseconds. Meep uses
  dimensionless units internally, so specifying this parameter is necessary to convert between Meep units and other units systems.
- ``hub``: an optional :class:`~maxwelllink.sockets.sockets.SocketHub` object for socket-based drivers.

.. note::

   With a ``SocketHub`` a step function :func:`maxwelllink.em_solvers.meep.update_molecules` is inserted in Meep FDTD simulation; 
   without a hub the step function falls back to :func:`maxwelllink.em_solvers.meep.update_molecules_no_socket`.

``SingleModeSimulation``, defined in :class:`~maxwelllink.em_solvers.single_mode_cavity.SingleModeSimulation`,
approximates the field as a single damped harmonic oscillator evolving in atomic
units. It supports the same socket and non-socket molecule interfaces, making it
useful for rapid prototyping or unit tests without launching Meep.

``LaserDrivenSimulation``, defined in :class:`~maxwelllink.em_solvers.laser_driven.LaserDrivenSimulation`,
applies user-defined classical electric fields to molecules without back-action from the molecular system.


Please read :doc:`em_solvers/index` section for detailed definitions of different EM solvers.

Molecular drivers
---------------------

While ``Molecule`` defines molecular locations and size in EM grid, a set of molecular 
drivers implement the actual dynamics. All Python-based drivers inherit from :func:`maxwelllink.mxl_drivers.python.models.dummy_model.DummyModel`
and use the unified API when communicating with the hub. The following Python drivers ship with **MaxwellLink**:

- **Two-level system (tls)**: a lightweight quantum model that propagates
  the von Neumann equation for a TLS.
- **QuTiP model Hamiltonians (qutip)**: an interface to user-defined Hamiltonians
  using the `QuTiP <https://qutip.org/>`_ package.
- **Psi4 RT-TDDFT (rttddft)**: real-time time-dependent density functional theory
  implemented using `Psi4 <https://psicode.org/>`_.
- **Psi4 RT-Ehrenfest dynamics (rtehrenfest)**: RT-TDDFT with nuclear Ehrenfest
  dynamics using `Psi4 <https://psicode.org/>`_.
- **ASE molecular mechanics (ase)**: first-principles Born-Oppenheimer molecular dynamics using
  the `Atomic Simulation Environment (ASE) <https://wiki.fysik.dtu.dk/ase/>`_.

An additional C++ `LAMMPS <https://www.lammps.org/>`_ driver implements ``fix mxl``, which communicates with the hub
using the same socket protocol. See :doc:`installation` for instructions on building
the LAMMPS binary with **MaxwellLink** support.

Please read :doc:`drivers/index` section for detailed definitions of different molecular drivers.

MPI Parallelism
--------------- 

EM solvers, such as `Meep <https://meep.readthedocs.io/en/latest/>`_ FDTD, can be launched under MPI. **MaxwellLink** is compatible with MPI, 
allowing for distributed simulations. Only the master
rank (rank 0) interacts with sockets; field integrals and returned molecule responses
are broadcast to the other ranks via ``mpi4py``.

Checkpointing
--------------------------

Driver classes that inherit from :class:`DummyModel`
support checkpointing. When ``checkpoint=true`` the driver writes state files
after each step; setting ``restart=true`` lets a reconnected driver resume from
disk. The hub blocks the EM solver inside ``wait_until_bound`` until all
expected molecules report back, so even long-lived RT-TDDFT simulations remain
consistent if a driver is restarted.
