Usage Guide
===========

This guide walks through **three** ways to couple :class:`~maxwelllink.molecule.molecule.Molecule` with
EM solvers using a single TLS molecule as the working example. Users can
check tutorials under :doc:`tutorials/index` for more detailed examples and
explanations.

.. note::

   **MaxwellLink** ships a legacy :class:`~maxwelllink.molecule.molecule_legacy.SocketMolecule` API
   used throughout ``tests/test_tls``. The patterns below focus on the unified
   :class:`~maxwelllink.molecule.molecule.Molecule` interface, which works for both socket and
   embedded (non-socket) drivers.

When using `Meep <https://meep.readthedocs.io/en/latest/>`_ FDTD as the EM solver, below we will introduce three ways
to run self-consistent light-matter simulations with a TLS molecule.

Single process (no sockets)
---------------------------

The simplest setup instantiates the TLS driver inside the `Meep <https://meep.readthedocs.io/en/latest/>`_ process. This
avoids socket traffic entirely and is ideal for prototyping or small-scale
benchmarks.

.. code-block:: python

   import meep as mp
   import maxwelllink as mxl

   tls = mxl.Molecule(
       driver="tls",
       center=mp.Vector3(0, 0, 0),
       size=mp.Vector3(1, 1, 1),
       sigma=0.1,
       dimensions=2,
       driver_kwargs=dict(
           omega=0.242,      # TLS frequency (a.u.)
           mu12=187.0,       # dipole moment (a.u.)
           orientation=2,    # Ez
           pe_initial=1e-3,  # initial excited population
       ),
   )

   sim = mxl.MeepSimulation(
       molecules=[tls],
       time_units_fs=0.1,
       cell_size=mp.Vector3(8, 8, 0),
       geometry=[],
       sources=[],
       boundary_layers=[mp.PML(3.0)],
       resolution=10,
   )

   sim.run(until=90)

Within the same interpreter, you can analyze the TLS diagnostics through
``tls.additional_data_history``.

Local multi-process run (UNIX socket)
-------------------------------------

When we want `Meep <https://meep.readthedocs.io/en/latest/>`_ and the molecular driver to run as separate processes on the
same machine, use a UNIX domain socket. The hub listens on ``/tmp/socketmxl_<name>``
and the driver connects with ``--unix``.

`Meep <https://meep.readthedocs.io/en/latest/>`_ script:

.. code-block:: python

   import meep as mp
   import maxwelllink as mxl

   hub = mxl.SocketHub(unixsocket="tls_demo", timeout=10.0, latency=1e-4)

   tls = mxl.Molecule(
       hub=hub,
       center=mp.Vector3(0, 0, 0),
       size=mp.Vector3(1, 1, 1),
       sigma=0.1,
       dimensions=2,
   )

   sim = mxl.MeepSimulation(
       hub=hub,
       molecules=[tls],
       time_units_fs=0.1,
       cell_size=mp.Vector3(8, 8, 0),
       boundary_layers=[mp.PML(3.0)],
       resolution=10,
   )

   sim.run(until=90)

Driver command (same laptop/workstation):

.. code-block:: bash

   mxl_driver --model tls --unix --address tls_demo \
     --param "omega=0.242, mu12=187, orientation=2, pe_initial=1e-3"

UNIX sockets avoid port collisions and usually takes less time for communication.
**MaxwellLink** waits for the driver to connect before advancing the simulation.

Distributed run (TCP socket)
----------------------------

For multi-node deployments (e.g., `Meep <https://meep.readthedocs.io/en/latest/>`_ on one node and ``mxl_driver`` on another),
use a TCP socket. Let the OS pick a free port to prevent clashes.

Meep script:

.. code-block:: python

   import meep as mp
   import maxwelllink as mxl
   from maxwelllink import sockets as mxs

   _, port = mxs.get_available_host_port()
   hub = mxl.SocketHub(host="", port=port, timeout=30.0, latency=1e-4)

   print(f"SocketHub listening on port {port}. Share the host/IP with the driver.")

   tls = mxl.Molecule(
       hub=hub,
       center=mp.Vector3(0, 0, 0),
       size=mp.Vector3(1, 1, 1),
       sigma=0.1,
       dimensions=2,
   )

   sim = mxl.MeepSimulation(
       hub=hub,
       molecules=[tls],
       time_units_fs=0.1,
       cell_size=mp.Vector3(8, 8, 0),
       boundary_layers=[mp.PML(3.0)],
       resolution=10,
   )

   sim.run(until=90)

Setting ``host=\"\"`` binds the hub to all interfaces (equivalent to ``0.0.0.0``).
We need to share the public hostname or IP of the `Meep <https://meep.readthedocs.io/en/latest/>`_ node and ``port``
with the driver.

Driver command (run on the remote node):

.. code-block:: bash

   mxl_driver --model tls --address <meep-hostname> --port <port> \
     --param "omega=0.242, mu12=187, orientation=2, pe_initial=1e-3"

Replace ``<meep-hostname>`` with the reachable address of the `Meep <https://meep.readthedocs.io/en/latest/>`_ node. Open
firewall ports if required by your cluster configuration.


Distributed run in HPC (TCP socket)
------------------------------------

For HPC users, if the EM solver and molecular driver run on separate nodes of a SLURM HPC system, the MaxwellLink
job can be submitted as a two-step dependent SLRUM job as follows:

.. code-block:: bash

    job_main_id=$(sbatch submit_main.sh | awk '{print $4}')
    sbatch --dependency=after:${job_main_id} submit_driver.sh

Here, two SLURM submission scripts
(``submit_main.sh`` and ``submit_driver.sh``) launch the EM solver main process and the driver process, respectively.

In the ``submit_main.sh`` bash script, the main MaxwellLink Python input (say, ``em_run.py``) contains the 
following code to automatically find an available host and port for the TCP socket hub: 

.. code-block:: python

    import maxwelllink as mxl
    from maxwelllink import sockets as mxs
    host, port = mxs.get_available_host_port(localhost=False, save_to_file="tcp_host_port_info.txt")
    # time out for HPC environments may need to be relatively long
    hub = mxl.SocketHub(host=host, port=port, timeout=200.0, latency=1e-4)

The available host and port will be saved to a text file (``tcp_host_port_info.txt`` here) 
that can be read by the driver.

In the ``submit_driver.sh`` bash script, the driver can read the host and port information from the text file and connect to the hub as follows:

.. code-block:: bash

    # wait for the main job to start and write the host and port info
    sleep 10s

    HOST_PORT_FILE="tcp_host_port_info.txt"
    if [[ ! -f "$HOST_PORT_FILE" ]]; then
        echo "Error: Host and port info file '$HOST_PORT_FILE' not found!"
        exit 1
    fi
    HOST=$(sed -n '1p' "$HOST_PORT_FILE")
    PORT=$(sed -n '2p' "$HOST_PORT_FILE")

    mxl_driver --model tls --address $HOST --port $PORT --param "..."

Then, only after the main job starts and writes the host and port information to the text file, the 
driver job will be submitted and started to connect to the hub. 

For large-scale simulations, multiple drivers can be launched on different nodes. For example, the following
input bash script launches one main job and two driver jobs. The two driver jobs will start only after the main job starts.

.. code-block:: bash

    job_main_id=$(sbatch submit_main.sh | awk '{print $4}')
    sbatch --dependency=after:${job_main_id} submit_driver.sh
    sbatch --dependency=after:${job_main_id} submit_driver.sh

Inspecting TLS output
---------------------

In all the above workflows, after ``sim.run`` completes, the code below recovers the excited-state
population and the simulation time in atomic units.

.. code-block:: python

   import numpy as np

   population = np.array([entry["Pe"] for entry in tls.additional_data_history])
   time_au = np.array([entry["time_au"] for entry in tls.additional_data_history])

We can then compare the electronic excited-state trajectory to the analytical golden-rule result as shown in
:doc:`tutorials/notebook/meep_tls_spontaneous_emission`.

MPI execution
-------------

**MaxwellLink** detects MPI automatically. Only rank 0 (the master) communicates with drivers
while field integrals and returned molecular response are broadcast to worker ranks via
``mpi4py``. We can launch a MPI run with:

.. code-block:: bash

   mpirun -np 4 python run_tls.py

Driver restarts
---------------

If a driver disconnects unexpectedly, the hub pauses the EM solver time loop and
waits for the driver to reconnect. Enabling ``checkpoint=true`` and
``restart=true`` in the driver parameters lets expensive molecular dynamics
recover from transient failures without restarting the EM simulation.

Single-mode cavity emulator
---------------------------

For quick prototyping without launching `Meep <https://meep.readthedocs.io/en/latest/>`_, use
:class:`~maxwelllink.em_solvers.single_mode_cavity.SingleModeSimulation`. It models the EM field as one damped
harmonic oscillator in atomic units and couples to the same ``Molecule``
objects. Example:

.. code-block:: python

   import maxwelllink as mxl

   tls = mxl.Molecule(
       driver="tls",
       driver_kwargs=dict(omega=0.242, mu12=187.0, orientation=2, pe_initial=1e-3),
   )

   sim = mxl.SingleModeSimulation(
       dt_au=0.05,
       frequency_au=0.242,
       damping_au=1e-3,
       molecules=[tls],
       drive=0.0,
       coupling_strength=1.0,
   )

   sim.run(steps=500)
   print(tls.additional_data_history[-1]["Pe"])
