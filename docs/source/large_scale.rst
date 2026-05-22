Exascale simulations
=======================

In **MaxwellLink**, each molecular driver can represent a single molecule or a large ensemble of molecules.
Depending on the molecular drivers, each driver may use its own OPENMP/MPI parallelization for the molecular dynamics.

Then, the EM solver can be coupled to a very large number of molecular drivers via TCP/UNIX socket via :class:`~maxwelllink.sockets.sockets.SocketHub`.
This approach can efficiently handle coupling the EM solver to **tens of thousands of molecular drivers**.

Here, we highlight an even more powerful way for HPC users beyond this number limit: a two-layer socket communication scheme:

- :class:`~maxwelllink.sockets.aggregated.AggregatedSocketHub`
  that aggregates many drivers behind one node-local bridge, mimicking the
  MPI/OpenMP hierarchy and scaling to far larger systems.


Before introducing this new communication scheme, let's first review the single-layer :class:`~maxwelllink.sockets.sockets.SocketHub` connection.

Single-layer: direct ``SocketHub`` connection
----------------------------------------------

In the direct scheme (see :doc:`usage`), the EM solver opens one
``SocketHub`` and every molecular driver connects to it directly over TCP (across
nodes) or UNIX sockets (same node)::

    EM solver -> SocketHub ==TCP/UNIX==> driver 0
                           ==TCP/UNIX==> driver 1
                           ==TCP/UNIX==> ...
                           ==TCP/UNIX==> driver N-1

A single ``SocketHub`` can serve **up to tens of thousands** of drivers (our tested largest number is 65,536), which
is sufficient for most production runs (recall each driver can represent a large ensemble of molecules such as LAMMPS MD). 


At the EM side, the input script looks as follows:

.. code-block:: python

   import meep as mp
   import maxwelllink as mxl
   from maxwelllink import sockets as mxs

   host, port = mxs.get_available_host_port(localhost=False,
                                            save_to_file="tcp_host_port_info.txt")
   hub = mxl.SocketHub(host=host, port=port, timeout=6000.0, latency=1e-4)

   # Many TLS molecules placed on a grid inside the FDTD cell.
   molecules = [
       mxl.Molecule(hub=hub, center=mp.Vector3(x, y, 0),
                    size=mp.Vector3(1, 1, 1), sigma=0.1, dimensions=2)
       for (x, y) in positions
   ]

   sim = mxl.MeepSimulation(
       hub=hub,
       molecules=molecules,
       time_units_fs=0.1,
       cell_size=mp.Vector3(40, 40, 0),
       boundary_layers=[mp.PML(3.0)],
       resolution=10,
   )
   sim.run(until=90)

Then, after running the EM solver, each molecular driver can be launched as follows [using two-level systems (TLS) as an example]:

.. code-block:: bash

   mxl_driver --model tls --address $HOST --port $PORT \
     --param "omega=0.242, mu12=187, orientation=2, pe_initial=1e-3"

.. warning::

   When **more than a few thousand** drivers connect simultaneously, the
   operating-system defaults become the bottleneck. As described in
   :doc:`usage`:

   - Before running the EM script, set ``ulimit -u N`` in the EM-side
     shell, with ``N`` larger than the total number
     of drivers (the default ``N`` varies in machines, spanning from 1024 to tens of thousands), and
   - up to **16,384** TCP drivers may wait in the connection queue at once; for
     larger counts insert a short ``sleep 0.1s`` between driver launches so each
     connection is accepted cleanly.

This direct scheme keeps the EM solver holding one socket per molecule. Beyond a
few tens of thousands of drivers, or when drivers are spread across many HPC
nodes, the per-connection bookkeeping and communication on the EM node becomes the limiting
factor. The two-layer scheme below removes this ceiling.


Two-layer: ``AggregatedSocketHub`` + ``mxl_bridge``
---------------------------------------------------

The two-layer communication protocol inserts a node-local **bridge** between the EM solver
and the drivers. Each HPC node runs a single ``mxl_bridge`` process that holds
**one** upstream TCP connection to the EM solver and also connects to many local
molecular drivers through an ordinary ``SocketHub`` over fast UNIX sockets::

    EM solver -> AggregatedSocketHub ==TCP==> mxl_bridge (node 0) ==UNIX==> drivers
                                     ==TCP==> mxl_bridge (node 1) ==UNIX==> drivers
                                     ==TCP==> ...

This mirrors the MPI/OpenMP model: the EM solver talks to a handful of *nodes*
(one TCP link each, like MPI ranks), and each node fans out to its *local*
drivers (like OpenMP threads). Because the EM solver now manages **one connection
per node instead of one per molecule**, the scheme scales to far larger systems.

.. note::

   The two-layer scheme is designed for multi-node HPC runs with more than **tens of thousands of drivers**. 
   It becomes more stable and requires less time for communications only when the driver number exceeds ~10,000.

   The initialization time for the two-layer scheme should be one order of magnitude (if more than 10 nodes are used) smaller than the single-layer scheme, as
   now each bridge connects to local molecular drivers concurrently.

   The communication time per driver can be as low as **0.025 ms** in this two-layer scheme; whereas the single-layer
   ``SocketHub`` can have a communication time per driver of **0.034 ms** for large driver counts.


Worked example: 2D FDTD + many TLS on HPC
-----------------------------------------

We distribute :math:`N` TLS across several HPC nodes, with one bridge per node.
As in the single-layer HPC workflow (:doc:`usage`), the job is submitted as a
two-step dependent SLURM job: a **main** job for the EM solver and a **driver**
job (one bridge plus its local drivers) per node.

**1. EM-side script** (``em_run.py``, launched by ``submit_main.sh``)

The only changes relative to the single-layer script are using
``AggregatedSocketHub`` and calling ``init_remote_bridges`` to partition the
molecules across bridges and write a manifest:

.. code-block:: python

   import meep as mp
   import maxwelllink as mxl
   from maxwelllink import sockets as mxs

   host, port = mxs.get_available_host_port(localhost=False,
                                            save_to_file="tcp_host_port_info.txt")
   
   # change #1: use AggregatedSocketHub instead of SocketHub
   hub = mxl.AggregatedSocketHub(host=host, port=port, timeout=6000.0, latency=1e-3)

   molecules = [
       mxl.Molecule(hub=hub, center=mp.Vector3(x, y, 0),
                    size=mp.Vector3(1, 1, 1), sigma=0.1, dimensions=2)
       for (x, y) in positions
   ]

   # change #2: initialize the remote bridges and allocate 1000 molecules per bridge/node
   # the manifest "aggregation.json" will be written to the shared filesystem for the bridge nodes to read
   hub.init_remote_bridges(
       molecules,
       molecules_per_bridge=1000,
       unix_prefix="bridge_",
       save_file="aggregation.json",
   )

   sim = mxl.MeepSimulation(
       hub=hub,
       molecules=molecules,
       time_units_fs=0.1,
       cell_size=mp.Vector3(40, 40, 0),
       boundary_layers=[mp.PML(3.0)],
       resolution=10,
   )
   sim.run(until=90)


.. note::

   ``aggregation.json`` must live on a filesystem shared by all nodes. The
   downstream UNIX sockets [``bridge_0``, …, created by ``unix_prefix`` + ``idx``(0, 1, 2, ...)] are node-local and are created by
   each ``mxl_bridge`` on its own node.

**2. Driver-side script** (``submit_driver.sh``, one task per node)

Each node starts its bridge from the shared ``aggregation.json`` manifest by index (0, 1, 2, ...), then launches its
local drivers against the node-local UNIX socket. Submitting this as a SLURM job
array gives one node (and one bridge ``idx``) per array task:

.. code-block:: bash

   #!/bin/bash
   #SBATCH --job-name=mxl_bridge
   #SBATCH --array=0-9            # 10 bridges -> 10 nodes
   #SBATCH --nodes=1
   #SBATCH --ntasks-per-node=1
   #SBATCH --cpus-per-task=8  # adjust as needed for the bridge's CPU requirements

   IDX=$SLURM_ARRAY_TASK_ID

   # Wait for the main job to write the shared manifest.
   until [[ -f aggregation.json ]]; do sleep 2; done

   # Start this node's bridge: one upstream TCP link, one local UNIX hub.
   mxl_bridge --info aggregation.json --idx ${IDX} &

   # Give the bridge a moment to create its local UNIX socket "bridge_${IDX}".
   sleep 10

   # Fan out the node's local drivers onto that UNIX socket.
   for m in $(seq 1 1000); do
     mxl_driver --model tls --unix --address bridge_${IDX} \
       --param "omega=0.242, mu12=187, orientation=2, pe_initial=1e-3" &
   done
   # wait for the background jobs finished before exiting the script
   wait


.. note::

   The unix address in each molecular driver must be ``unix_prefix`` + ``idx`` (e.g., ``bridge_0``, ``bridge_1``, etc.) to connect to the correct bridge on the same node.
**3. Submission** (dependent two-step job)

.. code-block:: bash

   job_main_id=$(sbatch submit_main.sh | awk '{print $4}')
   # The array job launches all bridge nodes once the main job has started.
   sbatch --dependency=after:${job_main_id} submit_driver.sh

The main job starts the EM solver and writes ``aggregation.json``; each array
task then brings up its bridge and local drivers. The EM solver advances only
once every bridge has connected and initialized, exactly as in the single-layer case.


Minimal migration from single-layer to two-layer
-------------------------------------------------

Moving an existing single-layer ``SocketHub`` script to the two-layer transport
requires only small edits:

**On the EM side**, swap the hub class and add one ``init_remote_bridges`` call:

.. code-block:: diff

   - hub = mxl.SocketHub(host=host, port=port, timeout=600.0, latency=1e-3)
   + hub = mxl.AggregatedSocketHub(host=host, port=port, timeout=600.0, latency=1e-3)
   + hub.init_remote_bridges(molecules, molecules_per_bridge=1000,
   +                         save_file="aggregation.json")

Everything else is unchanged.

**On the driver side**, wrap each node's existing ``mxl_driver`` launches with a
single ``mxl_bridge`` and point the drivers at the node-local UNIX socket
instead of the remote EM hub:

.. code-block:: diff

   + mxl_bridge --info aggregation.json --idx ${IDX} &
   + sleep 10
   - mxl_driver --model tls --address $HOST --port $PORT --param "..."
   + mxl_driver --model tls --unix --address bridge_${IDX} --param "..."


.. seealso::

   - :doc:`usage` for the single-layer socket workflows and the
     ``ulimit``/connection-queue caveats.
   - :doc:`architecture` for the EM/driver communication protocol.
   - :mod:`maxwelllink.sockets.aggregated` for the aggregate hub and bridge API.
