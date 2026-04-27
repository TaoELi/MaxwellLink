DFTB+ driver
============

**MaxwellLink** can couple EM solvers to the real-time Ehrenfest dynamics in a
modified version of `DFTB+ <https://www.dftbplus.org/>`_. The DFTB+ process
receives the regularized electric field
from :class:`~maxwelllink.sockets.sockets.SocketHub`, propagates one
``ElectronDynamics`` step, and returns the dipole information to the EM solver.

.. note::

  During the run DFTB+ applies the MaxwellLink electric field through the
  time-dependent DFTB external potential,

  .. math::

     \mathbf{H}(t) = \mathbf{H}_{\mathrm{DFTB}}(t)
       - \widetilde{\mathbf{E}}(t)\cdot \boldsymbol{\mu}_{\mathrm{DFTB}},

  and, when ``IonDynamics = Yes``, includes the corresponding field contribution
  in the Ehrenfest force evaluation. 

  For a MaxwellLink step from :math:`t_n` to :math:`t_{n+1}`, DFTB+ returns the
  midpoint dipole and finite-difference dipole current

  .. math::

     \boldsymbol{\mu}_{n+1/2}
       = \frac{\boldsymbol{\mu}_n + \boldsymbol{\mu}_{n+1}}{2},
     \qquad
     \dot{\boldsymbol{\mu}}_{n+1/2}
       = \frac{\boldsymbol{\mu}_{n+1} - \boldsymbol{\mu}_n}{\Delta t}.

Requirements
------------

- A source build of the DFTB+ fork with socket support enabled.
- Slater-Koster parameter files required by the DFTB+ input.

Usage
-----

DFTB+ build
^^^^^^^^^^^

Download and build the DFTB+ fork from source:

.. code-block:: bash

   git clone git@github.com:TEL-Research/dftbplus.git
   cd dftbplus
   git submodule update --init --recursive
   cmake -S . -B build -DWITH_SOCKETS=ON -DCMAKE_BUILD_TYPE=Release \
     -DCMAKE_INSTALL_PREFIX=$HOME/.local
   cmake --build build --parallel
   cmake --install build

After installation, ensure the resulting ``dftb+`` executable is on ``PATH`` or
set ``DFTBPLUS_BIN`` in scripts that launch DFTB+.

Socket preparation
^^^^^^^^^^^^^^^^^^

On the **MaxwellLink** side create a socket hub. For TCP sockets:

.. code-block:: python

   hub = mxl.SocketHub(host="0.0.0.0", port=31415, timeout=60.0)

For Unix-domain sockets:

.. code-block:: python

   hub = mxl.SocketHub(unixsocket="dftbplus_h2o", timeout=60.0)

The relative Unix socket name above corresponds to
``/tmp/socketmxl_dftbplus_h2o``.

DFTB+ input
^^^^^^^^^^^

Add ``MaxwellLinkSocket`` inside the ``ElectronDynamics`` block. The
``TimeStep`` must match the ``dt_au`` used by the EM solver.

TCP mode:

.. code-block:: none

   ElectronDynamics = {
     Steps = 20000
     TimeStep [au] = 0.2
     IonDynamics = Yes
     MaxwellLinkSocket = {
       Host = "localhost"
       Port = 31415
       MoleculeId = 0
       ResetDipole = Yes
     }
   }

Unix socket mode:

.. code-block:: none

   ElectronDynamics = {
     Steps = 20000
     TimeStep [au] = 0.2
     IonDynamics = Yes
     MaxwellLinkSocket = {
       File = "dftbplus_h2o"
       MoleculeId = 0
       ResetDipole = Yes
     }
   }

The DFTB+ process connects to the socket server started by **MaxwellLink**:

.. code-block:: bash

   dftb+ > dftbplus.out

Parameters
----------

.. list-table::
   :header-rows: 1

   * - Name
     - Description
   * - ``Host``
     - Hostname or IP address of the **MaxwellLink** process. Used for TCP mode.
       Default: ``localhost`` when ``File`` is not set.
   * - ``Port``
     - TCP port exposed by :class:`~maxwelllink.sockets.sockets.SocketHub`.
       Must be positive in TCP mode. Default: ``31415``.
   * - ``File``
     - Unix-domain socket path or relative socket name. Absolute paths are used
       directly; relative names are prefixed by ``Prefix``. Mutually exclusive
       with ``Host``.
   * - ``Prefix``
     - Prefix prepended to a relative ``File`` value. Default:
       ``/tmp/socketmxl_``.
   * - ``Verbosity``
     - Socket communication logging level in DFTB+. Default: ``0``.
   * - ``MoleculeId``
     - Optional molecule id expected from the MaxwellLink ``INIT`` packet.
       Negative values disable the check. Default: ``-1``.
   * - ``ResetDipole``
     - When ``Yes``, subtract the initial DFTB+ dipole from all dipoles reported
       to **MaxwellLink**. Default: ``No``.
   * - ``ElectronDynamics/TimeStep``
     - DFTB+ real-time propagation step in atomic units. It must match the
       ``dt_au`` sent by the EM solver in the MaxwellLink ``INIT`` payload.

Returned data
-------------

- ``time_au`` – DFTB+ simulation time in atomic units.
- ``mux_au``, ``muy_au``, ``muz_au`` – Molecular dipole components in atomic units.
- ``mux_m_au``, ``muy_m_au``, ``muz_m_au`` – Same dipole components as above.
- ``energy_au`` – Total DFTB+ energy in atomic units.
- ``energy_kin_au`` – Nuclear kinetic energy in atomic units.
- ``energy_pot_au`` – Potential energy contribution in atomic units.

Notes
-----

- ``IonDynamics = Yes`` enables moving nuclei.
- For Meep simulations, choose the FDTD resolution and time unit so that the
  Meep time step matches the DFTB+ ``ElectronDynamics`` time step.
