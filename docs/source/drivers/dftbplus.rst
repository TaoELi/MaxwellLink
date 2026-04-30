DFTB+ driver
===============
.. warning::
  
  This is a beta feature and may change in future versions. 

**MaxwellLink** can couple EM solvers directly to a modified version of
`DFTB+ <https://www.dftbplus.org/>`_ through the MaxwellLink socket protocol.
Two DFTB+ workflows are currently supported:

Real-time Ehrenfest dynamics
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::
  
  For ``ElectronDynamics``, DFTB+ applies the MaxwellLink electric field through
  the time-dependent DFTB external potential,
  
  .. math::
    
    \mathbf{H}(t) = \mathbf{H}_{\mathrm{DFTB}}(t)
     - \widetilde{\mathbf{E}}(t)\cdot \boldsymbol{\mu}_{\mathrm{DFTB}}.
     
  When ``IonDynamics = Yes``, the corresponding field contribution is included in
  the Ehrenfest force evaluation. For a MaxwellLink step from :math:`t_n` to
  :math:`t_{n+1}`, DFTB+ returns the midpoint dipole and finite-difference dipole
  current
  
  .. math::
    
    \boldsymbol{\mu}_{n+1/2}
     = \frac{\boldsymbol{\mu}_n + \boldsymbol{\mu}_{n+1}}{2},
     \qquad
     \dot{\boldsymbol{\mu}}_{n+1/2}
     = \frac{\boldsymbol{\mu}_{n+1} - \boldsymbol{\mu}_n}{\Delta t}.

Born--Oppenheimer molecular dynamics
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::
  
  For ``Driver = VelocityVerlet``, MaxwellLink couples to the slower nuclear
  motion instead of directly propagating electronic dynamics. DFTB+ solves the
  electronic ground state at each MD geometry and adds the external-field force
  
  .. math::
    
    \mathbf{F}^{\mathrm{ext}}_{i,\beta}
     = \sum_{\alpha} \widetilde{E}_{\alpha}(t)
       \frac{\partial \mu_{\alpha}}{\partial R_{i,\beta}}.
       
  The dipole derivative :math:`\partial\boldsymbol{\mu}/\partial\mathbf{R}` can be
  approximated by three different approaches:
  
  - fixed user charges, 
  - current DFTB+ Mulliken charges, 
  - Born effective charges computed on the fly with DFTB+ response theory
    using analytical H0/S coordinate derivatives.
  
  As MaxwellLink expects the dipole current on the staggered EM time grid at
  :math:`t_{n+1/2}`, for ``FixedCharges``, DFTB+ evaluates the current exactly
  from the Velocity-Verlet half-step velocities:
  
  .. math::
    
    \dot{\boldsymbol{\mu}}_{n+1/2}
     = \sum_{i,\beta}
       \frac{\partial \boldsymbol{\mu}}{\partial R_{i,\beta}}
       v_{i,\beta}(t_{n+1/2}).

  For ``MullikenCharges`` and ``BornChargesOnTheFly``, DFTB+ instead evaluates
   dipoles at :math:`t_n` and :math:`t_{n+1}` and returns the finite-difference current

  .. math::

    \dot{\boldsymbol{\mu}}_{n+1/2}
     = \frac{\boldsymbol{\mu}_{n+1} - \boldsymbol{\mu}_{n}}{\Delta t}.

Requirements
------------

- A source build of the modified DFTB+ code with socket support enabled.
- Slater--Koster parameter files required by the DFTB+ input.


DFTB+ build
-----------

Download and build the modified DFTB+ code from source:

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
------------------

On the **MaxwellLink** side create a socket hub before starting DFTB+.

For TCP sockets:

.. code-block:: python

   hub = mxl.SocketHub(host="0.0.0.0", port=31415, timeout=60.0)

and in DFTB+ use ``Host = "localhost"`` and ``Port = 31415`` when the two
processes run on the same machine.

For Unix-domain sockets:

.. code-block:: python

   hub = mxl.SocketHub(unixsocket="dftbplus_h2o", timeout=60.0)

The relative Unix socket name above corresponds to
``/tmp/socketmxl_dftbplus_h2o`` in DFTB+ when using the default ``Prefix``.

The DFTB+ process connects to the socket server started by **MaxwellLink**:

.. code-block:: bash

   dftb+ > dftbplus.out

Real-time Ehrenfest input
-------------------------

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
       ResetDipole = Yes
     }
   }

Born--Oppenheimer MD input
--------------------------

For BOMD, add ``MaxwellLinkSocket`` inside ``Driver = VelocityVerlet``. The
``VelocityVerlet/TimeStep`` must match the ``dt_au`` sent by MaxwellLink, after
DFTB+ converts the input unit to atomic units.

Current Mulliken charges
^^^^^^^^^^^^^^^^^^^^^^^^

This is the default BOMD dipole-derivative model. It uses the current DFTB+
Mulliken-like net atomic charge for each atom.

.. code-block:: none

   Driver = VelocityVerlet {
     Steps = 10000
     TimeStep [fs] = 0.5
     MovedAtoms = 1:-1
     Thermostat = None {}

     MaxwellLinkSocket = {
       Host = "localhost"
       Port = 31415
       ResetDipole = Yes
       DipoleDerivative = MullikenCharges
     }
   }

Fixed user charges
^^^^^^^^^^^^^^^^^^

Use this mode to reproduce the partial-charge approximation used by many
classical MD drivers. Provide one charge per atom in DFTB+ atom order.

.. code-block:: none

   Driver = VelocityVerlet {
     Steps = 10000
     TimeStep [fs] = 0.5
     MovedAtoms = 1:-1
     Thermostat = None {}

     MaxwellLinkSocket = {
       File = "dftbplus_h2o"
       ResetDipole = Yes
       DipoleDerivative = FixedCharges
       Charges = {
         0.42 -0.84 0.42
       }
     }
   }

On-the-fly Born effective charges
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In this mode, DFTB+ computes the Born effective charges during the MD run
using linear response theory.

.. code-block:: none

   Driver = VelocityVerlet {
     Steps = 10000
     TimeStep [fs] = 0.5
     MovedAtoms = 1:-1
     Thermostat = None {}

     MaxwellLinkSocket = {
       Host = "localhost"
       Port = 31415
       DipoleDerivative = BornChargesOnTheFly
       BornUpdateEvery = 1
     }
   }

This mode is more expensive than any of the other dipole-derivative models. ``BornUpdateEvery`` can be
increased to reuse the previous Born charges for several MD steps.

Parameters
----------

Common ``MaxwellLinkSocket`` options
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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
     - When ``Yes``, subtract the initial DFTB+ dipole value from dipoles reported to
       **MaxwellLink**, so the initial dipole for the **MaxwellLink** is zero. Default: ``No``.

Real-time Ehrenfest parameters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1

   * - Name
     - Description
   * - ``ElectronDynamics/TimeStep``
     - Real-time propagation step. It must match the ``dt_au`` sent by the EM
       solver in the MaxwellLink ``INIT`` payload.


BOMD parameters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1

   * - Name
     - Description
   * - ``VelocityVerlet/TimeStep``
     - MD time step. It must match the ``dt_au`` sent by the EM solver after
       DFTB+ unit conversion.
   * - ``DipoleDerivative``
     - Selects how :math:`\partial\boldsymbol{\mu}/\partial\mathbf{R}` is
       evaluated. Choices are ``MullikenCharges``, ``FixedCharges``, and ``BornChargesOnTheFly``. Default:
       ``MullikenCharges``.
   * - ``Charges``
     - User partial charges, one value per DFTB+ atom. Used with ``FixedCharges``.
   * - ``BornUpdateEvery``
     - Number of MD steps between Born-charge refreshes. Default: ``1``. Used with ``BornChargesOnTheFly``.


Returned data
-------------

Apart from dipole time derivatives, DFTB+ sends the following additional JSON data to **MaxwellLink**:

- ``time_au`` -- DFTB+ simulation time in atomic units.
- ``energy_au`` -- Total DFTB+ energy in atomic units.
- ``energy_kin_au`` -- Nuclear kinetic energy in atomic units.
- ``energy_pot_au`` -- Potential energy contribution in atomic units.

For ``VelocityVerlet`` BOMD:

- ``mux_au``, ``muy_au``, ``muz_au`` – Molecular dipole components in atomic units at half a time step after the force evaluation time.
- ``mux_m_au``, ``muy_m_au``, ``muz_m_au`` – Molecular dipole components in atomic units at the force evaluation time.

For ``ElectronDynamics``:

- ``mux_au``, ``muy_au``, ``muz_au`` – Molecular dipole components in atomic units at the midpoint of the time step.
- ``mux_m_au``, ``muy_m_au``, ``muz_m_au`` – The same as above.

Notes and limitations
---------------------

- DFTB+ must be compiled with ``WITH_SOCKETS=ON``.
- Do not combine the BOMD ``MaxwellLinkSocket`` block with a regular DFTB+
  ``ElectricField`` block; the MaxwellLink field is supplied through the socket.

