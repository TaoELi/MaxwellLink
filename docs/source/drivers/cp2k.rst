CP2K driver
===============
.. warning::

  This is a beta feature and may change in future versions.

**MaxwellLink** can couple EM solvers directly to a modified version of
`CP2K <https://www.cp2k.org/>`_ through the **MaxwellLink** socket protocol. Currently, real-time 
TDDFT and its Ehrenfest dynamics extension are supported for both non-periodic
(length-gauge) and periodic (velocity-gauge) systems.

.. note::

  For RT-TDDFT (``RUN_TYPE RT_PROPAGATION``), CP2K keeps the ions (nuclei) fixed and propagates the
  electronic wavefunction with ``DFT%REAL_TIME_PROPAGATION``. **MaxwellLink**
  supplies a uniform electric field to CP2K. 
  
  - In non-periodic cells, CP2K applies the electric field directly to the length-gauge Hamiltonian,

  .. math::

    \mathbf{H}(t) = \mathbf{H}_{\mathrm{KS}}(t)
     - \widetilde{\mathbf{E}}(t)\cdot\boldsymbol{\mu}.

  - In periodic systems, CP2K must use ``REAL_TIME_PROPAGATION%VELOCITY_GAUGE``.
  **MaxwellLink** still sends the electric field to CP2K, but CP2K integrates it
  into the vector potential for constructing the Kohn-Sham Hamiltonian,

  .. math::

    \mathbf{A}_n =
      \mathbf{A}_{n-1}
      - c\,\Delta t\,\widetilde{\mathbf{E}}_{n-1/2},

  For RT-Ehrenfest dynamics (``RUN_TYPE EHRENFEST_DYN``), CP2K runs its existing NVE Ehrenfest
  velocity-Verlet loop, where **MaxwellLink** supplies the external electric field for the electronic propagation 
  and receives the dipole time derivative (as well as dipole moments) for the Maxwell solver's source term.

  Note that for periodic systems, the Berry phase form of the dipole moments are reported to **MaxwellLink** by default.


Requirements
------------

- Install a modified CP2K code from source with the ``MXL_SOCKET`` section and socket support enabled.

CP2K build
----------

Download and build the modified CP2K code from source following the official tutorial of CP2K.

.. code-block:: bash

   git clone git@github.com:TEL-Research/cp2k.git

After installation, ensure the resulting ``cp2k.psmp`` executable is on
``PATH`` or set ``CP2K_BIN`` in scripts that launch CP2K.

Socket preparation
------------------

On the **MaxwellLink** side create a SocketHub before starting CP2K.

For TCP sockets:

.. code-block:: python

   hub = mxl.SocketHub(host="0.0.0.0", port=31415, timeout=60.0)

and in CP2K use ``HOST localhost`` and ``PORT 31415`` when the two processes
run on the same machine.

For Unix-domain sockets:

.. code-block:: python

   hub = mxl.SocketHub(unixsocket="cp2k_h2o", timeout=60.0)

The relative Unix socket name above corresponds to ``/tmp/socketmxl_cp2k_h2o``
in CP2K.

The CP2K process can then connects to the socket server started by **MaxwellLink**:

.. code-block:: bash

   cp2k.psmp -i cp2k.inp -o cp2k.out

Fixed-ion RT-TDDFT input
------------------------

Add ``MXL_SOCKET`` inside ``DFT%REAL_TIME_PROPAGATION``. The
``MOTION%MD%TIMESTEP`` value **must match** the ``dt_au`` used by the EM solver.

- TCP mode, non-periodic length gauge:

.. code-block:: none

   &GLOBAL
     PROJECT h2o_mxl_rtp
     RUN_TYPE RT_PROPAGATION
   &END GLOBAL

   &MOTION
     &MD
       ENSEMBLE NVE
       STEPS 20000
       TIMESTEP [au_t] 0.2
     &END MD
   &END MOTION

   &FORCE_EVAL
     METHOD QUICKSTEP
     &DFT
       BASIS_SET_FILE_NAME BASIS_SET
       POTENTIAL_FILE_NAME POTENTIAL
       &REAL_TIME_PROPAGATION
         PROPAGATOR EM
         INITIAL_WFN SCF_WFN
         MAX_ITER 25
         EPS_ITER 1.0E-9
         &MXL_SOCKET
           HOST localhost
           PORT 31415
           RESET_DIPOLE
         &END MXL_SOCKET
       &END REAL_TIME_PROPAGATION
     &END DFT
     &SUBSYS
       &CELL
         ABC 12.0 12.0 12.0
         PERIODIC NONE
       &END CELL
       ...
     &END SUBSYS
   &END FORCE_EVAL

- Unix socket mode for the same non-periodic length-gauge case:

.. code-block:: none

   &REAL_TIME_PROPAGATION
     PROPAGATOR EM
     INITIAL_WFN SCF_WFN
     MAX_ITER 25
     EPS_ITER 1.0E-9
     &MXL_SOCKET
       UNIX
       FILE cp2k_h2o
       RESET_DIPOLE
     &END MXL_SOCKET
   &END REAL_TIME_PROPAGATION

Periodic RT-TDDFT input
-----------------------

For periodic cells, enable velocity gauge and do not define a regular CP2K
``DFT%EFIELD`` section.

.. code-block:: none

   &GLOBAL
     PROJECT si_mxl_rtp
     RUN_TYPE RT_PROPAGATION
   &END GLOBAL

   &MOTION
     &MD
       ENSEMBLE NVE
       STEPS 20000
       TIMESTEP [au_t] 0.2
     &END MD
   &END MOTION

   &FORCE_EVAL
     METHOD QUICKSTEP
     &DFT
       BASIS_SET_FILE_NAME BASIS_SET
       POTENTIAL_FILE_NAME POTENTIAL
       &REAL_TIME_PROPAGATION
         PROPAGATOR EM
         INITIAL_WFN SCF_WFN
         MAX_ITER 25
         EPS_ITER 1.0E-9
         VELOCITY_GAUGE .TRUE.
         VG_COM_NL .FALSE.
         &MXL_SOCKET
           HOST localhost
           PORT 31415
           PERIODIC_DIPOLE_METHOD BERRY
           RESET_DIPOLE
         &END MXL_SOCKET
       &END REAL_TIME_PROPAGATION
     &END DFT
     &SUBSYS
       &CELL
         ABC 10.0 10.0 10.0
         PERIODIC XYZ
       &END CELL
       ...
     &END SUBSYS
   &END FORCE_EVAL

Ehrenfest input
---------------

For moving nuclei, use ``RUN_TYPE EHRENFEST_DYN`` together with
``MOTION%MD%ENSEMBLE NVE``. The same ``MXL_SOCKET`` subsection is placed under
``DFT%REAL_TIME_PROPAGATION``.

.. code-block:: none

   &GLOBAL
     PROJECT h2o_mxl_emd
     RUN_TYPE EHRENFEST_DYN
   &END GLOBAL

   &MOTION
     &MD
       ENSEMBLE NVE
       STEPS 20000
       TIMESTEP [au_t] 0.2
     &END MD
   &END MOTION

   &FORCE_EVAL
     METHOD QUICKSTEP
     &DFT
       BASIS_SET_FILE_NAME BASIS_SET
       POTENTIAL_FILE_NAME POTENTIAL
       &REAL_TIME_PROPAGATION
         PROPAGATOR EM
         INITIAL_WFN SCF_WFN
         MAX_ITER 25
         EPS_ITER 1.0E-9
         &MXL_SOCKET
           HOST localhost
           PORT 31415
           MOLECULE_ID 0
           RESET_DIPOLE
         &END MXL_SOCKET
       &END REAL_TIME_PROPAGATION
     &END DFT
     &SUBSYS
       &CELL
         ABC 12.0 12.0 12.0
         PERIODIC NONE
       &END CELL
       ...
     &END SUBSYS
   &END FORCE_EVAL

For periodic Ehrenfest dynamics, combine the periodic RT-TDDFT settings above
(``VELOCITY_GAUGE .TRUE.``, ``VG_COM_NL .FALSE.``, and
``PERIODIC_DIPOLE_METHOD``) with ``RUN_TYPE EHRENFEST_DYN`` and
``ENSEMBLE NVE``.

Parameters
----------

Common ``MXL_SOCKET`` options
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1

   * - Name
     - Description
   * - ``HOST``
     - Hostname or IP address of the **MaxwellLink** process. Used for TCP
       mode. Default: ``localhost``.
   * - ``PORT``
     - TCP port exposed by :class:`~maxwelllink.sockets.sockets.SocketHub`.
       Default: ``31415``.
   * - ``UNIX``
     - Use a Unix-domain socket instead of a TCP socket. When ``FILE`` is not
       set, ``HOST`` is interpreted as the Unix socket name.
   * - ``FILE``
     - Unix-domain socket path or relative socket name. Absolute paths are
       used directly; relative names are prefixed by ``PREFIX``.
   * - ``PREFIX``
     - Prefix prepended to a relative ``FILE`` value. Default:
       ``/tmp/socketmxl_``.
   * - ``MOLECULE_ID``
     - Optional molecule id expected from the MaxwellLink ``INIT`` packet.
       Negative values disable the check. Default: ``-1``.
   * - ``RESET_DIPOLE``
     - When enabled, subtract the initial CP2K dipole value from dipoles
       reported to **MaxwellLink**, so the initial reported dipole is zero.
       The returned source vector is unchanged apart from roundoff. Default:
       ``.FALSE.``.
   * - ``PERIODIC_DIPOLE_METHOD``
     - Periodic velocity-gauge dipole/source model. ``BERRY`` computes a
       direct Berry-phase periodic dipole and finite-differences it for the returned
       current density. ``CURRENT`` returns the endpoint current and integrates
       it to an effective reported dipole (which might be **unstable** over long-time simulations). Default: ``BERRY``.
   * - ``VERBOSITY``
     - Socket communication logging level in CP2K. Default: ``0``.

RT-TDDFT and Ehrenfest parameters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1

   * - Name
     - Description
   * - ``MOTION%MD%TIMESTEP``
     - Real-time propagation or Ehrenfest MD step. It **must match** the
       ``dt_au`` sent by the EM solver in the MaxwellLink ``INIT`` payload.
   * - ``GLOBAL%RUN_TYPE``
     - Use ``RT_PROPAGATION`` for fixed-ion RT-TDDFT or ``EHRENFEST_DYN`` for
       moving-ion Ehrenfest dynamics.
   * - ``REAL_TIME_PROPAGATION%PROPAGATOR``
     - CP2K electronic propagator. The MaxwellLink path supports ``EM`` (**preferred and tested**),
       ``ETRS``, and ``CN``.
   * - ``REAL_TIME_PROPAGATION%VELOCITY_GAUGE``
     - Required for periodic MaxwellLink coupling and currently restricted to
       periodic cells when ``MXL_SOCKET`` is active.
   * - ``REAL_TIME_PROPAGATION%VG_COM_NL``
     - Must be ``.FALSE.`` for periodic MaxwellLink velocity-gauge coupling.
   * - ``REAL_TIME_PROPAGATION%DENSITY_PROPAGATION``
     - Must remain ``.FALSE.`` for periodic MaxwellLink velocity-gauge
       coupling; the current implementation requires MO-based RTP.
   * - ``MOTION%MD%ENSEMBLE``
     - Must be ``NVE`` for ``RUN_TYPE EHRENFEST_DYN`` with ``MXL_SOCKET``.

Returned data
-------------

Apart from the binary source vector
:math:`\mathrm{d}\boldsymbol{\mu}/\mathrm{d}t`, CP2K sends the following JSON
data to **MaxwellLink** on every step:

- ``time_au`` -- CP2K simulation time in atomic units.
- ``energy_au`` -- Molecular energy in atomic units. For non-periodic
  length-gauge coupling, CP2K reports the field-free energy by adding back the
  instantaneous :math:`\boldsymbol{\mu}\cdot\mathbf{E}` coupling term. For
  Ehrenfest dynamics this includes the nuclear kinetic energy.
- ``mux_au``, ``muy_au``, ``muz_au`` -- Endpoint dipole components in atomic
  units at the current CP2K step. In periodic ``BERRY`` mode these are
  branch-unwrapped Berry-phase dipoles; in periodic ``CURRENT`` mode they
  are current-integrated effective dipoles.
- ``mux_m_au``, ``muy_m_au``, ``muz_m_au`` -- Arithmetic midpoint dipole
  components from the current and previous endpoint dipoles.
- ``energy_kin_au`` -- Nuclear kinetic energy in atomic units. Zero for
  fixed-ion ``RT_PROPAGATION``.
- ``energy_pot_au`` -- Field-free potential/electronic energy contribution
  for non-periodic length gauge, or CP2K's current potential/electronic
  energy for periodic velocity gauge.

Notes and limitations
---------------------

- CP2K must be compiled without ``__NO_SOCKETS``.
- The ``MXL_SOCKET`` section belongs under
  ``DFT%REAL_TIME_PROPAGATION``.
- The current interface is limited to Quickstep RT-TDDFT:
  ``RUN_TYPE RT_PROPAGATION`` and ``RUN_TYPE EHRENFEST_DYN`` with
  ``ENSEMBLE NVE``.
- Non-periodic systems use length-gauge electric-field coupling; periodic
  systems use velocity-gauge vector-potential coupling.
- Do not combine periodic MaxwellLink velocity-gauge calculations with a
  regular CP2K ``DFT%EFIELD`` section.
- Periodic MaxwellLink velocity-gauge coupling currently requires MO-based
  RTP and does not support ``VG_COM_NL``.
