---
name: mxl-driver-dftbplus
description: This skill should be used when users want to couple MaxwellLink to the MaxwellLink-aware DFTB+ fork via the `MaxwellLinkSocket` block, including real-time Ehrenfest dynamics, VelocityVerlet Born-Oppenheimer MD, dipole-derivative choices, build/install, and TCP/UNIX socket connection patterns.
---

# DFTB+ driver (`MaxwellLinkSocket`)

## Confirm prerequisites
- DFTB+ is an **external process driver** (Fortran binary launched separately) — it is socket-only and has no embedded mode.
- Supported workflows:
  - Real-time Ehrenfest dynamics: place `MaxwellLinkSocket` inside `ElectronDynamics`.
  - Born-Oppenheimer MD (BOMD): place `MaxwellLinkSocket` inside `Driver = VelocityVerlet`.
- Build a DFTB+ binary from the MaxwellLink-aware fork with socket support:
  - `git clone git@github.com:TEL-Research/dftbplus.git`
  - `git submodule update --init --recursive`
  - `cmake -S . -B build -DWITH_SOCKETS=ON -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=$HOME/.local`
  - `cmake --build build --parallel && cmake --install build`
- Ensure the resulting `dftb+` binary is on `PATH`, or export `DFTBPLUS_BIN=/abs/path/to/dftb+`.
- Provide the Slater-Koster `.skf` files required by the chosen elements (e.g. `H-H.skf`, `O-O.skf`, `O-H.skf`).

## Configure the socket hub
- Create a hub on the MaxwellLink (EM) side. Pick **one** transport:
  - TCP: `hub = mxl.SocketHub(host="0.0.0.0", port=31415, timeout=60.0)`
  - UNIX: `hub = mxl.SocketHub(unixsocket="dftbplus_h2o", timeout=60.0)`
    - Relative names map to `/tmp/socketmxl_<name>` (override via `Prefix`).

## Configure real-time Ehrenfest input (`dftb_in.hsd`)
- Add `MaxwellLinkSocket` **inside** the `ElectronDynamics` block. `TimeStep [au]` must equal the EM-side `dt_au`.
- `IonDynamics = Yes` enables Ehrenfest nuclear motion; DFTB+ includes the field contribution in the force evaluation.

TCP mode:
```
ElectronDynamics = {
  Steps = __STEPS__
  TimeStep [au] = __DT_AU__
  IonDynamics = Yes
  MovedAtoms = 1:N
  InitialTemperature [Kelvin] = 0.0
  MaxwellLinkSocket = {
    Host = "localhost"
    Port = 31415
    ResetDipole = Yes
  }
}
```

UNIX-socket mode:
```
ElectronDynamics = {
  Steps = __STEPS__
  TimeStep [au] = __DT_AU__
  IonDynamics = Yes
  MovedAtoms = 1:N
  MaxwellLinkSocket = {
    File = "__MXL_SOCKET__"
    MoleculeId = 0
    ResetDipole = Yes
  }
}
```

## Configure BOMD input (`dftb_in.hsd`)
- Add `MaxwellLinkSocket` **inside** `Driver = VelocityVerlet`.
- BOMD couples MaxwellLink to the slower nuclear motion: DFTB+ solves the electronic ground state at each MD geometry and adds the external-field force from `E(t) * d(mu)/dR`.
- `VelocityVerlet/TimeStep` must match the `dt_au` sent by MaxwellLink after DFTB+ converts the input time unit to atomic units.
- Do not combine the BOMD `MaxwellLinkSocket` block with a regular DFTB+ `ElectricField` block.

Current Mulliken charges (default dipole-derivative model):
```
Driver = VelocityVerlet {
  Steps = __STEPS__
  TimeStep [fs] = __DT_FS__
  MovedAtoms = 1:-1
  Thermostat = None {}

  MaxwellLinkSocket = {
    Host = "localhost"
    Port = 31415
    ResetDipole = Yes
    DipoleDerivative = MullikenCharges
  }
}
```

Fixed user charges (one charge per atom in DFTB+ atom order):
```
Driver = VelocityVerlet {
  Steps = __STEPS__
  TimeStep [fs] = __DT_FS__
  MovedAtoms = 1:-1
  Thermostat = None {}

  MaxwellLinkSocket = {
    File = "__MXL_SOCKET__"
    ResetDipole = Yes
    DipoleDerivative = FixedCharges
    Charges = {
      __Q1__ __Q2__ __Q3__
    }
  }
}
```

On-the-fly Born effective charges:
```
Driver = VelocityVerlet {
  Steps = __STEPS__
  TimeStep [fs] = __DT_FS__
  MovedAtoms = 1:-1
  Thermostat = None {}

  MaxwellLinkSocket = {
    Host = "localhost"
    Port = 31415
    DipoleDerivative = BornChargesOnTheFly
    BornUpdateEvery = 1
  }
}
```

- `BornChargesOnTheFly` computes Born effective charges during MD with DFTB+ response theory using analytical H0/S coordinate derivatives. It is the most expensive derivative model; increase `BornUpdateEvery` to reuse Born charges for several MD steps when appropriate.
- `FixedCharges` evaluates the staggered dipole current exactly from Velocity-Verlet half-step velocities.
- `MullikenCharges` and `BornChargesOnTheFly` return a finite-difference dipole current from dipoles at `t_n` and `t_{n+1}`.

## Launch
- After the EM-side hub is listening, launch DFTB+ in the run directory:
  - `dftb+ > dftbplus.out`
- The DFTB+ process connects to the MaxwellLink hub, receives the regularized field each step, advances either `ElectronDynamics` or `VelocityVerlet`, and returns dipole/current data plus energies.

## `MaxwellLinkSocket` parameters
- `Host` — TCP host of the MaxwellLink process (default `localhost`). Mutually exclusive with `File`.
- `Port` — TCP port (default `31415`, must be > 0).
- `File` — UNIX-domain socket path; absolute paths used as-is, relative names prefixed by `Prefix`.
- `Prefix` — prefix for relative `File` (default `/tmp/socketmxl_`).
- `Verbosity` — DFTB+ socket logging level (default `0`).
- `MoleculeId` — expected molecule id from the MaxwellLink `INIT` packet (default `-1`, disables check).
- `ResetDipole` — when `Yes`, subtracts the initial DFTB+ dipole from all reported dipoles (recommended for cavity coupling).
- `DipoleDerivative` — BOMD-only selection for `d(mu)/dR`; choices are `MullikenCharges`, `FixedCharges`, and `BornChargesOnTheFly` (default `MullikenCharges`).
- `Charges` — user partial charges for BOMD `FixedCharges`, one value per atom.
- `BornUpdateEvery` — number of MD steps between Born-charge refreshes for BOMD `BornChargesOnTheFly` (default `1`).

## Time-step rules
- Real-time Ehrenfest: `ElectronDynamics/TimeStep [au]` must equal the EM-side `dt_au`.
- BOMD: `VelocityVerlet/TimeStep` must equal the EM-side `dt_au` after DFTB+ unit conversion.
- For Meep simulations, choose Meep resolution and time unit so the FDTD step matches the selected DFTB+ time step.

## Returned data (per step, in `additional_data_history`)
- `time_au` — DFTB+ simulation time
- `energy_au` — total DFTB+ energy
- `energy_kin_au`, `energy_pot_au` — nuclear kinetic / potential energy
- For `VelocityVerlet` BOMD:
  - `mux_au`, `muy_au`, `muz_au` — molecular dipole components half a time step after the force evaluation time
  - `mux_m_au`, `muy_m_au`, `muz_m_au` — molecular dipole components at the force evaluation time
- For `ElectronDynamics`:
  - `mux_au`, `muy_au`, `muz_au` — molecular dipole components at the midpoint of the time step
  - `mux_m_au`, `muy_m_au`, `muz_m_au` — same midpoint dipole components

## Notes
- Use `skills/mxl-hpc-slurm/SKILL.md` when the EM hub and DFTB+ run on different nodes — pass the host/port from a `tcp_host_port_info.txt` handoff (same pattern as LAMMPS).
- Common SLURM pattern (mirrors LAMMPS):
  - Main job writes `tcp_host_port_info.txt` (host on line 1, port on line 2).
  - Driver job patches a template `.hsd` (`HOST` / `PORT` placeholders, or `__MXL_SOCKET__` for UNIX) and runs `dftb+`.
- Working notebook example: `tutorials/dftbplus.ipynb`; reference inputs in `tutorials/dftbplus_input/` (H2 electronic and single H2O vibrational examples).
- Read full details in `docs/source/drivers/dftbplus.rst`.
