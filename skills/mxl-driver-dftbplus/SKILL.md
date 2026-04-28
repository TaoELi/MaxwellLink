---
name: mxl-driver-dftbplus
description: This skill should be used when users want to couple MaxwellLink to DFTB+ real-time Ehrenfest dynamics via the bundled `MaxwellLinkSocket` block, including build/install and TCP/UNIX socket connection patterns.
---

# DFTB+ driver (`MaxwellLinkSocket`)

## Confirm prerequisites
- DFTB+ is an **external process driver** (Fortran binary launched separately) — it is socket-only and has no embedded mode.
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

## Configure DFTB+ input (`dftb_in.hsd`)
- Add `MaxwellLinkSocket` **inside** the `ElectronDynamics` block. `TimeStep [au]` must equal the EM-side `dt_au`.

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

## Launch
- After the EM-side hub is listening, launch DFTB+ in the run directory:
  - `dftb+ > dftbplus.out`
- The DFTB+ process connects to the MaxwellLink hub, receives the regularized field each step, propagates one electron-dynamics step, and returns the dipole + energies.

## `MaxwellLinkSocket` parameters
- `Host` — TCP host of the MaxwellLink process (default `localhost`). Mutually exclusive with `File`.
- `Port` — TCP port (default `31415`, must be > 0).
- `File` — UNIX-domain socket path; absolute paths used as-is, relative names prefixed by `Prefix`.
- `Prefix` — prefix for relative `File` (default `/tmp/socketmxl_`).
- `Verbosity` — DFTB+ socket logging level (default `0`).
- `MoleculeId` — expected molecule id from the MaxwellLink `INIT` packet (default `-1`, disables check).
- `ResetDipole` — when `Yes`, subtracts the initial DFTB+ dipole from all reported dipoles (recommended for cavity coupling).

## Returned data (per step, in `additional_data_history`)
- `time_au` — DFTB+ simulation time
- `mux_au`, `muy_au`, `muz_au` — molecular dipole components (a.u.)
- `mux_m_au`, `muy_m_au`, `muz_m_au` — midpoint dipole components
- `energy_au` — total DFTB+ energy
- `energy_kin_au`, `energy_pot_au` — nuclear kinetic / potential energy

## Notes
- `IonDynamics = Yes` enables Ehrenfest nuclear motion; the field contribution is included in the force evaluation.
- For Meep simulations, choose Meep resolution and time unit so the FDTD step matches `ElectronDynamics/TimeStep`.
- Use `skills/mxl-hpc-slurm/SKILL.md` when the EM hub and DFTB+ run on different nodes — pass the host/port from a `tcp_host_port_info.txt` handoff (same pattern as LAMMPS).
- Common SLURM pattern (mirrors LAMMPS):
  - Main job writes `tcp_host_port_info.txt` (host on line 1, port on line 2).
  - Driver job patches a template `.hsd` (`HOST` / `PORT` placeholders, or `__MXL_SOCKET__` for UNIX) and runs `dftb+`.
- Working notebook example: `tutorials/dftbplus.ipynb`; reference inputs in `tutorials/dftbplus_input/` (H2 electronic, single H2O vibrational, 32-water Meep + DFTB+).
- Read full details in `docs/source/drivers/dftbplus.rst`.
