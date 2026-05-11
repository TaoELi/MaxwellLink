# EM solver quick reference

Pick the solver that matches fidelity vs speed. All solvers share `Molecule` and optional `SocketHub`.

## Meep FDTD (`maxwelllink.em_solvers.meep`)
- Use for full grid EM, near-to-far, PMLs; supports MPI. Dependency: `pymeep` (optionally MPI build).
- Prefer the unified v2 API: `mxl.MeepSimulation` + `mxl.Molecule`.
- Core args: `molecules=[...]`, `time_units_fs=<fs per Meep unit>`, `cell_size`, `geometry`, `sources`, `boundary_layers`, `resolution`. Extra kwargs pass through to `meep.Simulation`.
- Coupling: add `hub=SocketHub(...)` when any molecule is socket-driven; otherwise embed drivers via `driver=...`.
- Notes: Courant fixed to 0.5; only rank 0 talks to drivers under MPI. Field fed to drivers is regularized over each molecule volume.

## SingleModeSimulation (`maxwelllink.em_solvers.single_mode_cavity`)
- One damped harmonic oscillator in a.u.; fastest for prototyping or regression. No external deps.
- Core args: `dt_au`, `frequency_au`, `damping_au`, `coupling_strength`, `coupling_axis` (e.g., `"z"` or `"xy"`), `molecules`, optional `drive`, `hub`, `record_history`.
- Supports socket and embedded drivers simultaneously.
- Thermal options: `temperature_au`, `initializer="maxwell_boltzmann"` for initial thermal sampling, and optional `langevin_tau_au` for a Langevin thermostat.
- Optional dipole self-energy via `include_dse=True`.

## LaserDrivenSimulation (`maxwelllink.em_solvers.laser_driven`)
- Applies user-supplied `drive(time_au)` directly; no EM feedback. No external deps.
- Core args: `dt_au`, `molecules`, `drive` (float or callable), `coupling_axis` (`"x"|"y"|"z"` combos), optional `hub`, `record_history`.
- Good for pump-probe style studies where the field is prescribed.
