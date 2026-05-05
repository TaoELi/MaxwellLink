---
name: mxl-multimode
description: This skill should be used when users need the MaxwellLink multimode Fabry-Pérot cavity solver to study a spatial grid of molecules coupled to many photonic modes (mesoscale VSC), in either socket or embedded mode.
---

# Multimode Fabry-Pérot cavity workflows (MaxwellLink)

## When to use
- Use `mxl.MultiModeSimulation` paired with `mxl.FabryPerotCavity` when the physics requires a 2D molecular grid coupled to many photon modes (mesoscale VSC, polariton dispersion, planar Fabry-Pérot dynamics). For 1-mode prototyping prefer `skills/mxl-singlemode/SKILL.md`; for full FDTD prefer `skills/mxl-meep/SKILL.md`.
- Reference: TE Li, *J. Chem. Theory Comput.* 20, 7016 (2024).

## Configure the cavity geometry (`FabryPerotCavity`)
- Required: `frequency_au` (cavity reference angular frequency at k_∥=0), `delta_omega_x_au`, `delta_omega_y_au` (planar dispersion spacings; mode frequencies are `sqrt(omega_c^2 + (l_x·dωx)^2 + (l_y·dωy)^2)`).
- Mode count: `n_mode_x`, `n_mode_y` (cavity modes per axis).
- Molecular grid: provide either `n_grid_x`/`n_grid_y` (uniform grid, overrides explicit lists) or `x_grid_1d`/`y_grid_1d` (explicit fractional positions in `[0, 1]` of `Lx`, `Ly`). `n_repeat_x`/`n_repeat_y` stack multiple molecules per spatial site.
- Coupling: `coupling_strength` is the prefactor ε for the lowest mode; per-mode couplings are auto-rescaled as `ε_k = ε · ω_k / min ω_k`. `coupling_axis` is a case-insensitive union of `"x"`, `"y"`, `"z"`.
- Boundaries: `abc_cutoff` (fractional grid units, default `0.0`) enables an absorbing-boundary smooth window on large planar grids to kill unphysical reflections — recommended whenever `n_grid_x*n_grid_y` is large.

## Configure the simulation (`MultiModeSimulation`)
- Required: `dt_au`, `cavity_geometry=cavity` (must be a `FabryPerotCavity` instance — the simulation transparently exposes `n_mode`, `n_grid`, `omega_k`, `ftilde_k`, ... via attribute lookup).
- Molecules: pass an iterable of `mxl.Molecule` matching `n_grid = n_grid_x * n_grid_y * n_repeat_x * n_repeat_y`. Socket and embedded molecules can be mixed. For socket runs, all molecules must share one `SocketHub`.
- Cavity dissipation: `damping_au` is a deterministic exponential damping on `pc` every step. Always-on; `0.0` disables.
- Light-matter physics: `include_dse=True` (default) is required for real-molecule drivers (LAMMPS, Psi4). Set `False` only for model drivers (TLS, QuTiP) when you intentionally want to drop dipole self-energy.
- VV interop: `molecule_half_step=True` extrapolates molecular response from half-step data — only set this if the molecular driver returns half-step VV data.
- Initial conditions: `qc_initial`, `pc_initial` (shape `(n_mode, 3)`), `mu_initial`, `dmudt_initial` (shape `(n_grid, 3)`). All default to zeros and are then passed through the initializer.
- Baseline shift: `shift_dipole_baseline=True` subtracts the initial dipole from every subsequent dipole — useful when permanent dipoles are large in strong-coupling runs.

## Initializer / thermostat (NVE vs NVT)
The thermal knobs are now **objects**, not scalar kwargs. Old-style `T_initial_au` / `NVT_T_au` / `langevin_tau_au` / `thermostat_seed` no longer exist.
- Defaults are `initializer=DummyInitializer()` and `thermostat=DummyThermostat()` (zero IC, NVE).
- For an NVT thermal IC, use `initializer=MaxwellBoltzmannInitializer(temperature_au=T_au, random_seed=...)`. It only fills `qc`/`pc` slots that are still all-zero, so user-provided `qc_initial`/`pc_initial` win.
- For a Langevin thermostat, use `thermostat=LangevinThermostat(temperature_au=T_au, dt_au=dt_au, tau_au=tau_au, random_seed=...)`. Note `dt_au` must match the simulation `dt_au`.
- Pair the same `random_seed` across initializer + thermostat for reproducibility. Use `from maxwelllink.units import AU_TO_K` for K↔a.u. conversions.

## Drives
- Photon-side pulse: pick mode indices via `excited_mode_list=[i, ...]`, give a constant or `callable(t_au)` as `photon_pulse_drive`, and choose `photon_pulse_axis` (default `"y"`).
- Molecule-side pulse: pick grid indices via `excited_grid_list=[i, ...]`, with `molecule_pulse_drive` (constant or callable) and `molecule_pulse_axis` (default `"y"`).
- Both are optional and additive; passing an empty list disables that channel.

## Run + recording
- `sim.run(...)` accepts `until` XOR `steps`. Recording is configured **on `run()`**, not the constructor:
  - `record_history=True` (default) keeps per-record arrays in memory.
  - `record_to_disk=True` requires `disk_folder_address` plus exactly one of `npz_filename` / `h5_filename`. NPZ uses memmap-backed temp files that are compacted on finalize; H5 writes directly via h5py.
  - `record_list` selects fields: `["all"]` or any subset of `"time"`, `"qc"`, `"pc"`, `"photon_drive"`, `"molecule_pulse"`, `"energy"`, `"effective_efield"`, `"molecule_response"`, `"molecule_dipole"`. Passing `[]` or `None` disables recording.
  - `record_every_steps` thins; `record_max_steps` caps the on-disk allocation (defaults to `steps // record_every_steps`).
- After `run()`, in-memory arrays are exposed as `simu.<name>_history` (e.g. `simu.qc_history`, `simu.molecule_dipole_history`).

## References
- Recipes: `skills/mxl-multimode/references/multimode_run_recipes.md`
- Docs: `docs/source/em_solvers/multimode_cavity.rst`
- Tutorial notebook: `docs/source/tutorials/notebook/11_multimode_sho.ipynb`
