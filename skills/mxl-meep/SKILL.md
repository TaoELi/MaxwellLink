---
name: mxl-meep
description: This skill should be used when users need MaxwellLink + Meep FDTD workflows, including embedded vs socket coupling, MPI considerations, and recommended templates.
---

# Meep FDTD workflows (MaxwellLink)

## Confirm prerequisites
- Ensure `pymeep` is installed and importable before generating Meep-based inputs.
- Use `mxl.MeepSimulation` (not raw `meep.Simulation`) when using the unified `mxl.Molecule` API.

## Build a Meep-coupled run
- Set `time_units_fs` explicitly and keep it consistent across analysis.
- Create molecules with either:
  - Embedded driver: `Molecule(driver=..., driver_kwargs=...)`
  - Socket driver: `SocketHub(...)` + `Molecule(hub=hub, ...)`
- For "baseline" EM-only runs (no molecules), use raw `mp.Simulation(...)`; switch to `mxl.MeepSimulation(...)` only when MaxwellLink molecules are present (common when generating reference spectra/fields first, then enabling coupling).
- Use `boundary_layers=[mp.PML(...)]`, set `resolution`, and keep `Courant=0.5` (Meep default).

## Prefer templates
- Start from the copy-ready templates:
  - `skills/mxl-project-scaffold/assets/templates/meep-tls-embedded`
  - `skills/mxl-project-scaffold/assets/templates/meep-tls-socket-unix`
- Use SLURM/HPC patterns from `skills/mxl-hpc-slurm/SKILL.md` for multi-node socket runs.

## References
- Recipes: `skills/mxl-meep/references/meep_run_recipes.md`
- Docs: `docs/source/em_solvers/meep.rst`

## MPI/libfabric init tips (macOS/local single-rank)
- Sandbox note: sandboxed runs often block the libfabric endpoint; if `MPI_Init_thread` fails with `ep_enable`/socket errors, rerun **unsandboxed** (`sandbox_permissions=require_escalated`).
- When Matplotlib is imported, set `MPLCONFIGDIR=/tmp/mplconfig` to avoid cache permission errors.

## Other common bugs 
- The `dimensions=...` input can only be assigned in `mxl.Molecule()` not `mxl.MeepSimulation()`. For example, assigning `dimensions=1` in both `mxl.Molecule()` and `mxl.MeepSimulation()` will lead to runtime error: `meep: cannot require a ez component in a 1D grid`.
- In 1D and 2D simulations, `mxl.Molecule` (or molecular drivers) should have a transition dipole moment along z-direction, because `mxl.MeepSimulation()` sets the `mxl.Molecule` to emit only along z-direction in 1D and 2D.
