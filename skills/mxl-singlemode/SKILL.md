---
name: mxl-singlemode
description: This skill should be used when users need the MaxwellLink single-mode cavity solver for fast prototyping and regression, in either socket or embedded mode.
---

# Single-mode cavity workflows (MaxwellLink)

## Use for fast prototyping
- Use `mxl.SingleModeSimulation` when a 1-mode cavity surrogate is sufficient and Meep is too heavy.

## Configure
- Set `dt_au`, `frequency_au`, `damping_au`, `coupling_strength`, and `coupling_axis`.
- Optional physics knobs used in strong-coupling workflows: `include_dse`.
- Drive routing knobs: `excite_ph` (default `True`) sends the `drive` term to the cavity EOM; `excite_mol` (default `False`) adds the `drive` term to the effective electric field acting on the molecules along `coupling_axis`. Enabling both at once is allowed but emits a warning.
- Thermal knobs: `temp_au` (a.u., default `0.0`) seeds initial `qc`/`pc` from a Maxwell-Boltzmann distribution when positive, overriding any user-provided `qc_initial`/`pc_initial`. Add `tau_au` (Langevin relaxation time, a.u.) to also thermostat the cavity momentum every step; leave `tau_au=None` for a one-shot thermal initial condition with NVE dynamics. Use `random_seed` for reproducible sampling/kicks.
- Attach molecules in embedded or socket mode (same `Molecule` interface as elsewhere).

## Prefer templates
- Template: `skills/mxl-project-scaffold/assets/templates/singlemode-tls-socket-tcp`

## References
- Recipes: `skills/mxl-singlemode/references/singlemode_run_recipes.md`
- Docs: `docs/source/em_solvers/single_mode_cavity.rst`
