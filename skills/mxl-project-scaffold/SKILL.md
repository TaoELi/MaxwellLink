---
name: mxl-project-scaffold
description: This skill should be used when users ask to prepare MaxwellLink input files; it scaffolds a new `projects/YYYY-MM-DD-NAME/` folder from copy-ready templates and provides basic validation.
---

# MaxwellLink project scaffolding (inputs under projects/)

## Create a new project folder
- Name the project using the convention `YYYY-MM-DD-NAME` under `projects/`.
- Copy a ready-to-run template from `skills/mxl-project-scaffold/assets/templates/` into the new project directory.

## Use the scaffolding script
- List available templates:
  - `python skills/mxl-project-scaffold/scripts/mxl_scaffold_project.py --list`
- Create a project:
  - `python skills/mxl-project-scaffold/scripts/mxl_scaffold_project.py --template meep-tls-embedded --name tls-coherent-meep-2d-vacuum`
- Edit the generated `config.json` and `README.md` before running.

## Validate a project
- Run:
  - `python skills/mxl-project-scaffold/scripts/mxl_validate_project.py projects/YYYY-MM-DD-NAME`
- Fix any reported schema/type issues before launching long jobs.

## Template catalog
- `meep-tls-embedded`: Meep FDTD + embedded TLS, writes `tls_history.csv`
- `meep-tls-socket-unix`: Meep FDTD + TLS driver over UNIX socket (local multi-process)
- `singlemode-tls-socket-tcp`: SingleModeSimulation + TLS over TCP (fast socket workflow)
- `laser-tls-embedded`: LaserDrivenSimulation + embedded TLS (prescribed field)
- `slurm-meep-tls-tcp`: SLURM two-step main+driver jobs over TCP (multi-node pattern), writes `tcp_host_port_info.txt`
- `slurm-meep-lammps-tcp`: SLURM two-step Meep+LAMMPS (`fix mxl`) jobs over TCP, patches `in_mxl.lmp` and runs `lmp_mxl`
