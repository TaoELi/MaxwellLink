---
name: mxl-hpc-slurm
description: This skill should be used when users need to run MaxwellLink in SLURM/HPC environments, including dependent job patterns and robust socket host/port handoff.
---

# SLURM/HPC workflows

## Choose socket transport
- Use UNIX sockets only when the EM solver and driver run on the same node.
- Use TCP sockets when the EM solver and drivers run on different nodes.

## Use the two-step SLURM pattern (main job + driver job)
- Make the main job start the hub and write `host` and `port` to a shared text file (e.g. `tcp_host_port_info.txt`).
- Make the driver job wait until the file exists, read the file, then run `mxl_driver --model ... --address "$HOST" --port "$PORT" ...`.
- Submit the driver job with `--dependency=after:<main_job_id>` so it starts only after the hub is scheduled and begins running.

## Prefer robust host/port discovery
- Recommended (most robust on locked-down clusters): bind the hub to all interfaces (`host=""` / `0.0.0.0`), pick a free port locally, and write a reachable hostname via `MXL_HOST` or `hostname -f`.
- Convenience helper: `maxwelllink.sockets.get_available_host_port(localhost=False, save_to_file=...)` can pick a port and write the file for you.
  - If cluster outbound networking is blocked, this helper may fail to discover an external IP; fall back to the "most robust" pattern above.

## Multi-molecule / multi-driver patterns
- For `N` molecules, launch `N` driver processes (hub assigns molecule IDs automatically as clients connect).
- Common HPC pattern: start `N-1` drivers in the background and keep one in the foreground so the job keeps running.
- For heavy drivers (Psi4 RT-TDDFT / RT-Ehrenfest), prefer `num_threads=1` per driver when launching many drivers in one job.

## LAMMPS as the "driver job"
- When coupling to LAMMPS (`fix mxl`), treat the LAMMPS job as the client: read host/port, patch the LAMMPS input (or pass host/port directly), then `srun lmp_mxl -in in.lmp`.

## Prefer templates
- TLS over TCP template: `skills/mxl-project-scaffold/assets/templates/slurm-meep-tls-tcp`
- LAMMPS over TCP template: `skills/mxl-project-scaffold/assets/templates/slurm-meep-lammps-tcp`

## References
- Recipes: `skills/mxl-hpc-slurm/references/slurm_recipes.md`
- Docs: `docs/source/usage.rst` (distributed run sections)
