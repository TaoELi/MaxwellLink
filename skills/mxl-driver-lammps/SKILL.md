---
name: mxl-driver-lammps
description: This skill should be used when users want to couple MaxwellLink to LAMMPS via the bundled `fix mxl`, including build/install and socket connection patterns.
---

# LAMMPS driver (`fix mxl`)

## Confirm prerequisites
- Build a LAMMPS binary that includes the MaxwellLink fix.
  - Preferred: run `mxl_install_lammps` to build an `lmp_mxl` binary.

## Configure the socket hub
- Create a TCP or UNIX socket hub on the MaxwellLink (EM) side:
  - `hub = mxl.SocketHub(host="0.0.0.0", port=31415, timeout=60.0)`

## Configure LAMMPS input
- Add the fix to the relevant atom group:
  - `fix 1 all mxl <host> <port> [unix] [reset_dipole]`

## Notes
- Ensure every atom has a charge; otherwise the coupling force is zero.
- Use `skills/mxl-hpc-slurm/SKILL.md` when the hub and LAMMPS run on different nodes.
- Prefer scaffold template: `skills/mxl-project-scaffold/assets/templates/slurm-meep-lammps-tcp`
- Common SLURM pattern:
  - Main job writes `tcp_host_port_info.txt` (host on line 1, port on line 2).
  - Driver job patches a template input and runs LAMMPS:
    - `cp in_mxl.lmp in.lmp`
    - `sed -i -e "s/HOST/$HOST/g" in.lmp && sed -i -e "s/PORT/$PORT/g" in.lmp`
    - `srun -n $SLURM_NTASKS lmp_mxl -in in.lmp`
- Read full details in `docs/source/drivers/lammps.rst`.
