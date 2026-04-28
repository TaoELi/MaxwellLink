---
name: mxl-index
description: This skill should be used when users ask how to set up, choose, or run a MaxwellLink simulation and the correct solver/driver/HPC/post-processing workflow must be selected.
---

# MaxwellLink skill index

## Route the request
- Classify the request as: input preparation, solver/driver selection, run execution, HPC deployment, post-processing, or troubleshooting.
- Load the corresponding specialized skill below and follow its checklist end-to-end.

## Choose solver/driver quickly
- Use `skills/mxl-core/references/solver_quickref.md` to pick an EM solver.
- Use `skills/mxl-core/references/driver_quickref.md` to pick a molecular driver.
- Confirm coupling mode:
  - Use embedded drivers when everything can run inside the EM process.
  - Use sockets when drivers must run as separate processes or nodes.

## Load the right skill
- Input scaffolding + templates: `skills/mxl-project-scaffold/SKILL.md`
- Core workflow invariants (socket vs embedded, MPI rank-0 rules): `skills/mxl-core/SKILL.md`
- EM solvers:
  - Meep FDTD: `skills/mxl-meep/SKILL.md`
  - Single-mode cavity: `skills/mxl-singlemode/SKILL.md`
  - Laser-driven dynamics: `skills/mxl-laser-driven/SKILL.md`
- Drivers:
  - TLS: `skills/mxl-driver-tls/SKILL.md`
  - QuTiP: `skills/mxl-driver-qutip/SKILL.md`
  - Psi4 RT-TDDFT: `skills/mxl-driver-psi4-rttddft/SKILL.md`
  - Psi4 RT-Ehrenfest: `skills/mxl-driver-psi4-rtehrenfest/SKILL.md`
  - ASE: `skills/mxl-driver-ase/SKILL.md`
  - LAMMPS `fix mxl`: `skills/mxl-driver-lammps/SKILL.md`
  - DFTB+ `MaxwellLinkSocket`: `skills/mxl-driver-dftbplus/SKILL.md`
- Operations:
  - CLI workspace management (`mxl init/clean/hpc`): `skills/mxl-cli/SKILL.md`
  - SLURM/HPC: `skills/mxl-hpc-slurm/SKILL.md`
  - Post-processing/export: `skills/mxl-postprocess/SKILL.md`
  - Troubleshooting: `skills/mxl-troubleshoot/SKILL.md`

## Advanced Topics
- Paper tutorial routing (water VSC manuscript track, Figure 3a-3c / requested Fig.4 scope alias): `skills/paper_tutorial_vsc_water/SKILL.md`
- Paper tutorial routing (plasmonic Pt/Si flux + HCN heating maps, Figure 5b/5d/5e): `skills/paper_tutorial_plasmon_heating/SKILL.md`
