# Meep + LAMMPS over TCP (SLURM two-step)

This template captures the HPC workflow for maxwell-md simulations of water under VSC:
- Main job starts `MeepSimulation` + `SocketHub` and writes `tcp_host_port_info.txt`
- LAMMPS job waits for host/port, patches `in_mxl.lmp` (`HOST`/`PORT`), then runs `lmp_mxl`

## Files
- `config.json`: run parameters for Meep + molecule wrapper + LAMMPS files
- `em_main.py`: Meep/MaxwellLink server job
- `lammps_driver.py`: host/port handoff + LAMMPS launcher
- `in_mxl.lmp`: LAMMPS template input with `HOST` and `PORT` placeholders
- `data.lmp`: LAMMPS structure file
- `submit_main.sh`: SLURM script for the main job
- `submit_lammps.sh`: SLURM script for the LAMMPS job

## Submit
```bash
job_main_id=$(sbatch submit_main.sh | awk '{print $4}')
sbatch --dependency=after:${job_main_id} submit_lammps.sh
```

Convenience:
- `./submit_all.sh`
- `./clean.sh`

## Notes
- If `socket.gethostname()` is not resolvable from the LAMMPS node, set:
  - `export MXL_HOST=$(hostname -f)`
- Ensure `lmp_mxl` is available in `PATH`.
- `config.json` has a Bragg-resonator option (`cavity.use_bragg_resonator=true`) matching the published-style setup; disable it for simple vacuum tests.
