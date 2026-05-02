# Single-mode cavity run recipes

Copy these into `projects/YYYY-MM-DD-NAME/` and adjust parameters/paths.

## Single-mode cavity + TLS (socket)
`em.py`
```python
import maxwelllink as mxl

hub = mxl.SocketHub(host="127.0.0.1", port=31415, timeout=10.0, latency=1e-5)
molecule = mxl.Molecule(hub=hub)

sim = mxl.SingleModeSimulation(
    dt_au=0.5,
    frequency_au=0.242,
    damping_au=0.0,
    molecules=[molecule],
    coupling_strength=1e-4,
    qc_initial=[0.0, 0.0, 1e-5],
    coupling_axis="z",
    hub=hub,
    record_history=True,
)

sim.run(steps=4000)
```
Driver:
`mxl_driver --model tls --address 127.0.0.1 --port 31415 --param "omega=0.242, mu12=187, orientation=2, pe_initial=1e-3"`

## Single-mode cavity at finite temperature (Langevin)
`em.py` (additions over the snippet above):
```python
sim = mxl.SingleModeSimulation(
    dt_au=0.5,
    frequency_au=0.242,
    damping_au=0.0,
    molecules=[molecule],
    coupling_strength=1e-4,
    coupling_axis="z",
    hub=hub,
    record_history=True,
    # Thermal initial condition + Langevin thermostat on the cavity:
    temp_au=9.5e-4,        # ~300 K (use AU_TO_K for conversions)
    tau_au=4000.0,         # Langevin relaxation time (a.u.)
    random_seed=2026,      # reproducible sampling/kicks
)
```
Drop `tau_au` (or set it to `None`) to keep only the Maxwell-Boltzmann initial sampling and run NVE afterwards.

## Notes
- For SLURM/HPC two-step runs, write a host/port file from the main job (e.g. via `maxwelllink.sockets.get_available_host_port(localhost=False, save_to_file="tcp_host_port_info.txt")`) and have the driver job read it.
- Thermal initialization: when `temp_au > 0`, `qc_initial`/`pc_initial` are overridden by Maxwell-Boltzmann samples at `temp_au` (Hartree). Pair with `tau_au` for a Langevin thermostat on the cavity momentum; use `random_seed` for reproducibility. Convert temperatures with `from maxwelllink.units import AU_TO_K` (e.g. `T_au = 300.0 / AU_TO_K`).
- In `mxl.SingleModeSimulation`: `include_dse=True` must be included for simulating vibrational strong coupling with real-molecule drivers (such as LAMMPS or Psi4). This option does not need to be included for model drivers (tls or qutip).
- Drive routing: by default `excite_ph=True` and `excite_mol=False`, so the optional `drive(t)` term acts only on the cavity. Set `excite_mol=True` (and typically `excite_ph=False`) to apply the drive directly to the molecules along `coupling_axis`, e.g. for comparing cavity-mediated vs. direct molecular excitation. Enabling both at once is supported and prints a warning.
