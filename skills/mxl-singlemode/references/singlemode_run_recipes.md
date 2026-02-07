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

## Notes
- For SLURM/HPC two-step runs, write a host/port file from the main job (e.g. via `maxwelllink.sockets.get_available_host_port(localhost=False, save_to_file="tcp_host_port_info.txt")`) and have the driver job read it.
- Optional parameter setting in `mxl.SingleModeSimulation`: `include_dse=True` should be included for simulating vibrational strong coupling with real-molecule drivers (such as LAMMPS or Psi4). This option does not need to be included for model drivers (tls or qutip).
