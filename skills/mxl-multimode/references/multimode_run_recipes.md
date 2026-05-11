# Multimode cavity run recipes

Copy these into `projects/YYYY-MM-DD-NAME/` and adjust parameters/paths.

## Multimode Fabry-Pérot + TLS grid (socket)
`em.py`
```python
import maxwelllink as mxl

hub = mxl.SocketHub(host="127.0.0.1", port=31415, timeout=10.0, latency=1e-5)

# n_grid_x * n_grid_y molecules — one per cavity grid point
N_X, N_Y = 4, 4
molecules = [mxl.Molecule(hub=hub) for _ in range(N_X * N_Y)]

cavity = mxl.FabryPerotCavity(
    frequency_au=0.242,
    coupling_strength=1e-4,
    coupling_axis="y",
    n_grid_x=N_X,
    n_grid_y=N_Y,
    delta_omega_x_au=0.05,
    delta_omega_y_au=0.05,
    n_mode_x=4,
    n_mode_y=4,
    abc_cutoff=0.0,            # raise to e.g. 0.1 on larger grids
)

sim = mxl.MultiModeSimulation(
    dt_au=0.5,
    damping_au=0.0,
    molecules=molecules,
    cavity_geometry=cavity,
    hub=hub,
    include_dse=True,
)

sim.run(steps=4000, record_history=True, record_list=["all"])
```
Driver (one per molecule):
`mxl_driver --model tls --address 127.0.0.1 --port 31415 --param "omega=0.242, mu12=187, orientation=2, pe_initial=1e-3"`

## Multimode Fabry-Pérot + embedded SHO grid (non-socket)
```python
import maxwelllink as mxl

N = 6  # 1D ring of N modes / N grid points
molecules = [
    mxl.Molecule(
        driver="sho",
        driver_kwargs=dict(omega_au=0.0106, mu1_au=4.0, q_initial=0.0, p_initial=0.0),
    )
    for _ in range(N)
]

cavity = mxl.FabryPerotCavity(
    frequency_au=0.0106,
    coupling_strength=2e-5,
    coupling_axis="y",
    n_grid_x=N,
    n_grid_y=1,
    delta_omega_x_au=2.3e-4,   # ~50 cm^-1 spacing
    delta_omega_y_au=0.0,
    n_mode_x=N,
    n_mode_y=1,
)

sim = mxl.MultiModeSimulation(
    dt_au=10.0,
    damping_au=0.0,
    molecules=molecules,
    cavity_geometry=cavity,
    include_dse=True,
)

sim.run(steps=20000, record_history=True, record_list=["time", "qc", "molecule_dipole"])
```

## NVT initial condition + Langevin thermostat
```python
from maxwelllink.tools.harmonic_oscillator_helper import (
    MaxwellBoltzmannInitializer, LangevinThermostat,
)
from maxwelllink.units import AU_TO_K

dt_au = 0.5
T_au  = 300.0 / AU_TO_K       # ~9.5e-4 a.u.

sim = mxl.MultiModeSimulation(
    dt_au=dt_au,
    damping_au=0.0,
    molecules=molecules,
    cavity_geometry=cavity,
    include_dse=True,
    initializer=MaxwellBoltzmannInitializer(temperature_au=T_au, random_seed=2026),
    thermostat=LangevinThermostat(
        temperature_au=T_au, dt_au=dt_au, tau_au=4000.0, random_seed=2026,
    ),
)

sim.run(steps=4000, record_history=True, record_list=["all"])
```
- For a thermal IC followed by NVE evolution, drop `thermostat=` (defaults to `DummyThermostat`).
- `MaxwellBoltzmannInitializer` only seeds the slots that are still zero; user-supplied `qc_initial` / `pc_initial` are preserved.

## Photon-side and molecule-side pulses
```python
import numpy as np

def gaussian_pulse(t, t0=200.0, sigma=40.0, amp=1e-3):
    return amp * np.exp(-0.5 * ((t - t0) / sigma) ** 2)

sim = mxl.MultiModeSimulation(
    dt_au=0.5,
    damping_au=0.0,
    molecules=molecules,
    cavity_geometry=cavity,
    include_dse=True,
    # Drive cavity modes 0 and 1 along y:
    excited_mode_list=[0, 1],
    photon_pulse_drive=gaussian_pulse,
    photon_pulse_axis="y",
    # Drive grid points 0..3 directly along y (e.g. local probe):
    excited_grid_list=[0, 1, 2, 3],
    molecule_pulse_drive=gaussian_pulse,
    molecule_pulse_axis="y",
)
sim.run(steps=4000, record_history=True, record_list=["all"])
```

## K-parallel molecule-side pulse
```python
from maxwelllink.tools import gaussian_pulse, k_parallel_pulse

envelope = gaussian_pulse(
    amplitude_au=1.0,
    t0_au=0.0,
    sigma_au=0.05 * steps * dt_au,
)
source = k_parallel_pulse(
    cavity=cavity,
    envelope=envelope,
    omega_au=2580.0 * cm_to_au,
    k_parallel_au=12.5 * cm_to_au,
    target="molecule",
    direction="y",          # "x", "y", "+x", "-x", "+y", or "-y"
    center=(0.5, 0.10),     # fractional cavity coordinates
    size=(0.16, 0.20),      # full source window size
    amplitude_au=1e-2,
)

sim = mxl.MultiModeSimulation(
    dt_au=dt_au,
    damping_au=0.0,
    molecules=molecules,
    cavity_geometry=cavity,
    include_dse=True,
    excited_grid_list=source.excited_grid_list,
    molecule_pulse_drive=source,
    molecule_pulse_axis="y",
)
```
- `k_parallel_au` uses the same effective in-plane frequency units as `delta_omega_x_au` / `delta_omega_y_au`.
- `direction` controls the in-plane phase gradient; `molecule_pulse_axis` controls the field polarization seen by the molecular driver.
- The returned `source` also exposes `spatial_window`, `spatial_phase`, `grid_xy`, and `k_order` for debugging or plotting.
- Use `envelope=1.0` for a continuous cosine source with grid-dependent phases.

## K-parallel photon-side pulse
```python
from maxwelllink.tools import k_parallel_pulse

source = k_parallel_pulse(
    cavity=cavity,
    target="photon",
    envelope=1.0,
    omega_au=2580.0 * cm_to_au,
    k_parallel_au=12.5 * cm_to_au,
    direction="y",
    projection_axis="y",
    center=(0.5, 0.10),
    size=(0.16, 0.20),
    amplitude_au=1e-2,
)

sim = mxl.MultiModeSimulation(
    dt_au=dt_au,
    damping_au=0.0,
    molecules=molecules,
    cavity_geometry=cavity,
    include_dse=True,
    excited_mode_list=source.excited_mode_list,
    photon_pulse_drive=source,
    photon_pulse_axis="y",
)
```
- Photon-side `target="photon"` projects the same real-space source window and phase pattern onto cavity modes.
- `projection_axis` should usually match `photon_pulse_axis`.

## Disk-backed recording (large/long runs)
```python
sim.run(
    steps=200000,
    record_history=True,
    record_to_disk=True,
    disk_folder_address="./out",
    h5_filename="multimode_run.h5",   # or npz_filename="multimode_run.npz"
    record_every_steps=50,             # thin trajectory
    record_list=["time", "qc", "pc", "energy", "molecule_dipole"],
)
```
- Pass exactly one of `h5_filename` / `npz_filename`.
- NPZ mode stages a `temp_memmap/` folder under `disk_folder_address` and compacts to `<npz_filename>` on finalize.
- After `run()`, history attributes (`simu.time_history`, ...) are populated only when `record_to_disk=False`.

## Notes
- `n_grid` (= `n_grid_x * n_grid_y * n_repeat_x * n_repeat_y`) must equal `len(molecules)` — each molecule binds to one grid point via its `molecule_id`.
- For VSC with real-molecule drivers (LAMMPS, Psi4) keep `include_dse=True`; only model drivers (TLS, QuTiP) typically run with DSE off.
- `coupling_axis` masks both the dipole that is fed to the cavity and the field that is returned to the molecules; pick it consistently with the driver's dipole convention.
- Use `abc_cutoff > 0` (e.g. 0.05–0.1) on larger planar grids to suppress photon reflections at the edges.
- For SLURM/HPC two-step runs, write a host/port file from the main job (e.g. via `maxwelllink.sockets.get_available_host_port(localhost=False, save_to_file="tcp_host_port_info.txt")`) and have driver jobs read it.
- The Langevin thermostat's `dt_au` must match the simulation `dt_au`; pair the same `random_seed` across initializer + thermostat for reproducibility.
