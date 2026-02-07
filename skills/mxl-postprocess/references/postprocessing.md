# Post-processing hints

MaxwellLink stores driver diagnostics on each `Molecule` in `additional_data_history` (list of dicts). Common keys: `time_au`, `mux_au/muy_au/muz_au`, `energy_au`, `Pe/Pg`, `rho_diag`, plus solver-specific histories.

## Extract arrays (Python)
```python
import numpy as np

data = np.array(tls.additional_data_history)
time = np.array([d["time_au"] for d in data])
pe = np.array([d.get("Pe") for d in data])
mux = np.array([d.get("mux_au") for d in data])
```

## Plot an observable
```python
import matplotlib.pyplot as plt

plt.plot(time, pe, label="Pe")
plt.xlabel("time (a.u.)")
plt.legend()
plt.show()
```

## Combine multiple molecules
```python
responses = [np.array([d["mux_au"] for d in mol.additional_data_history]) for mol in sim.molecules]
total = np.sum(responses, axis=0)
```

## Solver-specific notes
- SingleModeSimulation: when `record_history=True` use `sim.time_history`, `qc_history`, `pc_history`, `energy_history`, `molecule_response_history`.
- LaserDrivenSimulation: `time_history`, `drive_history`, and `molecule_response_history` hold per-step data.
- Meep: standard Meep outputs (flux/fields) are accessible via `sim.fields` etc.; molecular data still live on each `Molecule`.

## Checkpoint/restores
- Drivers with `checkpoint/restart` write numbered files (e.g., `tls_checkpoint_id_<n>.npz`, `rttddft_checkpoint_id_<n>.npy`, `ase_checkpoint_id_<n>.npz`). Inspect with `numpy.load` if needed.

