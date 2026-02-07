# SLURM two-step recipes (TCP sockets)

Use this pattern when the EM solver (server/hub) and molecular drivers (clients) run on different nodes.

## Key idea
- Main job starts the `SocketHub` and writes a reachable `HOST` + `PORT` into a shared file (e.g. `tcp_host_port_info.txt`).
- Driver job(s) wait for the file, read it, then connect with `mxl_driver --address "$HOST" --port "$PORT" ...`.
- Submit driver jobs with `--dependency=after:<main_job_id>` so they start only after the main job is running.

## Main job (server): `submit_main.sh`
```bash
#!/usr/bin/env bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH -J mxl_main
#SBATCH -o mxl_main.%j.out
#SBATCH -e mxl_main.%j.err

set -euo pipefail

# Prefer mpirun under SLURM; choose srun if required by specific HPC setting
mpirun -n "${SLURM_NTASKS:-1}" python -u em_main.py
```

## Driver job (client): `submit_driver.sh`
```bash
#!/usr/bin/env bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH -J mxl_driver
#SBATCH -o mxl_driver.%j.out
#SBATCH -e mxl_driver.%j.err

set -euo pipefail

HOST_PORT_FILE="tcp_host_port_info.txt"
for _ in $(seq 1 120); do
  [[ -f "$HOST_PORT_FILE" ]] && break
  sleep 1
done
if [[ ! -f "$HOST_PORT_FILE" ]]; then
  echo "Error: Host/port file '$HOST_PORT_FILE' not found after waiting." >&2
  exit 1
fi

HOST=$(sed -n '1p' "$HOST_PORT_FILE")
PORT=$(sed -n '2p' "$HOST_PORT_FILE")

# Single molecule (TLS) example:
mxl_driver --model tls --address "$HOST" --port "$PORT" --param "omega=0.242, mu12=187, orientation=2, pe_initial=1e-3" --verbose

# Multi-molecule pattern (common in the paper examples):
# nmol=${SLURM_NTASKS}
# for ((i=1; i<nmol; i++)); do
#   mxl_driver --model tls --address "$HOST" --port "$PORT" --param "omega=..., mu12=..., orientation=..., pe_initial=..." &
# done
# mxl_driver --model tls --address "$HOST" --port "$PORT" --param "omega=..., mu12=..., orientation=..., pe_initial=..."
```

## LAMMPS driver job (client): `submit_driver_lammps.sh`

This recipe is scaffolded directly by:
`skills/mxl-project-scaffold/assets/templates/slurm-meep-lammps-tcp`.

```bash
#!/usr/bin/env bash
#SBATCH -J mxl_lammps
#SBATCH -o mxl_lammps.%j.out
#SBATCH -e mxl_lammps.%j.err

set -euo pipefail

HOST_PORT_FILE="tcp_host_port_info.txt"
for _ in $(seq 1 120); do
  [[ -f "$HOST_PORT_FILE" ]] && break
  sleep 1
done
HOST=$(sed -n '1p' "$HOST_PORT_FILE")
PORT=$(sed -n '2p' "$HOST_PORT_FILE")

cp in_mxl.lmp in.lmp
sed -i -e "s/HOST/$HOST/g" in.lmp
sed -i -e "s/PORT/$PORT/g" in.lmp

command -v lmp_mxl >/dev/null 2>&1 || { echo "Missing lmp_mxl in PATH" >&2; exit 1; }
srun -n "${SLURM_NTASKS:-1}" lmp_mxl -in in.lmp
```

## EM script that writes host/port: `em_main.py`

Option A (simple, commonly used):
```python
import maxwelllink as mxl
from maxwelllink import sockets as mxs

host, port = mxs.get_available_host_port(
    localhost=False,
    save_to_file="tcp_host_port_info.txt",
)
hub = mxl.SocketHub(host=host, port=port, timeout=200.0, latency=1e-4)
print(f"SocketHub listening on {host}:{port}")
```

Option B (most robust on clusters that block outbound networking):
```python
import os
import socket

import maxwelllink as mxl
from maxwelllink import sockets as mxs

def _pick_free_port(bind_addr: str = "0.0.0.0") -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((bind_addr, 0))
        return int(s.getsockname()[1])

port = _pick_free_port("0.0.0.0")
host_for_clients = os.environ.get("MXL_HOST", socket.gethostname())
if mxs.am_master():
    with open("tcp_host_port_info.txt", "w", encoding="utf-8") as f:
        f.write(f"{host_for_clients}\n{port}\n")

hub = mxl.SocketHub(host="", port=port, timeout=200.0, latency=1e-4)
print(f"SocketHub listening on {host_for_clients}:{port}")
```

## Submit sequence
```bash
job_main_id=$(sbatch submit_main.sh | awk '{print $4}')
sbatch --dependency=after:${job_main_id} submit_driver.sh
```

## Notes
- Ensure the host/port file is written to a filesystem visible to both jobs (shared workdir or `$SLURM_SUBMIT_DIR`).
- For `N` molecules, launch `N` driver processes; the hub assigns molecule IDs automatically as clients connect.
- If `socket.gethostname()` is not resolvable from the driver node, set `MXL_HOST` explicitly (e.g. `export MXL_HOST=$(hostname -f)`).

## Parameter sweeps
- Prefer one directory per case (keeps outputs separated), with a small `submit_all.sh` that chains `sbatch` submissions.
- A typical sweep driver script:
  - loops over parameter values,
  - `mkdir -p case_<...>` and copies a template into it,
  - edits a few numbers (e.g. with `sed`),
  - then runs `./submit_all.sh` inside each case directory.
