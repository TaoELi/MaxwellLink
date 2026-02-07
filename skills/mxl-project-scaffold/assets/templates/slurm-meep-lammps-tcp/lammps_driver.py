import json
import os
import shutil
import subprocess
import time
from pathlib import Path


def _load_config() -> dict:
    config_path = Path(__file__).resolve().with_name("config.json")
    with config_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _wait_for_host_port(path: Path, timeout_s: int = 120) -> tuple[str, int]:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if path.exists():
            lines = path.read_text(encoding="utf-8").splitlines()
            if len(lines) >= 2:
                return lines[0].strip(), int(lines[1].strip())
        time.sleep(1.0)
    raise FileNotFoundError(f"Host/port file not found or incomplete: {path}")


def _render_lammps_input(template_path: Path, out_path: Path, host: str, port: int) -> None:
    text = template_path.read_text(encoding="utf-8")
    text = text.replace("HOST", host).replace("PORT", str(port))
    out_path.write_text(text, encoding="utf-8")


def main() -> None:
    cfg = _load_config()
    lammps_cfg = cfg["lammps"]

    host_port_file = Path(__file__).resolve().with_name(str(cfg["host_port_file"]))
    host, port = _wait_for_host_port(host_port_file, timeout_s=180)

    template_name = str(lammps_cfg["input_template"])
    data_name = str(lammps_cfg["data"])

    template_path = Path(__file__).resolve().with_name(template_name)
    data_path = Path(__file__).resolve().with_name(data_name)
    if not template_path.exists():
        raise FileNotFoundError(f"LAMMPS input template missing: {template_path}")
    if not data_path.exists():
        raise FileNotFoundError(f"LAMMPS data file missing: {data_path}")

    in_path = Path(__file__).resolve().with_name("in.lmp")
    _render_lammps_input(template_path, in_path, host=host, port=port)

    if shutil.which("lmp_mxl") is None:
        raise RuntimeError(
            "lmp_mxl was not found in PATH. Build/install LAMMPS with MaxwellLink first."
        )

    ntasks = int(os.environ.get("SLURM_NTASKS", "1"))
    if shutil.which("srun") is not None:
        cmd = ["srun", "-n", str(ntasks), "lmp_mxl", "-in", str(in_path)]
    else:
        cmd = ["lmp_mxl", "-in", str(in_path)]

    subprocess.check_call(cmd)


if __name__ == "__main__":
    main()
