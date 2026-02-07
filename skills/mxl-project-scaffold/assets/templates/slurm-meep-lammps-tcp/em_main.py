import json
import os
import socket
from pathlib import Path

import meep as mp
import numpy as np
import maxwelllink as mxl
from maxwelllink import sockets as mxs


def _load_config() -> dict:
    config_path = Path(__file__).resolve().with_name("config.json")
    with config_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _pick_free_port(bind_addr: str = "0.0.0.0") -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((bind_addr, 0))
        return int(s.getsockname()[1])


def _write_host_port(path: Path, host: str, port: int) -> None:
    if mxs.am_master():
        path.write_text(f"{host}\n{port}\n", encoding="utf-8")


def _build_meep_geometry(cfg: dict):
    cavity_cfg = cfg.get("cavity", {})
    use_bragg = bool(cavity_cfg.get("use_bragg_resonator", False))

    if not use_bragg:
        return (
            mp.Vector3(*cfg["cell_size"]),
            [],
            [mp.PML(float(cfg["pml_thickness"]))],
        )

    rescaling = float(cavity_cfg.get("rescaling", 1.0))
    pml_thickness = float(cfg.get("pml_thickness", 2.0 * rescaling))
    t1 = float(cavity_cfg.get("t1", 0.125)) * rescaling
    t2 = float(cavity_cfg.get("t2", 0.25)) * rescaling
    n1 = float(cavity_cfg.get("n1", 2.0))
    n2 = float(cavity_cfg.get("n2", 1.0))
    nlayer = int(cavity_cfg.get("nlayer", 5))
    center_thickness = float(cavity_cfg.get("center_thickness", 0.5)) * rescaling

    layer_indexes = np.array([n2, n1] * nlayer + [1.0] + [n1, n2] * nlayer)
    layer_thicknesses = np.array(
        [t2, t1] * nlayer + [center_thickness] + [t1, t2] * nlayer
    )

    layer_thicknesses[0] += pml_thickness
    layer_thicknesses[-1] += pml_thickness

    length = float(np.sum(layer_thicknesses))
    layer_centers = np.cumsum(layer_thicknesses) - layer_thicknesses / 2.0
    layer_centers = layer_centers - length / 2.0

    cell_size = mp.Vector3(length, 0, 0)
    pml_layers = [mp.PML(thickness=pml_thickness)]
    geometry = [
        mp.Block(
            mp.Vector3(float(layer_thicknesses[i]), mp.inf, mp.inf),
            center=mp.Vector3(float(layer_centers[i]), 0, 0),
            material=mp.Medium(index=float(layer_indexes[i])),
        )
        for i in range(layer_thicknesses.size)
    ]

    return cell_size, geometry, pml_layers


def _save_history_npz(molecule: mxl.Molecule, out_path: Path) -> None:
    rows = list(molecule.additional_data_history)
    if not rows:
        return

    keys = [
        "time_au",
        "mux_au",
        "muy_au",
        "muz_au",
        "mux_m_au",
        "muy_m_au",
        "muz_m_au",
        "energy_au",
        "ke_au",
        "pe_au",
        "temp_K",
    ]
    payload = {
        key: np.array([float(frame.get(key, 0.0)) for frame in rows], dtype=float)
        for key in keys
    }
    np.savez(out_path, **payload)


def main() -> None:
    cfg = _load_config()
    hub_cfg = cfg.get("hub", {})
    mol_cfg = cfg["molecule"]

    port = _pick_free_port("0.0.0.0")
    host_for_clients = os.environ.get("MXL_HOST", socket.gethostname())

    host_port_file = Path(__file__).resolve().with_name(str(cfg["host_port_file"]))
    _write_host_port(host_port_file, host_for_clients, port)

    hub = mxl.SocketHub(
        host="",
        port=port,
        timeout=float(hub_cfg.get("timeout", 200.0)),
        latency=float(hub_cfg.get("latency", 0.001)),
    )
    print(f"Hub listening on {host_for_clients}:{port}")

    molecule = mxl.Molecule(
        hub=hub,
        center=mp.Vector3(*mol_cfg["center"]),
        size=mp.Vector3(*mol_cfg["size"]),
        sigma=float(mol_cfg["sigma"]),
        dimensions=int(mol_cfg["dimensions"]),
        rescaling_factor=float(mol_cfg.get("rescaling_factor", 1.0)),
    )

    cell_size, geometry, pml_layers = _build_meep_geometry(cfg)

    sim = mxl.MeepSimulation(
        hub=hub,
        molecules=[molecule],
        time_units_fs=float(cfg["time_units_fs"]),
        cell_size=cell_size,
        geometry=geometry,
        boundary_layers=pml_layers,
        resolution=int(cfg["resolution"]),
    )

    if "steps" in cfg:
        sim.run(steps=int(cfg["steps"]))
    else:
        sim.run(until=float(cfg["until"]))

    if mp.am_master():
        out_npz = Path(__file__).resolve().with_name(
            str(cfg.get("output_npz", "lammps_history.npz"))
        )
        _save_history_npz(molecule, out_npz)
        print(f"Wrote LAMMPS history: {out_npz}")


if __name__ == "__main__":
    main()
