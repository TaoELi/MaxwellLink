---
name: mxl-matter-md
description: This skill should be used when users want standalone classical molecular dynamics with MaxwellLink's built-in MD engine (no EM solver, no sockets) via `from maxwelllink.matter import MD` — building a single MD cell (flexible water q-TIP4P/F, flexible CO2, COMPASS class2 CS2 and CH4, or a user-defined force field), NVT thermal equilibration, NVE/NVT production runs on the CPU or one GPU, and IR spectra from the dipole trajectory.
---

# Standalone classical MD (`maxwelllink.matter.MD`)

## When to use which tool
- Pure classical MD of one cell (equilibrate, NVE/NVT, IR spectrum), no EM feedback: use `maxwelllink.matter.MD` (this skill).
- Self-consistent light-matter MD (e.g. vibrational strong coupling): the `md` socket driver (`MDModel`) with an EM solver such as `SingleModeSimulation` — NOT this module.
- Many replica cells batched on one GPU behind the aggregated hub: the `md` batch driver through `AggregatedSocketHub`.
- All three share one engine (`maxwelllink.matter.md.md_engine.MDEngine`), so numbers agree bit for bit between the standalone and socket paths at the same seed and molecule ID.

## Build one cell
```python
from maxwelllink.matter import MD

cell = MD(
    ff="qtip4pf",         # "qtip4pf" flexible water | "co2jcp2021" flexible CO2 |
                          # "cs2compass" flexible CS2 | "ch4compass" flexible CH4
                          # (defaults load bundled bulk boxes: 32 waters / 36 CO2 /
                          #  36 CS2 / 100 CH4, periodic)
    n_molecules=None,     # None = the force field's default box; 1 = one molecule
    xyz=None,             # optional starting geometry (Angstrom, force-field atom order)
    box=None,             # periodic box lengths in Bohr; None = finite cluster
    replicate=None,       # LAMMPS-style replicate: 8 -> 2x2x2 images, 4 -> 2x2x1,
                          # 6 -> 6x1x1, or (nx, ny, nz); same density, bigger cell
    dt_fs=0.5,            # integration time step
    temperature_K=300.0,  # initial Maxwell-Boltzmann velocities and default bath
    backend="cpu",        # "cpu" (default) | "gpu" | None (auto)
    seed=0,
)
from maxwelllink.matter import available_force_fields
print(available_force_fields())
```
- Atom order matters for `xyz`/`positions`: `[O, H, H, ...]` for water, `[C, O, O, ...]` for CO2, `[C, S, S, ...]` for CS2, `[C, H, H, H, H, ...]` for CH4; a wrong order raises at construction.
- `cs2compass` and `ch4compass` are the COMPASS class2 models (Sun 1998) of the LAMMPS `lj/class2/coul/long` + `bond_style class2` + `angle_style class2` reference runs: quartic bonds, class2 angles with bond-bond (and, for CH4, bond-angle) coupling, 9-6 Lennard-Jones (sixth-power mixing for CS2, explicit C-H pair for CH4), point charges (CS2: C +0.0516; CH4: C -0.24, H +0.06). Bonded + LJ energies/forces reproduce LAMMPS to machine precision, the full forces agree with a converged LAMMPS Ewald sum to ~1e-8 (CS2) / ~1e-7 (CH4) Ha/Bohr, and differ from the production `pppm 1e-4` runs only by the PPPM grid error (~1e-5 Ha/Bohr). Liquid CH4 runs at 110 K (`temperature_K=110`), CS2 at 300 K. In the GPU batch driver: `--param "ff=cs2compass,thermostat=nvt,friction_fs=5000,temperature_K=300.0,pre_nvt=True,pre_nvt_duration_ps=10.0"` (and `ff=ch4compass,...,temperature_K=110.0`).
- `replicate` tiles the periodic cell like the LAMMPS `replicate` command: one integer is the *total* number of images, laid out as a cube when it is a cubic number (`27` -> 3x3x3), else as a square (`4` -> 2x2x1), else as a row (`6` -> 6x1x1); a 3-tuple gives the images per axis. `n_molecules`/`xyz`/`positions`/`box` describe the cell *before* replication, so `MD(ff="co2jcp2021", replicate=8)` is 288 CO2 in a 50.5 Bohr box at the bundled density. The same keyword works in the socket driver (`mxl_driver --model md --param "ff=qtip4pf, replicate=8"`) and in the GPU batch driver (`driver_kwargs={"ff": "qtip4pf", "replicate": 8}`); a ready-made force-field instance takes it in its own constructor instead. Only the scalar form fits on the `--param` command line (it splits on commas); a periodic box is required.
- Backend guidance: the GPU integrator assigns one CUDA block per system, so a *single* cell usually runs fastest on the CPU (`backend="cpu"`, compiled numba kernels). The GPU pays off in the many-cell batch driver, not here. Both `matter.MD` and the socket driver `MDModel` accept `backend="gpu"` (or `None` = auto) to force/auto-pick CuPy anyway; the aggregated-hub batch driver is GPU by default.
- `force_evaluator="numpy"` switches to the slow NumPy reference forces (validation, or when numba is unavailable).

## Equilibrate, run, analyze
```python
cell.equilibrate(duration_ps=10.0, temperature_K=300.0, friction_fs=100.0)  # Langevin NVT

result = cell.run(
    duration_ps=20.0,      # or n_steps=...
    thermostat="nve",      # "nve" for spectra | "nvt" for sampling
    record_every=1,        # thin the recorded arrays
    snapshot_every=None,   # optionally record geometries/velocities too
    field=None,            # optional E-field in a.u.: (3,), (n_steps, 3), or callable t_au -> (3,)
)

frequency_cm, intensity = result.ir_spectrum(component="total")  # or "x"|"y"|"z"
result.save("run.npz")
```
- `result` is an `MDResult` with `time_au`, `dipole_au`, `amplitude_au` (d mu/dt), `energy_au`, `potential_au`, `temperature_K`, the force field's `terms_au` (stretch/bend), and optional `snapshots`.
- `ir_spectrum()` demeans the dipole, autocorrelates and cosine-transforms it (`maxwelllink.tools.ir`); use a long NVE run of an equilibrated cell (tens of ps; `record_every=1`) for clean lineshapes.
- Successive `run()` calls continue the same trajectory; `equilibrate()` records nothing.
- Sanity checks: NVE total energy drift should be tiny (~1e-6 Ha scale for one CO2 at 0.25 fs); NVT mean `result.temperature_K` should hover near the target.

## User-defined force fields
Subclass `DummyForceField` (NumPy path is enough) and pass an *instance*:
```python
from maxwelllink.matter import MD
from maxwelllink.matter.md.force_fields import DummyForceField

class MyFF(DummyForceField):
    name = "myff"
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.masses = ...            # (na, 1), atomic units
    def _build_default_geometry(self, n_molecules, box): ...
    def compute(self, x, efield, terms=None):
        # return (forces (na,3), potential); forces must already include q_i * E
        ...
    def dipole(self, x): ...
    def dipole_velocity(self, v): ...

cell = MD(ff=MyFF())   # geometry keywords then belong to MyFF's constructor
```
- The instance form is the only hook for user-defined force fields; string names (`ff="qtip4pf"`) select built-ins only. The instance also works in the other paths when the model is built in-process: `MDModel(ff=MyFF())` for the socket driver, `run_bridge_node(..., driver_kwargs={"ff": MyFF()})` for the batch driver.
- Compiled CPU/GPU kernels are optional (`build_force_kernels`); without them the engine falls back to the NumPy reference and computes dipoles through `dipole()`/`dipole_velocity()`.

## Pitfalls
- `dt_fs=0.5` suits these flexible water/CO2/CS2/CH4 models (the LAMMPS references use 0.5 fs too); halve it if the NVE energy drifts.
- The reported dipole is relative to the starting geometry's dipole (`reset_dipole`); `ir_spectrum()` demeans anyway, so this never affects spectra.
- One water/CO2 *molecule* has large temperature fluctuations — judge NVT against the mean over many steps, or use the bulk boxes.
- GPU runs need CuPy + numba CUDA (`pip install maxwelllink[gpu-cuda12]`); on CUDA-less hosts keep `backend="cpu"`.
- Cutoffs under `replicate`: the LJ/pair cutoff `rcut` (nominal 9 Å for water, 18 Bohr for CO2, 14 Bohr for CS2, 10 Å for CH4) is clamped to half the shortest box side, i.e. 9.3 Bohr in the 32-water box, 12.6 Bohr in the 36-CO2 box and 17.35 Bohr in the 100-CH4 box (the 36-CS2 box, 14.49 Bohr half-side, keeps the nominal 14 Bohr); a replicated cell lifts the clamp back to the nominal value, which lowers the energy per molecule (CO2 -0.00191 -> -0.00254 Ha, water -0.01534 -> -0.01639 Ha) while changing forces by only ~1e-5 to 1e-4 Ha/Bohr. That is the force field's intended cutoff, so keep the default for production. For a size ladder that must stay comparable with the unreplicated cell, pass the unit-cell clamp explicitly (`rcut=12.63` for CO2, `rcut=9.315` for water) and leave `ewald_wrcut` alone: the Ewald real-space cutoff only sets the real/reciprocal split (forces move ~1e-6), and freezing it makes the k-space sum O(N^2) (1.5x slower at 8 images, 5x at 125).
- Cost of large cells: the force kernels are all-pairs O(N^2) per system (the CPU needs ~60 ms per force call at 864 atoms), so large replicated cells belong on the GPU batch driver. There, systems whose work arrays fit in 48 KB of shared memory (about 1300 water or 4000 CO2 atoms) run the fast shared-memory kernel; larger ones fall back to global-memory work arrays (10-15% slower per step at equal size) and are bounded only by device memory, at ~1 s/step for a single cell of tens of thousands of atoms.
