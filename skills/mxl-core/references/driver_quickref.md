# Driver quick reference

All Python drivers run via `mxl_driver --model <id> --param "k1=v1, ..."` (socket) or as embedded `Molecule(driver="<id>", driver_kwargs={...})`.

## TLS (`tls`)
- Use: lightweight two-level system benchmark. No extra deps.
- Key params: `omega` (a.u.), `mu12`, `orientation` (0:x,1:y,2:z), `pe_initial`, `checkpoint`, `restart`, `verbose`.
- Typical socket cmd: `mxl_driver --model tls --address <host> --port <port> --param "omega=0.242, mu12=187, orientation=2, pe_initial=1e-3"`

## QuTiP (`qutip`)
- Use: model Hamiltonians with optional Lindblad terms or custom module. Dep: `qutip`.
- Recommended CLI pattern (avoid nested comma parsing): pass TLS args as top-level tokens:
  - `mxl_driver --model qutip --port <port> --param "preset=tls, omega=0.242, mu12=187, orientation=2, pe_initial=1e-3"`
- Custom module pattern:
  - Relative (module next to the job): `mxl_driver --model qutip --port <port> --param "preset=custom, module=hcn_qutip.py"`
  - Absolute: `mxl_driver --model qutip --port <port> --param "preset=custom, module=/abs/path/spec.py"`

## RT-TDDFT (`rttddft`)
- Use: fixed-nuclei RT-TDDFT via Psi4. Dep: `psi4`.
- Key params: `molecule_xyz` (XYZ with charge/multiplicity line), `functional`, `basis`, `dt_rttddft_au`, `delta_kick_au`, `delta_kick_direction`, `electron_propagation=etrs|pc`, `threshold_pc`, `memory`, `num_threads`, `remove_permanent_dipole`, `checkpoint`, `restart`.
- Common HPC extras: `dft_grid_name=SG0|SG1`, `dft_radial_points`, `dft_spherical_points`.
- Example: `mxl_driver --model rttddft --port <port> --param "molecule_xyz=tests/data/hcn.xyz, functional=PBE0, basis=cc-pVDZ, dt_rttddft_au=0.04, dft_grid_name=SG1, memory=8GB, num_threads=4"`

## RT-Ehrenfest (`rtehrenfest`)
- Use: RT-TDDFT plus nuclear motion (Ehrenfest or BO). Dep: `psi4`.
- Key params: inherits RT-TDDFT plus `force_type=ehrenfest|bo`, `n_fock_per_nuc`, `n_elec_per_fock`, `mass_amu`, `friction_gamma_au`, `temperature_K`, `rng_seed`, `partial_charges`, `fix_nuclei_indices`, `save_xyz`, `homo_to_lumo`.
- CLI tip: when passing `partial_charges`, use a space-separated list in brackets (no commas), e.g. `partial_charges=[1.0 -1.0 0.0]`.

## ASE (`ase`)
- Use: Born–Oppenheimer MD with any ASE calculator (Psi4/ORCA/DFTB+/...). Dep: `ase` plus calculator deps.
- Key params: `atoms` (ASE-readable file or Atoms), `calculator` (psi4|orca|dftb|...), `calc_kwargs` (comma `k=v` list), `charges` (`"[0.3 -0.3 0.0]"`), `recompute_charges`, `temperature_K`, `checkpoint`, `restart`, `verbose`.
- Need charges via `charges` or `recompute_charges=true`; otherwise driver errors.
- Psi4 example (common HPC style): `mxl_driver --model ase --port <port> --param "atoms=tests/data/hcn.xyz, calculator=psi4, calc_kwargs=method=b3lyp, basis=cc-pvdz, memory=2GB, num_threads=1, charges=[-0.2467948 -0.00296554 0.24976034]"`
- Tip: keys not recognized by the ASE driver are forwarded to the calculator (so you can pass `basis`, `memory`, `num_threads`, etc. as top-level tokens).

## LAMMPS fix (`lammps`)
- Use: classical MD via `fix mxl`. Build helper `mxl_install_lammps` produces `lmp_mxl`. Dep: LAMMPS build with fix.
- In LAMMPS input: `fix 1 all mxl <host> <port> [unix] [reset_dipole]`.
- Hub must be reachable from the LAMMPS process; ensure atoms carry charges.
- Common SLURM pattern: driver job reads `tcp_host_port_info.txt` (written by main job) and runs `srun lmp_mxl -in in.lmp`.
- Preferred scaffold template for HPC: `skills/mxl-project-scaffold/assets/templates/slurm-meep-lammps-tcp`.
