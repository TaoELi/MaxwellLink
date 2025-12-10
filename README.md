<p align="center">
  <img src="media/icon.png" alt="MaxwellLink icon" width="400">
</p>

<p align="center">
  <a href="https://taoeli.github.io/MaxwellLink/">
    <img src="https://img.shields.io/badge/docs-latest-blue.svg" alt="Docs badge">
  </a>
  <a href="https://pypi.org/project/maxwelllink/">
    <img src="https://img.shields.io/pypi/v/maxwelllink.svg?label=pypi&logo=pypi" alt="PyPI version">
  </a>
  <a href="LICENSE">
    <img src="https://img.shields.io/badge/license-GPLv2-blue.svg" alt="License: GPLv2">
  </a>
  <img src="https://img.shields.io/badge/python-3.9%2B-brightgreen.svg" alt="Python versions">
</p>

**MaxwellLink** is a free and open-source framework for self-consistent light–matter simulations. It bridges electromagnetic solvers, such as [MEEP FDTD](https://meep.readthedocs.io/en/latest/) or the built-in single-mode cavity, with heterogeneous molecular drivers spanning from multilevel open quantum systems, force-field molecular mechanics, and (nonadiabatic) first-principles molecular dynamics. This code can be used for both demonstrative and production calculation purposes. Particularly, with a socket-based architecture, large-scale self-consistent light-matter simulations can be performed efficiently accross multiple HPC nodes.

## Key Features

- **Embracing state-of-the-art ecosystems** in both computational electrodynamics and quantum chemistry, extending the boundary of light-matter simulations.
- **Unified Python interfaces** for socket-connected and embedded molecular drivers in light-matter simulations.
- **Heterogeneous molecular theories** including TLS, [QuTiP](https://qutip.org/) model Hamiltonians, in-house RT-TDDFT/Ehrenfest dynamics using [Psi4](https://psicode.org/) integrals, [ASE](https://wiki.fysik.dtu.dk/ase/) classical dynamics, and modified [LAMMPS](https://www.lammps.org/) via `fix mxl`, all in one EM simulation.
- **Extensible code structure** to add custom EM solvers or molecular drivers with minimal efforts.

## Quick Start

Create a fresh conda environment and install using *pip*:

```bash
pip install maxwelllink
```

Optional drivers ([MEEP FDTD](https://meep.readthedocs.io/en/latest/), [QuTiP](https://qutip.org/), [Psi4](https://psicode.org/), [ASE](https://wiki.fysik.dtu.dk/ase/), [LAMMPS](https://www.lammps.org/)) can be added by following the instructions in the [documentation]((https://taoeli.github.io/MaxwellLink/)).

## Documentation

Visit the [documentation](https://taoeli.github.io/MaxwellLink/) for installation details, tutorials, API reference, and guidelines on extending **MaxwellLink**.

## Tutorials

The jupyter notebook tutorials are located at [tutorials/](./tutorials/). Users may also view the tutorials rendered at the [documentation website](https://taoeli.github.io/MaxwellLink/tutorials/index.html).

## Citation

If **MaxwellLink** helps your research, please cite this project together with the coupled solvers you employ (e.g., Meep, Psi4, QuTiP, ASE, LAMMPS).

- X Ji †, AF Bocanegra Vargas †, G Meng, and TE Li. *MaxwellLink: A Unified Framework for Self-Consistent Light-Matter Simulations*. [arXiv:2512.06173](https://arxiv.org/abs/2512.06173) (2025). [[data]](https://github.com/TaoELi/maxwelllink_examples)
