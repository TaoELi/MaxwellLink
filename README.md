<p align="center">
  <img src="media/icon_main.png" alt="MaxwellLink icon" width="300">
</p>

<p align="center">
  <a href="https://taoeli.github.io/MaxwellLink/overview.html"><img src="https://img.shields.io/badge/docs-latest-blue.svg" alt="Docs badge"></a>
  <a href="https://pypi.org/project/maxwelllink/"><img src="https://img.shields.io/pypi/v/maxwelllink.svg?label=pypi&logo=pypi" alt="PyPI version"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-GPLv2-blue.svg" alt="License: GPLv2"></a>
  <img src="https://img.shields.io/badge/python-3.9%2B-brightgreen.svg" alt="Python versions">
</p>


**MaxwellLink** is a free and open-source framework for self-consistent light–matter simulations. It bridges electromagnetic solvers, such as [MEEP FDTD](https://meep.readthedocs.io/en/latest/) or the built-in single-mode cavity, with heterogeneous molecular drivers spanning from multilevel open quantum systems, force-field molecular mechanics, and (nonadiabatic) first-principles molecular dynamics. 

This code can be used for both demonstrative and production calculation purposes. Particularly, with a socket-based architecture, large-scale self-consistent light-matter simulations can be performed efficiently accross multiple HPC nodes.

The latest version of **MaxwellLink** (v0.3) ships with [**AI Agent Skills**](https://taoeli.github.io/MaxwellLink/agent_skills.html). With simple natural language inputs, users can easily create the input files and run jobs in both local machines and HPC systems.

## Key Features

- **Embracing state-of-the-art ecosystems** in both computational electrodynamics and quantum chemistry, extending the boundary of light-matter simulations.
- **Unified Python interfaces** for socket-connected and embedded molecular drivers in light-matter simulations.
- **Heterogeneous molecular theories** including TLS, [QuTiP](https://qutip.org/) model Hamiltonians, in-house RT-TDDFT/Ehrenfest dynamics using [Psi4](https://psicode.org/) integrals, [ASE](https://wiki.fysik.dtu.dk/ase/) classical dynamics, and modified [LAMMPS](https://www.lammps.org/) via `fix mxl`, all in one EM simulation.
- **Extensible code structure** to add custom EM solvers or molecular drivers with minimal efforts.
- **Embedded AI Agent Skills** to allow users chat within, e.g., [Visual Code IDE + Codex](https://taoeli.github.io/MaxwellLink/agent_skills.html), to directly generate desired input files and even run jobs in both local machines and HPC systems.

## Quick Start

Create a fresh conda environment and install using *pip*:

```bash
pip install maxwelllink
```

Optional drivers ([MEEP FDTD](https://meep.readthedocs.io/en/latest/), [QuTiP](https://qutip.org/), [Psi4](https://psicode.org/), [ASE](https://wiki.fysik.dtu.dk/ase/), [LAMMPS](https://www.lammps.org/)) can be added by following the instructions in the [documentation](https://taoeli.github.io/MaxwellLink/overview.html).

## Documentation

Visit the [documentation](https://taoeli.github.io/MaxwellLink/overview.html) for installation details, tutorials, API reference, and guidelines on extending **MaxwellLink**.

## Tutorials

The jupyter notebook tutorials are located at [tutorials/](./tutorials/). Users may also view the tutorials rendered at the [documentation website](https://taoeli.github.io/MaxwellLink/tutorials/index.html).

## Citation

If you find **MaxwellLink** helpful for your research, please cite the following reference:

- X Ji †, AF Bocanegra Vargas †, G Meng, and TE Li. *MaxwellLink: A Unified Framework for Self-Consistent Light-Matter Simulations*. [arXiv:2512.06173](https://arxiv.org/abs/2512.06173) (2025). [[data]](https://github.com/TaoELi/maxwelllink_examples)
