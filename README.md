<p align="center">
  <img src="media/icon_main.png" alt="MaxwellLink icon" width="300">
</p>

<p align="center">
  <a href="https://taoeli.github.io/MaxwellLink/overview.html"><img src="https://img.shields.io/badge/docs-latest-blue.svg" alt="Docs badge"></a>
  <a href="https://pypi.org/project/maxwelllink/"><img src="https://img.shields.io/pypi/v/maxwelllink.svg?label=pypi&logo=pypi" alt="PyPI version"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-GPLv2-blue.svg" alt="License: GPLv2"></a>
  <img src="https://img.shields.io/badge/python-3.9%2B-brightgreen.svg" alt="Python versions">
  <a href="https://arxiv.org/abs/2512.06173"><img src="https://img.shields.io/badge/arXiv-2512.06173-b31b1b.svg" alt="arXiv:2512.06173"></a>
  <a href="https://doi.org/10.1021/acs.jctc.5c02028"><img src="https://img.shields.io/badge/DOI-10.1021%2Facs.jctc.5c02028-blue" alt="10.1021/acs.jctc.5c02028"></a>
</p>


**MaxwellLink** is a free and open-source framework for self-consistent light–matter simulations. It connects electromagnetic solvers, such as [MEEP FDTD](https://meep.readthedocs.io/en/latest/) or the built-in normal-mode cavity and laser driven dynamics, to a wide range of molecular drivers, from multilevel open quantum systems to (nonadiabatic) first-principles molecular dynamics.

This code supports both exploratory demonstrations and production-scale calculations. In particular, its socket-based architecture allows large-scale self-consistent light–matter simulations to run efficiently across multiple HPC nodes.

The latest version of **MaxwellLink** (v0.3) ships with [**AI Agent Skills**](https://taoeli.github.io/MaxwellLink/agent_skills.html). With simple natural language inputs, users can easily create input files and run jobs on both local machines and HPC systems.

## Key Features

- **Embracing state-of-the-art ecosystems** in both computational electrodynamics and quantum chemistry, extending the boundary of light-matter simulations.
- **Unified Python interfaces** for socket-connected and embedded molecular drivers in light-matter simulations.
- **EM dynamics** from simple to complex systems.
- **Heterogeneous molecular theories** in one simulation.
- **Extensible code structure** to add custom EM solvers or molecular drivers with minimal effort.
- **Embedded AI Agent Skills** to allow users to chat within, e.g., [Visual Code IDE + Codex](https://taoeli.github.io/MaxwellLink/agent_skills.html), to directly generate desired input files and even run jobs on both local machines and HPC systems.

## Supported light-matter simulation schemes

The most appealing feature of **MaxwellLink** is that it enables the self-consistent coupling of **one EM solver** plus **N different molecular drivers** concurrently. 

![MaxwellLink workflow](media/workflow.png)

The socket communication technique further allows distributed light-matter simulations on different machines and HPC nodes -- both the EM solver and molecular drivers can have their **own MPI parallel processes**.

As of today, the following [EM solvers](https://maxwelllink.org/em_solvers/index.html) and [molecular drivers](https://maxwelllink.org/drivers/index.html) are supported.

### Supported EM solvers

- Full-feature [MEEP FDTD](https://meep.readthedocs.io/en/latest/),
- Single-mode cavity, 
- Multimode Fabry-Perot cavity, and
- Arbitrary laser driven dynamics. 

### Supported molecular drivers

- Two-level systems and simple harmonic oscillators,
- [QuTiP](https://qutip.org/) model Hamiltonians, 
- In-house RT-TDDFT/Ehrenfest dynamics using [Psi4](https://psicode.org/) integrals, 
- [ASE](https://wiki.fysik.dtu.dk/ase/) classical dynamics, 
- Direct socket connection to modified [LAMMPS](https://github.com/TEL-Research/lammps) for classical MD via `fix mxl`, 
- (beta version) Direct socket connection to modified [DFTB+](https://github.com/TEL-Research/dftbplus) for tight-binding BOMD/real-time Ehrenfest dynamics.
- (beta version) Direct socket connection to modified [CP2K](https://github.com/TEL-Research/cp2k) for real-time TDDFT/Ehrenfest dynamcis in both non-periodic and periodic systems.

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

## Running simulations with AI Agents

Inspired by the recently developed [FermiLink agent framework](https://github.com/TaoELi/FermiLink), **MaxwellLink** now provides an elegant method for integrating with AI agents. All we need is to type in `mxl init` in a working directory:
```bash
mkdir myproject
cd myproject/
mxl init
```
Then we can interact with **any local AI agent** (Claude Code, OpenAI Codex, Gemini CLI, or their desktop apps, VS Code IDE extensions, etc) for autonomous light-matter simulations by simple natural language prompts.

`mxl init` will set up the package knowledge base (source code tree + agent skills layer) in your working directory for agent reasoning. After the simulation, we can simply clean up the package knowledge base by:
```bash
mxl clean
```

If your machine supports SLURM job management (such as HPCs), run the following command to set up the HPC environment, so the agent can automatically use the correct SLURM environments for large-scale HPC simulations.
```bash
mxl hpc
```

## Citation

If you find **MaxwellLink** helpful for your research, please cite the following reference:

- X Ji †, AF Bocanegra Vargas †, G Meng, and TE Li. *MaxwellLink: A Unified Framework for Self-Consistent Light-Matter Simulations*. [J. Chem. Theory Comput. ASAP](https://doi.org/10.1021/acs.jctc.5c02028) (2026). [[data](https://github.com/TaoELi/maxwelllink_examples)] [[arXiv:2512.06173](https://arxiv.org/abs/2512.06173)] 
