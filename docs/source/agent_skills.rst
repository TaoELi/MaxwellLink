Agent Skills
============

Agent Skills for **MaxwellLink** provide a simple way to get started with this code. Using natural language prompts, users can easily create the input files and run jobs in both local machines and HPC systems. Users can then inspect the input files and modify them as needed for more customized simulations.

Prerequisites
~~~~~~~~~~~~~~

This feature requires to clone the MaxwellLink Github repository to access the skills folder. First, either in your local machines or HPC clusters, clone the repository and install **MaxwellLink**:

.. code-block:: bash

   git clone https://github.com/TaoELi/MaxwellLink.git
   cd MaxwellLink
   pip install .

Then, follow :doc:`installation` to install the third-party EM solver (MEEP FDTD) and molecular drivers.

After installation, you can use MaxwellLink's agent skills to scaffold and run light-matter simulations automatically.

Automated light-matter simulations on local machines
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Please watch the following `walkthrough video <https://www.youtube.com/watch?v=ttAvvNByMLg>`_ for an introduction to using MaxwellLink's agent skills with VS Code and the Codex extension:

.. youtube:: ttAvvNByMLg

To use MaxwellLink's agent skills on your local machine via VS Code and the Codex extension, follow these steps:

1. Open VS Code -> ``File`` -> ``Open Folder...`` -> select ``path/to/MaxwellLink``.
2. Install/enable the **Codex** extension (from Marketplace). Make sure the extension has access to the workspace.
3. Open the Codex chat panel (usually the side activity bar) and provide your prompt. The agent will load the relevant skills at ``skills/`` and try to accomplish your request.
4. When prompted, let the agent run the suggested terminal commands in VS Code's integrated terminal; it will create ``projects/YYYY-MM-DD-<name>/`` and create input files for **MaxwellLink**.

The above video tutorial uses the following input prompt:

.. code-block:: text

   In my local machine, run an initially weakly excited two-level system coupled to 2d vacuum using meep fdtd and plot the excited-state population dynamics


Automated light-matter simulations on Anvil HPC
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Using `Purdue Anvil HPC system <https://www.rcac.purdue.edu/knowledge/anvil>`_ (available via the `NSF ACCESS program <https://www.access-ci.org/>`_) as an example, you can also use MaxwellLink's agent skills to run light-matter simulations on remote HPC clusters via VS Code and the Codex extension.

Please watch the following `demo video <https://www.youtube.com/watch?v=c0PVxcvriDM>`_ for an introduction to using MaxwellLink's agent skills on HPC systems:

.. youtube:: c0PVxcvriDM

Different from the local machine setup, you need to **configure VS Code to connect to the remote HPC** system via SSH. Please refer to the official VS Code documentation for detailed instructions on setting up SSH connections:

`https://code.visualstudio.com/docs/remote/ssh <https://code.visualstudio.com/docs/remote/ssh>`_

After setting up the SSH connection, follow these steps:

1. Open VS Code -> ``File`` -> ``Open Folder...`` -> select ``path/to/MaxwellLink`` on the remote HPC system.
2. Install/enable the **Codex** extension (from Marketplace). Make sure the extension has access to the workspace.
3. Open the Codex chat panel (usually the side activity bar), switch mode to **Agent (full access)** (bottom panel setting). This will allow to run commands without approval for each command.
4. Provide your prompt. The agent will load the relevant skills at ``skills/`` and try to accomplish your request.
5. When prompted, let the agent run the suggested terminal commands in VS Code's integrated terminal; it will create ``projects/YYYY-MM-DD-<name>/`` and create input files for **MaxwellLink**.

The above video tutorial uses the following input prompt:

.. code-block:: text

   In this Anvil HPC system, run a slurm job of an initially weakly excited two-level system coupled to 2d vacuum using meep fdtd and then plot the excited-state population dynamics after the slurm job


Customizing HPC settings
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Users can modify the HPC settings in the ``skills/mxl-hpc-slurm/resources/hpc_setting.md`` file to match their HPC system requirements. The default configuration is set up for the Anvil HPC system. Users can change parameters accordingly for different HPC systems.
