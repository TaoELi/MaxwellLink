Agent Skills
============

Agent Skills for **MaxwellLink** provide a simple way to get started with this package. Using natural language prompts, users can easily create input files and run jobs on both local machines and HPC systems. Users can then inspect the input files and modify them as needed for more customized simulations.


Autonomous light-matter simulations with command line interfaces (CLIs)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Inspired by the recently developed `FermiLink agent framework <https://github.com/TaoELi/FermiLink>`, **MaxwellLink** now provides an elegant method for integrating with AI agents. All we need is to type in `mxl init` in a working directory:

.. code-block:: bash
   
   mkdir myproject
   cd myproject/
   mxl init

Then we can interact with **any local AI agent** (Claude Code, OpenAI Codex, Gemini CLI, or their desktop apps, VS Code IDE extensions, etc) for autonomous light-matter simulations by simple natural language prompts.

`mxl init` will set up the package knowledge base (source code tree + agent skills layer) in your working directory for agent reasoning. After the simulation, we can simply clean up the package knowledge base by:

.. code-block:: bash

   mxl clean

If your machine supports SLURM job management (such as HPCs), run the following command to set up the HPC environment, so the agent can automatically use the correct SLURM environments for large-scale HPC simulations.

.. code-block:: bash
   
   mxl hpc

Autonomous light-matter simulations on local machines
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Below is a demo of talking with commerical AI agents directly in the repository of **MaxwellLink**. The agent will read the source code and documentation, and then create input files and run simulations based on your natural language prompts.

Please watch the following `walkthrough video <https://www.youtube.com/watch?v=ttAvvNByMLg>`_ for an introduction to using MaxwellLink's agent skills with VS Code and the Codex extension:

.. youtube:: ttAvvNByMLg

The aboe video tutorial contains the following steps:

- Open VS Code -> ``File`` -> ``Open Folder...`` -> select ``path/to/MaxwellLink``.
- Install/enable the **Codex** extension (from Marketplace). Make sure the extension has access to the workspace.
- Open the Codex chat panel (usually the side activity bar) and provide your prompt. The agent will load the relevant skills at ``skills/`` and try to accomplish your request.
- When prompted, let the agent run the suggested terminal commands in VS Code's integrated terminal; it will create ``projects/YYYY-MM-DD-<name>/`` and create input files for **MaxwellLink**.

The above video tutorial uses the following input prompt:

.. code-block:: text

   In my local machine, run an initially weakly excited two-level system coupled to 2d vacuum using meep fdtd and plot the excited-state population dynamics


Autonomous light-matter simulations on Anvil HPC
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Using `Purdue Anvil HPC system <https://www.rcac.purdue.edu/knowledge/anvil>`_ (available via the `NSF ACCESS program <https://www.access-ci.org/>`_) as an example, we can also run MaxwellLink light-matter simulations.

Please watch the following `demo video <https://www.youtube.com/watch?v=c0PVxcvriDM>`_ for an introduction to using MaxwellLink's agent skills on HPC systems:

.. youtube:: c0PVxcvriDM

Different from the local machine setup, we need to **configure VS Code to connect to the remote HPC** system via SSH. Please refer to the official VS Code documentation for detailed instructions on setting up SSH connections:

`https://code.visualstudio.com/docs/remote/ssh <https://code.visualstudio.com/docs/remote/ssh>`_

After setting up the SSH connection, follow these steps:

- Open VS Code -> ``File`` -> ``Open Folder...`` -> select ``path/to/MaxwellLink`` on the remote HPC system.
- Install/enable the **Codex** extension (from Marketplace). Make sure the extension has access to the workspace.
- Open the Codex chat panel (usually the side activity bar), switch mode to **Agent (full access)** (bottom panel setting). This will allow to run commands without approval for each command.
- Provide your prompt. The agent will load the relevant skills at ``skills/`` and try to accomplish your request.
- When prompted, let the agent run the suggested terminal commands in VS Code's integrated terminal; it will create ``projects/YYYY-MM-DD-<name>/`` and create input files for **MaxwellLink**.

The above video tutorial uses the following input prompt:

.. code-block:: text

   In this Anvil HPC system, run a slurm job of an initially weakly excited two-level system coupled to 2d vacuum using meep fdtd and then plot the excited-state population dynamics after the slurm job



Testing AI agents within **MaxwellLink**
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default, ``pytest`` does not run unit tests for ``vibe simulations`` that require agent providers. To run these tests, first install ``codex`` CLI (the other agent providers are not supported in the unit tests), 
then make sure you have logged in with your OpenAI API key or account credentials. 

Run the unit tests with the ``agent`` marker:

.. code-block:: bash

   # run agent tests, default run on local machines
   pytest -m agent -v
   # run agent tests on local machines
   pytest -m agent --codex-prompts local -v
   # run agent tests on hpc machines
   pytest -m agent --codex-prompts hpc -v
   # run agent tests by skipping the passed tests in the last run
   pytest -m agent -v --codex-resume

.. note::

   A **passed** simulation here means that the agent was able to generate a working simulation script and get some results. It does **not guarantee** that the simulation results are **physically accurate**.


Running a batch of prompts
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Users can also use this unit test feature to automatically run a set of Codex prompts (each line corresponds to one independent prompt) stored in a text file:

.. code-block:: bash

   # run agent tests, default run on local machines
   pytest -m agent -v --codex-prompts-file path/to/your/prompts.txt

The output is stored at ``tests/test_agents/your_prompt_file_name/`` by default.
