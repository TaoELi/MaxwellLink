MaxwellLink
======================================

.. image:: ../../media/icon_main.png
   :alt: MaxwellLink icon
   :align: center
   :scale: 20

**MaxwellLink** provides a flexible and general platform for self-consistent light-matter simulations. It
connects various electromagnetic (EM) solvers, such as the finite-difference time-domain (FDTD) approach, to
a range of quantum and molecular dynamics packages. The light and matter solvers can run either
within the same process or on separate computing nodes communicating through a TCP/Unix socket interface,
enabling self-consistent light-matter simulations at different scales and levels of theory.

Use this documentation to install the **MaxwellLink** package, and run sophisticated self-consistent light-matter simulations. For developers, the :doc:`architecture` and :doc:`contributing` sections provide an 
overview of the code structure and how to extend the framework with custom solvers, respectively.

**MaxwellLink** now also supports :doc:`agent_skills`: With simple natural language inputs, users can easily create input files and run jobs on both local machines and HPC systems.


.. toctree::
   :maxdepth: 1
   :caption: Get Started

   introduction
   installation
   usage
   agent_skills

.. toctree::
   :maxdepth: 1
   :caption: Tutorials

   tutorials/index

.. toctree::
   :maxdepth: 1
   :caption: Learn More

   architecture
   em_solvers/index
   drivers/index
   contributing

.. toctree::
   :maxdepth: 1
   :caption: Reference

   API reference <api/modules>

Citation
--------

If you use **MaxwellLink** in your research, please cite:

.. admonition:: Reference
   
   X Ji †, AF Bocanegra Vargas †, G Meng, and TE Li. MaxwellLink: A Unified Framework for Self-Consistent Light-Matter Simulations. `J. Chem. Theory Comput. 22, 2725–2738 (2026) <https://doi.org/10.1021/acs.jctc.5c02028>`_.
