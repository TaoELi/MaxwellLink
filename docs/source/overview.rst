MaxwellLink
======================================

.. image:: ../../media/icon_main.png
   :alt: MaxwellLink icon
   :align: center
   :scale: 20

**MaxwellLink** provides a flexible and general platform for self-consistent light-matter simulations. It
couples various electromagnetics (EM) solvers, such as finite-difference time-domain (FDTD) approach, with
a hierarchy of quantum and molecular dynamics packages. The light and matter solvers can run either
within the same process or in separate computing nodes communicating through a TCP/Unix socket interface, 
thus enabling productive light-matter simulations at different scales and levels of theory.

Use this documentation to install the **MaxwellLink** package, run your first self-consistent light-matter simulation, and
explore the available EM solvers and molecular drivers. For developers, the :doc:`architecture` and :doc:`contributing` sections provide an 
overview of the code structure, design principles, and how to extend the framework with custom solvers.

**MaxwellLink** now also supports :doc:`agent_skills` --- with simple natural language inputs, users can easily create the input files and run jobs in both local machines and HPC systems.


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

   api/modules

Citation
--------

If you use **MaxwellLink** in your research, please cite:

.. admonition:: Reference
   
   X Ji †, AF Bocanegra Vargas †, G Meng, and TE Li. MaxwellLink: A Unified Framework for Self-Consistent Light-Matter Simulations. arXiv:2512.06173 (2025).
