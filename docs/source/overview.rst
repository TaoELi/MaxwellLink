MaxwellLink
======================================

.. image:: ../../media/icon.png
   :alt: MaxwellLink icon
   :align: center
   :scale: 50

**MaxwellLink** provides a flexible and general platform for self-consistent light-matter simulations. It
couples various electromagnetics (EM) solvers, such as finite-difference time-domain (FDTD) approach, with
a hierarchy of quantum and molecular dynamics packages. The light and matter solvers can run either
within the same process or in separate computing nodes communicating through a TCP/Unix socket interface, 
thus enabling productive light-matter simulations at different scales and levels of theory.

Use this documentation to install the **MaxwellLink** package, run your first self-consistent light-matter simulation, and
explore the available EM solvers and molecular drivers. For developers, the :doc:`architecture` and :doc:`contributing` sections provide an 
overview of the code structure, design principles, and how to extend the framework with custom solvers.

.. toctree::
   :maxdepth: 1
   :caption: Get Started

   introduction
   installation
   usage

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

   api/maxwelllink

Citation
--------

If you use **MaxwellLink** in your research, please cite:

.. admonition:: Reference

   Li, T. E. MaxwellLink: A framework for self-consistent light-matter simulations. To be submitted.
