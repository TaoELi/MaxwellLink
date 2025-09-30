# LAMMPS driver connecting with FDTD engines via MaxwellLink

To use this functionality, after [installing MaxwellLink](../../../../README.md#install-from-source), please run
```bash
mxl_install_lammps
```
and then a modified LAMMPS executable file **lmp_mxl** will appear in your local $PATH.

Althernatively, for users faimilar with LAMMPS install, please copy **fix_maxwelllink.h** and **fix_maxwelllink.cpp** to the LAMMPS source code (lammps/src/MISC/) and then recompile LAMMPS.

Our modified LAMMPS can connect with MaxwellLink SocketHub via the following fix (similar as **fix ipi**):
```bash
fix 1 all mxl host port
```
Here, **host** is the IP address of the machine where the FDTD engine is running (such as **localhost**), and **port** should match the port number in **SocketHub**. Please check [../README.md](../README.md) for details regarding setting the **SocketHub**.

