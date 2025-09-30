# MaxwellLink

**MaxwellLink** is a flexible framework for self-consistent EM-molecular simulations via a socket protocol. In detail, it provides a socket communication interface between an external FDTD engine and molecular dynamics packages (**MaxwellLink**). With this socket interface, the Maxwell's equations are propagated by the external FDTD engine, whereas the molecular dynamics are taken care by exisiting quantum or classical molecular dynamics pakcages. Overall, the socket communication decouples the external FDTD engine from the molecular drivers, enabling the self-consistent simulation of EM interacting with **a wide range of molecular or material systems**.


At each time step of the EM propagation, **MaxwellLink** sends the E-field vector at the molecular locations via the **socket communication**, and the molecular driver uses the E-field vector information to propagate the molecular dynamics for one FDTD time step and then return the instantaneous time derivatives of the dipole moment vector to FDTD via **socket**. The returned quantities are used to propagate Maxwell's equations for the next time step in FDTD. 

Our implementation supports the connection between the FDTD engine with multiple molecular drivers at the same time. Moreover, because FDTD is unaware of which theory the drivers use for propagating the molecular dynamics, hetrogenenous molecular drivers using different theory can be connected to the same FDTD engine simutaneously. This highly flexible self-consistent EM-Molecule framework may serve as an advantageous tool in many fields.

## Install from source
Assuming Anaconda is installed, MaxwellLink can be installed from source to access the latest features:
```bash
# create a new conda environment
CONDA_ENV="mxl_build"
# install MEEP for the FDTD engine (the only supported FDTD engine for now)
conda create -n $CONDA_ENV -c conda-forge pymeep

# install MaxwellLink
git clone git@github.com:TaoELi/MaxwellLink.git
cd PATH_TO_MAXWELLLINK/
pip install .

# [optional] install Psi4 quantum chemistry code for RT-TDDFT driver
conda install conda-forge::psi4

# [optional] install modified LAMMPS code (with fix mxl support) for classical MD driver
# the command below works after installing MaxwellLink with pip install .
mxl_install_lammps
# If the above command fails, please try the bash code below instead for installing lammps [working for Linux and MacOS]
# bash ./src/maxwelllink/mxl_drivers/lammps/mxl_install_lammps.sh
```

## Uninstall 
```bash
pip uninstall maxwelllink
```

## Test
```bash
pytest -v
```

## Mimimum python code for using the socket interface (using MEEP as the external FDTD engine)

```python
import meep as mp
import numpy as np
import maxwelllink as mxl

cell = mp.Vector3(18, 18, 0)
geometry = []
sources_non_molecule = []

pml_layers = [mp.PML(2.0)]
resolution = 10

# Initialize the SocketHub
# If the drivers and MEEP are running in different computing nodes, 
# please set host="", latency=1e-3 or 1e-2 (depending on internet connection)
hub = mxl.SocketHub(host="localhost", port=31415, timeout=10.0, latency=1e-4) 

# Define a SocketMolecule with the location and size
molecule1 = mxl.SocketMolecule(hub=hub, 
                             molecule_id=0, 
                             resolution=resolution, 
                             center=mp.Vector3(0, 0, 0), 
                             size=mp.Vector3(1, 1, 1), 
                             sigma=1.0, 
                             dimensions=2, 
                             time_units_fs=0.1,
                             rescaling_factor=1)
# we can add more molecules...

sim = mp.Simulation(cell_size=cell,
                   geometry=geometry,
                   sources=sources_non_molecule,
                   boundary_layers=pml_layers,
                   resolution=resolution)

# Run with all molecules synchronized each step. Please provide all molecules in [molecule1, ...]
sim.run(mxl.update_molecules(hub, [molecule1], sources_non_molecule), until=10)

# Retrieve molecular data after the simulation
# Different molecular drivers may have different output data
t = np.array([np.real(additional_data["time_au"]) for additional_data in molecule1.additional_data_history]).flatten()
mu_z_au = np.array([np.real(additional_data["mu_z_au"]) for additional_data in molecule1.additional_data_history]).flatten()
```

## Usage of available molecular drivers

After running the above python code, we need to lunch the driver code in a separate terminal for connecting with FDTD. At this moment, both the **Python** and **C++** drivers are available for usage. 

To use the Python driver, please check 
```bash
which mxl_driver.py
```
and see whether **mxl_driver.py** has been installed properly. 

The molecular systems can be described by the following three levels of theory. 

#### **Model systems**

An **electronic two-level system (tls)** model is provided in the **Python driver**. 

A sample bash input for setting up the **tls** model is as follows: 

```bash
mxl_driver.py --model tls --port 31415 --param "omega=0.242, mu12=187, orientation=2, pe_initial=0.01" --verbose
```

Please check [python/](./src/maxwelllink/mxl_drivers/python/) for detailed usage. 


#### **Nonadiabatic electronic dynamics**

A **real-time time-dependent density functional theory (rttddft)** model is provided in the Python driver using the Psi4 interface.

A sample bash input for setting up the **rttddft** model is as follows: 

```bash
mxl_driver.py --model rttddft --port 31415 --param "molecule_xyz=PATH_TO_MaxwellLink/tests/data/hcn.xyz, checkpoint=false, restart=false" --verbose
```

 Please check [python/](./src/maxwelllink/mxl_drivers/python/) for detailed usage. 

#### **Classical force fields**

Connecting to the LAMMPS classical molecular dynamics code using **fix mxl** is provided. Please check [lammps/](./src/maxwelllink/mxl_drivers/lammps/) for details.

We are also working on connecting with more FDTD engines and molecular drivers. Please stay tuned!


