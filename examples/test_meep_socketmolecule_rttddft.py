import meep as mp
import maxwelllink as mxl
import numpy as np
import matplotlib.pyplot as plt

cell = mp.Vector3(8, 8, 0)
geometry = []
sources_non_molecule = []
pml_layers = [mp.PML(3.0)]
resolution = 10


hub = mxl.SocketHub(
    host="localhost", port=31880, timeout=30.0, latency=1e-4
)

molecule1 = mxl.SocketMolecule(
    hub=hub,
    molecule_id=0,
    resolution=resolution,
    center=mp.Vector3(0, 0, 0),
    size=mp.Vector3(1, 1, 1),
    sigma=0.1,
    dimensions=2,
    time_units_fs=0.1,
)

sim = mp.Simulation(
    cell_size=cell,
    geometry=geometry,
    sources=sources_non_molecule,
    boundary_layers=pml_layers,
    resolution=resolution,
)

sim.run(
    mxl.update_molecules(hub=hub, sources_non_molecule=[], molecules=[molecule1]),
    until=90,
)

hub.stop()


# if we are running this script directly, plot the results
if __name__ == "__main__" and mp.am_master():
    time_au = np.array([additional_data["time_au"] for additional_data in molecule1.additional_data_history])
    mu_x_au = np.array([additional_data["mu_x_au"] for additional_data in molecule1.additional_data_history])
    mu_y_au = np.array([additional_data["mu_y_au"] for additional_data in molecule1.additional_data_history])
    mu_z_au = np.array([additional_data["mu_z_au"] for additional_data in molecule1.additional_data_history])
    plt.figure()
    plt.plot(time_au, mu_z_au)
    plt.xlabel("Time [a.u.]")
    plt.ylabel(r"$\mu_z$ [a.u.]")
    plt.title("RT-TDDFT Dipole Moment of HCN")
    plt.legend()
    plt.grid()
    plt.show()
