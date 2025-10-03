import numpy as np
import meep as mp
import maxwelllink as mxl
import matplotlib.pyplot as plt

cell = mp.Vector3(8, 8, 0)
geometry = []
sources_non_molecule = []
pml_layers = [mp.PML(3.0)]
resolution = 10

# define a TLS molecule
dipole_moment = 1e-1
frequency = 1.0

hub = mxl.SocketHub(unixsocket="mx10", timeout=10.0, latency=1e-5)

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

#proc = mxl.launch_driver(
#    command=f' --model tls --unix --address mx10 --param "omega=0.242, mu12=187, orientation=2, pe_initial=1e-4"'
#)

sim.run(
    mxl.update_molecules(hub=hub, sources_non_molecule=[], molecules=[molecule1]),
    until=2000,
)

# if we are running this script directly, plot the results
if __name__ == "__main__" and mp.am_master():

    # plot the TLS population relaxation dynamics
    population = np.array(
        [
            np.real(additional_data["Pe"])
            for additional_data in molecule1.additional_data_history
        ]
    )
    time_au = np.array(
        [
            np.real(additional_data["time_au"])
            for additional_data in molecule1.additional_data_history
        ]
    )
    time_mu = time_au * 0.02418884254 / 0.1  # convert to fs
    # analytical golden rule rate in 2D
    gamma = dipole_moment**2 * (frequency) ** 2 / 2.0

    population_analytical = population[0] * np.exp(-time_mu * gamma)

    # calculate the standard deviation of the difference
    std_dev = np.std(population - population_analytical) / population[0]
    print("Standard deviation of the difference / population[0]:", std_dev)
    # calculate the maximum absolute difference
    max_abs_diff = np.max(np.abs(population - population_analytical)) / population[0]

    print("Maximum absolute difference / population[0]:", max_abs_diff)
    assert std_dev < 0.003 and max_abs_diff < 0.008

    plt.figure()
    plt.plot(time_mu, population, label="Numerical")
    plt.plot(time_mu, population_analytical, label="Analytical", linestyle="--")
    plt.xlabel("Time [MEEP units (0.1 fs per unit time)]")
    plt.ylabel("Excited State Population")
    plt.title("TLS Population Relaxation Dynamics")
    plt.legend()
    plt.grid()
    plt.show()
