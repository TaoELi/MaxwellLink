import numpy as np
import meep as mp
import maxwelllink as mxl
import matplotlib.pyplot as plt
import sys

cell = mp.Vector3(8, 8, 0)
geometry = []
sources_non_molecule = []
pml_layers = [mp.PML(3.0)]
resolution = 10

# define a TLS molecule
dipole_moment = 1e-1
frequency = 1.0
n_tls = int(sys.argv[1]) if len(sys.argv) > 1 else 1
tls_lst = []
for idx in range(n_tls):
    tls = mxl.TLSMolecule(
    resolution=resolution,
    center=mp.Vector3(0, 0, 0),
    size=mp.Vector3(1, 1, 1),
    frequency=frequency,
    dipole_moment=dipole_moment,
    sigma=0.1,
    dimensions=2,
    orientation=mp.Ez,
    )

    # switch from ground state to excited-state population with 1e-4
    tls.reset_tls_population(1e-4)
    tls_lst.append(tls)

sim = mp.Simulation(
    cell_size=cell,
    geometry=geometry,
    sources=sources_non_molecule,
    boundary_layers=pml_layers,
    resolution=resolution,
)

sim.run(
    mxl.update_molecules_no_socket(sources_non_molecule=[], molecules=tls_lst), until=2000
)

# if we are running this script directly, plot the results
if __name__ == "__main__" and mp.am_master():

    tls = tls_lst[0]  # check only the first TLS

    print("final population:", tls.additional_data_history[-1]["Pe"].real)

    # plot the TLS population relaxation dynamics
    dt = 0.5 / resolution
    population = np.array(
        [
            np.real(additional_data["Pe"])
            for additional_data in tls.additional_data_history
        ]
    )
    time = np.array(
        [
            np.real(additional_data["time"])
            for additional_data in tls.additional_data_history
        ]
    )
    # analytical golden rule rate in 2D
    gamma = dipole_moment**2 * (frequency) ** 2 / 2.0

    population_analytical = population[0] * np.exp(-time * gamma)

    # calculate the standard deviation of the difference
    std_dev = np.std(population - population_analytical) / population[0]
    print("Standard deviation of the difference / population[0]:", std_dev)
    # calculate the maximum absolute difference
    max_abs_diff = np.max(np.abs(population - population_analytical)) / population[0]
    print("Maximum absolute difference / population[0]:", max_abs_diff)
    assert std_dev < 0.003 and max_abs_diff < 0.008

    plt.figure()
    plt.plot(time, population, label="Numerical")
    plt.plot(time, population_analytical, label="Analytical", linestyle="--")
    plt.xlabel("Time")
    plt.ylabel("Excited State Population")
    plt.title("TLS Population Relaxation Dynamics")
    plt.legend()
    plt.grid()
    plt.show()
