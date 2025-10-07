import numpy as np
import pytest

mp = pytest.importorskip("meep", reason="MEEP/pymeep is required for this test")
mxl = pytest.importorskip("maxwelllink", reason="maxwelllink is required for this test")


@pytest.mark.slow
def test_ntls_relaxation_matches_analytical(plotting=False):
    """
    Numerically integrate TLS population relaxation and compare
    against the analytical golden-rule rate in 2D.

    Pass criteria (normalized to initial pop):
        std_dev < 3e-3 and max_abs_diff < 8e-3
    """
    # --- simulation setup in vacuum ---
    cell = mp.Vector3(8, 8, 0)
    geometry = []
    sources_non_molecule = []
    pml_layers = [mp.PML(3.0)]
    resolution = 10

    # TLS definition
    n_tls = 10
    dipole_moment = 1e-1 / np.sqrt(n_tls)
    frequency = 1.0
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
        # small initial excited-state population
        tls.reset_tls_population(1e-4)
        tls_lst.append(tls)

    sim = mp.Simulation(
        cell_size=cell,
        geometry=geometry,
        sources=sources_non_molecule,
        boundary_layers=pml_layers,
        resolution=resolution,
    )

    # run coupled update (no sockets path)
    sim.run(
        mxl.update_molecules_no_socket(sources_non_molecule=[], molecules=tls_lst),
        until=90,
    )

    # Only rank 0 collects/asserts (safe under MPI or serial)
    if mp.am_master():
        tls = tls_lst[0]  # check only the first TLS
        population = np.array([np.real(ad["Pe"]) for ad in tls.additional_data_history])
        time = np.array([np.real(ad["time"]) for ad in tls.additional_data_history])

        # analytical golden-rule rate in 2D
        gamma = dipole_moment**2 * (frequency) ** 2 / 2.0 * n_tls
        population_analytical = population[0] * np.exp(-time * gamma)

        std_dev = np.std(population - population_analytical) / population[0]
        max_abs_diff = (
            np.max(np.abs(population - population_analytical)) / population[0]
        )

        if plotting:
            import matplotlib.pyplot as plt

            plt.plot(time, population, label="meep")
            plt.plot(time, population_analytical, label="analytical")
            plt.xlabel("time")
            plt.ylabel("excited population")
            plt.legend()
            plt.show()

        assert (
            std_dev < 3e-3 and max_abs_diff < 8e-3
        ), f"std_dev={std_dev:.3g}, max_abs_diff={max_abs_diff:.3g}"


if __name__ == "__main__":
    test_ntls_relaxation_matches_analytical(plotting=True)
