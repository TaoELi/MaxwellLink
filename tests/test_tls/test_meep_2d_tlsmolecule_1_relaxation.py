# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

import numpy as np
import pytest

mp = pytest.importorskip("meep", reason="MEEP/pymeep is required for this test")

import maxwelllink as mxl


@pytest.mark.core
def test_2d_1tls_relaxation_matches_analytical(plotting=False):
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
    dipole_moment = 1e-1
    frequency = 1.0
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
    tls.reset_tls_population(0.9)

    sim = mp.Simulation(
        cell_size=cell,
        geometry=geometry,
        sources=sources_non_molecule,
        boundary_layers=pml_layers,
        resolution=resolution,
    )

    # run coupled update (no sockets path)
    sim.run(
        mxl.update_molecules_no_socket(sources_non_molecule=[], molecules=[tls]),
        until=90,
    )

    # Only rank 0 collects/asserts (safe under MPI or serial)
    if mp.am_master():
        population = np.array([np.real(ad["Pe"]) for ad in tls.additional_data_history])
        time = np.array([np.real(ad["time"]) for ad in tls.additional_data_history])

        # analytical golden-rule rate in 2D
        gamma = dipole_moment**2 * (frequency) ** 2 / 2.0
        # this form is correct only near ground state
        population_analytical = population[0] * np.exp(-time * gamma)
        # this form is correct for all times [see https://journals.aps.org/pra/pdf/10.1103/PhysRevA.97.032105 Eq. A13]
        population_analytical = np.exp(-time * gamma) / (
            np.exp(-time * gamma) + (1.0 - population[0]) / population[0]
        )

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


@pytest.mark.core
def test_2d_1tls_relaxation_matches_analytical_v2(plotting=False):
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
    dipole_moment = 1e-1
    frequency = 1.0
    tls = mxl.Molecule(
        driver="tls",
        center=mp.Vector3(0, 0, 0),
        size=mp.Vector3(1, 1, 1),
        sigma=0.1,
        dimensions=2,
        driver_kwargs={
            "omega": 2.4188843e-1,
            "mu12": 1.870819866e2,
            "orientation": 2,
            "pe_initial": 0.9,
            "verbose": False,
        },
    )

    sim = mxl.MeepSimulation(
        cell_size=cell,
        geometry=geometry,
        sources=sources_non_molecule,
        boundary_layers=pml_layers,
        resolution=resolution,
        time_units_fs=0.1,
        molecules=[tls],
        hub=None,
    )

    # run coupled update (no sockets path)
    sim.run(until=90)

    # Only rank 0 collects/asserts (safe under MPI or serial)
    if mp.am_master():
        population = np.array([np.real(ad["Pe"]) for ad in tls.additional_data_history])
        time = np.array([np.real(ad["time_au"]) for ad in tls.additional_data_history])
        time *= 0.02418884254 / 0.1  # convert to fs

        # analytical golden-rule rate in 2D
        gamma = dipole_moment**2 * (frequency) ** 2 / 2.0
        # this form is correct only near ground state
        population_analytical = population[0] * np.exp(-time * gamma)
        # this form is correct for all times [see https://journals.aps.org/pra/pdf/10.1103/PhysRevA.97.032105 Eq. A13]
        population_analytical = np.exp(-time * gamma) / (
            np.exp(-time * gamma) + (1.0 - population[0]) / population[0]
        )

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
    test_2d_1tls_relaxation_matches_analytical_v2(plotting=True)
