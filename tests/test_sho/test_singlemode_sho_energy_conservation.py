# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

import numpy as np
import pytest

mxl = pytest.importorskip("maxwelllink", reason="maxwelllink is required for this test")


@pytest.mark.core
def test_singlemode_sho_energy_conservation(
    plotting=False,
):
    """
    End-to-end (non-socket) strong coupling energy conservation test.

    Starts the mxl_driver (SHO model), runs a single mode simulation that
    couples to the driver via MaxwellLink in the absence of sockets, 
    and collects the energy trace.

    Pass criteria (maximum energy deviation):
        max_abs_diff < 8e-5
    """

    # SHO physical parameters
    dipole_moment = 187
    frequency_au = 0.242

    # one non-socket molecule
    molecule = mxl.Molecule(
        driver="sho",
        driver_kwargs={
            "omega": frequency_au,
            "mu0": dipole_moment,
            "orientation": 2,
            "p_initial": 1e-2,
        }
    )


    coupling_strength = 3e-5
    dt_au = 0.5
    damping_au = 0e-4
    total_steps = 40960

    sim = mxl.SingleModeSimulation(
            molecules=[molecule],
            frequency_au=frequency_au,
            coupling_strength=coupling_strength,
            damping_au=damping_au,
            coupling_axis="z",
            drive=0.0,
            dt_au=dt_au,
            qc_initial=[0, 0, 0.0],
            record_history=True,
            # excluding dipole self-energy term for SHO model
            include_dse=True,
    )
    sim.run(steps=total_steps)

    energy_molecule = molecule.extra["energy_au"]
    energy_total = np.array(sim.energy_history)
    time = np.array(sim.time_history)

    if plotting:
        import matplotlib.pyplot as plt
        
        plt.plot(time, energy_total, label="total energy")
        # plt.plot(time, energy_molecule, label="molecule energy")
        plt.xlabel("Time (a.u.)")
        plt.ylabel("Energy (a.u.)")
        plt.legend()
        plt.show()

    max_abs_diff = np.max(np.abs(energy_total - energy_total[0]))

    print(f"Max absolute difference in energy: {max_abs_diff:.3g}")
    assert (max_abs_diff < 2e-7), f"max_abs_diff={max_abs_diff:.3g}"

if __name__ == "__main__":
    test_singlemode_sho_energy_conservation(plotting=True)
