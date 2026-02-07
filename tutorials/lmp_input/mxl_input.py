# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

import numpy as np
import maxwelllink as mxl

address = "socket_cavmd"
hub = mxl.SocketHub(unixsocket=address, timeout=10.0, latency=1e-5)

# the rescaling 0.73 is to account for the difference between the TIP4P water model verus that from a straightforward
# sum_i Q_i * r_i calculation, where the oxygen atom is used for the position instead of using the M charge site.
# In other words, the dipole and dmudt calculation from MD driver needs to be rescaled by 0.73 to match the real TIP4P
# water model values (used in i-pi CavMD).
molecule = mxl.Molecule(
    hub=hub,
    rescaling_factor=0.73,
)

au_to_cminverse = 219474.63

frequency_au = 3550 / au_to_cminverse
coupling_strength = 4e-4
print(f"Coupling strength: {coupling_strength:.3e} au")
damping_au = 0e-4
au_to_fs = 41.3413745758
dt_au = 0.5 * au_to_fs

angstrom_to_au = 1.8897259886

# use the info of the water molecule to construct unexcited photon state
# z-axis info is not used since the cavity is only coupled to x and y directions
dipole_initial = [16.704, -18.040, 0.0]
dmudt_initial = [-0.00355972, 0.01098518, 0.0]
qc_initial = -np.array(dipole_initial) * coupling_strength / frequency_au**2

sim = mxl.SingleModeSimulation(
    hub=hub,
    molecules=[molecule],
    frequency_au=frequency_au,
    coupling_strength=coupling_strength,
    damping_au=damping_au,
    coupling_axis="xy",
    drive=0.0,
    dt_au=dt_au,
    qc_initial=qc_initial,
    mu_initial=dipole_initial,
    dmudt_initial=dmudt_initial,
    record_history=True,
    include_dse=True,
)

sim.run(steps=2000)

for data in molecule.additional_data_history[-5:]:
    print(data)

from maxwelllink.tools import ir_spectrum
import matplotlib.pyplot as plt

mux = np.array([ad["mux_m_au"] for ad in molecule.additional_data_history])[1:-1]
muy = np.array([ad["muy_m_au"] for ad in molecule.additional_data_history])[1:-1]
muz = np.array([ad["muz_m_au"] for ad in molecule.additional_data_history])[1:-1]
t = np.array([ad["time_au"] for ad in molecule.additional_data_history])[1:-1]

plt.figure(figsize=(6, 4))
plt.plot(t, mux, label="X")
plt.plot(t, muy, label="Y")
plt.plot(t, muz, label="Z")
plt.xlabel("Time (fs)")
plt.ylabel("Dipole Moment (a.u.)")
plt.title("Dipole Moment vs Time")
plt.legend()
plt.tight_layout()
plt.show()

freq, sp_x = ir_spectrum(mux, 0.5, field_description="square")
freq, sp_y = ir_spectrum(muy, 0.5, field_description="square")
freq, sp_z = ir_spectrum(muz, 0.5, field_description="square")

plt.figure(figsize=(6, 4))
plt.plot(freq, sp_x, label="X")
plt.plot(freq, sp_y, label="Y")
plt.plot(freq, sp_z, label="Z")
plt.xlim(0, 5500)
plt.xlabel("Frequency (cm$^{-1}$)")
plt.ylabel("Spectral Power")
plt.title("Infrared Spectrum")
plt.legend()
plt.tight_layout()

plt.show()
