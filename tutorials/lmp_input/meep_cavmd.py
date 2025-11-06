import numpy as np
import maxwelllink as mxl
import meep as mp

address = "socket_cavmd"
hub = mxl.SocketHub(unixsocket=address, timeout=10.0, latency=1e-5)

resolution = 20
time_units_fs = 20
rescaling = 0.47
pmlThickness = 2.0 * rescaling
t1 = 0.125 * rescaling
t2 = 0.25 * rescaling
n1 = 2.0
n2 = 1.0

nlayer = 10

layerIndexes = np.array([n2, n1] * nlayer + [1.0] + [n1, n2] * nlayer)
layerThicknesses = np.array([t2, t1] * nlayer + [0.5 * rescaling] + [t1, t2] * nlayer)


layerThicknesses[0] += pmlThickness
layerThicknesses[-1] += pmlThickness
length = np.sum(layerThicknesses)
layerCenters = np.cumsum(layerThicknesses) - layerThicknesses/2
layerCenters = layerCenters - length/2
cellSize = mp.Vector3(length, 0, 0)
pmlLayers = [mp.PML(thickness=pmlThickness)]

geometry = [mp.Block(mp.Vector3(layerThicknesses[i], mp.inf, mp.inf),
    center=mp.Vector3(layerCenters[i], 0, 0), material=mp.Medium(index=layerIndexes[i]))
    for i in range(layerThicknesses.size)]

molecule = mxl.Molecule(
    hub=hub,
    center=mp.Vector3(0, 0, 0), 
    size=mp.Vector3(0.25, 0, 0), 
    sigma=0.05, 
    dimensions=1, 
    rescaling_factor=1e5
)

sources_non_molecule = []
sources = sources_non_molecule + molecule.sources
sim = mxl.MeepSimulation(
    hub=hub,
    molecules=[molecule],
    cell_size=cellSize,
    resolution=resolution,
    time_units_fs=time_units_fs,
    sources=sources,
    geometry=geometry,
    boundary_layers=pmlLayers
)


sim.run(until=2e3/resolution)   

if mp.am_master():
    from maxwelllink.tools import ir_spectrum
    import matplotlib.pyplot as plt

    fs_to_au = 1 / 0.02418884254

    mux = np.array([ad["mux_au"] for ad in molecule.additional_data_history])
    muy = np.array([ad["muy_au"] for ad in molecule.additional_data_history])
    muz = np.array([ad["muz_au"] for ad in molecule.additional_data_history])
    t = np.array([ad["time_au"] for ad in molecule.additional_data_history]) / fs_to_au

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
    
    freq, sp_x = ir_spectrum(mux, 0.5*time_units_fs/resolution, field_description="square")
    freq, sp_y = ir_spectrum(muy, 0.5*time_units_fs/resolution, field_description="square")
    freq, sp_z = ir_spectrum(muz, 0.5*time_units_fs/resolution, field_description="square")
    
    plt.figure(figsize=(6, 4))
    plt.plot(freq, sp_x, label="X")
    plt.plot(freq, sp_y, label="Y")
    plt.plot(freq, sp_z, label="Z")
    plt.xlim(0, 5000)
    plt.xlabel("Frequency (cm$^{-1}$)")
    plt.ylabel("Spectral Power")
    plt.title("Infrared Spectrum")
    plt.legend()
    plt.tight_layout()
    
    plt.show()
