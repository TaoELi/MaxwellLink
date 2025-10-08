import maxwelllink as mxl

model = mxl.mxl_drivers.python.models.RTTDDFTModel(
    engine="psi4",
    molecule_xyz="../../tests/data/hcn.xyz",
    functional="SCF",
    basis="sto-3g",
    dt_rttddft_au=0.04,
    delta_kick_au=1.0e-3,
    delta_kick_direction="xyz",
    memory="2GB",
    verbose=False,
    remove_permanent_dipole=False,
)

model.initialize(dt_new=0.12, molecule_id=0)

model._get_lr_tddft_spectrum(states=20, tda=False)

model._propagate_full_rt_tddft(nsteps=1000)
