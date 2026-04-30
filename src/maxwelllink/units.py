# --------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
# --------------------------------------------------------------------------------------#

# time units
PS_TO_FS = 1000.0
FS_TO_PS = 1.0 / PS_TO_FS
FS_TO_AU = 41.341373335
PS_TO_AU = PS_TO_FS * FS_TO_AU
AU_TO_FS = 1.0 / FS_TO_AU
AU_TO_PS = 1.0 / PS_TO_AU

# energy units
AU_TO_EV = 27.211399
EV_TO_CM_INV = 8065.540106923572
AU_TO_CM_INV = AU_TO_EV * EV_TO_CM_INV
CM_INV_TO_AU = 1.0 / AU_TO_CM_INV
CM_INV_TO_EV = 1.0 / EV_TO_CM_INV
EV_TO_AU = 1.0 / AU_TO_EV
K_TO_AU = 3.166811563e-6  # 1 K in atomic units of energy
AU_TO_K = 1.0 / K_TO_AU

# other units
FS_INV_TO_EV = 4.135668
BOHR_PER_ANG = 1.889726124565062
# E(a.u.) = 5.142206747e11 V/m; F(eV/Angstrom) for q=1 is E(V/m) * 1e-10
# So F(eV/Angstrom) = q * E(a.u.) * 51.422067476  (5.1422e11 * 1e-10)
FORCE_PER_EFIELD_AU_EV_PER_ANG = 51.422067476
AMU_TO_AU = 1822.888486209  # 1 amu in atomic units

def unit(from_unit, to_unit='au'):
    """Return the conversion factor from one unit to another."""

    from_unit = str(from_unit).upper()
    to_unit = str(to_unit).upper()

    if from_unit not in ['PS', 'FS', 'AU', 'EV', 'CM_INV', 'K']:
        raise ValueError(f"Unsupported from_unit: {from_unit}")
    if to_unit not in ['PS', 'FS', 'AU', 'EV', 'CM_INV', 'K']:
        raise ValueError(f"Unsupported to_unit: {to_unit}")
    if from_unit == to_unit:
        return 1.0

    factor_name = f"{from_unit}_TO_{to_unit}"
    if factor_name not in globals():
        raise ValueError(f"Incompatible unit conversion: {from_unit} -> {to_unit}")

    return float(globals()[factor_name])
