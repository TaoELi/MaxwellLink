#!/usr/bin/env bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=2-00:00:00
#SBATCH -J __PROJECT___lmp
#SBATCH -o lammps.%j.out
#SBATCH -e lammps.%j.err
#SBATCH -p shared

set -euo pipefail
python lammps_driver.py
