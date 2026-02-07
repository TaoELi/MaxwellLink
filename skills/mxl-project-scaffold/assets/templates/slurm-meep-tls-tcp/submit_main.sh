#!/usr/bin/env bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH -J mxl_main
#SBATCH -o mxl_main.%j.out
#SBATCH -e mxl_main.%j.err

set -euo pipefail
mpirun -np "${SLURM_NTASKS:-1}" python -u em_main.py
