#!/usr/bin/env bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=2-00:00:00
#SBATCH -J __PROJECT___main
#SBATCH -o main.%j.out
#SBATCH -e main.%j.err
#SBATCH -p shared

set -euo pipefail
mpirun -n "${SLURM_NTASKS:-1}" python -u em_main.py
