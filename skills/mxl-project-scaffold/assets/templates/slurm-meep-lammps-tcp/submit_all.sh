#!/usr/bin/env bash

set -euo pipefail

job_main_id=$(sbatch submit_main.sh | awk '{print $4}')
echo "Submitted main job with Job ID: ${job_main_id}"

sbatch --dependency=after:${job_main_id} submit_lammps.sh
