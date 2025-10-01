#!/bin/bash

python test_meep_socketmolecule_rttddft.py &

echo "After launching MEEP, sleeping 10 seconds to wait for the socket server to start..."
sleep 10

mxl_driver.py --model rttddft --port 31880 --param "molecule_xyz=../tests/data/hcn.xyz, delta_kick_au=1e-3, checkpoint=false, restart=false" --verbose
