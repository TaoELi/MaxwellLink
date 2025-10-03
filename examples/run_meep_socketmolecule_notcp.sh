#!/bin/bash

N=2

python meep_socketmolecule_notcp_Nsites.py $N &
echo "After launching MEEP, sleeping 20 seconds to wait for the socket server to start..."
sleep 20


for i in $(seq 1 $(($N-1))); do
    mxl_driver.py --model tls --unix --address mx10 --param "omega=0.242, mu12=187, orientation=2, pe_initial=1e-4" &
    sleep 1
    echo "Launched $i-th TLS driver in background"

done

echo "Launching the last (N-th) TLS driver in foreground..."
mxl_driver.py --model tls --unix --address mx10 --param "omega=0.242, mu12=187, orientation=2, pe_initial=1e-4"



