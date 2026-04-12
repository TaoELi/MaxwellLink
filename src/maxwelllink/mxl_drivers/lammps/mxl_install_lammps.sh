#!/bin/bash

#--------------------------------------------------------------------------------------#
# Copyright (c) 2026 MaxwellLink                                                       #
# This file is part of MaxwellLink. Repository: https://github.com/TaoELi/MaxwellLink  #
# If you use this code, always credit and cite arXiv:2512.06173.                       #
# See AGENTS.md and README.md for details.                                             #
#--------------------------------------------------------------------------------------#

if [ ! -d build ]; then
    mkdir build
fi

cd build/

echo "Installing modified LAMMPS with MaxwellLink..."
wget -c https://github.com/lammps/lammps/releases/download/stable_29Aug2024_update1/lammps-src-29Aug2024_update1.tar.gz
tar -xvf lammps-src-29Aug2024_update1.tar.gz
cd lammps-29Aug2024
mkdir build 
cd build

transport=${MXL_LAMMPS_TRANSPORT:-socket}
binary_name="lmp_mxl"

rm -f ../src/MISC/fix_maxwelllink*.cpp ../src/MISC/fix_maxwelllink*.h

if [ "$transport" = "socket" ]; then
    cp ../../../src/maxwelllink/mxl_drivers/lammps/fix_maxwelllink.* ../src/MISC
elif [ "$transport" = "ucx" ]; then
    cp ../../../src/maxwelllink/mxl_drivers/lammps/fix_maxwelllink_ucx.* ../src/MISC
    binary_name="lmp_mxl_ucx"
elif [ "$transport" = "both" ]; then
    cp ../../../src/maxwelllink/mxl_drivers/lammps/fix_maxwelllink.* ../src/MISC
    cp ../../../src/maxwelllink/mxl_drivers/lammps/fix_maxwelllink_ucx.* ../src/MISC
    binary_name="lmp_mxl_ucx"
else
    echo "Unsupported transport: ${transport}"
    exit 2
fi

# build LAMMPS with no GPU and minimal packages and install
cmake_args=()
if [ "$transport" = "ucx" ] || [ "$transport" = "both" ]; then
    ucxx_cflags=${MXL_LAMMPS_UCXX_CFLAGS:-$(pkg-config --cflags ucxx 2>/dev/null)}
    ucxx_ldflags=${MXL_LAMMPS_UCXX_LDFLAGS:-$(pkg-config --libs ucxx 2>/dev/null)}
    if [ -z "$ucxx_cflags" ] && [ -z "$ucxx_ldflags" ]; then
        echo "UCX transport requested, but UCXX flags were not found. Set MXL_LAMMPS_UCXX_CFLAGS/MXL_LAMMPS_UCXX_LDFLAGS."
        exit 3
    fi
    if [ -n "$CONDA_PREFIX" ]; then
        cmake_args+=("-DCMAKE_PREFIX_PATH=${CONDA_PREFIX}")
    fi
    if [ -n "$ucxx_cflags" ]; then
        cmake_args+=("-DCMAKE_CXX_FLAGS=${ucxx_cflags}")
    fi
    if [ -n "$ucxx_ldflags" ]; then
        cmake_args+=("-DCMAKE_EXE_LINKER_FLAGS=${ucxx_ldflags}")
        cmake_args+=("-DCMAKE_SHARED_LINKER_FLAGS=${ucxx_ldflags}")
        cmake_args+=("-DCMAKE_MODULE_LINKER_FLAGS=${ucxx_ldflags}")
    fi
fi

cmake -C ../cmake/presets/most.cmake -C ../cmake/presets/nolib.cmake -D PKG_GPU=off "${cmake_args[@]}" ../cmake
make -j4

# copy the lmp executable to a known location
location=$(which mxl_driver.py)
# at the end of location is /mxl_driver.py, remove it to get the directory
dir=${location%/*}
echo "Copying lmp executable to ${dir}/${binary_name}"
cp lmp ${dir}/${binary_name}
