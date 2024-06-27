#!/bin/bash

export CC="gcc" CXX="g++"

git submodule update --init --recursive

set -e
# assume current path is under webcachesim
sudo apt-get update
sudo apt install -y git cmake build-essential libboost-all-dev python3-pip parallel libprocps-dev software-properties-common
# install openjdk 1.8. This is for simulating Adaptive-TinyLFU. The steps has to be one by one
sudo add-apt-repository ppa:openjdk-r/ppa -y
sudo apt-get update
sudo apt-get install -y openjdk-8-jdk
java -version  # output should be 1.8

cd ./lib
# install LightGBM
cd ./LightGBM/build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j
sudo make -j install
cd ../..
# dependency for mongo c driver
sudo apt-get install -y cmake libssl-dev libsasl2-dev
# installing mongo c
cd ./mongo-c-driver/cmake-build
cmake -DENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF ..
make -j
sudo make -j install
cd ../..
# installing mongo-cxx
cd ./mongo-cxx-driver/build
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local ..
sudo make -j EP_mnmlstc_core
make -j
sudo make -j install
cd ../..
# installing libbf
cd ./libbf/build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j
sudo make -j install
cd ../..
cd ..
# building webcachesim, install the library with api
cd ./build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j
sudo ldconfig
cd ../