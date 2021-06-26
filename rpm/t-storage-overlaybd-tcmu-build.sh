#!/bin/bash

sudo yum install alios7u-gcc-6-repo.noarch -y
sudo yum install alios7u-2_17-gcc-6-repo.noarch -y
sudo yum install gcc gcc-c++ libstdc++-static gdb coreutils binutils bash -y
sudo yum install cmake -b test -y
sudo yum install libaio-devel libcurl-devel openssl-devel libnl3-devel glib2-devel -y

mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j8

RPM_VERSION=$3

cpack

for ONE in `find -name "*.rpm"`; do
  echo $ONE
  cp $ONE $1
done