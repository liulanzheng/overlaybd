#!/bin/bash

## HOW TO USE:
# ABS_BUILD_NUMBER=`date +%Y%m%d%H%M%S`; rpm/t-storage-lsmd-build.sh <aone_place_holder> 3.1.8 $ABS_BUILD_NUMBER aliyun

set -x

VERSION=$2
RELEASE=$3
TAG=$4

if [ -z "$TAG" ]; then
    echo "wrong arguments !"
    echo "such as: ABS_BUILD_NUMBER=\`date +%Y%m%d%H%M%S\`; rpm/t-storage-lsmd-build.sh 1 1.0.0 \$ABS_BUILD_NUMBER aliyun"
    exit 1;
fi

CUR_DIR=$(cd "$(dirname "$0")" && pwd)
cd $CUR_DIR
ROOT=$CUR_DIR/..
echo "root dir $ROOT"


echo "install deps"
CMAKE_BIN=cmake
CPACK_BIN=cpack

if [[ $(uname -r) == *".el7."* ]]; then
    CMAKE_BIN=cmake3
    CPACK_BIN=cpack3
    sudo yum install gcc gcc-c++ libstdc++-static gdb coreutils binutils bash -y
    sudo yum install -y cmake3
    sudo yum install -y libaio-devel libcurl-devel openssl-devel libnl3-devel glib2-devel
else
    sudo yum clean all
    sudo yum install alios7u-gcc-6-repo.noarch -y
    sudo yum install alios7u-2_17-gcc-6-repo.noarch -y
    sudo yum install gcc gcc-c++ libstdc++-static gdb coreutils binutils bash -y

    sudo yum install cmake -b test -y
    sudo yum install libaio-devel-0.3.109 libcurl-devel-7.29.0 openssl-devel-1.0.2k libnl3-devel-3.2.28 glib2-devel-2.56.1 -y
fi


echo "cmake"
RELEASE=$RELEASE.$TAG

cd $ROOT
mkdir build
cd build
$CMAKE_BIN .. -DCMAKE_BUILD_TYPE=Release -DTARGET_TAG=$TAG -DRPM_VERSION=$VERSION-$RELEASE
make -j32


echo "cpack"
sudo $CPACK_BIN


echo "collect"
cd $CUR_DIR
RPM_DIR=$ROOT/build
for rpm in `find $RPM_DIR -name "*.rpm"`; do
    mv $rpm .
done
