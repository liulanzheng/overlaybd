#!/bin/bash

## HOW TO USE:
# ABS_BUILD_NUMBER=`date +%Y%m%d%H%M%S` rpm/t-storage-lsmd-build.sh `pwd` 1 3.1.8 $ABS_BUILD_NUMBER aliyun

set -x

VERSION=$3
if [ -z "$VERSION" ]
then
    echo "need specify version number !"
    echo "such as: ./t-storage-lsmd-build.sh 0 1 1.0.0"
    exit 1;
fi
CUR_DIR=$(cd "$(dirname "$0")" && pwd)
cd $CUR_DIR
ROOT=$CUR_DIR/../
echo "install deps"


TAG=$5
sudo yum clean all
sudo yum install alios7u-gcc-6-repo.noarch -y
sudo yum install alios7u-2_17-gcc-6-repo.noarch -y
sudo yum install gcc gcc-c++ libstdc++-static gdb coreutils binutils bash -y

sudo yum install cmake -b test -y
sudo yum install libaio-devel-0.3.109 libcurl-devel-7.29.0 openssl-devel-1.0.2k libnl3-devel-3.2.28 glib2-devel-2.56.1 -y


RELEASE=$4
if [ -z "$RELEASE" ]; then
    RELEASE=`git log --pretty=format:%h -1`
else
    RELEASE="$RELEASE.`git log --pretty=format:%h -1`"
fi

if [ -n "$TAG" ]; then
    RELEASE=$RELEASE.$TAG
fi

echo "cmake"

cd $ROOT
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DTARGET_TAG=$5 -DRPM_VERSION=$3-$RELEASE
make -j32

echo "cpack"

sudo cpack

cd $CUR_DIR


RPM_DIR=$ROOT/build
for rpm in `find $RPM_DIR -name "*.rpm"`; do
    mv $rpm .
done