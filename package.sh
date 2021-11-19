#!/bin/bash
TARFILE=`pwd`/release.tar
cd flink-python/dist
tar cvf $TARFILE *.whl
cd ../apache-flink-libraries/dist
tar -rvf $TARFILE *.whl
cd ../../../build-target
tar -rvf $TARFILE *
cd ..
gzip $TARFILE
