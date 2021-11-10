#!/bin/bash
TARFILE=`pwd`/release.tar
cd flink-python/dist
tar cvf $TARFILE *.tar.gz
cd ../apache-flink-libraries/dist
tar -rvf $TARFILE *.tar.gz
cd ../../../build-target
tar -rvf $TARFILE *
cd ..
gzip $TARFILE
