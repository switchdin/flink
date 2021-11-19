#!/bin/bash
BRANCH=`git rev-parse --abbrev-ref HEAD`
HASH=`git rev-parse HEAD`
VERS=$(mvn -q -Dexec.executable=echo -Dexec.args='${project.version}' --non-recursive exec:exec)
mv release.tar.gz "/opt/switchdin/archive/flink_${VERS}_${BRANCH}_${HASH}.tar.gz"
