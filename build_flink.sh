#!/bin/bash
mvn clean install -DskipTests -Drat.skip=true -Dscala-2.12 -Dfast
