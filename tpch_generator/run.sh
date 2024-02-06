#!/bin/bash

shopt -s globstar

cd /tpch-dbgen

for sf in 0.001 1 2; do
  echo "Generating data for sf"${sf}
  ./dbgen -vf -s ${sf}
  #Remove dots because most dbs can't handle it
  noDot="$(echo ${sf} | sed 's/\.//g')"
  mkdir -p /data/sf${noDot}
  mv *.tbl /data/sf${noDot}/
done
