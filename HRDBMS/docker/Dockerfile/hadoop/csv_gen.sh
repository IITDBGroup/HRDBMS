#!/usr/bin/env bash
for i in `seq 1 1000000`;
do
   echo "$i,A$i,B$i"  >> /tmp/file.csv
done