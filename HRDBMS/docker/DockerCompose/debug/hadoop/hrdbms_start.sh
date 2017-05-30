#!/usr/bin/env bash

docker network create -d bridge --subnet 17.17.0.0/8 --gateway 17.17.0.1 appnet

docker-compose -f ./../docker-compose.yml -f ./docker-compose.yml down
# merging two compose yml files: fist one sets up hrdbms, second - hadoop
docker-compose -f ./../docker-compose.yml -f ./docker-compose.yml up -d

#start hrdbms
docker exec --user hrdbms hrdbmscoordinator java -cp /home/hrdbms/app/build/HRDBMS.jar: StartDB

# wait until hadoop is launched
sleep 10
# first command fails for some reasons
docker exec hadoop /usr/local/hadoop/bin/hadoop fs -Ddfs.block.size=1048576 -put /tmp/csv_type.csv csv_type.csv
# second command should work
echo Copy csv file to HDFS
docker exec hadoop /usr/local/hadoop/bin/hadoop fs -Ddfs.block.size=1048576 -put /tmp/csv_type.csv csv_type.csv

echo Copy large csv file to HDFS
docker exec hadoop /usr/local/hadoop/bin/hadoop fs -Ddfs.block.size=1048576 -put /tmp/file.csv file.csv
