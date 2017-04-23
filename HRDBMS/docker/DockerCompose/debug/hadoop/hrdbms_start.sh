#!/usr/bin/env bash

sudo docker-compose -f ./../docker-compose.yml -f ./docker-compose.yml down
# merging two compose yml files: fist one sets up hrdbms, second - hadoop
sudo docker-compose -f ./../docker-compose.yml -f ./docker-compose.yml up -d

#start hrdbms
sudo docker exec --user hrdbms hrdbmscoordinator java -cp /home/hrdbms/app/build/HRDBMS.jar: StartDB

# wait until hadoop is launched
sleep 10
# first command fails for some reasons
sudo docker exec hadoop /usr/local/hadoop/bin/hadoop fs -Ddfs.block.size=1048576 -put /tmp/csv_type.csv csv_type.csv
# second command should work
echo Copy csv file to HDFS
sudo docker exec hadoop /usr/local/hadoop/bin/hadoop fs -Ddfs.block.size=1048576 -put /tmp/csv_type.csv csv_type.csv

echo Copy large csv file to HDFS
sudo docker exec hadoop /usr/local/hadoop/bin/hadoop fs -Ddfs.block.size=1048576 -put /tmp/file.csv file.csv
