#!/usr/bin/env bash
docker-compose down
docker-compose up -d
docker exec --user hrdbms hrdbmscoordinator java -cp /home/hrdbms/app/build/HRDBMS.jar: StartDB
