docker-compose down
docker-compose up -d
docker exec --user hrdbms hrdbms_coordinator java -cp /home/hrdbms/HRDBMS.jar: StartDB
