docker exec --user hrdbms hrdbms_coordinator pkill -f java
docker exec --user hrdbms hrdbms_worker_1 pkill -f java
docker exec --user hrdbms hrdbms_worker_2 pkill -f java
docker exec --user hrdbms hrdbms_coordinator java -cp /home/hrdbms/HRDBMS.jar: StartDB
