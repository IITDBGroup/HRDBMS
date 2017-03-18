docker exec --user hrdbms hrdbms_coordinator pkill -f java
docker exec --user hrdbms hrdbms_worker pkill -f java
docker exec --user hrdbms hrdbms_worker2 pkill -f java
docker exec --user hrdbms hrdbms_coordinator java -cp /home/hrdbms/app/build/HRDBMS.jar: StartDB