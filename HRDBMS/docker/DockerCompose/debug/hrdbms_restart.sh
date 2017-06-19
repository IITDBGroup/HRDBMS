docker exec --user hrdbms hrdbmscoordinator pkill -f java
docker exec --user hrdbms hrdbmsworker pkill -f java
docker exec --user hrdbms hrdbmsworker2 pkill -f java
docker exec --user hrdbms hrdbmscoordinator java -cp /home/hrdbms/app/build/HRDBMS.jar: StartDB
