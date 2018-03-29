docker exec --user hrdbms hrdbmscoordinator pkill -f java
docker exec --user hrdbms hrdbmsworker pkill -f java
docker exec --user hrdbms hrdbmsworker2 pkill -f java
docker exec --user hrdbms hrdbmsworker3 pkill -f java
docker exec --user hrdbms hrdbmsworker4 pkill -f java
docker exec --user hrdbms hrdbmsworker5 pkill -f java
docker exec --user hrdbms hrdbmsworker6 pkill -f java
docker exec --user hrdbms hrdbmsworker7 pkill -f java
docker exec --user hrdbms hrdbmsworker8 pkill -f java
docker exec --user hrdbms hrdbmsworker9 pkill -f java
docker exec --user hrdbms hrdbmsworker10 pkill -f java
docker exec --user hrdbms hrdbmsworker11 pkill -f java
docker exec --user hrdbms hrdbmsworker12 pkill -f java
docker exec --user hrdbms hrdbmscoordinator java -cp /home/hrdbms/app/build/HRDBMS.jar: StartDB
