docker-compose down
docker-compose up -d
docker-compose exec --user hrdbms -d coordinator nohup java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5010 -XX:+UseG1GC -XX:G1HeapRegionSize=2m -XX:+ParallelRefProcEnabled -XX:MaxDirectMemorySize=27772160000 -XX:+AggressiveOpts -cp /home/hrdbms/app/build/HRDBMS.jar: com.exascale.managers.HRDBMSWorker 0 &

# HRDBMS can be started using docker exec:
# docker exec --user hrdbms hrdbms_coordinator nohup java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5010 -XX:+UseG1GC -XX:G1HeapRegionSize=2m -XX:+ParallelRefProcEnabled -XX:MaxDirectMemorySize=27772160000 -XX:+AggressiveOpts -cp /home/hrdbms/app/build/HRDBMS.jar: com.exascale.managers.HRDBMSWorker 0 &