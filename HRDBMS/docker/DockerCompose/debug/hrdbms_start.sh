docker-compose down
docker-compose up -d
docker exec --user hrdbms hrdbms_coordinator nohup java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5010 -XX:+UseG1GC -XX:G1HeapRegionSize=2m -XX:+ParallelRefProcEnabled -XX:MaxDirectMemorySize=27772160000 -XX:+AggressiveOpts -cp /home/hrdbms/app/build/HRDBMS.jar: com.exascale.managers.HRDBMSWorker 0 &