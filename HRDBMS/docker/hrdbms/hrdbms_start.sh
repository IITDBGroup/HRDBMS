ulimit -n 65000
nohup java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5010 -XX:+UseG1GC -XX:G1HeapRegionSize=2m -XX:+ParallelRefProcEnabled -XX:MaxDirectMemorySize=27772160000 -XX:+AggressiveOpts -classpath /home/hrdbms/app/bin: com.exascale.managers.HRDBMSWorker 0 >/dev/null 2>&1 &
