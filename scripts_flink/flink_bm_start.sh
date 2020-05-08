cp ../modules/flink/target/iot-bm-flink-0.1.jar flink_bm.jar

python experiments_run.py flink_bm.jar stats --ExecutionTimes 2