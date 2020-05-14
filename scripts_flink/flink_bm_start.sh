cp ../modules/flink/target/iot-bm-flink-0.1.jar flink_bm.jar

python experiments_run.py flink_bm.jar etl --ExecutionTimes 2
python experiments_run.py flink_bm.jar pred --ExecutionTimes 2
python experiments_run.py flink_bm.jar stats --ExecutionTimes 2
python experiments_run.py flink_bm.jar train --ExecutionTimes 2