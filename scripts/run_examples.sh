cp ../modules/storm/target/iot-bm-storm-0.1-jar-with-dependencies.jar topologies.jar

python run_exp.py topologies.jar etl_l
#python run_exp.py topologies.jar etl_h

#python run_exp.py topologies.jar pred_l
#python run_exp.py topologies.jar pred_h

#python run_exp.py topologies.jar stat_l
#python run_exp.py topologies.jar stat_h

#python run_exp.py topologies.jar train_l
#python run_exp.py topologies.jar train_h

rm topologies.jar
