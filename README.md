# ATC'19 EdgeWise Prototype - Benchmarks
This repository contains the Benchmarks used by the EdgeWise Prototype for the [ATC'19](https://www.usenix.org/conference/atc19) paper:

*Xinwei Fu, Talha Ghaffar, James C. Davis, Dongyoon Lee, "[EdgeWise: A Better Stream Processing Engine for the Edge](http://people.cs.vt.edu/fuxinwei/)", USENIX Annual Technical Conference (ATC), Renton, WA, USA, July 2019.*

This repository modify the [riot-bench](https://github.com/dream-lab/riot-bench).

### Setup
Download resources [here](https://drive.google.com/drive/folders/1DOrSu4r-Lzf1BE-5qRYdC-HRM9vjhmN4?usp=sharing).
Copy *pc_resources* folder into the nimbus PC and copy *pi_resources* folder into distributed PIs.
Setup env in the nimbus PC:
```sh
$ export STORM=MYPATH/apache-storm-1.1.0/bin/storm
$ export RIOT_INPUT_PROP_PATH="MYPATH/pc_resources"
$ export RIOT_RESOURCES="MYPATH/pi_resources/"
```

### How to Compile and Package
We used 4 topologies: [ETL](https://github.com/XinweiFu/EdgeWise-ATC-19-Benchmarks/blob/atc19/modules/storm/src/main/java/in/dream_lab/bm/stream_iot/storm/topo/apps/ETLTopology.java), [PRED](https://github.com/XinweiFu/EdgeWise-ATC-19-Benchmarks/blob/atc19/modules/storm/src/main/java/in/dream_lab/bm/stream_iot/storm/topo/apps/IoTPredictionTopologySYS.java), [STAT](https://github.com/XinweiFu/EdgeWise-ATC-19-Benchmarks/blob/atc19/modules/storm/src/main/java/in/dream_lab/bm/stream_iot/storm/topo/apps/IoTStatsTopology.java), [TRAIN](https://github.com/XinweiFu/EdgeWise-ATC-19-Benchmarks/blob/atc19/modules/storm/src/main/java/in/dream_lab/bm/stream_iot/storm/topo/apps/IoTTrainTopologySYS.java). You can modify the configureations before compiling and packaging as descirbed [here](https://github.com/XinweiFu/EdgeWise-ATC-19).
```sh
$ mvn compile package -DskipTests
# Generated jar file is here: modules/storm/target/iot-bm-storm-0.1-jar-with-dependencies.jar
# If it shows errors, please run it again.
```

### Run scripts
Script parameters, including input rate and total events for each topology, can be set in [scripts/exp_setup.py](https://github.com/XinweiFu/EdgeWise-ATC-19-Benchmarks/blob/atc19/scripts/exp_setup.py).
There are running examples in [scripts/run_examples.sh](https://github.com/XinweiFu/EdgeWise-ATC-19-Benchmarks/blob/atc19/scripts/run_examples.sh)
```sh
$ cd scripts/
$ ./run_examples.sh
# Generated output jar file is in the folder: scripts/exp_archive_dir
```
