## RIoTBench: A Real-time IoT Benchmark for Distributed Stream Processing Platforms
### IoT  Micro-benchmarks 
| Annotate                     | ANN | Parse         | Transform        | 1:1   | No  |
|------------------------------|-----|---------------|------------------|-------|-----|
| CsvToSenML                   | C2S | Parse         | Transform        | 1:1   | No  |
| SenML Parsing                | SML | Parse         | Transform        | 1:1   | No  |
| XML Parsing                  | XML | Parse         | Transform        | 1:1   | No  |
| Bloom Filter                 | BLF | Filter        | Filter           | 1:0/1 | No  |
| Range Filter                 | RGF | Filter        | Filter           | 1:0/1 | No  |
| Accumlator                   | ACC | Statistical   | Aggregate        | N:1   | Yes |
| Average                      | AVG | Statistical   | Aggregate        | N:1   | Yes |
| Distinct Appox. Count        | DAC | Statistical   | Transform        | 1:1   | Yes |
| Kalman Filter                | KAL | Statistical   | Transform        | 1:1   | Yes |
| Second Order Moment          | SOM | Statistical   | Transform        | 1:1   | Yes |
| Decision Tree Classify       | DTC | Predictive    | Transform        | 1:1   | No  |
| Decision Tree Train          | DTT | Predictive    | Aggregate        | N:1   | No  |
| Interpolation                | INP | Predictive    | Transform        | 1:1   | Yes |
| Multi-var. Linear Reg.       | MLR | Predictive    | Transform        | 1:1   | No  |
| Multi-var. Linear Reg. Train | MLT | Predictive    | Aggregate        | N:1   | No  |
| Sliding Linear Regression    | SLR | Predictive    | Flat Map         | N:M   | Yes |
| Azure Blob D/L               | ABD | IO            | Source/Transform | 1:1   | No  |
| Azure Blob U/L               | ABU | IO            | Sink             | 1:1   | No  |
| Azure Table Lookup           | ATL | IO            | Source/Transform | 1:1   | No  |
| Azure Table Range            | ATR | IO            | Source/Transform | 1:1   | No  |
| Azure Table Insert           | ATI | IO            | Transform        | 1:1   | No  |
| MQTT Publish                 | MQP | IO            | Sink             | 1:1   | No  |
| MQTT Subscribe               | MQS | IO            | Sink             | 1:1   | No  |
| Local Files Zip              | LZP | IO            | Sink             | 1:1   | No  |
| Remote Files Zip             | RZP | IO            | Sink             | 1:1   | No  |
| MultiLine Plot               | PLT | Visualization | Transform        | 1:1   | No  |

### Application  benchmarks 
| App. Name  | Code |
| ------------- | ------------- |
| Extraction, Transform and Load  dataflow  | ETL   |
| Statistical Summarization dataflow  | STATS   |
| Model Training dataflow  | TRAIN   |
| Predictive Analytics dataflow   | PRED   |



 ![FCAST](https://github.com/anshuiisc/FIG/blob/master/ETL-1.png)
 ![FCAST](https://github.com/anshuiisc/FIG/blob/master/stats-1.png)
 ![FCAST](https://github.com/anshuiisc/FIG/blob/master/pred-1.png)
 ![FCAST](https://github.com/anshuiisc/FIG/blob/master/train-1.png)


- Steps to run benchmark's
- Once cloned  run 
    ```
   mvn clean compile package -DskipTests
    ```
- To submit jar microbenchmarks- 
 ```
 storm jar <stormJarPath>   in.dream_lab.bm.stream_iot.storm.topo.micro.MicroTopologyDriver  C  <microTaskName>  <inputDataFilePath used by CustomEventGen and spout>   PLUG-<expNum>  <rate as 1x,2x>  <outputLogPath>   <tasks.properties File Path>   <TopoName>
 
 
 ```
- For microTaskName please refer  switch logic in  MicroTopologyFactory class in package   "in.dream_lab.bm.stream_iot.storm.topo.micro"   

Please refer the paper for detailed info  - <https://arxiv.org/abs/1701.08530> 





