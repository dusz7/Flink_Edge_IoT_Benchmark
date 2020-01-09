package in.hitcps.iot_edge.bm.flink.jobs;

import in.hitcps.iot_edge.bm.flink.data_entrys.FileDataEntry;
import in.hitcps.iot_edge.bm.flink.data_entrys.SensorDataStreamEntry;
import in.hitcps.iot_edge.bm.flink.sink_operators.etl.MQTTSinkETLFunction;
import in.hitcps.iot_edge.bm.flink.sink_operators.prediction.MQTTPredSinkFunction;
import in.hitcps.iot_edge.bm.flink.source_operators.SourceFromSysFile;
import in.hitcps.iot_edge.bm.flink.trans_operators.prediction.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.Properties;

public class PredictionJob {
    private static Logger l = LoggerFactory.getLogger(PredictionJob.class);

    public static void main(String[] args) throws Exception {
        // Flink env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        String resourceDir = System.getenv("RIOT_RESOURCES");  // pi_resource
        String resourceDir = "/home/dusz512/Projects/edgeStreamingForIoT/riotResource/pi_resources";
        String inputFilePath = resourceDir + "/" + "SYS_sample_data_senml_test.csv";
        String taskPropertiesFileName = resourceDir + "/" + "my_prediction.properties";
        System.out.println("inputDataFilePath : " + inputFilePath + "     taskPropertiesFilePath : " + taskPropertiesFileName);
        Properties p = new Properties();
        p.load(new FileInputStream(taskPropertiesFileName));

        double scalingFactor = 1;
        int inputRate = 10;
        int numData = 20;

        // data source
        SourceFromSysFile sourceFromSysFile = new SourceFromSysFile(inputFilePath, scalingFactor, inputRate, numData);
        DataStream<FileDataEntry> dataSource = env.addSource(sourceFromSysFile);
//        dataSource.print();

        SingleOutputStreamOperator<SensorDataStreamEntry> parsedData = dataSource.map(new SenMLParsePredMapFunction(p)).setParallelism(1);
//        parsedData.print();

        SingleOutputStreamOperator<SensorDataStreamEntry> dtcRes = parsedData.map(new DecisionTreeClassifyMapFunction(p));
        SingleOutputStreamOperator<SensorDataStreamEntry> mlrRes = parsedData.map(new LinearRegressionMapFunction(p));
        SingleOutputStreamOperator<SensorDataStreamEntry> avgRes = parsedData.keyBy(new KeySelector<SensorDataStreamEntry, String>() {
            @Override
            public String getKey(SensorDataStreamEntry value) throws Exception {
                return value.getSensorId();
            }
        })
                .countWindow(Integer.parseInt(p.getProperty("AGGREGATE.BLOCK_COUNT.WINDOW_SIZE")))
                .reduce(new AverageReduceFunction(p));

        ErrorEstimationFlatMapFunction errorEstimationFlatMapFunction = new ErrorEstimationFlatMapFunction(p);
        avgRes.flatMap(errorEstimationFlatMapFunction);
        SingleOutputStreamOperator<SensorDataStreamEntry> errRes = mlrRes.flatMap(errorEstimationFlatMapFunction);
//        errRes.print();

        // sink
        MQTTPredSinkFunction mqttPredSinkFunction = new MQTTPredSinkFunction(p, numData);
        dtcRes.addSink(mqttPredSinkFunction).setParallelism(1);
        errRes.addSink(mqttPredSinkFunction).setParallelism(1);

//        System.out.println(env.getExecutionPlan());
        env.execute("PredictionJob");
    }
}
