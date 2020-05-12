package in.hitcps.iot_edge.bm.flink.jobs;

import in.hitcps.iot_edge.bm.flink.data_entrys.FileDataEntry;
import in.hitcps.iot_edge.bm.flink.data_entrys.SensorDataStreamEntry;
import in.hitcps.iot_edge.bm.flink.sink_operators.prediction.MQTTPredSinkFunction;
import in.hitcps.iot_edge.bm.flink.sink_operators.stats.MQTTStatsSinkFunction;
import in.hitcps.iot_edge.bm.flink.source_operators.SourceFromFile;
import in.hitcps.iot_edge.bm.flink.trans_operators.prediction.*;
import in.hitcps.iot_edge.bm.flink.trans_operators.stats.KalmanFilterFunction;
import in.hitcps.iot_edge.bm.flink.trans_operators.stats.SecondOrderMomentFlatMapFunction;
import in.hitcps.iot_edge.bm.flink.trans_operators.stats.SimpleLinearRegressionFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Properties;

public class PredictionJob {
    private static Logger l = LoggerFactory.getLogger(PredictionJob.class);

    public static void main(String[] args) throws Exception {
        // Flink env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Operator chain
//        env.disableOperatorChaining();

        double scalingFactor = 1;
        int inputRate = 100;
        int numData = 20000;

        ParameterTool parameters = ParameterTool.fromArgs(args);
        inputRate = parameters.getInt("input", 100);
        numData = parameters.getInt("total", 2000);
        System.out.println("inputRate : " + inputRate + " ;  totalDataNum : " + numData);

        String resourceDir = parameters.get("res_path", "/Users/craig/Projects/edgeStreamingForIoT/bm_resources");
        String inputFilePath = resourceDir + "/" + parameters.get("data_file", "SYS_sample_data_senml.csv");
        String taskPropertiesFileName = resourceDir + "/" + parameters.get("prop_file", "my_prediction.properties");
        System.out.println("inputDataFilePath : " + inputFilePath + " ;  taskPropertiesFilePath : " + taskPropertiesFileName);

        Properties p = new Properties();
        p.load(new FileInputStream(taskPropertiesFileName));

        // data source
        SourceFromFile sourceFromFile = new SourceFromFile(inputFilePath, scalingFactor, inputRate, numData);
        DataStream<FileDataEntry> dataSource = env.addSource(sourceFromFile, "Source");
//        dataSource.print();

        SingleOutputStreamOperator<SensorDataStreamEntry> parsedData = dataSource.map(new SenMLParsePredMapFunction(p)).name("SenML_Parse").setParallelism(2);

        // split
        SplitStream<SensorDataStreamEntry> splitPData = parsedData.split(new OutputSelector<SensorDataStreamEntry>() {
            @Override
            public Iterable<String> select(SensorDataStreamEntry sensorDataStreamEntry) {
                ArrayList<String> output = new ArrayList<>();
                output.add("dTree");
                output.add("lr");
                output.add("ave");
                return output;
            }
        });

        // Linear Reg.
        SingleOutputStreamOperator<SensorDataStreamEntry> mlrRes = splitPData.select("lr").map(new LinearRegressionMapFunction(p)).name("Linear_Reg").setParallelism(1);
        // Average
        SingleOutputStreamOperator<SensorDataStreamEntry> avgRes = splitPData.select("ave").keyBy(new KeySelector<SensorDataStreamEntry, String>() {
            @Override
            public String getKey(SensorDataStreamEntry value) throws Exception {
                return value.getSensorId();
            }
        })
                .countWindow(Integer.parseInt(p.getProperty("AGGREGATE.BLOCK_COUNT.WINDOW_SIZE")))
                .reduce(new AverageReduceFunction(p)).name("Average").setParallelism(1);
        // Error Est.
        DataStream<SensorDataStreamEntry> unionRes1 = mlrRes.union(avgRes);
        SingleOutputStreamOperator<SensorDataStreamEntry> errRes = unionRes1.flatMap(new ErrorEstimationFlatMapFunction(p)).name("Error_Est").setParallelism(1);


        // Decision Tree
        SingleOutputStreamOperator<SensorDataStreamEntry> dtcRes = splitPData.select("dTree").map(new DecisionTreeClassifyMapFunction(p)).name("Decision_Tree").setParallelism(1);

        DataStream<SensorDataStreamEntry> unionRes2 = dtcRes.union(errRes);

        // sink
        unionRes2.addSink(new MQTTPredSinkFunction(p, numData)).name("MQTT_Publish").setParallelism(3);

//        System.out.println(env.getExecutionPlan());

        env.execute("PredictionJob");
    }
}
