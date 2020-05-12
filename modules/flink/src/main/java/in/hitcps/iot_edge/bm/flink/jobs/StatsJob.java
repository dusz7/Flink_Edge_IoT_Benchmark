package in.hitcps.iot_edge.bm.flink.jobs;

import in.hitcps.iot_edge.bm.flink.data_entrys.FileDataEntry;
import in.hitcps.iot_edge.bm.flink.data_entrys.SensorDataStreamEntry;
import in.hitcps.iot_edge.bm.flink.sink_operators.stats.MQTTStatsSinkFunction;
import in.hitcps.iot_edge.bm.flink.source_operators.SourceFromFile;
import in.hitcps.iot_edge.bm.flink.trans_operators.stats.*;
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

public class StatsJob {
    private static Logger l = LoggerFactory.getLogger(StatsJob.class);

    public static void main(String[] args) throws Exception {
        // Flink env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Operator chain
//        env.disableOperatorChaining();

//        String resourceDir = "/Users/craig/Projects/edgeStreamingForIoT/bm_resources";

//        flink run -c in.hitcps.iot_edge.bm.flink.jobs.StatsJob flink_bm.jar -input 20 -total 4000 -res_path /usr/local/etc/flink-remote/bm_files/bm_resources -data_file train_input_data_test.csv  -prop_file my_stats.properties

        double scalingFactor = 1;
        int inputRate = 100;
        int numData = 2000;

        ParameterTool parameters = ParameterTool.fromArgs(args);
        inputRate = parameters.getInt("input", 100);
        numData = parameters.getInt("total", 2000);
        System.out.println("inputRate : " + inputRate + " ;  totalDataNum : " + numData);

        String resourceDir = parameters.get("res_path", "/Users/craig/Projects/edgeStreamingForIoT/bm_resources");
        String inputFilePath = resourceDir + "/" + parameters.get("data_file", "train_input_data.csv");
        String taskPropertiesFileName = resourceDir + "/" + parameters.get("prop_file", "my_stats.properties");
        System.out.println("inputDataFilePath : " + inputFilePath + " ;  taskPropertiesFilePath : " + taskPropertiesFileName);

        Properties p = new Properties();
        p.load(new FileInputStream(taskPropertiesFileName));

        // data source
        SourceFromFile sourceFromFile = new SourceFromFile(inputFilePath, scalingFactor, inputRate, numData);
//        PSourceFromFile pSourceFromFile = new PSourceFromFile(inputFilePath, scalingFactor, inputRate, numData);
        DataStream<FileDataEntry> dataSource = env.addSource(sourceFromFile, "Source");
//        DataStream<FileDataEntry> dataSource = env.addSource(pSourceFromFile, "Source").setParallelism(2);
//        dataSource.print();

        SingleOutputStreamOperator<SensorDataStreamEntry> parsedRes = dataSource.flatMap(new SenMLParseStatsFlatMapFunction(p)).name("SenML_Parse").setParallelism(1);
        SingleOutputStreamOperator<SensorDataStreamEntry> bFilterRes = parsedRes.filter(new BloomFilterStatsFunction(p)).name("Bloom_Filter").setParallelism(1);

        // split
        SplitStream<SensorDataStreamEntry> splitBFliter = bFilterRes.split(new OutputSelector<SensorDataStreamEntry>() {
            @Override
            public Iterable<String> select(SensorDataStreamEntry sensorDataStreamEntry) {
                ArrayList<String> output = new ArrayList<>();
                output.add("kFilter");
                output.add("som");
                output.add("dac");
                return output;
            }
        });

        // kFilter & SLR
        SingleOutputStreamOperator<SensorDataStreamEntry> kFilterRes = splitBFliter.select("kFilter").filter(new KalmanFilterFunction(p)).name("Kalman_Filter").setParallelism(1);
        SingleOutputStreamOperator<SensorDataStreamEntry> slrRes = kFilterRes.flatMap(new SimpleLinearRegressionFlatMapFunction(p)).name("Linear_Reg").setParallelism(1);

        // SOM
        SingleOutputStreamOperator<SensorDataStreamEntry> somRes = splitBFliter.select("som").flatMap(new SecondOrderMomentFlatMapFunction(p)).name("Average").setParallelism(1);

        // DAC
        SingleOutputStreamOperator<SensorDataStreamEntry> dacRes = splitBFliter.select("dac").flatMap(new DistinctApproxCountFlatMapFunction(p)).name("Distinct_Count").setParallelism(1);

        // union
        DataStream<SensorDataStreamEntry> unionRes = slrRes.union(somRes).union(dacRes);

        // mqtt sink
        unionRes.addSink(new MQTTStatsSinkFunction(p, numData)).name("MQTT_Publish").setParallelism(3);

//        System.out.println(env.getExecutionPlan());

        env.execute("StatsJob");
    }
}
