package in.hitcps.iot_edge.bm.flink.jobs;

import in.hitcps.iot_edge.bm.flink.data_entrys.FileDataEntry;
import in.hitcps.iot_edge.bm.flink.data_entrys.SensorDataStreamEntry;
import in.hitcps.iot_edge.bm.flink.sink_operators.stats.MQTTStatsSinkFunction;
import in.hitcps.iot_edge.bm.flink.source_operators.SourceFromSysFile;
import in.hitcps.iot_edge.bm.flink.trans_operators.stats.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.*;
import java.io.FileInputStream;
import java.util.Properties;

public class StatsJob {
    private static Logger l = LoggerFactory.getLogger(StatsJob.class);

    public static void main(String[] args) throws Exception {
        // Flink env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        String resourceDir = System.getenv("RIOT_RESOURCES");  // pi_resource
        String resourceDir = "/home/dusz512/Projects/edgeStreamingForIoT/riotResource/pi_resources";
        String inputFilePath = resourceDir + "/" + "train_input_data_test.csv";
        String taskPropertiesFileName = resourceDir + "/" + "my_stats.properties";
        System.out.println("inputDataFilePath : " + inputFilePath + "     taskPropertiesFilePath : " + taskPropertiesFileName);
        Properties p = new Properties();
        p.load(new FileInputStream(taskPropertiesFileName));

        double scalingFactor = 1;
        int inputRate = 10;
        int numData = 200;

        // data source
        SourceFromSysFile sourceFromSysFile = new SourceFromSysFile(inputFilePath, scalingFactor, inputRate, numData);
        DataStream<FileDataEntry> dataSource = env.addSource(sourceFromSysFile);
//        dataSource.print();

        SingleOutputStreamOperator<SensorDataStreamEntry> parsedRes = dataSource.flatMap(new ParseStatsMapFunction(p));
        SingleOutputStreamOperator<SensorDataStreamEntry> bFilterRes = parsedRes.filter(new BloomFilterStatsFunction(p));


        SingleOutputStreamOperator<SensorDataStreamEntry> kFilterRes = bFilterRes.filter(new KalmanFilterFunction(p));
        SingleOutputStreamOperator<SensorDataStreamEntry> slrRes = kFilterRes.flatMap(new SimpleLinearRegressionFlatMapFunction(p)).setParallelism(1);
//        slrRes.print();
        slrRes.addSink(new MQTTStatsSinkFunction(p, numData)).setParallelism(1);

        SingleOutputStreamOperator<SensorDataStreamEntry> somRes = bFilterRes.flatMap(new SecondOrderMomentFlatMapFunction(p));
        somRes.print();
        somRes.addSink(new MQTTStatsSinkFunction(p, numData)).setParallelism(1);


        env.execute("StatsJob");
    }
}
