package in.hitcps.iot_edge.bm.flink.jobs;

import in.hitcps.iot_edge.bm.flink.data_entrys.FileDataEntry;
import in.hitcps.iot_edge.bm.flink.data_entrys.SensorDataStreamEntry;
import in.hitcps.iot_edge.bm.flink.sink_operators.etl.MQTTSinkETLFunction;
import in.hitcps.iot_edge.bm.flink.source_operators.SourceFromFile;
import in.hitcps.iot_edge.bm.flink.trans_operators.etl.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.Properties;

public class ETLJob {

    private static Logger l = LoggerFactory.getLogger(ETLJob.class);

    public static void main(String[] args) throws Exception {
        // Flink env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // local env
//        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

//        String resourceDir = System.getenv("RIOT_RESOURCES");  // pi_resource
        String resourceDir = "/Users/craig/Projects/edgeStreamingForIoT/riotResource/pi_resources";
        String inputFilePath = resourceDir + "/" + "SYS_sample_data_senml.csv";
        String taskPropertiesFileName = resourceDir + "/" + "my_etl.properties";
        System.out.println("inputDataFilePath : " + inputFilePath + "     taskPropertiesFilePath : " + taskPropertiesFileName);
        Properties p = new Properties();
        p.load(new FileInputStream(taskPropertiesFileName));

        double scalingFactor = 1;
        int inputRate = 1000;
        int numData = 1000000;

        // data source
        SourceFromFile sourceFromFile = new SourceFromFile(inputFilePath, scalingFactor, inputRate, numData);
        DataStream<FileDataEntry> dataSource = env.addSource(sourceFromFile);
//        dataSource.print();

        // SenML Parse Map
        SenMLParseETLFlatMapFunction senMLParseETLFlatMapFunction = new SenMLParseETLFlatMapFunction(p);
        SingleOutputStreamOperator<SensorDataStreamEntry> parseResult = dataSource.flatMap(senMLParseETLFlatMapFunction);
//        parseResult.print();

        SingleOutputStreamOperator<SensorDataStreamEntry> filterResult = parseResult.filter(new RangeFilterFunction(p))
                .filter(new BloomFilterFunction(p));
//        filterResult.print();

        SingleOutputStreamOperator<SensorDataStreamEntry> interResult = filterResult.map(new InterpolationMapFunction(p));

        // flink auto_chain these operators' subtask, so the #1subtask of SenMLParseFlatMap's data can be send to #1subtask of JoinFlatMap..
        SingleOutputStreamOperator<SensorDataStreamEntry> joined = interResult.flatMap(new JoinFlatMapFunction(p));
        SingleOutputStreamOperator<SensorDataStreamEntry> annotated = joined.map(new AnnotationMapFunction(p));
        SingleOutputStreamOperator<SensorDataStreamEntry> senml = annotated.map(new CSVToSenMLMapFunction(p));
//        senml.print();
        senml.addSink(new MQTTSinkETLFunction(p, numData)).setParallelism(1);
//        parseResult.addSink(new MQTTSinkFunction(p, numData)).setParallelism(1);

//        System.out.println(env.getExecutionPlan());
        env.execute("ETLJob");

    }
}
