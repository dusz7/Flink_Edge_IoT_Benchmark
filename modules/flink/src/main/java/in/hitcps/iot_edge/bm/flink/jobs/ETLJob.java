package in.hitcps.iot_edge.bm.flink.jobs;

import in.hitcps.iot_edge.bm.flink.data_entrys.FileDataEntry;
import in.hitcps.iot_edge.bm.flink.data_entrys.SensorDataStreamEntry;
import in.hitcps.iot_edge.bm.flink.sink_operators.etl.MQTTSinkETLFunction;
import in.hitcps.iot_edge.bm.flink.source_operators.SourceFromFile;
import in.hitcps.iot_edge.bm.flink.trans_operators.etl.*;
import org.apache.flink.api.java.utils.ParameterTool;
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
        String taskPropertiesFileName = resourceDir + "/" + parameters.get("prop_file", "my_etl.properties");
        System.out.println("inputDataFilePath : " + inputFilePath + " ;  taskPropertiesFilePath : " + taskPropertiesFileName);

        Properties p = new Properties();
        p.load(new FileInputStream(taskPropertiesFileName));

        // data source
        SourceFromFile sourceFromFile = new SourceFromFile(inputFilePath, scalingFactor, inputRate, numData);
        DataStream<FileDataEntry> dataSource = env.addSource(sourceFromFile, "Source");
//        dataSource.print();

        // SenML Parse Map
        SingleOutputStreamOperator<SensorDataStreamEntry> parseResult = dataSource.flatMap(new SenMLParseETLFlatMapFunction(p)).name("SenML_Parse").setParallelism(3);

        SingleOutputStreamOperator<SensorDataStreamEntry> filterResult = parseResult.filter(new RangeFilterFunction(p)).name("Range_Filter").setParallelism(1)
                .filter(new BloomFilterFunction(p)).name("Bloom_Filter").setParallelism(1);
//        filterResult.print();

        SingleOutputStreamOperator<SensorDataStreamEntry> interResult = filterResult.map(new InterpolationMapFunction(p)).name("Interpolation").setParallelism(1);

        // flink auto_chain these operators' subtask, so the #1subtask of SenMLParseFlatMap's data can be send to #1subtask of JoinFlatMap..
        SingleOutputStreamOperator<SensorDataStreamEntry> joined = interResult.flatMap(new JoinFlatMapFunction(p)).name("Join").setParallelism(1);
        SingleOutputStreamOperator<SensorDataStreamEntry> annotated = joined.map(new AnnotationMapFunction(p)).name("Annotate").setParallelism(1);
        SingleOutputStreamOperator<SensorDataStreamEntry> senml = annotated.map(new CSVToSenMLMapFunction(p)).name("CsvToSenML").setParallelism(3);

        senml.addSink(new MQTTSinkETLFunction(p, numData)).name("MQTT_Publish").setParallelism(3);

//        System.out.println(env.getExecutionPlan());

        env.execute("ETLJob");

    }
}
