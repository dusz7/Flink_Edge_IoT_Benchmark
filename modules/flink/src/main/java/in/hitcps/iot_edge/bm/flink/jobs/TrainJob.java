package in.hitcps.iot_edge.bm.flink.jobs;

import in.hitcps.iot_edge.bm.flink.data_entrys.FileDataEntry;
import in.hitcps.iot_edge.bm.flink.data_entrys.FileDataTrainEntry;
import in.hitcps.iot_edge.bm.flink.data_entrys.SensorDataStreamEntry;
import in.hitcps.iot_edge.bm.flink.data_entrys.TrainDataStreamEntry;
import in.hitcps.iot_edge.bm.flink.sink_operators.train.MQTTTrainSinkFunction;
import in.hitcps.iot_edge.bm.flink.source_operators.SourceFromFile;
import in.hitcps.iot_edge.bm.flink.source_operators.SourceFromFileTrain;
import in.hitcps.iot_edge.bm.flink.trans_operators.train.AnnotateDTCClassMapFunction;
import in.hitcps.iot_edge.bm.flink.trans_operators.train.DecisionTreeTrainFlatMapFunction;
import in.hitcps.iot_edge.bm.flink.trans_operators.train.LinearRegressionTrainFlatMapFunction;
import in.hitcps.iot_edge.bm.flink.trans_operators.train.ReadTrainDataMapFunction;
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

public class TrainJob {
    private static Logger l = LoggerFactory.getLogger(TrainJob.class);

    public static void main(String[] args) throws Exception {
        // Flink env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        double scalingFactor = 1;
        int inputRate = 100;
        int numData = 2000;

        ParameterTool parameters = ParameterTool.fromArgs(args);
        inputRate = parameters.getInt("input", 10);
        numData = parameters.getInt("total", 200);
        System.out.println("inputRate : " + inputRate + " ;  totalDataNum : " + numData);

        String resourceDir = parameters.get("res_path", "/Users/craig/Projects/edgeStreamingForIoT/bm_resources");
        String inputFilePath = resourceDir + "/" + parameters.get("data_file", "inputFileForTimerSpout-CITY_NoAz.csv");
        String taskPropertiesFileName = resourceDir + "/" + parameters.get("prop_file", "my_train.properties");
        System.out.println("inputDataFilePath : " + inputFilePath + " ;  taskPropertiesFilePath : " + taskPropertiesFileName);

        Properties p = new Properties();
        p.load(new FileInputStream(taskPropertiesFileName));

        String trainInputFile = resourceDir + "/train_input_data.csv";

        // data source
        // new type of source
        SourceFromFileTrain sourceFromSysFile = new SourceFromFileTrain(inputFilePath, scalingFactor, inputRate, numData);
        DataStream<FileDataTrainEntry> dataSource = env.addSource(sourceFromSysFile, "Source");
//        dataSource.print();

        // Table Read
        SingleOutputStreamOperator<TrainDataStreamEntry> tableRead = dataSource.map(new ReadTrainDataMapFunction(p, trainInputFile)).name("Table_Read").setParallelism(1);

        // split
        SplitStream<TrainDataStreamEntry> splitTRead = tableRead.split(new OutputSelector<TrainDataStreamEntry>() {
            @Override
            public Iterable<String> select(TrainDataStreamEntry entry) {
                ArrayList<String> output = new ArrayList<>();
                output.add("mlr");
                output.add("ann");
                return output;
            }
        });

        // Linear Reg.
        SingleOutputStreamOperator<TrainDataStreamEntry> linearReg = splitTRead.select("mlr").flatMap(new LinearRegressionTrainFlatMapFunction(p)).name("Linear_Reg").setParallelism(2);

        // Annotate
        SingleOutputStreamOperator<TrainDataStreamEntry> anno = splitTRead.select("ann").map(new AnnotateDTCClassMapFunction(p)).name("Annotate").setParallelism(1);
        // Decision Tree
        SingleOutputStreamOperator<TrainDataStreamEntry> dtc = anno.flatMap(new DecisionTreeTrainFlatMapFunction(p)).name("Decision_Tree").setParallelism(2);

        // union
        DataStream<TrainDataStreamEntry> unionRes = linearReg.union(dtc);

        // MQTT Publish
        unionRes.addSink(new MQTTTrainSinkFunction(p, numData)).name("MQTT_Publish").setParallelism(1);

//        System.out.println(env.getExecutionPlan());

        env.execute("Train Job");
    }
}
