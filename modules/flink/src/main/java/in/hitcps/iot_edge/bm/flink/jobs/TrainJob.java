package in.hitcps.iot_edge.bm.flink.jobs;

import in.hitcps.iot_edge.bm.flink.data_entrys.FileDataEntry;
import in.hitcps.iot_edge.bm.flink.source_operators.SourceFromSysFile;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.Properties;

public class TrainJob {
    private static Logger l = LoggerFactory.getLogger(TrainJob.class);

    public static void main(String[] args) throws Exception {
        // Flink env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        String resourceDir = System.getenv("RIOT_RESOURCES");  // pi_resource
        String resourceDir = "/home/dusz512/Projects/edgeStreamingForIoT/riotResource/pi_resources";
        String inputFilePath = resourceDir + "/" + "inputFileForTimerSpout-CITY_NoAz.csv";
        String taskPropertiesFileName = resourceDir + "/" + "my_train.properties";
        System.out.println("inputDataFilePath : " + inputFilePath + "     taskPropertiesFilePath : " + taskPropertiesFileName);
        Properties p = new Properties();
        p.load(new FileInputStream(taskPropertiesFileName));

        double scalingFactor = 1;
        int inputRate = 10;
        int numData = 10;

        // data source
//        SourceFromSysFile sourceFromSysFile = new SourceFromSysFile(inputFilePath, scalingFactor, inputRate, numData);
//        DataStream<FileDataEntry> dataSource = env.addSource(sourceFromSysFile);
//        dataSource.print();
    }
}
