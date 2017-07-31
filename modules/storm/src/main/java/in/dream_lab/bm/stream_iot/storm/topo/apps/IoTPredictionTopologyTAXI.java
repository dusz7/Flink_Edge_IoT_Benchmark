package in.dream_lab.bm.stream_iot.storm.topo.apps;

/**
 * Created by anshushukla on 03/06/16.
 */


//import in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.SYS.*;
import in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.SYS.AzureBlobDownloadTaskBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.TAXI.*;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentClass;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentParser;
import in.dream_lab.bm.stream_iot.storm.sinks.Sink;
import in.dream_lab.bm.stream_iot.storm.spouts.MQTTSubscribeSpout;
import in.dream_lab.bm.stream_iot.storm.spouts.SampleSenMLSpout;
import vt.lee.lab.storm.riot_resources.RiotResourceFileProps;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;


/**
 * Created by anshushukla on 18/05/15.
 */
public class IoTPredictionTopologyTAXI {

    public static void main(String[] args) throws Exception {

        ArgumentClass argumentClass = ArgumentParser.parserCLI(args);
        if (argumentClass == null) {
            System.out.println("ERROR! INVALID NUMBER OF ARGUMENTS");
            return;
        }
		String resourceDir = System.getenv("RIOT_RESOURCES");
		String inputPath = System.getenv("RIOT_INPUT_PROP_PATH");

        String logFilePrefix = argumentClass.getTopoName() + "-" + argumentClass.getExperiRunId() + "-" + argumentClass.getScalingFactor() + ".log";
        String sinkLogFileName = argumentClass.getOutputDirName() + "/sink-" + logFilePrefix;
        String spoutLogFileName = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix;
        String taskPropFilename = inputPath + "/" + argumentClass.getTasksPropertiesFilename();
        System.out.println("taskPropFilename-"+taskPropFilename);
		int inputRate = argumentClass.getInputRate();
		long numEvents = argumentClass.getNumEvents();

		List<String> resourceFileProps = RiotResourceFileProps.getPredTaxiResourceFileProps();


        Config conf = new Config();
		conf.put(Config.TOPOLOGY_BACKPRESSURE_ENABLE, true);
		conf.setDebug(false);
		conf.setNumAckers(0);


        Properties p_=new Properties();
        InputStream input = new FileInputStream(taskPropFilename);
        p_.load(input);

		Enumeration e = p_.propertyNames();

		while (e.hasMoreElements()) {
			String key = (String) e.nextElement();
			if (resourceFileProps.contains(key)) {
				String prop_fike_path = resourceDir + "/" + p_.getProperty(key);
				p_.put(key, prop_fike_path);
				System.out.println(key + " -- " + p_.getProperty(key));
			}
		}

        TopologyBuilder builder = new TopologyBuilder();


//        String basePathForMultipleSpout="/Users/anshushukla/PycharmProjects/DataAnlytics1/Storm-Scheduler-SC-scripts/TAXI-inputcsv-predict-10spouts200mps-480sec-file/";

        String spout1InputFilePath= resourceDir + "/TAXI_sample_data_senml.csv";


        builder.setSpout("spout1", new SampleSenMLSpout(spout1InputFilePath, spoutLogFileName, argumentClass.getScalingFactor(), inputRate, numEvents),
                1);


        builder.setBolt("SenMLParseBoltPREDTAXI",
                new SenMLParseBoltPREDTAXI(p_), 1)
                .shuffleGrouping("spout1");

//

        builder.setSpout("mqttSubscribeTaskBolt",
                new MQTTSubscribeSpout(p_,"dummyLog"), 1); // "RowString" should have path of blob
//
        builder.setBolt("AzureBlobDownloadTaskBolt",
                new AzureBlobDownloadTaskBolt(p_), 1)
                .shuffleGrouping("mqttSubscribeTaskBolt");


        builder.setBolt("DecisionTreeClassifyBolt",
                new in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.TAXI.DecisionTreeClassifyBolt(p_), 1)
                .shuffleGrouping("SenMLParseBoltPREDTAXI")
                .fieldsGrouping("AzureBlobDownloadTaskBolt",new Fields("ANALAYTICTYPE"));
//
//
        builder.setBolt("LinearRegressionPredictorBolt",
                new in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.TAXI.LinearRegressionPredictorBolt(p_), 1)
                .shuffleGrouping("SenMLParseBoltPREDTAXI")
                .fieldsGrouping("AzureBlobDownloadTaskBolt",new Fields("ANALAYTICTYPE"));
////
////
        builder.setBolt("BlockWindowAverageBolt",
                new BlockWindowAverageBolt(p_), 1)
                .shuffleGrouping("SenMLParseBoltPREDTAXI");
////
////
        builder.setBolt("ErrorEstimationBolt",
                new ErrorEstimationBolt(p_), 1)
                .shuffleGrouping("BlockWindowAverageBolt")
                .shuffleGrouping("LinearRegressionPredictorBolt");
////
        builder.setBolt("MQTTPublishBolt",
                new MQTTPublishBolt(p_), 1)
                .fieldsGrouping("ErrorEstimationBolt",new Fields("ANALAYTICTYPE"))
                .fieldsGrouping("DecisionTreeClassifyBolt",new Fields("ANALAYTICTYPE")) ;
////
////
        builder.setBolt("sink", new Sink(sinkLogFileName), 1).shuffleGrouping("MQTTPublishBolt");


        StormTopology stormTopology = builder.createTopology();

        if (argumentClass.getDeploymentMode().equals("C")) {
			System.out.println("Spout Log File: " + spoutLogFileName);
			System.out.println("Sink Log File: " + sinkLogFileName);
            StormSubmitter.submitTopology(argumentClass.getTopoName(), conf, stormTopology);
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(argumentClass.getTopoName(), conf, stormTopology);
            Utils.sleep(600000);
            cluster.killTopology(argumentClass.getTopoName());
            cluster.shutdown();
			System.out.println("Input Rate: " + metric_utils.Utils.getInputRate(spoutLogFileName));
			System.out.println("Throughput: " + metric_utils.Utils.getThroughput(sinkLogFileName));
        }
    }
}


//    L   IdentityTopology   /Users/anshushukla/PycharmProjects/DataAnlytics1/Storm-Scheduler-SC-scripts/SYS-inputcsvSCTable-1spouts100mps-480sec.csv     SYS-210  0.001   /Users/anshushukla/data/output/temp    /Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks.properties  test

//    L   IdentityTopology  /Users/anshushukla/PycharmProjects/DataAnlytics1/Storm-Scheduler-SC-scripts/TAXI-inputcsv-predict-10spouts200mps-480sec-file/TAXI-inputcsv-predict-10spouts200mps-480sec-file1.csv    SYS-210  0.001   /Users/anshushukla/data/output/temp    /Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks.properties  test

//    L   IdentityTopology   /Users/anshushukla/PycharmProjects/DataAnlytics1/Storm-Scheduler-SC-scripts/SYS-inputcsvSCTable-1spouts100mps-480sec.csv     TAXI-210  1.0   /Users/anshushukla/data/output/temp    /Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks_TAXI.properties  test