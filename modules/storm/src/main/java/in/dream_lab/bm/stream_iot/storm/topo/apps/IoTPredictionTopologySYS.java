package in.dream_lab.bm.stream_iot.storm.topo.apps;

/**
 * Created by anshushukla on 03/06/16.
 */


import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.SenMLParseBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.SYS.*;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentClass;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentParser;
import in.dream_lab.bm.stream_iot.storm.sinks.IoTPredictionTopologySinkBolt;
import in.dream_lab.bm.stream_iot.storm.sinks.Sink;
import in.dream_lab.bm.stream_iot.storm.spouts.MQTTSubscribeSpout;
import in.dream_lab.bm.stream_iot.storm.spouts.SampleSenMLSpout;
import vt.lee.lab.storm.riot_resources.RiotResourceFileProps;
import in.dream_lab.bm.stream_iot.storm.spouts.SampleSenMLSpout;
//import in.dream_lab.bm.stream_iot.storm.spouts.TimeSpout;
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
public class IoTPredictionTopologySYS {

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
        String taskPropFilename = inputPath + "/" +  argumentClass.getTasksPropertiesFilename();
        System.out.println("taskPropFilename-"+taskPropFilename);
		int inputRate = argumentClass.getInputRate();
		long numEvents = argumentClass.getNumEvents();

		List<String> resourceFileProps = RiotResourceFileProps.getPredSysResourceFileProps();


        Config conf = new Config();
		conf.put(Config.TOPOLOGY_BACKPRESSURE_ENABLE, true);
		conf.setDebug(false);
		conf.setNumAckers(0);
		conf.put("policy", "signal");
		conf.put("consume", "half");
		
		
		System.out.println("policy" + "signal");
		System.out.println("consume" + "HALF");

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


        String spout1InputFilePath= resourceDir + "/SYS_sample_data_senml.csv";
        
         builder.setSpout("spout1", new SampleSenMLSpout(spout1InputFilePath, spoutLogFileName, argumentClass.getScalingFactor(), inputRate, numEvents),
                1);

        builder.setBolt("SenMLParseBoltPREDSYS",
                new SenMLParseBoltPREDSYS(p_), 10)
                .shuffleGrouping("spout1");


/*        builder.setSpout("mqttSubscribeTaskBolt",
                new MQTTSubscribeSpout(p_,"dummyLog-SYS"), 1); // "RowString" should have path of blob

        builder.setBolt("AzureBlobDownloadTaskBolt",
                new AzureBlobDownloadTaskBolt(p_), 1)
                .shuffleGrouping("mqttSubscribeTaskBolt");*/

        builder.setBolt("DecisionTreeClassifyBolt",
                new DecisionTreeClassifyBolt(p_), 10)
                .shuffleGrouping("SenMLParseBoltPREDSYS");
//                .fieldsGrouping("AzureBlobDownloadTaskBolt",new Fields("ANALAYTICTYPE"));
//
//
        builder.setBolt("LinearRegressionPredictorBolt",
                new LinearRegressionPredictorBolt(p_), 1)
                .shuffleGrouping("SenMLParseBoltPREDSYS");
//                .fieldsGrouping("AzureBlobDownloadTaskBolt",new Fields("ANALAYTICTYPE"));


        builder.setBolt("BlockWindowAverageBolt",
                new BlockWindowAverageBolt(p_), 10)
                .shuffleGrouping("SenMLParseBoltPREDSYS");

//
        builder.setBolt("ErrorEstimationBolt",
                new ErrorEstimationBolt(p_), 10)
                .shuffleGrouping("BlockWindowAverageBolt")
                .shuffleGrouping("LinearRegressionPredictorBolt");

        builder.setBolt("MQTTPublishBolt",
                new MQTTPublishBolt(p_), 10)
                .shuffleGrouping("ErrorEstimationBolt")
                .shuffleGrouping("DecisionTreeClassifyBolt") ;


/*        builder.setBolt("sink", new Sink(sinkLogFileName), 1).shuffleGrouping("MQTTPublishBolt");
*/
        builder.setBolt("sink", new IoTPredictionTopologySinkBolt(sinkLogFileName), 1).shuffleGrouping("MQTTPublishBolt");


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


//    L   IdentityTopology  /Users/anshushukla/PycharmProjects/DataAnlytics1/Storm-Scheduler-SC-scripts/SYS-inputcsv-predict-10spouts600mps-480sec-file/SYS-inputcsv-predict-10spouts600mps-480sec-file1.csv     SYS-210  0.001   /Users/anshushukla/data/output/temp    /Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks_CITY.properties  test
