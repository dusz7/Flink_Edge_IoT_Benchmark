package in.dream_lab.bm.stream_iot.storm.topo.apps;

/**
 * Created by anshushukla on 03/06/16.
 */

import in.dream_lab.bm.stream_iot.storm.bolts.TRAIN.SYS.*;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentClass;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentParser;
import in.dream_lab.bm.stream_iot.storm.sinks.IoTTrainTopologySinkBolt;
import in.dream_lab.bm.stream_iot.storm.sinks.Sink;
import in.dream_lab.bm.stream_iot.storm.spouts.SampleSenMLSpout;
import in.dream_lab.bm.stream_iot.storm.spouts.SampleSpoutTimerForTrain;
import in.dream_lab.bm.stream_iot.storm.spouts.TimeSpout;
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

//import in.dream_lab.bm.stream_iot.storm.spouts.TimeSpout;

/**
 * Created by anshushukla on 18/05/15.
 */
public class IoTTrainTopologySYS {

	public static void main(String[] args) throws Exception {

		ArgumentClass argumentClass = ArgumentParser.parserCLI(args);
		if (argumentClass == null) {
			System.out.println("ERROR! INVALID NUMBER OF ARGUMENTS");
			return;
		}
		String resourceDir = System.getenv("RIOT_RESOURCES");
		String inputPath = System.getenv("RIOT_INPUT_PROP_PATH");

		String logFilePrefix = argumentClass.getTopoName() + "-" + argumentClass.getExperiRunId() + "-"
				+ argumentClass.getScalingFactor() + ".log";
		String sinkLogFileName = argumentClass.getOutputDirName() + "/sink-" + logFilePrefix;
		String spoutLogFileName = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix;
		String taskPropFilename = inputPath + "/" + argumentClass.getTasksPropertiesFilename();
		System.out.println("taskPropFilename-" + taskPropFilename);
		int inputRate = argumentClass.getInputRate();
		long numEvents = argumentClass.getNumEvents();

		List<String> resourceFileProps = RiotResourceFileProps.getPredTaxiResourceFileProps();

		Config conf = new Config();
		conf.put(Config.TOPOLOGY_BACKPRESSURE_ENABLE, true);
		conf.setDebug(false);
		conf.setNumAckers(0);
		
		// conf.put("policy", "eda-random");
		conf.put("policy", "eda-dynamic");
		// conf.put("policy", "eda-static");
		// conf.put("static-bolt-ids", "SenMLParseBoltPREDSYS,DecisionTreeClassifyBolt,LinearRegressionPredictorBolt,BlockWindowAverageBolt,ErrorEstimationBolt,MQTTPublishBolt,sink");
		// conf.put("static-bolt-weights", "30,17,21,14,14,37,45");
		// conf.put("static-bolt-weights", "17,19,25,15,15,27,47");
		
		// conf.put("consume", "all");
		conf.put("consume", "constant");
		conf.put("constant", 100);
		
		conf.put("get_wait_time", true);
		conf.put("get_empty_time", true);
		conf.put("info_path", argumentClass.getOutputDirName());
		conf.put("get_queue_time", true);
		conf.put("queue_time_sample_freq", inputRate * 3);

		Properties p_ = new Properties();
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

		String spout1InputFilePath = resourceDir + "/inputFileForTimerSpout-CITY.csv";

		builder.setSpout("TimeSpout",
				new SampleSpoutTimerForTrain(spout1InputFilePath, spoutLogFileName, argumentClass.getScalingFactor(), inputRate, numEvents),
				1);

		builder.setBolt("AzureTableRangeQueryBolt", new AzureTableRangeQueryBolt(p_), 1)
				.shuffleGrouping("TimeSpout");

		builder.setBolt("LinearRegressionTrainBolt", new LinearRegressionTrainBolt(p_), 1)
				.shuffleGrouping("AzureTableRangeQueryBolt");

		builder.setBolt("AzureBlobUploadTaskBolt", new AzureBlobUploadTaskBolt(p_), 1)
				.shuffleGrouping("DecisionTreeTrainBolt")
				.shuffleGrouping("LinearRegressionTrainBolt");

		builder.setBolt("MQTTPublishBolt", new MQTTPublishBolt(p_), 1)
				.shuffleGrouping("AzureBlobUploadTaskBolt");

		builder.setBolt("AnnotateDTClassBolt", new AnnotateDTClassBolt(p_), 1)
				.shuffleGrouping("AzureTableRangeQueryBolt");

		builder.setBolt("DecisionTreeTrainBolt", new DecisionTreeTrainBolt(p_), 1)
				.shuffleGrouping("AnnotateDTClassBolt");

		builder.setBolt("sink", new IoTTrainTopologySinkBolt(sinkLogFileName), 1).shuffleGrouping("AzureBlobUploadTaskBolt");

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

// L IdentityTopology
// /Users/anshushukla/PycharmProjects/DataAnlytics1/Storm-Scheduler-SC-scripts/SYS-inputcsv-predict-10spouts600mps-480sec-file/SYS-inputcsv-predict-10spouts600mps-480sec-file1.csv
// SYS-210 0.001 /Users/anshushukla/data/output/temp
// /Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks_CITY.properties
// test

// for taxi args
// L IdentityTopology
// /Users/anshushukla/PycharmProjects/DataAnlytics1/Storm-Scheduler-SC-scripts/SYS-inputcsvSCTable-1spouts100mps-480sec.csv
// PLUG-213 1.0 /Users/anshushukla/data/output/temp
// /Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks_CITY.properties
// test