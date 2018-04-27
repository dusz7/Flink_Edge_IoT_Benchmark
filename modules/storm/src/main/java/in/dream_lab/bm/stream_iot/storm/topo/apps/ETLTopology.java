package in.dream_lab.bm.stream_iot.storm.topo.apps;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;


import com.github.staslev.storm.metrics.MetricReporter;
import com.github.staslev.storm.metrics.MetricReporterConfig;
import com.github.staslev.storm.metrics.yammer.SimpleStormMetricProcessor;

import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.AnnotationBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.AzureTableInsertBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.BloomFilterCheckBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.CsvToSenMLBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.InterpolationBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.JoinBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.MQTTPublishBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.RangeFilterBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.SenMLParseBolt;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentClass;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentParser;
import in.dream_lab.bm.stream_iot.storm.sinks.EtlTopologySinkBolt;
import in.dream_lab.bm.stream_iot.storm.spouts.SampleSenMLSpout;
import vt.lee.lab.storm.riot_resources.RiotResourceFileProps;

public class ETLTopology {
	public static void main(String[] args) throws Exception {
		ArgumentClass argumentClass = ArgumentParser.parserCLI(args);
		if (argumentClass == null) {
			System.out.println("ERROR! INVALID NUMBER OF ARGUMENTS");
			return;
		}

		String resourceDir = System.getenv("RIOT_RESOURCES");
		String inputPath = System.getenv("RIOT_INPUT_PROP_PATH");

		System.out.println("RESOURCE DIR: " + resourceDir);
		
		String logFilePrefix = argumentClass.getTopoName() + "-" + argumentClass.getExperiRunId() + "-"
				+ argumentClass.getScalingFactor() + ".log";
		String sinkLogFileName = argumentClass.getOutputDirName() + "/sink-" + logFilePrefix;
		String spoutLogFileName = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix;
		String taskPropFilename = inputPath + "/" + argumentClass.getTasksPropertiesFilename();
		int inputRate = argumentClass.getInputRate();
		long numEvents = argumentClass.getNumEvents();
		int numWorkers = argumentClass.getNumWorkers();

		List<Integer> boltInstances = argumentClass.getBoltInstances();
		if (boltInstances != null) {
			if ((boltInstances.size() != 9)) {
				System.out.println("Invalid Number of bolt instances provided. Exiting");
				System.exit(-1);
			}
		} else {
			// boltInstances = new ArrayList<Integer>(Arrays.asList(1,1,1,1,1,1,1,1,1));
			// boltInstances = new ArrayList<Integer>(Arrays.asList(4,4,4,4,4,4,4,4,4));
			// boltInstances = new ArrayList<Integer>(Arrays.asList(1,1,1,1,1,1,1,1,2));
			boltInstances = new ArrayList<Integer>(Arrays.asList(2,1,1,1,1,1,1,1,2));
		}
		
		List<String> resourceFileProps = RiotResourceFileProps.getRiotResourceFileProps();

		
		Config conf = new Config();
		conf.put(Config.TOPOLOGY_BACKPRESSURE_ENABLE, true);
		conf.setDebug(false);
		conf.setNumAckers(0);
		conf.setNumWorkers(numWorkers);
		
		//SimpleStormMetricProcessor processor;
		
		//Map config = new HashMap();
		//config.put(Config.TOPOLOGY_NAME, "ETLTopology");
		//processor = new SimpleStormMetricProcessor(config);
		
		/*only get capacity metrics*/
        MetricReporterConfig metricReporterConfig = new MetricReporterConfig(".*",
				SimpleStormMetricProcessor.class.getCanonicalName(), Long.toString(inputRate), Long.toString((long) (numEvents*1.95)));
		
		
		conf.registerMetricsConsumer(MetricReporter.class, metricReporterConfig, 1);
		
		// conf.put("policy", "eda-random");
		// conf.put("policy", "eda-dynamic");
		conf.put("policy", "eda-static");
		conf.put("static-bolt-ids", "SenMlParseBolt,RangeFilterBolt,BloomFilterBolt,InterpolationBolt,JoinBolt,AnnotationBolt,AzureInsert,CsvToSenMLBolt,PublishBolt,sink");
		// conf.put("static-bolt-weights", "31,15,15,25,17,9,8,19,14,22");
		conf.put("static-bolt-weights", "12,17,16,33,16,8,8,23,14,27");
        
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

		String spout1InputFilePath = resourceDir + "/SYS_sample_data_senml.csv";

		System.out.println(spout1InputFilePath);

		builder.setSpout("spout", new SampleSenMLSpout(spout1InputFilePath, spoutLogFileName,
				argumentClass.getScalingFactor(), inputRate, numEvents), 1);

		builder.setBolt("SenMlParseBolt", new SenMLParseBolt(p_), boltInstances.get(0)).shuffleGrouping("spout");

//		builder.setBolt("RangeFilterBolt", new RangeFilterBolt(p_), boltInstances.get(1)).fieldsGrouping("SenMlParseBolt",
//				new Fields("OBSTYPE"));

		builder.setBolt("RangeFilterBolt", new RangeFilterBolt(p_), boltInstances.get(1)).shuffleGrouping("SenMlParseBolt");
		
//		builder.setBolt("BloomFilterBolt", new BloomFilterCheckBolt(p_), boltInstances.get(2)).fieldsGrouping("RangeFilterBolt",
//				new Fields("OBSTYPE"));

		builder.setBolt("BloomFilterBolt", new BloomFilterCheckBolt(p_), boltInstances.get(2)).shuffleGrouping("RangeFilterBolt");
		
//		builder.setBolt("InterpolationBolt", new InterpolationBolt(p_), boltInstances.get(3)).fieldsGrouping("BloomFilterBolt",
//				new Fields("OBSTYPE"));
		
		builder.setBolt("InterpolationBolt", new InterpolationBolt(p_), boltInstances.get(3)).shuffleGrouping("BloomFilterBolt");
		
		builder.setBolt("JoinBolt", new JoinBolt(p_), boltInstances.get(4)).fieldsGrouping("InterpolationBolt", new Fields("MSGID"));
		
//		builder.setBolt("JoinBolt", new JoinBolt(p_), boltInstances.get(4)).shuffleGrouping("InterpolationBolt");

		builder.setBolt("AnnotationBolt", new AnnotationBolt(p_), boltInstances.get(5)).shuffleGrouping("JoinBolt");

		builder.setBolt("AzureInsert", new AzureTableInsertBolt(p_), boltInstances.get(6)).shuffleGrouping("AnnotationBolt");

		builder.setBolt("CsvToSenMLBolt", new CsvToSenMLBolt(p_), boltInstances.get(7)).shuffleGrouping("AnnotationBolt");

		builder.setBolt("PublishBolt", new MQTTPublishBolt(p_), boltInstances.get(8)).shuffleGrouping("CsvToSenMLBolt");

		/*
		 * builder.setBolt("sink", new Sink(sinkLogFileName),
		 * 1).shuffleGrouping("PublishBolt") .shuffleGrouping("AzureInsert");
		 */

		builder.setBolt("sink", new EtlTopologySinkBolt(sinkLogFileName), 1).shuffleGrouping("PublishBolt")
				.shuffleGrouping("AzureInsert");

		StormTopology stormTopology = builder.createTopology();

		if (argumentClass.getDeploymentMode().equals("C")) {
			System.out.println("Spout Log File: " + spoutLogFileName);
			System.out.println("Sink Log File: " + sinkLogFileName);
			StormSubmitter.submitTopology(argumentClass.getTopoName(), conf, stormTopology);
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(argumentClass.getTopoName(), conf, stormTopology);
			Utils.sleep(1800000);
			cluster.killTopology(argumentClass.getTopoName());
			cluster.shutdown();
			System.out.println("Input Rate: " + metric_utils.Utils.getInputRate(spoutLogFileName));
			System.out.println("Throughput: " + metric_utils.Utils.getThroughput(sinkLogFileName));
		}
	}
}
