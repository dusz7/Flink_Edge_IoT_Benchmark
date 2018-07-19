package vt.lee.lab.storm.test.configurable;

import java.util.List;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import com.github.staslev.storm.metrics.MetricReporter;
import com.github.staslev.storm.metrics.MetricReporterConfig;
import com.github.staslev.storm.metrics.yammer.SimpleStormMetricProcessor;

//import com.github.staslev.storm.metrics.MetricReporter;
//import com.github.staslev.storm.metrics.MetricReporterConfig;
//import com.github.staslev.storm.metrics.yammer.SimpleStormMetricProcessor;

import in.dream_lab.bm.stream_iot.storm.sinks.Sink;
import vt.lee.lab.storm.test.RandomSentenceSpout;
import vt.lee.lab.storm.test.TopologyArgumentClass;
import vt.lee.lab.storm.test.TopologyArgumentParser;;

public class ConfigurableTopology {
	public static void main(String[] args) {

		TopologyArgumentClass argumentClass = TopologyArgumentParser.parserCLI(args);
		if (argumentClass == null) {
			System.out.println("ERROR! INVALID NUMBER OF ARGUMENTS");
			return;
		}
		String logFilePrefix = argumentClass.getTopoName() + "-" + argumentClass.getExperiRunId() + "-" + ".log";
		int inputRate = argumentClass.getInputRate();
		String outDir = argumentClass.getOutputDirName();
		String topologyName = argumentClass.getTopoName();
		String sinkLogFileName = outDir + "/sink-" + logFilePrefix;
		String spoutLogFileName = outDir + "/spout-" + logFilePrefix;
		long numEvents = argumentClass.getNumEvents();
		int numWorkers = argumentClass.getNumWorkers();
		
		List<Integer> boltInstances = argumentClass.getBoltInstances();
		List<Integer> boltComplexities = argumentClass.getBoltComplexities();
		List<Integer> boltInputRatios = argumentClass.getBoltInputRatios();
		List<Integer> boltOutputRatios = argumentClass.getBoltOutputRatios();
		
		int confBoltNum = boltInstances.size();
		if (confBoltNum == 0 || 
				boltComplexities.size() != confBoltNum ||
				boltInputRatios.size() != confBoltNum || 
				boltOutputRatios.size() != confBoltNum) {
			System.out.println("Invalid Number of bolt instances provided. Exiting");
			System.exit(-1);
		}
		
		long experimentDuration = 300000L; // get it as a CLI arg , 5 minutes

		TopologyBuilder builder = new TopologyBuilder();

		Config config = new Config();
		config.setNumAckers(0);
		config.put(Config.TOPOLOGY_BACKPRESSURE_ENABLE, true);
		config.setDebug(false);
		config.setNumWorkers(numWorkers);
		
		
		long metricNumEvents;
		double sinkNumEvents = numEvents;
		for (int i = 0; i < confBoltNum; i++) {
			sinkNumEvents = sinkNumEvents * boltOutputRatios.get(i);
		}
		for (int i = 0; i < confBoltNum; i++) {
			sinkNumEvents = sinkNumEvents / boltInputRatios.get(i);
		}
		metricNumEvents = (long) (sinkNumEvents * 0.95);
		
		System.out.println("metricNumEvents: " + metricNumEvents);;
		
		/*only get capacity metrics*/
        MetricReporterConfig metricReporterConfig = new MetricReporterConfig(".*",
				SimpleStormMetricProcessor.class.getCanonicalName(), Long.toString(inputRate), Long.toString(metricNumEvents));
		
        config.registerMetricsConsumer(MetricReporter.class, metricReporterConfig, 1);
		
		builder.setSpout("random_sentence_spout",
				new RandomSentenceSpout(spoutLogFileName, inputRate, experimentDuration, numEvents));
		
		String predecessorName = "random_sentence_spout";
		String weightIDs = "";
		for (int i = 0; i < confBoltNum; i++) {
			String currName = "configurable_bolt_" + i;
			int complexity = boltComplexities.get(i);
			int inputRatio = boltInputRatios.get(i);
			int outputRatio = boltOutputRatios.get(i);
			int instance = boltInstances.get(i);
			builder.setBolt(currName, new ConfBolt(complexity, inputRatio, outputRatio), instance)
					.shuffleGrouping(predecessorName);
			predecessorName = currName;
			
			weightIDs += currName + ",";
		}
		
		builder.setBolt("sink", new ConfSink(sinkLogFileName, (long) sinkNumEvents), 1).shuffleGrouping(predecessorName);

		StormTopology stormTopology = builder.createTopology();
		
		
		/*
		config.put("policy", "eda-weight");
		config.put("weight-ids", 			weightIDs.substring(0, weightIDs.length() - 1));
		config.put("weight-instances", 		argumentClass.getBoltInstancesStr());
		config.put("weight-complexities", 	argumentClass.getBoltComplexitiesStr());
		config.put("weight-inputRatios", 	argumentClass.getBoltInputRatiosStr());
		config.put("weight-outputRatios", 	argumentClass.getBoltOutputRatiosStr());
		*/
		config.put("policy", "signal-simple");
		
		//config.put("consume", "half");
		config.put("consume", "constant");
        config.put("constant", 10);
       
        config.put("fog-runtime-debug", "false");
        config.put("debug-path", "/home/fuxinwei");
        
        config.put("input-rate-adjust-enable", false);
        config.put("init-freqency", 5000);
        config.put("delta-threshold", 50);
        config.put("expire-threshold", 60);
        config.put("force-stable", true);
		
		if (argumentClass.getDeploymentMode().equals("C")) {
			try {
				System.out.println(spoutLogFileName);
				System.out.println(sinkLogFileName);
				StormSubmitter.submitTopology(argumentClass.getTopoName(), config, stormTopology);
			} catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
				e.printStackTrace();
			}
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(argumentClass.getTopoName(), config, stormTopology);
			Utils.sleep(experimentDuration);
			cluster.killTopology(argumentClass.getTopoName());
			cluster.shutdown();
			System.out.println("Input Rate: " + metric_utils.Utils.getInputRate(spoutLogFileName));
			System.out.println("Throughput: " + metric_utils.Utils.getThroughput(sinkLogFileName));
		}
	}
}