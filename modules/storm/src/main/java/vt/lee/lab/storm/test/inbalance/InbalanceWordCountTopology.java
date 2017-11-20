package vt.lee.lab.storm.test.inbalance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.core.config.ConfigurationFactory;
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

import in.dream_lab.bm.stream_iot.storm.sinks.Sink;
import vt.lee.lab.storm.test.RandomSentenceSpout;
import vt.lee.lab.storm.test.TopologyArgumentClass;
import vt.lee.lab.storm.test.TopologyArgumentParser;;

public class InbalanceWordCountTopology {
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
		if (boltInstances != null) {
			if ((boltInstances.size() != 8)) {
				System.out.println("Invalid Number of bolt instances provided. Exiting");
				System.exit(-1);
			}
		}
		else {
			boltInstances = new ArrayList<Integer>(Arrays.asList(1,1,1,1,1,1,1,1));
		}
		
		long experimentDuration = 300000L; // get it as a CLI arg , 5 minutes

		TopologyBuilder builder = new TopologyBuilder();

		Config config = new Config();
		config.setNumAckers(0);
		config.put(Config.TOPOLOGY_BACKPRESSURE_ENABLE, false);
		config.setDebug(false);
		config.setNumWorkers(numWorkers);
		

		/*only get capacity metrics*/
        MetricReporterConfig metricReporterConfig = new MetricReporterConfig(".*",
				SimpleStormMetricProcessor.class.getCanonicalName(), Long.toString(inputRate), Long.toString(numEvents));
		
        config.registerMetricsConsumer(MetricReporter.class, metricReporterConfig, 1);
		
		builder.setSpout("random_sentence_spout",
				new RandomSentenceSpout(spoutLogFileName, inputRate, experimentDuration, numEvents));
		builder.setBolt("word_count_bolt_1", new InbalanceCountBolt(10), boltInstances.get(0)).shuffleGrouping("random_sentence_spout");
		builder.setBolt("word_count_bolt_2", new InbalanceCountBolt(100), boltInstances.get(1)).shuffleGrouping("word_count_bolt_1");
		builder.setBolt("word_count_bolt_3", new InbalanceCountBolt(20), boltInstances.get(2)).shuffleGrouping("word_count_bolt_2");
		builder.setBolt("word_count_bolt_4", new InbalanceCountBolt(20), boltInstances.get(3)).shuffleGrouping("word_count_bolt_3");
		builder.setBolt("word_count_bolt_5", new InbalanceCountBolt(100), boltInstances.get(4)).shuffleGrouping("word_count_bolt_4");
		builder.setBolt("word_count_bolt_6", new InbalanceCountBolt(10), boltInstances.get(5)).shuffleGrouping("word_count_bolt_5");
		builder.setBolt("word_count_bolt_7", new InbalanceCountBolt(100), boltInstances.get(6)).shuffleGrouping("word_count_bolt_6");
		builder.setBolt("word_count_bolt_8", new InbalanceCountBolt(25), boltInstances.get(7)).shuffleGrouping("word_count_bolt_7");
		builder.setBolt("sink", new Sink(sinkLogFileName), 1).shuffleGrouping("word_count_bolt_8");

		StormTopology stormTopology = builder.createTopology();

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