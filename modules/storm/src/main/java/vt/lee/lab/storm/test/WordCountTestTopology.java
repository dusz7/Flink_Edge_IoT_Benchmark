package vt.lee.lab.storm.test;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import in.dream_lab.bm.stream_iot.storm.sinks.Sink;;

/**
 * 
 * */

public class WordCountTestTopology {
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
		long numEvents = argumentClass.getNumEvents();
		String sinkLogFileName = outDir + "/sink-" + logFilePrefix;
		String spoutLogFileName = outDir + "/spout-" + logFilePrefix;

		long experimentDuration = 600000L; // get it as a CLI arg

		System.out.println("Will emit " + numEvents + " events at " + inputRate + " (events/sec) rate");
		
		TopologyBuilder builder = new TopologyBuilder();

		Config config = new Config();
		config.setNumAckers(0);
		config.put(Config.TOPOLOGY_BACKPRESSURE_ENABLE, true);
		config.setDebug(false);

		builder.setSpout("random_sentence_spout",
				new RandomSentenceSpout(spoutLogFileName, inputRate, experimentDuration, numEvents));
		builder.setBolt("word_count_bolt_1", new CountBolt(20), 1).shuffleGrouping("random_sentence_spout");
		builder.setBolt("word_count_bolt_2", new CountBolt(10), 1).shuffleGrouping("word_count_bolt_1");
		builder.setBolt("word_count_bolt_3", new CountBolt(100), 1).shuffleGrouping("word_count_bolt_2");
		builder.setBolt("word_count_bolt_4", new CountBolt(50), 1).shuffleGrouping("word_count_bolt_3");
		builder.setBolt("word_count_bolt_5", new CountBolt(30), 1).shuffleGrouping("word_count_bolt_4");
		builder.setBolt("word_count_bolt_6", new CountBolt(10), 1).shuffleGrouping("word_count_bolt_5");
		builder.setBolt("word_count_bolt_7", new CountBolt(70), 1).shuffleGrouping("word_count_bolt_6");
		builder.setBolt("word_count_bolt_8", new CountBolt(1), 1).shuffleGrouping("word_count_bolt_7");
		builder.setBolt("word_count_bolt_9", new CountBolt(30), 1).shuffleGrouping("word_count_bolt_8");
		builder.setBolt("sink", new Sink(sinkLogFileName), 1).shuffleGrouping("word_count_bolt_9");

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