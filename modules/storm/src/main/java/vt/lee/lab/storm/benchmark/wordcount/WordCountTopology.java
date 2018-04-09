package vt.lee.lab.storm.benchmark.wordcount;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

import in.dream_lab.bm.stream_iot.storm.sinks.Sink;
import vt.lee.lab.storm.test.TopologyArgumentClass;
import vt.lee.lab.storm.test.TopologyArgumentParser;

public class WordCountTopology {
	public static void main(String[] args) throws Exception {

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
			if ((boltInstances.size() != 2)) {
				System.out.println("Invalid Number of bolt instances provided. Exiting");
				System.exit(-1);
			}
		}
		else {
			// boltInstances = new ArrayList<Integer>(Arrays.asList(1,1));
			boltInstances = new ArrayList<Integer>(Arrays.asList(4,4));
		}
		
		TopologyBuilder builder = new TopologyBuilder();

		Config conf = new Config();
		conf.setNumAckers(0);
		conf.put(Config.TOPOLOGY_BACKPRESSURE_ENABLE, true);
		conf.setDebug(false);
		conf.setNumWorkers(numWorkers);
		

		/*only get capacity metrics*/
        MetricReporterConfig metricReporterConfig = new MetricReporterConfig(".*",
				SimpleStormMetricProcessor.class.getCanonicalName(), Long.toString(inputRate), Long.toString(numEvents));
		
        conf.registerMetricsConsumer(MetricReporter.class, metricReporterConfig, 1);
        
        conf.put("policy", "signal-simple");
        //conf.put("policy", "signal-group");
        //conf.put("policy", "signal-fair");
        //conf.put("waited_t", 8);
        
		conf.put("consume", "constant");
		conf.put("constant", 100);
       
        conf.put("fog-runtime-debug", "false");
        conf.put("debug-path", "/home/fuxinwei");
        
        conf.put("input-rate-adjust-enable", false);
        conf.put("init-freqency", inputRate);
        conf.put("delta-threshold", 50);
        conf.put("expire-threshold", 60);
        conf.put("force-stable", true);
		
        builder.setSpout("spout",
				new BookReaderSpout(spoutLogFileName, inputRate, numEvents));
		
		 builder.setBolt("split", new SplitSentenceBolt(), boltInstances.get(0))
     		.shuffleGrouping("spout");
     
	     builder.setBolt("count", new WordCountBolt(), boltInstances.get(1))
	     	.fieldsGrouping("split", new Fields("word"));
	     
	     builder.setBolt("report", new ReportBolt())
	     	.globalGrouping("count");
	     
	     builder.setBolt("sink", new Sink(sinkLogFileName))
	     	.globalGrouping("report");

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
