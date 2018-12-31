package in.dream_lab.bm.stream_iot.storm.topo.apps;

/**
 * Created by anshushukla on 03/06/16.
 */

import in.dream_lab.bm.stream_iot.storm.bolts.IoTStatsBolt.*;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentClass;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentParser;
import in.dream_lab.bm.stream_iot.storm.sinks.IoTPredictionTopologySinkBolt;
import in.dream_lab.bm.stream_iot.storm.sinks.Sink;
import in.dream_lab.bm.stream_iot.storm.spouts.SampleSenMLSpout;
import in.dream_lab.bm.stream_iot.storm.spouts.SampleSenMLTimerSpout;
import in.dream_lab.bm.stream_iot.storm.spouts.SampleSpout;
import vt.lee.lab.storm.riot_resources.RiotResourceFileProps;

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

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * Created by anshushukla on 18/05/15.
 */
public class IoTStatsTopology {

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
        //String spoutLogFileName = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix;
        
        String spoutLogFileName = argumentClass.getOutputDirName() + "/" + argumentClass.getTopoName();
        
        String taskPropFilename= inputPath + "/" +  argumentClass.getTasksPropertiesFilename();
        // System.out.println("taskPropFilename-"+taskPropFilename);

        int inputRate = argumentClass.getInputRate();
		long numEvents = argumentClass.getNumEvents();
		int numWorkers = argumentClass.getNumWorkers();
        
		List<String> resourceFileProps = RiotResourceFileProps.getRiotResourceFileProps();
        
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_BACKPRESSURE_ENABLE, true);
		conf.setDebug(false);
		conf.setNumAckers(0);
		conf.put(conf.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS, 30);
		
		/*
		MetricReporterConfig metricReporterConfig = new MetricReporterConfig(".*",
				SimpleStormMetricProcessor.class.getCanonicalName(), Long.toString(inputRate), Long.toString((long) (numEvents*14.8*0.95)));
		conf.registerMetricsConsumer(MetricReporter.class, metricReporterConfig, 1);
		*/
		
		Map<String, Object> metricArg = new HashMap<String, Object>();
		metricArg.put("InputRate", (long) inputRate);
		metricArg.put("TotalEvents", (long) (numEvents*14.8*0.95));
		metricArg.put("OutputPrefix", argumentClass.getOutputDirName() + "/" + argumentClass.getTopoName());
		conf.registerMetricsConsumer(MyMetricsConsumer.class, metricArg, 1);
		
		// conf.put("policy", "eda-random");
		// conf.put("policy", "eda-dynamic");
		// conf.put("policy", "eda-static");
		// conf.put("static-bolt-ids", "SenMLParseBoltPREDSYS,DecisionTreeClassifyBolt,LinearRegressionPredictorBolt,BlockWindowAverageBolt,ErrorEstimationBolt,MQTTPublishBolt,sink");
		// conf.put("static-bolt-weights", "30,17,21,14,14,37,45");
		// conf.put("static-bolt-weights", "17,19,25,15,15,27,47");
        
		//conf.put("policy", "eda-chain");
		//conf.put("chain-bolt-ids", "ParseProjectSYSBolt,BloomFilterCheckBolt,KalmanFilterBolt,SimpleLinearRegressionPredictorBolt,SecondOrderMomentBolt,DistinctApproxCountBolt,MQTTPublishTaskBolt_Sink");
		//conf.put("chain-bolt-prios", "2,1,1,1,1,1,1");
				
		conf.put("policy", "eda-min-lat");
		conf.put("min-lat-bolt-ids", "ParseProjectSYSBolt,BloomFilterCheckBolt,KalmanFilterBolt,SimpleLinearRegressionPredictorBolt,SecondOrderMomentBolt,DistinctApproxCountBolt,MQTTPublishTaskBolt_Sink");
		conf.put("min-lat-bolt-prios", "1,2,3,4,6,5,7");
		
		conf.put("consume", "all");
		// conf.put("consume", "half");
		//conf.put("consume", "constant");
		//conf.put("constant", 50);
		
		conf.put("get_wait_time", true);
		conf.put("get_empty_time", true);
		conf.put("info_path", argumentClass.getOutputDirName());
		conf.put("get_queue_time", true);
		conf.put("queue_time_sample_freq", inputRate * 3);
		
		conf.put(Config.WORKER_HEAP_MEMORY_MB, 1024);
		
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

		String spout1InputFilePath = resourceDir + "/train_input_data.csv";
		
        TopologyBuilder builder = new TopologyBuilder();

        // builder.setSpout("spout", new SampleSpout(spout1InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
         //       1);
        //builder.setSpout("spout", new SampleSenMLSpout(spout1InputFilePath, spoutLogFileName, argumentClass.getScalingFactor(), inputRate, numEvents),
        //               1);
        
        builder.setSpout("spout", new SampleSenMLTimerSpout(spout1InputFilePath, spoutLogFileName,
				argumentClass.getScalingFactor(), inputRate, numEvents), 1);
        
        builder.setBolt("ParseProjectSYSBolt",
                new ParseProjectSYSBolt(p_), 1)
                .shuffleGrouping("spout");

        builder.setBolt("BloomFilterCheckBolt",
                new BloomFilterCheckBolt(p_), 1)
                .fieldsGrouping("ParseProjectSYSBolt",new Fields("obsType")); // filed grouping on obstype

        builder.setBolt("KalmanFilterBolt",
                new KalmanFilterBolt(p_), 1)
                .fieldsGrouping("BloomFilterCheckBolt",new Fields("sensorID","obsType"));

        builder.setBolt("SimpleLinearRegressionPredictorBolt",
                new SimpleLinearRegressionPredictorBolt(p_), 1)
                .fieldsGrouping("KalmanFilterBolt",new Fields("sensorID","obsType"));

        builder.setBolt("SecondOrderMomentBolt",
                new SecondOrderMomentBolt(p_), 1)
                .fieldsGrouping("BloomFilterCheckBolt",new Fields("sensorID","obsType"));

        builder.setBolt("DistinctApproxCountBolt",
                new DistinctApproxCountBolt(p_), 1)
                .fieldsGrouping("BloomFilterCheckBolt",new Fields("obsType")); // another change done already

        builder.setBolt("MQTTPublishTaskBolt_Sink",
                new MQTTPublishTaskBolt(p_), 3)
                .shuffleGrouping("SimpleLinearRegressionPredictorBolt")
                .shuffleGrouping("SecondOrderMomentBolt")
                .shuffleGrouping("DistinctApproxCountBolt");

        //builder.setBolt("sink", new Sink(sinkLogFileName), 1).shuffleGrouping("MQTTPublishTaskBolt");
        //builder.setBolt("sink", new IoTPredictionTopologySinkBolt(sinkLogFileName), 1).shuffleGrouping("MQTTPublishTaskBolt");


        StormTopology stormTopology = builder.createTopology();

        if (argumentClass.getDeploymentMode().equals("C")) {
            StormSubmitter.submitTopology(argumentClass.getTopoName(), conf, stormTopology);
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(argumentClass.getTopoName(), conf, stormTopology);
            Utils.sleep(100000);
            cluster.killTopology(argumentClass.getTopoName());
            cluster.shutdown();
        }
    }
}


// L   IdentityTopology   /Users/anshushukla/PycharmProjects/DataAnlytics1/Storm-Scheduler-SC-scripts/SYS-inputcsvSCTable-1spouts100mps-480sec.csv     PLUG-210  1.0   /Users/anshushukla/data/output/temp    /Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks.properties  test

// L   IdentityTopology   /Users/anshushukla/data/dataset-SYS-12min-100x.csv     SYS-210  1.0   /Users/anshushukla/data/output/temp    /Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks.properties  test

// L   IdentityTopology   /Users/anshushukla/data/dataset-TAXI-12min-100x.csv     TAXI-210  1.0   /Users/anshushukla/data/output/temp    /Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks.properties  test