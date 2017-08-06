package in.dream_lab.bm.stream_iot.storm.topo.apps;



import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.SenMLParseBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.IoTStatsBolt.BlockWindowAverageBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.IoTStatsBolt.DistinctApproxCountBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.IoTStatsBolt.KalmanFilterBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.IoTStatsBolt.MultiLinePlotBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.IoTStatsBolt.SimpleLinearRegressionPredictorBolt;                                                                                                                                                                                                                                                                                                                                                                  
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentClass;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentParser;
import in.dream_lab.bm.stream_iot.storm.sinks.Sink;
import in.dream_lab.bm.stream_iot.storm.spouts.SampleSenMLSpout;
import vt.lee.lab.storm.riot_resources.RiotResourceFileProps;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * @author shilpa
 *
 */
public class StatsWithVisualizationTopology 
{

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
        String taskPropFilename= inputPath + "/" + argumentClass.getTasksPropertiesFilename();

        int inputRate = argumentClass.getInputRate();
		long numEvents = argumentClass.getNumEvents();

		List<String> resourceFileProps = RiotResourceFileProps.getStatsTopoResourceFileProps();
		
        Config conf = new Config();
		conf.put(Config.TOPOLOGY_BACKPRESSURE_ENABLE, true);
		conf.setDebug(false);
		conf.setNumAckers(0);
/*		conf.put("policy", "signal");
		conf.put("consume", "all");
		
		System.out.println("\n\n LIST CONSUME ALL \n\n");*/
		
		conf.put("policy", "signal");
		conf.put("consume", "constant");
		conf.put("constant", 100);
		
		System.out.println("\n\n LIST CONSUME CONSTANT 100 \n\n");
        
        
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

		String spout1InputFilePath = resourceDir + "/SYS_sample_data_senml.csv";
        
 		 		 
        builder.setSpout("spout", new SampleSenMLSpout(spout1InputFilePath, spoutLogFileName, argumentClass.getScalingFactor(), inputRate, numEvents),
                1);
        
        builder.setBolt("ParseSenML",
                new SenMLParseBolt(p_), 1)
                	.shuffleGrouping("spout");

        builder.setBolt("BlockWindowAverageBolt",
                new BlockWindowAverageBolt(p_),1)
                .fieldsGrouping("ParseSenML",new Fields("SENSORID","OBSTYPE"));
        
        builder.setBolt("KalmanFilterBolt",
                new KalmanFilterBolt(p_), 1)
                .fieldsGrouping("ParseSenML",new Fields("SENSORID","OBSTYPE"));

        builder.setBolt("SimpleLinearRegressionPredictorBolt",
                new SimpleLinearRegressionPredictorBolt(p_), 1)
                .fieldsGrouping("KalmanFilterBolt",new Fields("SENSORID","OBSTYPE"));

        builder.setBolt("DistinctApproxCountBolt",
                new DistinctApproxCountBolt(p_), 1)
                .fieldsGrouping("ParseSenML",new Fields("OBSTYPE")); // another change done already

        builder.setBolt("Visualization",
        		new MultiLinePlotBolt(p_), 1)
		        .fieldsGrouping("BlockWindowAverageBolt",new Fields("SENSORID","OBSTYPE") )
		        .fieldsGrouping("SimpleLinearRegressionPredictorBolt", new Fields("SENSORID","OBSTYPE"))
		        .fieldsGrouping("DistinctApproxCountBolt" , new Fields("OBSTYPE"));
        
//        builder.setBolt("AzureBlobUploadTaskBolt",
//                new AzureBlobUploadTaskBolt(p_), 1)
//                .shuffleGrouping("Visualization");
//        
        builder.setBolt("sink", new Sink(sinkLogFileName), 1)
                       .shuffleGrouping("Visualization");


        StormTopology stormTopology = builder.createTopology();

        if (argumentClass.getDeploymentMode().equals("C")) {
        	System.out.println("Spout Log File: " + spoutLogFileName);
			System.out.println("Sink Log File: " + sinkLogFileName);
            StormSubmitter.submitTopology(argumentClass.getTopoName(), conf, stormTopology);
        } else {
            LocalCluster cluster = new LocalCluster();
            
            cluster.submitTopology(argumentClass.getTopoName(), conf, stormTopology);
            Utils.sleep(7800000);
            System.out.println("Killing topo");
            cluster.killTopology(argumentClass.getTopoName());
            cluster.shutdown();
			System.out.println("Input Rate: " + metric_utils.Utils.getInputRate(spoutLogFileName));
			System.out.println("Throughput: " + metric_utils.Utils.getThroughput(sinkLogFileName));
        }
    }
}
