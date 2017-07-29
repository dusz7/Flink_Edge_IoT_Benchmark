package vt.lee.lab.storm.test;

public class TopologyArgumentParser {
	/*
	 * Convention is: Command Meaning: topology-fully-qualified-name
	 * <local-or-cluster> <Topo-name> <input-dataset-path-name> <Experi-Run-id>
	 * <scaling-factor> Example command: SampleTopology L NA
	 * /var/tmp/bangalore.csv E01-01 0.001
	 */
	
	/**
	 * Sample execution command:
	 * 
	 * storm jar ./target/iot-bm-storm-0.1-jar-with-dependencies.jar 
	 * 	vt.lee.lab.storm.test.WordCountTestTopology 
	 * 	C 
	 * 	WordCountTestTopology 
	 * 	1 
	 * 	<output directory> 
	 * 	10000
	 * */
	
	public static TopologyArgumentClass parserCLI(String[] args) {
		if (args == null || args.length != 6) {
			System.out.println("invalid number of arguments");
			return null;
		} else {
			TopologyArgumentClass argumentClass = new TopologyArgumentClass();
			argumentClass.setDeploymentMode(args[0]);
			argumentClass.setTopoName(args[1]);
			argumentClass.setExperiRunId(args[2]);
			argumentClass.setOutputDirName(args[3]);
			argumentClass.setInputRate(Integer.parseInt(args[4]));
			argumentClass.setNumEvents(Integer.parseInt(args[5]));
			return argumentClass;
		}
	}

	public static void main(String[] args) {
		try {
		} catch (Exception e) {
			e.printStackTrace();
		}
		TopologyArgumentClass argumentClass = parserCLI(args);
		if (argumentClass == null) {
			System.out.println("Improper Arguments");
		} else {
			System.out.println(argumentClass.getDeploymentMode() + " : " + argumentClass.getExperiRunId() + ":");
		}
	}

}
