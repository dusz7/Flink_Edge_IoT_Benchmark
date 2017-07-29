package vt.lee.lab.storm.test;

public class TopologyArgumentClass {
	String deploymentMode; // Local ('L') or Distributed-cluster ('C') Mode
	String topoName;
	String experiRunId;
	String outputDirName; // Path where the output log file from spout and sink
							// has to be kept
	int inputRate;
	long numEvents;

	public String getOutputDirName() {
		return outputDirName;
	}

	public void setOutputDirName(String outputDirName) {
		this.outputDirName = outputDirName;
	}

	public String getDeploymentMode() {
		return deploymentMode;
	}

	public void setDeploymentMode(String deploymentMode) {
		this.deploymentMode = deploymentMode;
	}

	public String getTopoName() {
		return topoName;
	}

	public void setTopoName(String topoName) {
		this.topoName = topoName;
	}

	public String getExperiRunId() {
		return experiRunId;
	}

	public void setExperiRunId(String experiRunId) {
		this.experiRunId = experiRunId;
	}

	public int getInputRate() {
		return inputRate;
	}

	public void setInputRate(int inputRate) {
		this.inputRate = inputRate;
	}
	
	public long getNumEvents() {
		return numEvents;
	}

	public void setNumEvents(long numEvents) {
		this.numEvents = numEvents;
	}

}
