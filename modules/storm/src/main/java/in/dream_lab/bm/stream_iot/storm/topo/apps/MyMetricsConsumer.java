package in.dream_lab.bm.stream_iot.storm.topo.apps;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.utils.Utils;

public class MyMetricsConsumer implements IMetricsConsumer {
	private long inputRate;
	private long totalEvents;
	private String outputPrefix;
	private double completedEvents = 0;
	private boolean reported = false;
	
	private String nimbusIp;
	private int port = 38999;
	
	private final Map<String, Map<Integer, Map<String, List<Object>>>> OpMetrics = 
			new HashMap<String, Map<Integer, Map<String, List<Object>>>>();
	private final Map<String, Map<Integer, Map<String, List<Object>>>> TopoMetrics = 
			new HashMap<String, Map<Integer, Map<String, List<Object>>>>();
	
	@Override
    public void prepare(
    		Map stormConf, 
    		Object registrationArgument, 
    		TopologyContext context, 
    		IErrorReporter errorReporter) { 
		Map<String, Object> argument = (Map<String, Object>) registrationArgument;
		inputRate = (long) argument.get("InputRate");
		totalEvents = (long) argument.get("TotalEvents");
		outputPrefix = (String) argument.get("OutputPrefix");
		
		Map stormConfig = Utils.readStormConfig();
		nimbusIp = stormConfig.get("nimbus.seeds").toString();
		nimbusIp = nimbusIp.substring(1, nimbusIp.length()-1);
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
		String componentId = taskInfo.srcComponentId;
		if (componentId.startsWith("spout") ||
				componentId.startsWith("__")) {
			return;
		}
		
		int taskId = taskInfo.srcTaskId;
		int updateIntervalSecs = taskInfo.updateIntervalSecs;
		
		/*
		String padding = "                       ";
		StringBuilder sb = new StringBuilder();
        String header = String.format("%d\t%15s:%-4d\t%3d:%-11s\t",
            taskInfo.timestamp,
            taskInfo.srcWorkerHost, taskInfo.srcWorkerPort,
            taskInfo.srcTaskId,
            taskInfo.srcComponentId);
        sb.append(header);
		*/
		
		Map<String, Long> executeCountMap = null;
		Map<String, Double> executeLatencyMap = null;
		Long waitLatency = -1L;
		Long emptyTime = -1L;
		Double queueTime = -1.0;
		Double totalLatency = -1.0;
		for (DataPoint dataPoint : dataPoints) {
			/*
			DataPoint p = dataPoint;
			sb.delete(header.length(), sb.length());
            sb.append(p.name)
                .append(padding).delete(header.length()+23,sb.length()).append("\t")
                .append(p.value);
            
            System.out.println(sb.toString());
			*/
			
			String name = dataPoint.name;
			switch (name) {
			case "__execute-count":
				executeCountMap = (Map<String, Long>) dataPoint.value;
				break;
			case "__execute-latency":
				executeLatencyMap = (Map<String, Double>) dataPoint.value;
				break;
			case "__wait-latency":
				//waitLatency = (Long) dataPoint.value;
				Map<String, Long> waitLatencyMap = (Map<String, Long>) dataPoint.value;
				waitLatency = waitLatencyMap.get("default");
				break;
			case "__empty-time":
				//emptyTime = (Long) dataPoint.value;
				Map<String, Long> emptyTimeMap = (Map<String, Long>) dataPoint.value;
				emptyTime = emptyTimeMap.get("default");
				break;
			case "__queue-time":
				//queueTime = (Double) dataPoint.value;
				Map<String, Double> queueTimeMap = (Map<String, Double>) dataPoint.value;
				queueTime = queueTimeMap.get("default");
				break;
			case "total-latency":
				totalLatency = (Double) dataPoint.value;
				break;
			default:
				break;
			}
		}
		
		Long executeCount = 0L;
		Double ececuteTime = 0.0;
		for (Entry<String, Long> entry : executeCountMap.entrySet()) {
			String key = entry.getKey();
			Long count = entry.getValue();
			Double latency = executeLatencyMap.get(key);
			
			executeCount += count;
			ececuteTime += count * latency;
		}
		
		Double executeLatency = ececuteTime / executeCount;
		Double capacity = ececuteTime / (1000 * updateIntervalSecs);
		
		updateMetrics(OpMetrics, componentId, taskId, "execute-count", executeCount);
		updateMetrics(OpMetrics, componentId, taskId, "execute-latency", executeLatency);
		updateMetrics(OpMetrics, componentId, taskId, "capacity", capacity);
		
		updateMetrics(OpMetrics, componentId, taskId, "wait-latency", waitLatency);
		updateMetrics(OpMetrics, componentId, taskId, "empty-time", emptyTime);
		updateMetrics(OpMetrics, componentId, taskId, "queue-time", queueTime);
		
		if (componentId.contains("_Sink")) {
			updateMetrics(TopoMetrics, componentId, taskId, "topo-throughput", executeCount);
			updateMetrics(TopoMetrics, componentId, taskId, "topo-latency", totalLatency);
			
			completedEvents += executeCount;
		}
		
		if (completedEvents >= totalEvents && !reported) {
			reported = true;
			writeToFile(OpMetrics, outputPrefix + "OpMetrics");
			writeToFile(TopoMetrics, outputPrefix + "TopoMetrics");
			
			try {
				InetAddress addr = InetAddress.getByName(nimbusIp);
				Socket socket = new Socket(addr, port);
				PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
				out.println(inputRate);
				out.close();
				socket.close();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} 
		}
	}
	
	private void updateMetrics(
			Map<String, Map<Integer, Map<String, List<Object>>>> metrics,
			String componentId,
			int taskId,
			String metricName,
			Object metricVal) {
		if (!metrics.containsKey(componentId)) {
			metrics.put(componentId, new HashMap<Integer, Map<String, List<Object>>>());
		}
		if (!metrics.get(componentId).containsKey(taskId)) {
			metrics.get(componentId).put(taskId, new HashMap<String, List<Object>>());
		}
		if (!metrics.get(componentId).get(taskId).containsKey(metricName)) {
			metrics.get(componentId).get(taskId).put(metricName, new ArrayList<Object>());
		}
		metrics.get(componentId).get(taskId).get(metricName).add(metricVal);
	}
	
	private void writeToFile(Object o, String path) {
		try {
			FileOutputStream fileOut = new FileOutputStream(path);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
	        out.writeObject(o);
	        out.close();
	        fileOut.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}
