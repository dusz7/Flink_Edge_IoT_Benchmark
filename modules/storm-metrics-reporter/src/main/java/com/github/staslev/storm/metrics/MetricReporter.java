package com.github.staslev.storm.metrics;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.core.appender.SyslogAppender;
import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import java.io.*;
import java.net.*;

/**
 * A metric consumer implementation that reports storm metrics to Graphite. The
 * metrics to be reported are specified using a regular expression, so as to
 * avoid burdening Graphite with undesired metrics. <br/>
 * This metric consumer also reports a capacity metric, computed for each taskId
 * based on the number of executions and per-execution latency reported by Storm
 * internals. <br/>
 * <br/>
 * <url>Inspired by
 * <url>https://github.com/endgameinc/storm-metrics-statsd</url>
 */
public class MetricReporter implements IMetricsConsumer {

	public static final Logger LOG = LoggerFactory.getLogger(MetricReporter.class);

	private MetricMatcher allowedMetrics;
	private StormMetricProcessor stormMetricProcessor;
	
	private Map<String, Double> capMap;
	private Map<String, Integer> capCountMap;
	private Map<String, Double> execLatencyMap;
	private Map<String, Integer> execLatencyObs;
	private Map<String, Double> execCountMap;
	private Map<String, Integer> execCountObs;
	private Map<String, String> componentToWorker;
	
	
	private long totalEvents = 0;
	private long inputRate = 0;
	private double completedEvents = 0;
	private boolean reported = false;
	private String nimbusIp;
	Socket socket = null;
	PrintWriter out = null;
	int port = 38999;

	private double value(final Object value) {
		return ((Number) value).doubleValue();
	}

	private Map<String, List<Metric>> toMetricsByComponent(final Collection<DataPoint> dataPoints,
			final TaskInfo taskInfo) {

		final Map<String, List<Metric>> component2metrics = Maps.newHashMap();
		
		/* A mapping from component to the Pi it is running on */
		String key=taskInfo.srcComponentId+"#"+taskInfo.srcTaskId;
		if (!componentToWorker.containsKey(key)) {
			componentToWorker.put(key, taskInfo.srcWorkerHost);
		}
		
//		System.out.println("TASKINFO: ");
//		System.out.println("taskInfo.srcComponentId: " + taskInfo.srcComponentId + ", taskInfo.srcTaskId: " + taskInfo.srcTaskId +
//				"\ntaskInfo.srcWorkerHost" + taskInfo.srcWorkerHost + ", taskInfo.srcWorkerPort" + taskInfo.srcWorkerPort);
		
		for (final DataPoint dataPoint : dataPoints) {
			final String component = Metric.cleanNameFragment(taskInfo.srcComponentId);

			if (!component2metrics.containsKey(component)) {
				component2metrics.put(component, new LinkedList<Metric>());
			}

			component2metrics.get(component).addAll(extractMetrics(dataPoint, component));
		}

		return component2metrics;
	}

	private List<Metric> extractMetrics(final DataPoint dataPoint, final String component) {

		List<Metric> metrics = Lists.newArrayList();

		if (dataPoint.value instanceof Number) {
			metrics.add(new Metric(component, Metric.cleanNameFragment(dataPoint.name), value(dataPoint.value)));
		} else if (dataPoint.value instanceof Map) {
			@SuppressWarnings("rawtypes")
			final Map map = (Map) dataPoint.value;
			for (final Object subName : map.keySet()) {
				final Object subValue = map.get(subName);
				if (subValue instanceof Number) {
					metrics.add(new Metric(component, Metric.joinNameFragments(Metric.cleanNameFragment(dataPoint.name),
							Metric.cleanNameFragment(subName.toString())), value(subValue)));
				} else if (subValue instanceof Map) {
					metrics.addAll(extractMetrics(
							new DataPoint(Metric.joinNameFragments(dataPoint.name, subName), subValue), component));
				}
			}
		}

		return metrics;
	}

	@Override
	public void prepare(final Map stormConf, final Object registrationArgument, final TopologyContext context,
			final IErrorReporter errorReporter) {
		@SuppressWarnings("unchecked")
		final MetricReporterConfig config = MetricReporterConfig.from((List<String>) registrationArgument);
		allowedMetrics = new MetricMatcher(config.getAllowedMetricNames());
		stormMetricProcessor = config.getStormMetricProcessor(stormConf);
		inputRate = config.getInputRate();
		totalEvents = config.getTotalEvents();

		//System.out.println("TOTAL EVENTS = " + totalEvents);
		capMap = new HashMap<String, Double>();
		capCountMap = new HashMap<String, Integer>();
		execLatencyMap = new HashMap<String, Double>();
		execLatencyObs = new HashMap<String, Integer>();
		execCountMap = new HashMap<String, Double>();
		execCountObs = new HashMap<String, Integer>();
		componentToWorker = new HashMap<String, String>();
		
		Map stormConfig = Utils.readStormConfig();
		nimbusIp = stormConfig.get("nimbus.seeds").toString();
		nimbusIp = nimbusIp.substring(1, nimbusIp.length()-1);
	}

	private boolean setCompletedEvents(Map<String, List<Metric>> component2metrics) {
		boolean flag = false;
//		System.out.println("COMPONENT2METRICS");
//		System.out.println(component2metrics);
		List<Metric> sinkMetrics = component2metrics.get("sink");
		if (sinkMetrics != null) {
			for (Metric m : sinkMetrics) {
				if (m.getMetricName().contains("execute-count")) {
					completedEvents += m.getValue();
					flag = true;
				}
			}
		}
		return flag;
	}

	@Override
	public void handleDataPoints(final TaskInfo taskInfo, final Collection<DataPoint> dataPoints) {

		final Map<String, List<Metric>> component2metrics = toMetricsByComponent(dataPoints, taskInfo);
		final ImmutableList<Metric> capacityMetrics = CapacityCalculator.calculateCapacityMetrics(component2metrics,
				taskInfo);
		final Iterable<Metric> providedMetrics = Iterables.concat(component2metrics.values());
		final Iterable<Metric> allMetrics = Iterables.concat(providedMetrics, capacityMetrics);

		for (final Metric metric : FluentIterable.from(allMetrics).filter(allowedMetrics).toList()) {
			stormMetricProcessor.process(metric, taskInfo);
		}
		
		/* Send the metrics over the network to nimbus node. */
		// may be measure capacity only during stable state
		
		/*save values for capacity and the capacity reporting frequency*/
		for (Metric m : capacityMetrics) {
			capMap.put(m.getComponent(), capMap.getOrDefault(m.getComponent(), 0.0) + m.getValue());
			capCountMap.put(m.getComponent(), capCountMap.getOrDefault(m.getComponent(), 0) + 1);
		}		
		setCompletedEvents(component2metrics);
		
		/*Checking some other metrics*/
		for (Metric m : allMetrics) {
			if (m.getMetricName().contains("execute-latency")) {
				execLatencyMap.put(m.getComponent(), execLatencyMap.getOrDefault(m.getComponent(), 0.0) + m.getValue());
				execLatencyObs.put(m.getComponent(), execLatencyObs.getOrDefault(m.getComponent(), 0) + 1);
			}
			if (m.getMetricName().contains("execute-count")) {
				execCountMap.put(m.getComponent(), execCountMap.getOrDefault(m.getComponent(), 0.0) + m.getValue());
				execCountObs.put(m.getComponent(), execCountObs.getOrDefault(m.getComponent(), 0) + 1);
			}
		}

		/* Now the topology has finished execution. Calculate averages of capacity
		 * over all recorded measurement s and send to the nimbus node. */
		if (completedEvents >= totalEvents && !reported) {
			Map<String, Double> approxCap = new HashMap<String, Double>();
			Map<String, Double> approxLat = new HashMap<String, Double>();
			Map<String, Double> approxCnt = new HashMap<String, Double>();
			
			for (String cap : capMap.keySet()) {
				approxCap.put(cap, capMap.get(cap) / capCountMap.get(cap));
			}
			
			for (String lat : execLatencyMap.keySet()) {
				approxLat.put(lat, execLatencyMap.get(lat) / execLatencyObs.get(lat));
			}
			
			for (String cnt : execCountMap.keySet()) {
				approxCnt.put(cnt, execCountMap.get(cnt) / execCountObs.get(cnt));
			}

			try {
				InetAddress addr = InetAddress.getByName(nimbusIp);
				socket = new Socket(addr, port);
				out = new PrintWriter(socket.getOutputStream(), true);
				/* Send data to server */
				
				out.println(inputRate + "&" + approxCap + "&" + approxLat + "&" + componentToWorker);
				out.close();
				try {
					socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			LOG.info("Reported back the results for topo with input rate = " + inputRate);
			LOG.info(approxCap.toString());
			LOG.info("COMPLETED EVENTS = " + completedEvents);
			LOG.info(capMap.toString());
			LOG.info(capCountMap.toString());
			LOG.info(approxLat.toString());
			LOG.info(approxCnt.toString());
			LOG.info("componentToWorker mapping: ");
			LOG.info(componentToWorker.toString());	
			
			reported = true;
		}
	}

	@Override
	public void cleanup() {
	}
}