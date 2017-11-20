package com.github.staslev.storm.metrics.yammer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.Map;

import javax.management.ObjectName;

import org.apache.storm.Config;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.junit.Test;

import com.github.staslev.storm.metrics.Metric;
import com.github.staslev.storm.metrics.MetricReporterConfig;

public class SimpleStormMetricProcessorTest {
	SimpleStormMetricProcessor processor;

	@Test
	public void testValidObjectName() throws Exception {

		final String topologyName = "someTopology";

		Map config = new HashMap();
		config.put(Config.TOPOLOGY_NAME, topologyName);
		processor = new SimpleStormMetricProcessor(config);

		MetricReporterConfig metricReporterConfig = new MetricReporterConfig(".*",
				SimpleStormMetricProcessor.class.getCanonicalName());

		final SimpleStormMetricProcessor stormMetricProcessor = (SimpleStormMetricProcessor) metricReporterConfig
				.getStormMetricProcessor(config);
	}

}
