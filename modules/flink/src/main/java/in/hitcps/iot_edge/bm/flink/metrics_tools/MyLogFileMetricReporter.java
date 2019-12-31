package in.hitcps.iot_edge.bm.flink.metrics_tools;

import org.apache.flink.metrics.*;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;

import java.util.HashMap;
import java.util.Map;

public class MyLogFileMetricReporter implements MetricReporter, Scheduled, CharacterFilter {
    private static Logger l = LoggerFactory.getLogger(MyLogFileMetricReporter.class);

    private final Map<Counter, String> counters = new HashMap<>();
    private final Map<Gauge<?>, String> gauges = new HashMap<>();
    private final Map<Histogram, String> histograms = new HashMap<>();
    private final Map<Meter, String> meters = new HashMap<>();

    // filter the scope
    @Override
    public String filterCharacters(String s) {
        return null;
    }

    @Override
    public void open(MetricConfig metricConfig) {

    }

    @Override
    public void close() {

    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup metricGroup) {
        String name = metricGroup.getMetricIdentifier(metricName);
        synchronized (this) {
            if (metric instanceof Counter) {
                counters.put((Counter) metric, name);
            } else if (metric instanceof Gauge<?>) {
                gauges.put((Gauge<?>) metric, name);
            } else if (metric instanceof Histogram) {
                histograms.put((Histogram) metric, name);
            } else if (metric instanceof Meter) {
                meters.put((Meter) metric, name);
            }
        }
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup metricGroup) {
        synchronized (this) {
            if (metric instanceof Counter) {
                counters.remove(metric);
            } else if (metric instanceof Gauge) {
                gauges.remove(metric);
            } else if (metric instanceof Histogram) {
                histograms.remove(metric);
            } else if (metric instanceof Meter) {
                meters.remove(metric);
            }
        }
    }

    // report to the extend system
    @Override
    public void report() {
        l.warn("============  starting metric report  =============");

        for (Map.Entry metric : counters.entrySet()) {
            l.warn("  counter :  {} -- {}", metric.getValue(), metric.getKey());
        }

        for (Map.Entry metric : gauges.entrySet()) {
            l.warn("  gauge :  {} -- {}", metric.getValue(), metric.getKey());
        }

        for (Map.Entry metric : histograms.entrySet()) {
            l.warn("  histogram :  {} -- {}", metric.getValue(), metric.getKey());
        }

        for (Map.Entry metric : meters.entrySet()) {
            l.warn("  meter :  {} -- {}", metric.getValue(), metric.getKey());
        }
    }
}
