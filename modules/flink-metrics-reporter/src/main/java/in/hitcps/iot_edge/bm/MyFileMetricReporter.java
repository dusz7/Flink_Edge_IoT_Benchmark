package in.hitcps.iot_edge.bm;

import org.apache.flink.metrics.*;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import java.io.File;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;

public class MyFileMetricReporter implements MetricReporter, Scheduled {

    private static final String lineSeparator = System.lineSeparator();
    // the initial size roughly fits ~150 metrics with default scope settings
    private int previousSize = 16384;

    private final Map<Counter, String> counters = new HashMap<>();
    private final Map<Gauge<?>, String> gauges = new HashMap<>();
    private final Map<Histogram, String> histograms = new HashMap<>();
    private final Map<Meter, String> meters = new HashMap<>();

    @Override
    public void open(MetricConfig metricConfig) {

    }

    @Override
    public void close() {

    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup metricGroup) {
        // get the name of metric, e.g. host-7.taskmanager-2.window_word_count.my-mapper.metricName
        String name = metricGroup.getMetricIdentifier(metricName);
        if (name.contains("jobmanager")) {
            return;
        }
//        String simpleName = name
        // save metric to hashMap
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

        try {
            File file = new File("/usr/local/etc/flink-remote/bm_files/metrics_logs/metrics.txt");
            if (!file.exists()) {
                file.createNewFile();
            }

            FileOutputStream fileOut = new FileOutputStream(file, true);

            StringBuilder builder = new StringBuilder((int) (this.previousSize * 1.1D));

            builder.append(lineSeparator);

            builder.append(lineSeparator).append("-- Counters -------------------------------------------------------------------").append(lineSeparator);
            for (Map.Entry metric : counters.entrySet()) {
                builder.append(metric.getValue()).append(": ").append(((Counter) metric.getKey()).getCount()).append(lineSeparator);
            }

            builder.append(lineSeparator).append("-- Gauges ---------------------------------------------------------------------").append(lineSeparator);
            for (Map.Entry metric : gauges.entrySet()) {
                builder.append(metric.getValue()).append(": ").append(((Gauge) metric.getKey()).getValue()).append(lineSeparator);
            }

            builder.append(lineSeparator).append("-- Meters ---------------------------------------------------------------------").append(lineSeparator);
            for (Map.Entry metric : meters.entrySet()) {
                builder.append(metric.getValue()).append(": ").append(((Meter) metric.getKey()).getRate()).append(lineSeparator);
            }

            builder.append(lineSeparator).append("-- Histograms -----------------------------------------------------------------").append(lineSeparator);
            for (Map.Entry metric : histograms.entrySet()) {
                HistogramStatistics stats = ((Histogram) metric.getKey()).getStatistics();
                builder.append(metric.getValue()).append(": count=").append(stats.size()).append(", min=").append(stats.getMin()).append(", max=").append(stats.getMax()).append(", mean=").append(stats.getMean()).append(", stddev=").append(stats.getStdDev()).append(", p50=").append(stats.getQuantile(0.5D)).append(", p75=").append(stats.getQuantile(0.75D)).append(", p95=").append(stats.getQuantile(0.95D)).append(", p98=").append(stats.getQuantile(0.98D)).append(", p99=").append(stats.getQuantile(0.99D)).append(", p999=").append(stats.getQuantile(0.999D)).append(lineSeparator);
            }

            fileOut.write(builder.toString().getBytes());
            fileOut.flush();
            fileOut.close();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
