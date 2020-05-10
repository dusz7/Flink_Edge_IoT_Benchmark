package in.hitcps.iot_edge.bm;

import org.apache.flink.metrics.*;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import java.time.LocalDateTime;
import java.time.Instant;
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
        if (metricName.contains("endExperiment")) {
            return;
        }

        // gets the scope as an array of the scope components, e.g. ["host-7", "taskmanager-2", "window_word_count", "my-mapper"]
        String[] scopeComponents = metricGroup.getScopeComponents();
        // gets the identifier of metric, e.g. host-7.taskmanager-2.window_word_count.my-mapper.metricName
//        String identifier = metricGroup.getMetricIdentifier(metricName);

        if (scopeComponents.length > 2 && scopeComponents[1].contains("jobmanager")) {
            return;
        }

        StringBuilder sb = new StringBuilder();
        sb.append(scopeComponents[3]);
        for (int i = 4; i < scopeComponents.length; i++) {
            sb.append(".").append(scopeComponents[i]);
        }
        sb.append(metricName);

//        String name = identifier;
        String name = sb.toString();

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
            File mFile = new File("/usr/local/etc/flink-remote/bm_files/metrics_logs/metrics.txt");
            File eFile = new File("/usr/local/etc/flink-remote/bm_files/metrics_logs/latency_throughput.txt");
            if (!mFile.exists()) {
                mFile.createNewFile();
            }
            if (!eFile.exists()) {
                eFile.createNewFile();
            }

            FileOutputStream mFileOut = new FileOutputStream(mFile, true);
            FileOutputStream eFileOut = new FileOutputStream(eFile, true);

            StringBuilder builder = new StringBuilder((int) (this.previousSize * 1.1D));
            StringBuilder eBuilder = new StringBuilder((int) (this.previousSize * 1.1D));

//            Instant now = Instant.now();
            LocalDateTime now = LocalDateTime.now();

            builder.append(lineSeparator).append(lineSeparator).append(now).append(lineSeparator);
            eBuilder.append(lineSeparator).append(lineSeparator).append(now).append(lineSeparator);

            builder.append(lineSeparator).append("---------- Counters ----------").append(lineSeparator);
            eBuilder.append(lineSeparator).append("---------- records counter ----------").append(lineSeparator);
            for (Map.Entry metric : counters.entrySet()) {
                builder.append(metric.getValue()).append(": ").append(((Counter) metric.getKey()).getCount()).append(lineSeparator);
                if (( (String)metric.getValue()).contains("numRecords")) {
                    eBuilder.append(metric.getValue()).append(": ").append(((Counter) metric.getKey()).getCount()).append(lineSeparator);
                }
            }

            builder.append(lineSeparator).append("---------- Gauges ----------").append(lineSeparator);
            for (Map.Entry metric : gauges.entrySet()) {
                builder.append(metric.getValue()).append(": ").append(((Gauge) metric.getKey()).getValue()).append(lineSeparator);
            }

            builder.append(lineSeparator).append("---------- Meters ----------").append(lineSeparator);
            eBuilder.append(lineSeparator).append("---------- throughput ----------").append(lineSeparator);
            for (Map.Entry metric : meters.entrySet()) {
                builder.append(metric.getValue()).append(": ").append(((Meter) metric.getKey()).getRate()).append(lineSeparator);
                if (((String) metric.getValue()).contains("numRecords")) {
                    eBuilder.append(metric.getValue()).append(": ").append(((Meter) metric.getKey()).getRate()).append(lineSeparator);
                }
            }

            builder.append(lineSeparator).append("---------- Histograms ----------").append(lineSeparator);
            eBuilder.append(lineSeparator).append("---------- lantency ----------").append(lineSeparator);
            for (Map.Entry metric : histograms.entrySet()) {
                HistogramStatistics stats = ((Histogram) metric.getKey()).getStatistics();
                builder.append(metric.getValue()).append(": mean=").append(stats.getMean()).append(", min=").append(stats.getMin()).append(", max=").append(stats.getMean()).append(", p25=").append(stats.getQuantile(0.25D)).append(", p50=").append(stats.getQuantile(0.5D)).append(", p75=").append(stats.getQuantile(0.75D)).append(", p95=").append(stats.getQuantile(0.95D)).append(", p98=").append(stats.getQuantile(0.98D)).append(", p99=").append(stats.getQuantile(0.99D)).append(", p999=").append(stats.getQuantile(0.999D)).append(lineSeparator);
                eBuilder.append(metric.getValue()).append(": mean=").append(stats.getMean()).append(", min=").append(stats.getMin()).append(", max=").append(stats.getMean()).append(", p25=").append(stats.getQuantile(0.25D)).append(", p50=").append(stats.getQuantile(0.5D)).append(", p75=").append(stats.getQuantile(0.75D)).append(", p95=").append(stats.getQuantile(0.95D)).append(", p98=").append(stats.getQuantile(0.98D)).append(", p99=").append(stats.getQuantile(0.99D)).append(", p999=").append(stats.getQuantile(0.999D)).append(lineSeparator);
            }

            mFileOut.write(builder.toString().getBytes());
            eFileOut.write(eBuilder.toString().getBytes());
            mFileOut.flush();
            eFileOut.flush();
            mFileOut.close();
            eFileOut.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
