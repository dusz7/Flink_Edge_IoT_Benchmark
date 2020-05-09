package in.hitcps.iot_edge.bm;

import org.apache.flink.metrics.*;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;


public class MySocksReporter implements MetricReporter, Scheduled {

    private final String ENDEXPERIMENT = "endExperiment";

    private final Map<Gauge<?>, String> gauges = new HashMap<>();

    private int port = 38999;

    @Override
    public void open(MetricConfig metricConfig) {

    }

    @Override
    public void close() {

    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup metricGroup) {
        if (metricName.contains(ENDEXPERIMENT)) {
            String name = metricGroup.getMetricIdentifier(metricName);
            gauges.put((Gauge<?>) metric, name);
        }
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup metricGroup) {
        gauges.remove(metric);
    }

    // report to the extend system
    @Override
    public void report() {
        for (Map.Entry metric : gauges.entrySet()) {
            if (((String) metric.getValue()).contains(ENDEXPERIMENT)) {
                boolean flag = (Boolean) ((Gauge) metric.getKey()).getValue();
                if (flag) {
                    try {
//                        InetAddress addr = InetAddress.getByName(nimbusIp);
//                        Socket socket = new Socket(addr, port);
                        Socket socket = new Socket("192.168.88.247", port);
                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                        out.println("ending");
                        out.close();
                        socket.close();
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

    }
}
