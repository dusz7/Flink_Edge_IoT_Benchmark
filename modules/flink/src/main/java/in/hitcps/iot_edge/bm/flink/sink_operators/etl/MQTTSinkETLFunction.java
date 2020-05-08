package in.hitcps.iot_edge.bm.flink.sink_operators.etl;

import com.codahale.metrics.SlidingWindowReservoir;
import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.io.MQTTPublishTask;
import in.hitcps.iot_edge.bm.flink.data_entrys.SensorDataStreamEntry;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;

public class MQTTSinkETLFunction extends RichSinkFunction<SensorDataStreamEntry> {

    // metric
    private Histogram histogram;
    //    private int index;

    private Properties p;
    private static Logger l = LoggerFactory.getLogger(MQTTSinkETLFunction.class);

    private int dataNum;

    private MQTTPublishTask mqttPublishTask;

    public MQTTSinkETLFunction(Properties p_, int dataNum) {
        p = p_;
        this.dataNum = dataNum;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        mqttPublishTask = new MQTTPublishTask();
        mqttPublishTask.setup(l, p);
        com.codahale.metrics.Histogram dropwizardHistogram = new com.codahale.metrics.Histogram(new SlidingWindowReservoir(dataNum));
//        index = getRuntimeContext().getIndexOfThisSubtask() + 1;
        histogram = getRuntimeContext().getMetricGroup()
                .addGroup("flink-etl")
                .histogram("job-latency", new DropwizardHistogramWrapper(dropwizardHistogram));
//        getRuntimeContext().getMetricGroup()
//                .addGroup("flink-metrics")
//                .gauge("gaugeTest", new Gauge<Double>() {
//                    @Override
//                    public Double getValue() {
//                        return histogram.getStatistics().getMean();
//                    }
//                });
    }

    @Override
    public void close() throws Exception {
        super.close();
        mqttPublishTask.tearDown();
    }

    @Override
    public void invoke(SensorDataStreamEntry value, Context context) throws Exception {
        HashMap<String, String> map = new HashMap<>();
        map.put(AbstractTask.DEFAULT_KEY, value.getObsValue());
        mqttPublishTask.doTask(map);
        if (value.getSourceInTimestamp() > 0) {
            histogram.update(System.currentTimeMillis() - value.getSourceInTimestamp());
            System.out.println("count = " + histogram.getCount() + "   mean = " + histogram.getStatistics().getMean() );
//            l.warn("count = {}   mean = {}, this = {}", histogram.getCount(), histogram.getStatistics().getMean(), System.currentTimeMillis() - value.getSourceInTimestamp());
        }
    }
}
