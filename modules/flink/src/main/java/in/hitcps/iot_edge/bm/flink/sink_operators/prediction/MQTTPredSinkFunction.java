package in.hitcps.iot_edge.bm.flink.sink_operators.prediction;

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

public class MQTTPredSinkFunction extends RichSinkFunction<SensorDataStreamEntry> {

    private static Logger l = LoggerFactory.getLogger(MQTTPredSinkFunction.class);
    private Properties p;

    // metric
    private Histogram histogram;

    private int dataNum;

    private MQTTPublishTask mqttPublishTask;

    public MQTTPredSinkFunction(Properties p_, int dataNum) {
        this.p = p_;
        this.dataNum = dataNum;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        mqttPublishTask = new MQTTPublishTask();
        mqttPublishTask.setup(l, p);
        com.codahale.metrics.Histogram dropwizardHistogram = new com.codahale.metrics.Histogram(new SlidingWindowReservoir(dataNum));
        histogram = getRuntimeContext().getMetricGroup()
                .addGroup("flink-prediction")
                .histogram("job-latency", new DropwizardHistogramWrapper(dropwizardHistogram));
    }

    @Override
    public void close() throws Exception {
        super.close();
        mqttPublishTask.tearDown();
    }

    @Override
    public void invoke(SensorDataStreamEntry value, Context context) throws Exception {
        String anaType = value.getAnalyticType();

        StringBuilder builder = new StringBuilder();
        if (anaType.equals(SensorDataStreamEntry.ANATYPE_DTC)) {
            builder.append(value.getMsgId()).append(",").append(value.getMetaValues()).append(",")
                    .append(anaType).append(",obsVal:").append(",RES:").append(value.getCalculateResult());
        }

        if (anaType.equals(SensorDataStreamEntry.ANATYPE_MLR)) {
            // error
            builder.append(value.getMsgId()).append(",").append(value.getMetaValues()).append(",")
                    .append(anaType).append(",obsVal:").append(",ERROR:").append(value.getCalculateResult());
        }

        HashMap<String, String> map = new HashMap<>();
        map.put(AbstractTask.DEFAULT_KEY, builder.toString());
        mqttPublishTask.doTask(map);

        if (value.getSourceInTimestamp() > 0) {
            histogram.update(System.currentTimeMillis() - value.getSourceInTimestamp());
            l.warn("count = {}   mean = {}, this = {}", histogram.getCount(), histogram.getStatistics().getMean(), System.currentTimeMillis() - value.getSourceInTimestamp());
        }
    }
}
