package in.hitcps.iot_edge.bm.flink.sink_operators.train;

import com.codahale.metrics.SlidingWindowReservoir;
import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.io.MQTTPublishTask;
import in.hitcps.iot_edge.bm.flink.data_entrys.TrainDataStreamEntry;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Properties;

public class MQTTTrainSinkFunction extends RichSinkFunction<TrainDataStreamEntry> {

    private static Logger l = LoggerFactory.getLogger(MQTTTrainSinkFunction.class);

    private Properties p;

    // metric
    private Gauge endExpGauge;
    private Histogram latencyHistogram;
    private int dataNum;
    private Boolean isExperimentEnding = false;

    MQTTPublishTask mqttPublishTask;

    public MQTTTrainSinkFunction(Properties p_, int dataNum) {
        p = p_;
        this.dataNum = dataNum;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        mqttPublishTask = new MQTTPublishTask();
        mqttPublishTask.setup(l, p);

        com.codahale.metrics.Histogram dropwizardHistogram = new com.codahale.metrics.Histogram(new SlidingWindowReservoir(dataNum * 2));
        latencyHistogram = getRuntimeContext().getMetricGroup()
                .addGroup("MyMetrics")
                .histogram("latency", new DropwizardHistogramWrapper(dropwizardHistogram));
        endExpGauge = getRuntimeContext().getMetricGroup()
                .addGroup("MyMetrics")
                .gauge("endExperiment", new Gauge<Boolean>() {
                    @Override
                    public Boolean getValue() {
                        return isExperimentEnding;
                    }
                });
    }

    @Override
    public void close() throws Exception {
        super.close();
        mqttPublishTask.tearDown();
    }

    @Override
    public void invoke(TrainDataStreamEntry value, Context context) throws Exception {
        String filename = value.getFileName();

        HashMap<String, String> map = new HashMap<String, String>();
        map.put(AbstractTask.DEFAULT_KEY, filename);
        Float res = mqttPublishTask.doTask(map);

        if (value.getSourceInTimestamp() > 0) {
            latencyHistogram.update(Instant.now().toEpochMilli() - value.getSourceInTimestamp());
        }
        if (value.getSourceInTimestamp() < -1) {
            isExperimentEnding = true;
        }
    }
}
