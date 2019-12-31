package in.hitcps.iot_edge.bm.flink.trans_operators.etl;

import in.dream_lab.bm.stream_iot.tasks.statistics.Interpolation;
import in.hitcps.iot_edge.bm.flink.data_entrys.SensorDataStreamEntry;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;

/**
 * to interpolate the null value of 'temperature,humidity,light,dust,airquality_raw' fields based on the last few values
 */
public class InterpolationMapFunction extends RichMapFunction<SensorDataStreamEntry, SensorDataStreamEntry> {

    private static Logger l = LoggerFactory.getLogger(InterpolationMapFunction.class);
    private Properties p;

    private Interpolation interpolation;

    public InterpolationMapFunction(Properties p_) {
        p = p_;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        interpolation = new Interpolation();
        interpolation.setup(l, p);
    }

    @Override
    public void close() throws Exception {
        super.close();

        interpolation.tearDown();
    }

    @Override
    public SensorDataStreamEntry map(SensorDataStreamEntry value) throws Exception {

        HashMap<String, String> map = new HashMap<>();
        map.put("SENSORID", value.getSensorId());
        map.put(value.getObsField(), value.getObsValue());

        Float result = interpolation.doTask(map);

        if (result == null) {
            // timeStamp
            return value;
        } else if (result != Float.MIN_VALUE) {
            l.info("Interpolation Map : msg#{} obsField is {} and obsValue is {} >> {}", value.getMsgId(), value.getObsField(), value.getObsValue(), result.toString());
            value.setObsValue(result.toString());

            // timeStamp
            return value;
        } else {
            if (l.isWarnEnabled()) {
                l.warn("Error in Interpolation Map");
            }
            throw new RuntimeException();
        }
    }
}
