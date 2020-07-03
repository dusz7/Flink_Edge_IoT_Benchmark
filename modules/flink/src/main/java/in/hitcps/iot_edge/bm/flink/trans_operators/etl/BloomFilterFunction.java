package in.hitcps.iot_edge.bm.flink.trans_operators.etl;

import in.dream_lab.bm.stream_iot.tasks.filter.MultipleBloomFilterCheck;
import in.hitcps.iot_edge.bm.flink.data_entrys.SensorDataStreamEntry;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;

/**
 * to filter illegal sensorId based on bloomfilter_sensor_id
 */
public class BloomFilterFunction extends RichFilterFunction<SensorDataStreamEntry> {

    private static Logger l = LoggerFactory.getLogger(BloomFilterFunction.class);
    private Properties p;

    private MultipleBloomFilterCheck bloomFilterCheck;
    private int useMsgField;

    public BloomFilterFunction(Properties p_) {
        p = p_;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        bloomFilterCheck = new MultipleBloomFilterCheck();
        bloomFilterCheck.setup(l, p);
        useMsgField = Integer.parseInt(p.getProperty("FILTER.BLOOM_FILTER_CHECK.USE_MSG_FIELD"));
    }

    @Override
    public void close() throws Exception {
        super.close();
        bloomFilterCheck.tearDown();
    }

    @Override
    public boolean filter(SensorDataStreamEntry value) throws Exception {

        if (useMsgField > 0) {
            HashMap<String, String> map = new HashMap<>();
//            map.put(value.getObsField(), value.getObsValue());
            map.put("source", value.getSensorId());

            Float result = bloomFilterCheck.doTask(map);

            // timeStamp

            if (result != 0) {
                return true;
            } else {
//                l.info("Bloom Filter catch one : msg#{} obsField is {} and obsValue is {}", value.getMsgId(), value.getObsField(), value.getObsValue());
                return false;
            }
        } else {
            // ?
            return true;
        }
    }
}
