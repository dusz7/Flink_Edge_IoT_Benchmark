package in.hitcps.iot_edge.bm.flink.trans_operators.stats;

import in.dream_lab.bm.stream_iot.tasks.filter.MultipleBloomFilterCheck;
import in.hitcps.iot_edge.bm.flink.data_entrys.SensorDataStreamEntry;
import in.hitcps.iot_edge.bm.flink.trans_operators.etl.BloomFilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;

public class BloomFilterStatsFunction extends RichFilterFunction<SensorDataStreamEntry> {

    private static Logger l = LoggerFactory.getLogger(BloomFilterStatsFunction.class);

    private Properties p;

    private MultipleBloomFilterCheck multipleBloomFilterCheck;

    public BloomFilterStatsFunction(Properties p_) {
        p = p_;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        multipleBloomFilterCheck = new MultipleBloomFilterCheck();
        multipleBloomFilterCheck.setup(l,p);
    }

    @Override
    public void close() throws Exception {
        super.close();
        multipleBloomFilterCheck.tearDown();
    }

    @Override
    public boolean filter(SensorDataStreamEntry value) throws Exception {
        HashMap<String, String> map = new HashMap<>();
        map.put(value.getObsField(), value.getObsValue());
        Float result = multipleBloomFilterCheck.doTask(map);

        if (result != null) {
            if (result == 1) {
                return true;
            }
        }

        return false;
    }
}
