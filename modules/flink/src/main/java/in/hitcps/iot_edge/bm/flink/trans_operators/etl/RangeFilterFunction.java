package in.hitcps.iot_edge.bm.flink.trans_operators.etl;

import in.dream_lab.bm.stream_iot.tasks.filter.RangeFilterCheck;
import in.hitcps.iot_edge.bm.flink.data_entrys.SensorDataStreamEntry;
import org.apache.flink.api.common.functions.FilterFunction;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;

public class RangeFilterFunction extends RichFilterFunction<SensorDataStreamEntry> {

    private static Logger l = LoggerFactory.getLogger(RangeFilterFunction.class);
    private Properties p;

    private RangeFilterCheck rangeFilterCheck;

    public RangeFilterFunction(Properties p_) {
        p = p_;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        rangeFilterCheck = new RangeFilterCheck();
        rangeFilterCheck.setup(l, p);
    }

    @Override
    public void close() throws Exception {
        super.close();

        rangeFilterCheck.tearDown();
    }

    @Override
    public boolean filter(SensorDataStreamEntry value) throws Exception {

        HashMap<String, String> map = new HashMap<>();
        map.put(value.getObsField(), value.getObsValue());
        Float result = rangeFilterCheck.doTask(map);

        // timestamp

        if (result != 0) {
            return true;
        } else {
            l.info("Rang Filter catch one : msg#{} obsField is {} and obsValue is {}", value.getMsgId(), value.getObsField(), value.getObsValue());
            // interpolation after
            value.setObsValue("null");
            return true;
        }
    }
}
