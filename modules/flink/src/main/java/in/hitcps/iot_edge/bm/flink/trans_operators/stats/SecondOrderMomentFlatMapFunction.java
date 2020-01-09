package in.hitcps.iot_edge.bm.flink.trans_operators.stats;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.statistics.SecondOrderMoment;
import in.hitcps.iot_edge.bm.flink.data_entrys.SensorDataStreamEntry;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SecondOrderMomentFlatMapFunction extends RichFlatMapFunction<SensorDataStreamEntry, SensorDataStreamEntry> {
    private static Logger l = LoggerFactory.getLogger(SecondOrderMomentFlatMapFunction.class);

    private Properties p;

    private Map<String, SecondOrderMoment> momentMap;

    public SecondOrderMomentFlatMapFunction(Properties p_) {
        p = p_;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        momentMap = new HashMap<>();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void flatMap(SensorDataStreamEntry value, Collector<SensorDataStreamEntry> out) throws Exception {
        String key = value.getSensorId() + value.getObsField();
        SecondOrderMoment secondOrderMoment = momentMap.get(key);
        if (secondOrderMoment == null) {
            secondOrderMoment = new SecondOrderMoment();
            secondOrderMoment.setup(l, p);
            momentMap.put(key, secondOrderMoment);
        }

        HashMap<String, String> map = new HashMap<>();
        map.put(AbstractTask.DEFAULT_KEY, value.getObsValue());
        secondOrderMoment.doTask(map);

        Float result = (Float) secondOrderMoment.getLastResult();

        if (result != null) {
            if (result != Float.MIN_VALUE) {
                value.setAnalyticType(SensorDataStreamEntry.ANATYPE_SOM);
                value.setCalculateResult(result.toString());
                out.collect(value);
            }
        }
    }
}
