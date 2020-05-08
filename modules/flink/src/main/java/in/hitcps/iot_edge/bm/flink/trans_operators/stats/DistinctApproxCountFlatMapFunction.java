package in.hitcps.iot_edge.bm.flink.trans_operators.stats;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.aggregate.DistinctApproxCount;
import in.hitcps.iot_edge.bm.flink.data_entrys.SensorDataStreamEntry;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DistinctApproxCountFlatMapFunction extends RichFlatMapFunction<SensorDataStreamEntry, SensorDataStreamEntry> {

    private static Logger l = LoggerFactory.getLogger(DistinctApproxCountFlatMapFunction.class);

    private Properties p;

    private Map<String, DistinctApproxCount> countMap;

    public DistinctApproxCountFlatMapFunction(Properties p_) {
        p = p_;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        countMap = new HashMap<>();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void flatMap(SensorDataStreamEntry value, Collector<SensorDataStreamEntry> collector) throws Exception {

        String key = value.getObsField();

        if (!countMap.containsKey(key)) {
            DistinctApproxCount distinctApproxCount = new DistinctApproxCount();
            distinctApproxCount.setup(l, p);
            countMap.put(key, distinctApproxCount);
        }

        DistinctApproxCount distinctApproxCount = countMap.get(key);

        HashMap<String, String> map = new HashMap();
        map.put(AbstractTask.DEFAULT_KEY, value.getSensorId());

        Float res = null;
        distinctApproxCount.doTask(map);
        res = (Float) distinctApproxCount.getLastResult();

        if (res != null) {
            if (res != Float.MIN_VALUE){
                value.setAnalyticType(SensorDataStreamEntry.ANATYPE_DAC);
                value.setCalculateResult(res.toString());
                collector.collect(value);
            }
        }
    }
}
