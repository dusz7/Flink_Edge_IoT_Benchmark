package in.hitcps.iot_edge.bm.flink.trans_operators.stats;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.statistics.KalmanFilter;
import in.hitcps.iot_edge.bm.flink.data_entrys.SensorDataStreamEntry;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class KalmanFilterFunction extends RichFilterFunction<SensorDataStreamEntry> {
    private static Logger l = LoggerFactory.getLogger(KalmanFilterFunction.class);

    private Properties p;

    private Map<String, KalmanFilter> kMap;
    private ArrayList<String> useMsgList;

    public KalmanFilterFunction(Properties p_) {
        p = p_;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        kMap = new HashMap<>();
        String msgFieldS = p.getProperty("STATISTICS.KALMAN_FILTER.USE_MSG_FIELDLIST");
        useMsgList = new ArrayList<>();
        useMsgList.addAll(Arrays.asList(msgFieldS.split(",")));
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public boolean filter(SensorDataStreamEntry value) throws Exception {
        if (useMsgList.contains(value.getObsField())) {
            String key = value.getSensorId() + value.getObsField();
            KalmanFilter kalmanFilter = kMap.get(key);
            if (kalmanFilter == null) {
                kalmanFilter = new KalmanFilter();
                kalmanFilter.setup(l, p);
                kMap.put(key, kalmanFilter);
            }
            HashMap<String, String> map = new HashMap<>();
            map.put(AbstractTask.DEFAULT_KEY, value.getObsValue());
            Float result = kalmanFilter.doTask(map);

            if (result != null) {
                value.setCalculateResult(result.toString());
                return true;
            }
        }
        return false;
    }
}
