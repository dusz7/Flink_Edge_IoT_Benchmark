package in.hitcps.iot_edge.bm.flink.trans_operators.stats;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.predict.SimpleLinearRegressionPredictor;
import in.hitcps.iot_edge.bm.flink.data_entrys.SensorDataStreamEntry;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SimpleLinearRegressionFlatMapFunction extends RichFlatMapFunction<SensorDataStreamEntry, SensorDataStreamEntry> {
    private static Logger l = LoggerFactory.getLogger(SimpleLinearRegressionFlatMapFunction.class);

    private Properties p;

    private Map<String, SimpleLinearRegressionPredictor> slrMap;

    public SimpleLinearRegressionFlatMapFunction(Properties p_) {
        p = p_;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        slrMap = new HashMap<>();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void flatMap(SensorDataStreamEntry value, Collector<SensorDataStreamEntry> out) throws Exception {

        String key = value.getSensorId() + value.getObsField();
        String kalmanRes = value.getCalculateResult();

        SimpleLinearRegressionPredictor simpleLinearRegressionPredictor = slrMap.get(key);
        if (simpleLinearRegressionPredictor == null) {
            simpleLinearRegressionPredictor = new SimpleLinearRegressionPredictor();
            simpleLinearRegressionPredictor.setup(l, p);
            slrMap.put(key, simpleLinearRegressionPredictor);
        }

        HashMap<String, String> map = new HashMap<>();
        map.put(AbstractTask.DEFAULT_KEY, kalmanRes);
        simpleLinearRegressionPredictor.doTask(map);

        float[] result = simpleLinearRegressionPredictor.getLastResult();

        if (result != null) {
            StringBuffer buffer = new StringBuffer();
            for (float r : result) {
                buffer.append(r).append("#");
            }
            value.setAnalyticType(SensorDataStreamEntry.ANATYPE_SLR);
            value.setCalculateResult(buffer.toString());
            out.collect(value);
        }
    }
}
