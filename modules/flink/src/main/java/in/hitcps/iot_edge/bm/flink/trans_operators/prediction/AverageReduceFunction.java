package in.hitcps.iot_edge.bm.flink.trans_operators.prediction;

import in.hitcps.iot_edge.bm.flink.data_entrys.SensorDataStreamEntry;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class AverageReduceFunction implements ReduceFunction<SensorDataStreamEntry> {

    // should ues aggre method

    private static Logger l = LoggerFactory.getLogger(AverageReduceFunction.class);
    private Properties p;

    public AverageReduceFunction(Properties p_) {
        p = p_;
    }

    @Override
    public SensorDataStreamEntry reduce(SensorDataStreamEntry v1, SensorDataStreamEntry v2) throws Exception {
        Float avg = 0F;
        if (v1.getCalculateResult() != null) {
            avg = Float.parseFloat(v1.getCalculateResult());
        } else {
            avg = Float.parseFloat(v1.getObsValue().split(",")[4]) / Float.parseFloat(p.getProperty("AGGREGATE.BLOCK_COUNT.WINDOW_SIZE"));
        }
        avg += Float.parseFloat(v2.getObsValue().split(",")[4]) / Float.parseFloat(p.getProperty("AGGREGATE.BLOCK_COUNT.WINDOW_SIZE"));

        v1.setAnalyticType(SensorDataStreamEntry.ANATYPE_AVG);
        v1.setCalculateResult(avg.toString());
        return v1;
    }
}
