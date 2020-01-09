package in.hitcps.iot_edge.bm.flink.trans_operators.prediction;

import in.hitcps.iot_edge.bm.flink.data_entrys.SensorDataStreamEntry;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ErrorEstimationFlatMapFunction extends RichFlatMapFunction<SensorDataStreamEntry, SensorDataStreamEntry> {

    private static Logger l = LoggerFactory.getLogger(ErrorEstimationFlatMapFunction.class);
    private Properties p;

    private String res = "0";
    private static String avgRes = "0";

    public ErrorEstimationFlatMapFunction(Properties p_) {
        p = p_;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void flatMap(SensorDataStreamEntry value, Collector<SensorDataStreamEntry> collector) throws Exception {
        String anaType = value.getAnalyticType();

        float airQua = Float.parseFloat(value.getObsValue().split(",")[4]);

        if (anaType.equals(SensorDataStreamEntry.ANATYPE_AVG)) {
            avgRes = value.getCalculateResult();

//            if (l.isWarnEnabled()) {
//                l.warn("avgRes : " + avgRes);
//            }
        }

        if (anaType.equals(SensorDataStreamEntry.ANATYPE_MLR)) {
            res = value.getCalculateResult();

            Float errval = (airQua - Float.parseFloat(res)) / Float.parseFloat(avgRes);

            value.setCalculateResult(errval.toString());

            collector.collect(value);
        }
    }
}
