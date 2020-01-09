package in.hitcps.iot_edge.bm.flink.trans_operators.prediction;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.predict.DecisionTreeClassify;
import in.hitcps.iot_edge.bm.flink.data_entrys.SensorDataStreamEntry;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.management.Sensor;
import weka.classifiers.trees.J48;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

public class DecisionTreeClassifyMapFunction extends RichMapFunction<SensorDataStreamEntry, SensorDataStreamEntry> {

    private static Logger l = LoggerFactory.getLogger(DecisionTreeClassifyMapFunction.class);
    private Properties p;

    private DecisionTreeClassify decisionTreeClassify;

    public DecisionTreeClassifyMapFunction(Properties p_) {
        this.p = p_;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        decisionTreeClassify = new DecisionTreeClassify();
        decisionTreeClassify.setup(l, p);
    }

    @Override
    public void close() throws Exception {
        super.close();
        decisionTreeClassify.tearDown();
    }

    @Override
    public SensorDataStreamEntry map(SensorDataStreamEntry value) throws Exception {
        String msgType = value.getMsgType();
        String anaType = value.getAnalyticType();

        // dummy
        String obsValues = "22.7,49.3,0,1955.22,27";

        // model update
//        if (msgType.equals(SensorDataStreamEntry.MSGTYPE_MODELUPDATE) && anaType.equals(SensorDataStreamEntry.ANATYPE_DTC)) {
//            byte[] blobModelObject = (byte[]) value.getBlobModelObject();
//            InputStream bytesInputStream = new ByteArrayInputStream(blobModelObject);
//
//            try {
//                DecisionTreeClassify.j48tree = (J48) weka.core.SerializationHelper.read(bytesInputStream);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }

        if (!msgType.equals(SensorDataStreamEntry.MSGTYPE_MODELUPDATE)) {
            obsValues = value.getObsValue();
        }

        HashMap<String, String> map = new HashMap<>();
        map.put(AbstractTask.DEFAULT_KEY, obsValues);
        Float result = decisionTreeClassify.doTask(map);

        if (result != null) {
            if (result != Float.MIN_VALUE) {
                value.setAnalyticType(SensorDataStreamEntry.ANATYPE_DTC);
                value.setCalculateResult(result.toString());
                return value;
            } else {
                if (l.isWarnEnabled()) {
                    l.warn("Error in DecisionTreeClassify");
                }
                throw new RuntimeException();
            }
        }

        return null;
    }
}
