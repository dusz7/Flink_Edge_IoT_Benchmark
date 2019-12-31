package in.hitcps.iot_edge.bm.flink.trans_operators.etl;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.annotate.Annotate;
import in.hitcps.iot_edge.bm.flink.data_entrys.SensorDataStreamEntry;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;

public class AnnotationMapFunction extends RichMapFunction<SensorDataStreamEntry, SensorDataStreamEntry> {

    private static Logger l = LoggerFactory.getLogger(AnnotationMapFunction.class);

    private Properties p;

    private Annotate annotate;

    public AnnotationMapFunction(Properties p_) {
        p = p_;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        annotate = new Annotate();
        annotate.setup(l, p);
    }

    @Override
    public void close() throws Exception {
        super.close();
        annotate.tearDown();
    }

    @Override
    public SensorDataStreamEntry map(SensorDataStreamEntry value) throws Exception {
        HashMap<String,String> map = new HashMap<>();
        map.put(AbstractTask.DEFAULT_KEY, value.getObsValue());
        annotate.doTask(map);
        String updatedValue = annotate.getLastResult();
        if (updatedValue != null) {
            // timeStamp
            value.setObsField("annotatedValue");
            value.setObsValue(updatedValue);
        }
        return value;
    }
}
