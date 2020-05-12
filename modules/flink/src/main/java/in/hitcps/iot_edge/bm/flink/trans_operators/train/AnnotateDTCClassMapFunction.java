package in.hitcps.iot_edge.bm.flink.trans_operators.train;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.annotate.AnnotateDTClass;
import in.hitcps.iot_edge.bm.flink.data_entrys.TrainDataStreamEntry;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;

public class AnnotateDTCClassMapFunction extends RichMapFunction<TrainDataStreamEntry, TrainDataStreamEntry> {
    private static Logger l = LoggerFactory.getLogger(AnnotateDTCClassMapFunction.class);
    private Properties p;

    AnnotateDTClass annotateDTClass;

    public AnnotateDTCClassMapFunction(Properties p_) {
        p = p_;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        annotateDTClass = new AnnotateDTClass();
        annotateDTClass.setup(l, p);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public TrainDataStreamEntry map(TrainDataStreamEntry entry) throws Exception {
        HashMap<String, String> map = new HashMap<String, String>();
        map.put(AbstractTask.DEFAULT_KEY, entry.getTrainData());
        Float res = annotateDTClass.doTask(map);
        String annotData = annotateDTClass.getLastResult();

        entry.setAnnoData(annotData);
        return entry;
    }
}
