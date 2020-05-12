package in.hitcps.iot_edge.bm.flink.trans_operators.train;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.predict.DecisionTreeTrainBatched;
import in.hitcps.iot_edge.bm.flink.data_entrys.TrainDataStreamEntry;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Properties;

public class DecisionTreeTrainFlatMapFunction extends RichFlatMapFunction<TrainDataStreamEntry, TrainDataStreamEntry> {
    private static Logger l = LoggerFactory.getLogger(DecisionTreeTrainFlatMapFunction.class);
    private Properties p;

    DecisionTreeTrainBatched decisionTreeTrainBatched;
    String datasetName = "";

    public DecisionTreeTrainFlatMapFunction(Properties p_) {
        p = p_;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        datasetName = p.getProperty("TRAIN.DATASET_NAME").toString();
        decisionTreeTrainBatched = new DecisionTreeTrainBatched();
        decisionTreeTrainBatched.setup(l, p);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void flatMap(TrainDataStreamEntry value, Collector<TrainDataStreamEntry> out) throws Exception {

        HashMap<String, String> map = new HashMap();
        map.put(AbstractTask.DEFAULT_KEY, value.getAnnoData());

        String filename = datasetName + "-DTC-" + value.getRowEnd() + ".model";
        map.put("FILENAME", filename);

        Float res = decisionTreeTrainBatched.doTask(map);

        ByteArrayOutputStream model = (ByteArrayOutputStream) decisionTreeTrainBatched.getLastResult();

        if (res != null) {
            if (res != Float.MIN_VALUE) {
                value.setModel(model);
                value.setAnalyticType(TrainDataStreamEntry.ANATYPE_DTC);
                value.setFileName(filename);

                out.collect(value);
            } else {
                if (l.isWarnEnabled())
                    l.warn("Error in DecisionTreeClassifyBolt");
                throw new RuntimeException();
            }
        }
    }
}
