package in.hitcps.iot_edge.bm.flink.trans_operators.train;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.predict.LinearRegressionTrainBatched;
import in.hitcps.iot_edge.bm.flink.data_entrys.TrainDataStreamEntry;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Properties;

public class LinearRegressionTrainFlatMapFunction extends RichFlatMapFunction<TrainDataStreamEntry, TrainDataStreamEntry> {
    private static Logger l = LoggerFactory.getLogger(LinearRegressionTrainFlatMapFunction.class);
    private Properties p;

    LinearRegressionTrainBatched linearRegressionTrainBatched;
    String datasetName = "";

    public LinearRegressionTrainFlatMapFunction(Properties p_) {
        this.p = p_;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        linearRegressionTrainBatched = new LinearRegressionTrainBatched();
        datasetName = p.getProperty("TRAIN.DATASET_NAME").toString();
        linearRegressionTrainBatched.setup(l, p);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void flatMap(TrainDataStreamEntry value, Collector<TrainDataStreamEntry> out) throws Exception {
        HashMap<String, String> map = new HashMap();
        // obsVal="22.7,49.3,0,1955.22,27"; //dummy
        map.put(AbstractTask.DEFAULT_KEY, value.getTrainData());

        String filename = datasetName + "-MLR-" + value.getRowEnd() + ".model";
        map.put("FILENAME", filename);

        Float res = linearRegressionTrainBatched.doTask(map);
        ByteArrayOutputStream model = (ByteArrayOutputStream) linearRegressionTrainBatched.getLastResult();

        if (res != null) {
            if (res != Float.MIN_VALUE) {
                value.setAnalyticType(TrainDataStreamEntry.ANATYPE_MLR);
                value.setModel(model);
                value.setFileName(filename);
                out.collect(value);
            } else {
                if (l.isWarnEnabled())
                    l.warn("Error in LinearRegressionTrainFlatMapFunction");
                throw new RuntimeException();
            }
        }
    }


}
