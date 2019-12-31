package in.hitcps.iot_edge.bm.flink.trans_operators.etl;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.parse.CsvToSenMLParse;
import in.hitcps.iot_edge.bm.flink.data_entrys.SensorDataStreamEntry;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;

public class CSVToSenMLMapFunction extends RichMapFunction<SensorDataStreamEntry, SensorDataStreamEntry> {

    private static Logger l = LoggerFactory.getLogger(CSVToSenMLMapFunction.class);
    private Properties p;

    private CsvToSenMLParse csvToSenMLParse;

    public CSVToSenMLMapFunction(Properties p_) {
        p = p_;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        csvToSenMLParse = new CsvToSenMLParse();
        csvToSenMLParse.setup(l, p);
    }

    @Override
    public void close() throws Exception {
        super.close();
        csvToSenMLParse.tearDown();
    }

    @Override
    public SensorDataStreamEntry map(SensorDataStreamEntry value) throws Exception {
        HashMap<String, String> map = new HashMap<>();
        map.put(AbstractTask.DEFAULT_KEY, value.getObsValue());

        csvToSenMLParse.doTask(map);
        String updatedValue = csvToSenMLParse.getLastResult();

        value.setObsField("senml");
        value.setObsValue(updatedValue);

        return value;
    }
}
