package in.hitcps.iot_edge.bm.flink.trans_operators.prediction;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.parse.SenMLParse;
import in.hitcps.iot_edge.bm.flink.data_entrys.FileDataEntry;
import in.hitcps.iot_edge.bm.flink.data_entrys.SensorDataStreamEntry;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

public class SenMLParsePredMapFunction extends RichMapFunction<FileDataEntry, SensorDataStreamEntry> {

    private static Logger l = LoggerFactory.getLogger(SenMLParsePredMapFunction.class);
    private Properties p;

    private SenMLParse senMLParse;

    private String[] metaFields;
    private String[] observableFields;
    private String idField;

    public SenMLParsePredMapFunction(Properties p_) {
        this.p = p_;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        senMLParse = new SenMLParse();
        senMLParse.setup(l, p);

        try {
            metaFields = p.getProperty("PARSE.META_FIELD_SCHEMA").split(","); // timestamp,longitude,latitude
            idField = p.getProperty("PARSE.ID_FIELD_SCHEMA"); // source

            FileReader reader = new FileReader(p.getProperty("PARSE.CSV_SCHEMA_FILEPATH"));
            BufferedReader br = new BufferedReader(reader);
            List<String> metaList = new ArrayList<>(Arrays.asList(metaFields));
            List<String> obsList = new ArrayList<>();

            for (String ot : br.readLine().split(",")) { // timestamp,source,longitude,latitude,temperature,humidity,light,dust,airquality_raw
                if (!metaList.contains(ot) && !ot.equals(idField)) {
                    obsList.add(ot);
                }
            }
            observableFields = obsList.toArray(new String[0]); // temperature,humidity,light,dust,airquality_raw
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        senMLParse.tearDown();
    }

    @Override
    public SensorDataStreamEntry map(FileDataEntry value) throws Exception {
        String msgId = value.getMsgId();
        String msg = value.getPayLoad();

        HashMap<String, String> map = new HashMap<>();
        map.put(AbstractTask.DEFAULT_KEY, msg);
        senMLParse.doTask(map);

        HashMap<String, String> resultMap = (HashMap) senMLParse.getLastResult();

        StringBuilder builder = new StringBuilder();
        for (String metaField : metaFields) {
            builder.append(resultMap.get(metaField)).append(",");
        }
        builder.deleteCharAt(builder.lastIndexOf(","));
        String metaValues = builder.toString();

        builder = new StringBuilder();
        for (String obsField : observableFields) {
            builder.append(resultMap.get(obsField)).append(",");
        }
        builder.deleteCharAt(builder.lastIndexOf(","));
        String obsValues = builder.toString();

        SensorDataStreamEntry sensorDataStreamEntry = new SensorDataStreamEntry(msgId, resultMap.get(idField), metaValues,
                SensorDataStreamEntry.OBSFIELD_DUMMY, obsValues, value.getSourceInTimestamp());
        sensorDataStreamEntry.setAnalyticType(SensorDataStreamEntry.ANATYPE_DUMB);

        return sensorDataStreamEntry;
    }
}
