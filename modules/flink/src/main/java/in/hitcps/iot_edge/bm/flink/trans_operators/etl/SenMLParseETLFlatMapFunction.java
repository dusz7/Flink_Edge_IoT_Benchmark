package in.hitcps.iot_edge.bm.flink.trans_operators.etl;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.parse.SenMLParse;
import in.hitcps.iot_edge.bm.flink.data_entrys.FileDataEntry;
import in.hitcps.iot_edge.bm.flink.data_entrys.SensorDataStreamEntry;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;


import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SenMLParseETLFlatMapFunction extends RichFlatMapFunction<FileDataEntry, SensorDataStreamEntry> {

    private static Logger l = LoggerFactory.getLogger(SenMLParseETLFlatMapFunction.class);
    private Properties p;

    private SenMLParse senMLParse;

    private String[] metaFields;
    private String[] observableFields;
    private String idField;

    public SenMLParseETLFlatMapFunction(Properties p_) {
        p = p_;
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
                if (!metaList.contains(ot) && !ot.equals(idField)) { // remove idfield 'source'
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
    }

    @Override
    public void flatMap(FileDataEntry value, Collector<SensorDataStreamEntry> out) throws Exception {
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
        builder = builder.deleteCharAt(builder.lastIndexOf(","));
        String metaValues = builder.toString();

        for (String observableField : observableFields) {
            // timeStamp
            out.collect(new SensorDataStreamEntry(msgId, resultMap.get(idField), metaValues, observableField, resultMap.get(observableField), value.getSourceInTimestamp()));
        }
    }
}
