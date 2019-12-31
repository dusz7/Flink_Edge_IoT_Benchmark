package in.hitcps.iot_edge.bm.flink.trans_operators.etl;

import in.hitcps.iot_edge.bm.flink.data_entrys.SensorDataStreamEntry;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.management.Sensor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

public class JoinFlatMapFunction extends RichFlatMapFunction<SensorDataStreamEntry, SensorDataStreamEntry> {

    private Properties p;
    private static Logger l = LoggerFactory.getLogger(JoinFlatMapFunction.class);

    private int dataCount;
    private HashMap<Long, HashMap<String, String>> msgIdMap;

    private ArrayList<String> schemaFieldsOrder;
    private String[] metaFields;
    private String idField;

    public JoinFlatMapFunction(Properties p_) {
        p = p_;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        dataCount = Integer.parseInt(p.getProperty("JOIN.MAX_COUNT_VALUE"));
        String schemaFieldsFilePath = p.getProperty("JOIN.SCHEMA_FILE_PATH");
        schemaFieldsOrder = new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(schemaFieldsFilePath));
            String line = reader.readLine();
            if (line != null) {
                schemaFieldsOrder.addAll(Arrays.asList(line.split(",")));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        metaFields = p.getProperty("JOIN.META_FIELD_SCHEMA").split(",");
        idField = p.getProperty("JOIN.ID_FIELD_SCHEMA");

        msgIdMap = new HashMap<>();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void flatMap(SensorDataStreamEntry value, Collector<SensorDataStreamEntry> collector) throws Exception {
        Long msgIdL = Long.parseLong(value.getMsgId());
//        l.warn("join subStask index: {}   map@id {}" , getRuntimeContext().getIndexOfThisSubtask(), msgIdMap.toString());

        HashMap<String, String> map;
        if (!msgIdMap.containsKey(msgIdL)) {
            map = new HashMap<>();
            // add metaValue and id
            String[] metaValues = value.getMetaValues().split(",");
            for (int i = 0; i < metaFields.length; i++) {
                map.put(metaFields[i], metaValues[i]);
            }
            map.put(idField, value.getSensorId());

            map.put(value.getObsField(), value.getObsValue());

            msgIdMap.put(msgIdL, map);
        } else {
            map = msgIdMap.get(msgIdL);
            map.put(value.getObsField(), value.getObsValue());
            msgIdMap.put(msgIdL, map);
        }

        if (map.size() == dataCount) {
            StringBuilder joinedValues = new StringBuilder();
            for (String s : schemaFieldsOrder) {
                joinedValues.append(map.get(s)).append(",");
            }
            joinedValues = joinedValues.deleteCharAt(joinedValues.length() - 1);

            msgIdMap.remove(msgIdL);

            collector.collect(new SensorDataStreamEntry(value.getMsgId(), value.getSensorId(), value.getMetaValues(), "joinedValue", joinedValues.toString(), value.getSourceInTimestamp()));

            // timeStamp
        }

    }
}
