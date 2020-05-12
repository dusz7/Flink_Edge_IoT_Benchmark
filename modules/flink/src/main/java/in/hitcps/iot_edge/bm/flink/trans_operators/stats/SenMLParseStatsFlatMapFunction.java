package in.hitcps.iot_edge.bm.flink.trans_operators.stats;

import in.hitcps.iot_edge.bm.flink.data_entrys.FileDataEntry;
import in.hitcps.iot_edge.bm.flink.data_entrys.SensorDataStreamEntry;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class SenMLParseStatsFlatMapFunction extends RichFlatMapFunction<FileDataEntry, SensorDataStreamEntry> {

    private static Logger l = LoggerFactory.getLogger(SenMLParseStatsFlatMapFunction.class);

    private Properties p;

    private String[] observableFields;
    private String[] observableValues;


    public SenMLParseStatsFlatMapFunction(Properties p_) {
        p = p_;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        observableFields = new String[] {"temp", "humid", "light", "dust", "airq"};
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void flatMap(FileDataEntry value, Collector<SensorDataStreamEntry> out) {
        String msg = value.getPayLoad();
        String msgId = value.getMsgId();

        String[] msgArray = msg.split(",");

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 3; i++) {
            builder.append(msgArray[i]).append(",");
        }
        builder.deleteCharAt(builder.lastIndexOf(","));
        String sensorDetails = builder.toString();

        String sensorId = msgArray[0];
        observableValues = Arrays.copyOfRange(msgArray, 3, 8);

        for (int obsIndex = 0; obsIndex < observableValues.length; obsIndex++) {
            SensorDataStreamEntry sensorDataStreamEntry = new SensorDataStreamEntry(msgId, sensorId, sensorDetails,
                    observableFields[obsIndex], observableValues[obsIndex], value.getSourceInTimestamp());
            out.collect(sensorDataStreamEntry);
        }

    }
}
