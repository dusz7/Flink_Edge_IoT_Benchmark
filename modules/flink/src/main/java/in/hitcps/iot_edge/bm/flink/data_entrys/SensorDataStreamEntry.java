package in.hitcps.iot_edge.bm.flink.data_entrys;

public class SensorDataStreamEntry {
    String msgId;
    String sensorId;
    String metaValues;
    String obsField;
    String obsValue;
    Long sourceInTimestamp;
    Long opTimestamp;

    public SensorDataStreamEntry(String msgId, String sensorId, String metaValues, String obsField, String obsValue, Long sourceTimestamp) {
        this.msgId = msgId;
        this.sensorId = sensorId;
        this.metaValues = metaValues;
        this.obsField = obsField;
        this.obsValue = obsValue;
        this.sourceInTimestamp = sourceTimestamp;
    }

    @Override
    public String toString() {
        return "SensorData : { " +
                "#" + msgId +
                " source in at " + sourceInTimestamp +
                " msg's payload is : " +
                "sensorId = " + sensorId +
                ", metaValues = " + metaValues +
                ", obsField = " + obsField +
                ", obsValue = " + obsValue;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public String getSensorId() {
        return sensorId;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    public String getMetaValues() {
        return metaValues;
    }

    public void setMetaValues(String metaValues) {
        this.metaValues = metaValues;
    }

    public String getObsField() {
        return obsField;
    }

    public void setObsField(String obsField) {
        this.obsField = obsField;
    }

    public String getObsValue() {
        return obsValue;
    }

    public void setObsValue(String obsValue) {
        this.obsValue = obsValue;
    }

    public Long getSourceInTimestamp() {
        return sourceInTimestamp;
    }

    public void setSourceInTimestamp(Long sourceInTimestamp) {
        this.sourceInTimestamp = sourceInTimestamp;
    }

    public Long getOpTimestamp() {
        return opTimestamp;
    }

    public void setOpTimestamp(Long opTimestamp) {
        this.opTimestamp = opTimestamp;
    }
}
