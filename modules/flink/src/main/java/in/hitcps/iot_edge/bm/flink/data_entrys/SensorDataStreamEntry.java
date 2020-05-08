package in.hitcps.iot_edge.bm.flink.data_entrys;

public class SensorDataStreamEntry {
    public static final String MSGTYPE_DEFAULT = "MSGTYPE_DEFAULT";
    public static final String MSGTYPE_MODELUPDATE = "MSGTYPE_MODELUPDATE";
    public static final String ANATYPE_DEFAULT = "ANATYPE_DEFAULT";
    public static final String ANATYPE_DTC = "DTC";
    public static final String ANATYPE_MLR = "MLR";
    public static final String ANATYPE_AVG = "AVG";
    public static final String ANATYPE_SLR = "SLR";
    public static final String ANATYPE_SOM = "SOM";
    public static final String ANATYPE_DAC = "DAC";
    public static final String ANATYPE_DUMB = "ANATYPE_DUMB";
    public static final String OBSFIELD_DUMMY = "OBSFIELD_DUMMY";

    private String msgId;
    private String sensorId;
    private String metaValues;
    private String obsField;
    private String obsValue;
    private Long sourceInTimestamp;
    private Long opTimestamp;
    private String calculateResult;

    private String msgType = MSGTYPE_DEFAULT;
    private String analyticType = ANATYPE_DEFAULT;

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
                ", obsValue = " + obsValue +
                ", msgType = " + msgType +
                ", anaType = " + analyticType +
                ", calRes = " + calculateResult +
                " }";
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

    public String getMsgType() {
        return msgType;
    }

    public void setMsgType(String msgType) {
        this.msgType = msgType;
    }

    public String getAnalyticType() {
        return analyticType;
    }

    public void setAnalyticType(String analyticType) {
        this.analyticType = analyticType;
    }

    public String getCalculateResult() {
        return calculateResult;
    }

    public void setCalculateResult(String calculateResult) {
        this.calculateResult = calculateResult;
    }
}
