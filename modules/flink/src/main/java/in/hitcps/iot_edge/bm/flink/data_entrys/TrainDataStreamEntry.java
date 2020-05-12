package in.hitcps.iot_edge.bm.flink.data_entrys;

import java.io.OutputStream;

public class TrainDataStreamEntry {
    public static final String ANATYPE_DEFAULT = "ANATYPE_DEFAULT";
    public static final String ANATYPE_MLR = "MLR";
    public static final String ANATYPE_DTC = "DTC";

    private String msgId;
    private String trainData;
    private String rowEnd;

    private OutputStream model;
    private String fileName;

    private String annoData;

    private Long sourceInTimestamp;
    private Long opTimestamp;

    private String analyticType = ANATYPE_DEFAULT;

    public TrainDataStreamEntry(String msgId, String trainData, String rowEnd, OutputStream model, String fileName, String annoData, Long sourceInTimestamp, Long opTimestamp, String analyticType) {
        this.msgId = msgId;
        this.trainData = trainData;
        this.rowEnd = rowEnd;
        this.model = model;
        this.fileName = fileName;
        this.annoData = annoData;
        this.sourceInTimestamp = sourceInTimestamp;
        this.opTimestamp = opTimestamp;
        this.analyticType = analyticType;
    }

    @Override
    public String toString() {
        return "TrainDataStreamEntry{" + "msgId='" + msgId + '\'' + ", trainData='" + trainData + '\'' + ", rowEnd='" + rowEnd + '\'' + ", sourceInTimestamp=" + sourceInTimestamp + ", opTimestamp=" + opTimestamp + '}';
    }

    public TrainDataStreamEntry() {
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public String getTrainData() {
        return trainData;
    }

    public void setTrainData(String trainData) {
        this.trainData = trainData;
    }

    public String getRowEnd() {
        return rowEnd;
    }

    public void setRowEnd(String rowEnd) {
        this.rowEnd = rowEnd;
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

    public OutputStream getModel() {
        return model;
    }

    public void setModel(OutputStream model) {
        this.model = model;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getAnalyticType() {
        return analyticType;
    }

    public void setAnalyticType(String analyticType) {
        this.analyticType = analyticType;
    }

    public String getAnnoData() {
        return annoData;
    }

    public void setAnnoData(String annoData) {
        this.annoData = annoData;
    }
}
