package in.hitcps.iot_edge.bm.flink.data_entrys;

public class FileDataTrainEntry {

    String msgId;
    String rowString;
    String rowStart;
    String rowEnd;

    Long sourceInTimestamp;

    public FileDataTrainEntry() {
        this.rowString = "";
        this.msgId = "0";
        this.sourceInTimestamp = -1L;
        this.rowStart = "";
        this.rowEnd = "";
    }

    public FileDataTrainEntry(String msgId, String rowString, String rowStart, String rowEnd, Long sourceInTimestamp) {
        this.msgId = msgId;
        this.rowString = rowString;
        this.rowStart = rowStart;
        this.rowEnd = rowEnd;
        this.sourceInTimestamp = sourceInTimestamp;
    }

    @Override
    public String toString() {
        return "{ #" + msgId +
                " : { rowString : " + rowString +
                " , rowStart : " + rowStart +
                " , rowEnd : " + rowEnd +
                " , timeStamp : " + sourceInTimestamp + " }";
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public String getRowString() {
        return rowString;
    }

    public void setRowString(String rowString) {
        this.rowString = rowString;
    }

    public String getRowStart() {
        return rowStart;
    }

    public void setRowStart(String rowStart) {
        this.rowStart = rowStart;
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
}
