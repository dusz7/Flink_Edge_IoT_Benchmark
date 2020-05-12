package in.hitcps.iot_edge.bm.flink.source_operators;

import in.hitcps.iot_edge.bm.flink.data_entrys.FileDataEntry;
import in.hitcps.iot_edge.bm.flink.data_entrys.FileDataTrainEntry;
import in.hitcps.iot_edge.bm.flink.utils.event_gens.DataTimerEventGen;
import in.hitcps.iot_edge.bm.flink.utils.listeners.DataReadListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SourceFromFileTrain extends RichSourceFunction<FileDataTrainEntry> implements DataReadListener {

    private static Logger l = LoggerFactory.getLogger(SourceFromFile.class);

    private volatile boolean isRunning = true;

    // read data from somewhere
    private DataTimerEventGen eventGen;
    private BlockingQueue<List<String>> dataReadingQueue;

    private long msgId;
    private long startMsgId;

    // some parameters
    private String csvFileName;
    private long numData; // num of this test's data sample
    private int inputRate;
    // scalingFactor means what  default: 1
    private double scalingFactor;

    // ts

    public SourceFromFileTrain(String csvFileName, double scalingFactor, int inputRate, long numData) {
        this.csvFileName = csvFileName;
        this.scalingFactor = scalingFactor;
        this.inputRate = inputRate;
        this.numData = numData;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        //msgid
        Random random = new Random();
        try {
            msgId = (long) (1 * Math.pow(10, 12) + random.nextInt(1000) * Math.pow(10, 9) + random.nextInt(10));
            startMsgId = msgId;
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("eventGen create");
        eventGen = new DataTimerEventGen(scalingFactor, inputRate);
        dataReadingQueue = new LinkedBlockingQueue<List<String>>();

        eventGen.setDataReadListener(this);
        eventGen.launch(csvFileName, "TRAIN");
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void run(SourceContext<FileDataTrainEntry> sourceContext) throws Exception {
//        System.out.println("Source Read Running");
        while (isRunning) {
            List<String> dataE = dataReadingQueue.poll();
            // use numData to control use how many data
            if (dataE == null || msgId > startMsgId + numData) {
                continue;
            }

            StringBuilder rowStringBuf = new StringBuilder();
            for (String s : dataE) {
                rowStringBuf.append(",").append(s);
            }
            String rowString = rowStringBuf.toString().substring(1);

            FileDataTrainEntry entry = new FileDataTrainEntry();
            entry.setMsgId(Long.toString(msgId));
            msgId++;
            entry.setRowString(rowString);
            entry.setRowStart(dataE.get(2));
            entry.setRowEnd(dataE.get(3));

            // source in time stamp
            // note to min the influence of early startup_phase
            if ((msgId > startMsgId + numData / 3) && (msgId < startMsgId + numData * 3 / 4)) {
                entry.setSourceInTimestamp(Long.parseLong(dataE.get(1)));
            }

            // mark the end of the exp
            if (msgId > startMsgId + numData - 50) {
                entry.setSourceInTimestamp(-999L);
            }

            // send data
            sourceContext.collect(entry);

            // monitoring bp
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void receive(List<String> data) {
//        System.out.println("receive data from EventGen");
        try {
            this.dataReadingQueue.put(data);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
