package in.hitcps.iot_edge.bm.flink.source_operators;

import in.hitcps.iot_edge.bm.flink.data_entrys.FileDataEntry;
import in.hitcps.iot_edge.bm.flink.utils.event_gens.DataTimerEventGen;
import in.hitcps.iot_edge.bm.flink.utils.listeners.DataReadListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class PSourceFromFile extends RichParallelSourceFunction<FileDataEntry> implements DataReadListener {

    private static Logger l = LoggerFactory.getLogger(SourceFromFile.class);

    private volatile boolean isRunning = true;

    // read data from somewhere
    private DataTimerEventGen eventGen;
    private ConcurrentLinkedQueue<List<String>> dataReadingQueue;

    private long msgId;
    private long startMsgId;

    // some parameters
    private String csvFileName;
    private long numData; // num of this test's data sample
    private int inputRate;
    // scalingFactor means what  default: 1
    private double scalingFactor;

    public PSourceFromFile(String csvFileName, double scalingFactor, int inputRate, long numData) {
        this.csvFileName = csvFileName;
        this.scalingFactor = scalingFactor;
        this.inputRate = inputRate;
        this.numData = numData;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
//        System.out.println("Source Read Start");
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
        dataReadingQueue = new ConcurrentLinkedQueue<List<String>>();

        eventGen.setDataReadListener(this);
        eventGen.launch(csvFileName);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void run(SourceContext sourceContext) throws Exception {
        while (isRunning) {
            List<String> dataE = dataReadingQueue.poll();
            // use numData to control use how many data
            if (dataE == null || msgId > startMsgId + numData) {
                continue;
            }
            FileDataEntry entry = new FileDataEntry();
            entry.setMsgId(Long.toString(msgId));
            msgId++;
            entry.setPayLoad(dataE.get(1));

            entry.setSourceInTimestamp(System.currentTimeMillis());

            // send data
            sourceContext.collect(entry);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void receive(List<String> data) {
        try {
            this.dataReadingQueue.offer(data);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
