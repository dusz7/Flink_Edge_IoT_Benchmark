package in.hitcps.iot_edge.bm.flink.utils.event_gens;

import in.hitcps.iot_edge.bm.flink.utils.io.JsonDataRead;
import in.hitcps.iot_edge.bm.flink.utils.listeners.DataReadListener;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class DataTimerEventGen {

    // num of threads to read data
    private final int numThreads = 1;

    private DataReadListener dataReadListener;

    // some parameters to control the data input
    private double scalingFactor; // num of reading data
    private int inputRate;
    private int period = 100;
    private int batchSize;

    public DataTimerEventGen(double scalingFactor, int inputRate) {
        this.dataReadListener = null;
        this.scalingFactor = scalingFactor;
        this.inputRate = inputRate;
        this.batchSize = this.inputRate / (1000 / period);
//        System.out.println("############# batch size : " + batchSize);
    }

    public void setDataReadListener(DataReadListener listener) {
        this.dataReadListener = listener;
    }

    public void launch(String csvFileName) {
//        System.out.println("eventGen launch");
        // data lists from different reading threads
        try {
            List<List<List<String>>> dataReadList = JsonDataRead.roundRobinSplitJsonToMemory(csvFileName, numThreads, scalingFactor);
            for (int i = 0; i < numThreads; i++) {
                Timer timer = new Timer("dataEventGen", true);
                TimerTask task = new EventGenTimerTask(dataReadList.get(i));
                timer.scheduleAtFixedRate(task, 0, period);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    class EventGenTimerTask extends TimerTask {

        private List<List<String>> dataRows;

        private int rowLen;
        private int rowIndex;

        EventGenTimerTask(List<List<String>> dataRows) {
            this.dataRows = dataRows;
            this.rowLen = dataRows.size();
            this.rowIndex = 0;
        }

        @Override
        public void run() {
            for (int i = 0; i < batchSize; i++) {
                if (rowIndex == rowLen) {
                    rowIndex = 0;
                }
                List<String> data = dataRows.get(rowIndex);
                data.set(0, String.valueOf(Instant.now().toEpochMilli()));
                dataReadListener.receive(data);
                rowIndex++;
            }
        }
    }
}