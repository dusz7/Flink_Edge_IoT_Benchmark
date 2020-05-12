package in.hitcps.iot_edge.bm.flink.trans_operators.train;

import in.dream_lab.bm.stream_iot.tasks.io.AzureTableRangeQueryTaskSYS;
import in.hitcps.iot_edge.bm.flink.data_entrys.FileDataTrainEntry;
import in.hitcps.iot_edge.bm.flink.data_entrys.TrainDataStreamEntry;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

public class ReadTrainDataMapFunction extends RichMapFunction<FileDataTrainEntry, TrainDataStreamEntry> {
    private static Logger l = LoggerFactory.getLogger(ReadTrainDataMapFunction.class);
    private Properties p;
    private String trainInputFile;
    File trainFile;
    Scanner fileScanner;

    public ReadTrainDataMapFunction(Properties p_, String train_input_file) {
        p = p_;
        this.trainInputFile = train_input_file;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        trainFile = new File(trainInputFile);
        try {
            fileScanner = new Scanner(trainFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public TrainDataStreamEntry map(FileDataTrainEntry value) throws Exception {
        String startRow = value.getRowStart();
        String endRow = value.getRowEnd();

        long start = Long.parseLong(startRow);
        long end = Long.parseLong(endRow);

        List<AzureTableRangeQueryTaskSYS.SYS_City> data = readData(start, end);
        StringBuffer bf = new StringBuffer();
        // Loop through the results, displaying information about the entity
        for (AzureTableRangeQueryTaskSYS.SYS_City entity : data) {
            bf.append(entity.getTemperature()).append(",").append(entity.getHumidity()).append(",")
                    .append(entity.getLight()).append(",").append(entity.getDust()).append(",")
                    .append(entity.getAirquality_raw()).append("\n");

        }

        TrainDataStreamEntry entry = new TrainDataStreamEntry();
        entry.setMsgId(value.getMsgId());
        entry.setTrainData(bf.toString());
        entry.setRowEnd(endRow);
        entry.setSourceInTimestamp(value.getSourceInTimestamp());

        return entry;
    }

    private List<AzureTableRangeQueryTaskSYS.SYS_City> readData(long start, long end) {
        List<AzureTableRangeQueryTaskSYS.SYS_City> list = new ArrayList<>();
        int counter = 0;
        while (counter++ < start && fileScanner.hasNextLine())
            fileScanner.nextLine();
        while (fileScanner.hasNextLine() && counter < end) {
            String input = fileScanner.nextLine();
            list.add(AzureTableRangeQueryTaskSYS.SYS_City.parseString(input, counter));
            counter++;
        }
        if (!fileScanner.hasNextLine())
            try {
                fileScanner = new Scanner(trainFile);
            } catch (IOException e) {
                e.printStackTrace();
            }
        return list;
    }
}
