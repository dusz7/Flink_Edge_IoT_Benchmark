package in.hitcps.iot_edge.bm.flink.utils.io;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;


/*
 * SenML is organized by Json
 * Splits the JSON file in round-robin manner and stores it to individual files
 * based on the number of threads
 */
public class JsonDataRead {
    public static Logger LOG = LoggerFactory.getLogger(JsonDataRead.class);

    public static int numThreads;
    public static int peakRate;

    public static List<List<List<String>>> roundRobinSplitJsonToMemory(String inputSortedCSVFileName, int numThreads, double accFactor) throws IOException{
        return roundRobinSplitJsonToMemory(inputSortedCSVFileName, numThreads, accFactor, "SENML");
    }

    //Assumes sorted on timestamp csv file
    //It also treats the first event to be at 0 relative time
    public static List<List<List<String>>> roundRobinSplitJsonToMemory(String inputSortedCSVFileName, int numThreads, double accFactor, String dataSetType) throws IOException {

        // init
        List<List<List<String>>> dataRowsList = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            List<List<String>> dataRows = new ArrayList<>();
            dataRowsList.add(dataRows);
        }

        List<List<String>> dataRows = null;
        boolean flag = true;
        long startTs = 0, deltaTs = 0;
        // to control how many data to be loaded in mem
        int numMins = 90000;  // Keeping it fixed for current set of experiments

        Double cutOffTimeStamp = 0.0;
        MyJSONReader jsonReader = new MyJSONReader(inputSortedCSVFileName, dataSetType);
        String[] nextLine;
        nextLine = jsonReader.readLine();
        while ((nextLine = jsonReader.readLine()) != null) {
            List<String> row = new ArrayList<String>();
            for (int i = 0; i < nextLine.length; i++) {
                row.add(nextLine[i]);
            }

            int ctr = 0;
            dataRows = dataRowsList.get(ctr);
            ctr = (ctr + 1) % numThreads;

            // get the time Stamp
            int timestampColIndex;
            if (dataSetType.equals("SENML")) {
                timestampColIndex = 0;
            } else if (dataSetType.equals("TRAIN")) {
                timestampColIndex = 1;
            } else {
                timestampColIndex = 0;
            }
            DateTime date = new DateTime(Long.parseLong(nextLine[timestampColIndex]));
            long ts = date.getMillis();

            // judge if the data read is enough
            if (flag) {
                startTs = ts;
                flag = false;
                cutOffTimeStamp = startTs + numMins * (1.0 / accFactor) * 60 * 1000;  // accFactor is actually the scaling factor or deceleration factor
            }
            if (ts > cutOffTimeStamp) {
                break; // No More data to be loaded
            }

//            deltaTs = ts - startTs;
//            deltaTs = (long) (accFactor * deltaTs);

            dataRows.add(row);
        }

        jsonReader.close();
        return dataRowsList;
    }

    /**
     * @param args
     * @throws ParseException
     * @throws IOException
     */
    public static void main(String[] args) throws ParseException, IOException {
        // TODO Auto-generated method stub
        int defaultNumThreads = 4, defaultPeakRate = 100;
        switch (args.length) {
            case 2:
                numThreads = Integer.parseInt(args[0]);
                peakRate = Integer.parseInt(args[1]);
                break;
            case 1:
                numThreads = Integer.parseInt(args[0]);
                peakRate = defaultPeakRate;
                break;
            case 0:
                numThreads = defaultNumThreads;
                peakRate = defaultPeakRate;
                break;
            default:
                LOG.warn("Invalid Number of Arguments! args = numThreads peakRate");
                return;
        }

        String inputFileName = "/////////////";

//        List<TableClass> list = roundRobinSplitJsonToMemory(inputFileName, numThreads, 0.001, "SYS");
//        for (int i = 0; i < numThreads; i++)
//            System.out.println(list.get(i).getRows().size());

        LOG.info("jkl");
    }
}

interface MyReader {
    String[] readLine() throws IOException;

    void init() throws FileNotFoundException;

    public void close() throws IOException;
}

class MyJSONReader implements MyReader {
    public BufferedReader bufferedReader;
    public FileReader reader;
    public String inputFileName;

    String dataSetType;

    public MyJSONReader(String inputFileName_, String dataSetType_) {
        inputFileName = inputFileName_;
        dataSetType = dataSetType_;
        try {
            init();
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void init() throws FileNotFoundException {
        reader = new FileReader(inputFileName);
        bufferedReader = new BufferedReader(reader);
    }

    public void close() throws IOException {
        bufferedReader.close();
    }

    @Override
    public String[] readLine() throws IOException {
        String nextLine = bufferedReader.readLine();
        while (nextLine != null) {
            if(dataSetType.equals("TRAIN")) {
                return nextLine.split(",");
            }
            String[] values = new String[2];
            // values[0] : timeStamp
            values[0] = (nextLine.split(","))[0];
            // values[1] : data load
            values[1] = nextLine.substring(nextLine.indexOf(",") + 1);
//            System.out.println("&*&(&*( values[1]" + values[1]);
            return values;
        }
        return null;
    }
}