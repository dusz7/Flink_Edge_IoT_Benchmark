package in.dream_lab.bm.stream_iot.storm.genevents;

//import main.in.dream_lab.genevents.factory.CsvSplitter;
//import main.in.dream_lab.genevents.factory.TableClass;
//import main.in.dream_lab.genevents.utils.GlobalConstants;

import in.dream_lab.bm.stream_iot.storm.genevents.factory.CsvSplitter;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.TableClass;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.JsonSplitter;
import in.dream_lab.bm.stream_iot.storm.genevents.utils.GlobalConstants;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

public class EventGen2 {
	ISyntheticEventGen iseg;
	ExecutorService executorService;
	double scalingFactor;
	int rate = 0;

	public EventGen2(ISyntheticEventGen iseg) {
		this(iseg, GlobalConstants.accFactor);
	}

	public EventGen2(ISyntheticEventGen iseg, double scalingFactor) {
		this.iseg = iseg;
		this.scalingFactor = scalingFactor;
	}

	public EventGen2(ISyntheticEventGen iseg, double scalingFactor, int rate) {
		this.iseg = iseg;
		this.scalingFactor = scalingFactor;
		this.rate = rate;
	}

	public static List<String> getHeadersFromCSV(String csvFileName) {
		return CsvSplitter.extractHeadersFromCSV(csvFileName);
	}

	public TableClass launch(String csvFileName, String outCSVFileName, long experimentDurationMillis, boolean isJson) {
		// 1. Load CSV to in-memory data structure
		// 2. Assign a thread with (new SubEventGen(myISEG, eventList))
		// 3. Attach this thread to ThreadPool
		List<TableClass> nestedList = null;
		try {
			int numThreads = GlobalConstants.numThreads;
			// double scalingFactor = GlobalConstants.accFactor;
			String datasetType = "";
			if (outCSVFileName.indexOf("TAXI") != -1) {
				datasetType = "TAXI";// GlobalConstants.dataSetType = "TAXI";
			} else if (outCSVFileName.indexOf("SYS") != -1) {
				datasetType = "SYS";// GlobalConstants.dataSetType = "SYS";
			} else if (outCSVFileName.indexOf("PLUG") != -1) {
				datasetType = "PLUG";// GlobalConstants.dataSetType = "PLUG";
			} else if (outCSVFileName.indexOf("SENML") != -1) {
				datasetType = "SENML";// GlobalConstants.dataSetType = "PLUG";
			}
			nestedList = JsonSplitter.roundRobinSplitJsonToMemory(csvFileName, numThreads, scalingFactor, datasetType);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return nestedList.get(0);
	}
}
