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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;

import com.esotericsoftware.minlog.Log;

public class EventTimerGen {
	private final ISyntheticEventGen iseg;
	private final double scalingFactor;
	private final int rate;
	
	private final int period = 100;
	private final int batchSize;

	public EventTimerGen(ISyntheticEventGen iseg, double scalingFactor, int rate) {
		this.iseg = iseg;
		this.scalingFactor = scalingFactor;
		this.rate = rate;
		this.batchSize = this.rate / (1000 / period);
	}

	public static List<String> getHeadersFromCSV(String csvFileName) {
		return CsvSplitter.extractHeadersFromCSV(csvFileName);
	}

	public void launch(String csvFileName, String outCSVFileName) {
		// 1. Load CSV to in-memory data structure
		// 2. Assign a thread with (new SubEventGen(myISEG, eventList))
		// 3. Attach this thread to ThreadPool
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
			List<TableClass> nestedList = JsonSplitter.roundRobinSplitJsonToMemory(csvFileName, numThreads,
					scalingFactor, datasetType);

			
			for (int i = 0; i < numThreads; i++) {
				Timer timer = new Timer("EvenGen", true);
				TimerTask task = new EvenGenTimerTask(nestedList.get(i));
				timer.scheduleAtFixedRate(task, 0, period);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private class EvenGenTimerTask extends TimerTask {
		private final List<List<String>> rows;
		private final int rowLen;
		private int rowIndex = 0;
		
		EvenGenTimerTask (TableClass eventList) {
			rows = eventList.getRows();
			rowLen = rows.size();
		}
		
		@Override
		public void run() {
			for (int i = 0; i < batchSize; i++) {
				if (rowIndex == rowLen) {
					rowIndex = 0;
				}
				List<String> event = rows.get(rowIndex);
				iseg.receive(event);
				rowIndex++;
			}
		}
	}
}
