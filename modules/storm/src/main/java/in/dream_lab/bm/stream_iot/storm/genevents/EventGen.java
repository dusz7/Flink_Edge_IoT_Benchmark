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

import com.esotericsoftware.minlog.Log;

public class EventGen {
	ISyntheticEventGen iseg;
	ExecutorService executorService;
	double scalingFactor;
	int rate = 0;

	public EventGen(ISyntheticEventGen iseg) {
		this(iseg, GlobalConstants.accFactor);
	}

	public EventGen(ISyntheticEventGen iseg, double scalingFactor) {
		this.iseg = iseg;
		this.scalingFactor = scalingFactor;
	}

	public EventGen(ISyntheticEventGen iseg, double scalingFactor, int rate) {
		this.iseg = iseg;
		this.scalingFactor = scalingFactor;
		this.rate = rate;
	}

	public static List<String> getHeadersFromCSV(String csvFileName) {
		return CsvSplitter.extractHeadersFromCSV(csvFileName);
	}

	public void launch(String csvFileName, String outCSVFileName) {
		System.out.println(csvFileName);
		System.out.println(outCSVFileName);
		launch(csvFileName, outCSVFileName, 600000L);
	}

	// Launches all the threads
	public void launch(String csvFileName, String outCSVFileName, long experimentDurationMillis) {
		try {
			int numThreads = GlobalConstants.numThreads; // currently, 1
			// double scalingFactor = GlobalConstants.accFactor;
			String datasetType = "";

			if (outCSVFileName.indexOf("TAXI") != -1) {
				datasetType = "TAXI";// GlobalConstants.dataSetType = "TAXI";
			} else if (outCSVFileName.indexOf("SYS") != -1) {
				datasetType = "SYS";// GlobalConstants.dataSetType = "SYS";
			} else if (outCSVFileName.indexOf("PLUG") != -1) {
				datasetType = "PLUG";// GlobalConstants.dataSetType = "PLUG";
			}

			List<TableClass> nestedList = CsvSplitter.roundRobinSplitCsvToMemory(csvFileName, numThreads, scalingFactor,
					datasetType);

			this.executorService = Executors.newFixedThreadPool(numThreads);

			SubEventGen[] subEventGenArr = new SubEventGen[numThreads];
			for (int i = 0; i < numThreads; i++) {
				subEventGenArr[i] = new SubEventGen(this.iseg, nestedList.get(i), this.rate);
				this.executorService.execute(subEventGenArr[i]);
			}

			long experiStartTs = System.currentTimeMillis();
			// set the start time to all the thread objects
			for (int i = 0; i < numThreads; i++) {

				subEventGenArr[i].experiStartTime = experiStartTs;
				if (experimentDurationMillis > 0)
					subEventGenArr[i].experiDuration = experimentDurationMillis;
				this.executorService.execute(subEventGenArr[i]);
			}
			// sem2.release(numThreads);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void launch(String csvFileName, String outCSVFileName, long experimentDurationMillis, boolean isJson) {
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

			this.executorService = Executors.newFixedThreadPool(numThreads);

			SubEventGen[] subEventGenArr = new SubEventGen[numThreads];
			for (int i = 0; i < numThreads; i++) {
				subEventGenArr[i] = new SubEventGen(this.iseg, nestedList.get(i), this.rate);
				this.executorService.execute(subEventGenArr[i]);
			}

			// sem1.acquire(numThreads);
			// set the start time to all the thread objects
			long experiStartTs = System.currentTimeMillis();
			for (int i = 0; i < numThreads; i++) {
				// this.executorService.execute(new SubEventGen(this.iseg,
				// nestedList.get(i)));
				subEventGenArr[i].experiStartTime = experiStartTs;
				if (experimentDurationMillis > 0)
					subEventGenArr[i].experiDuration = experimentDurationMillis;
				this.executorService.execute(subEventGenArr[i]);
			}
			// sem2.release(numThreads);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// catch (InterruptedException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
	}
}

class SubEventGen implements Runnable {
	ISyntheticEventGen iseg;
	TableClass eventList;
	Long experiStartTime; // in millis since epoch
	Semaphore sem1, sem2;
	Long experiDuration = 1800000L;
	double delay = 0;

	public SubEventGen(ISyntheticEventGen iseg, TableClass eventList) {
		this.iseg = iseg;
		this.eventList = eventList;
	}

	public SubEventGen(ISyntheticEventGen iseg, TableClass eventList, Semaphore sem1, Semaphore sem2) {
		this.iseg = iseg;
		this.eventList = eventList;
		this.sem1 = sem1;
		this.sem2 = sem2;
	}

	public SubEventGen(ISyntheticEventGen iseg, TableClass eventList, Semaphore sem1, Semaphore sem2, int rate) {
		this(iseg, eventList, sem1, sem2);
		if (rate != 0) {
			this.delay = (1 / (double) rate) * 1000000000; /* delay in ns */
			System.out.println(Thread.currentThread().getName() + "Delay: " + this.delay);
		}
	}

	public SubEventGen(ISyntheticEventGen iseg, TableClass eventList, int rate) {
		this(iseg, eventList);
		if (rate != 0) {
			this.delay = (1 / (double) rate) * 1000000000; /* delay in ns */
			System.out.println(Thread.currentThread().getName() + "Delay: " + this.delay);
		}
	}

	@Override
	public void run() {
		Thread.currentThread().setPriority(Thread.MAX_PRIORITY);

		// sem1.release();
		// try {
		// sem2.acquire();
		// } catch (InterruptedException e1) {
		// e1.printStackTrace();
		// }
		List<List<String>> rows = this.eventList.getRows();
		int rowLen = rows.size();
		List<Long> timestamps = this.eventList.getTs();
		Long experiRestartTime = experiStartTime;
		boolean runOnce = (experiDuration < 0);
		System.out.println(this.getClass().getName() + " - runOnce: " + runOnce);
		long currentRuntime = 0;
		long emitCount = 0;
		long initTime = System.currentTimeMillis();
		long event_emit_time;
		long event_emit_period;
		long emitPeriodCount = 0;
		long emitPeriodSum = 0;

		do {
			// initTime = System.currentTimeMillis();
			for (int i = 0; i < rowLen && (runOnce || (currentRuntime < experiDuration)); i++) {
				// Long deltaTs = timestamps.get(i);
				List<String> event = rows.get(i);
				Long currentTs = System.currentTimeMillis();

				long start = System.nanoTime();
				while (start + delay >= System.nanoTime())
					;

				this.iseg.receive(event);
				emitCount++;

				// if (emitCount%10000 == 0) {
				// emitPeriodCount++;
				// event_emit_time = System.currentTimeMillis();
				// event_emit_period = event_emit_time - initTime;
				// emitPeriodSum += event_emit_period;
				// Log.info("10000 EVENTS EMITTED IN: " + (event_emit_period));
				// Log.info("Moving average event emit: " + (double)
				// emitPeriodSum/emitPeriodCount);
				// initTime = System.currentTimeMillis();
				// }

				currentRuntime = (long) ((currentTs - experiStartTime) + (delay / 1000000));

			}
			experiRestartTime = System.currentTimeMillis();

		} while (!runOnce && (currentRuntime < experiDuration));

	}
}
