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
		// 1. Load CSV to in-memory data structure
		// 2. Assign a thread with (new SubEventGen(myISEG, eventList))
		// 3. Attach this thread to ThreadPool
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

/*			System.out.println(this.getClass().getName() + " splitted csv files to memory. nestedList");
			for (TableClass c : nestedList)
				System.out.println(this.getClass().getName() + " " + c.toString() + " \n + Header" + c.getHeader()
						+ "\n # Rows" + c.getRows());
*/
			this.executorService = Executors.newFixedThreadPool(numThreads);

			Semaphore sem1 = new Semaphore(0);

			Semaphore sem2 = new Semaphore(0);

			SubEventGen[] subEventGenArr = new SubEventGen[numThreads];
			for (int i = 0; i < numThreads; i++) {
				// this.executorService.execute(new SubEventGen(this.iseg,
				// nestedList.get(i)));
				subEventGenArr[i] = new SubEventGen(this.iseg, nestedList.get(i), sem1, sem2, this.rate);
				//System.out.println(this.getClass().getName() + " submitting first runnable");
				this.executorService.execute(subEventGenArr[i]);
			}

			sem1.acquire(numThreads);

			//System.out.println(this.getClass().getName() + " acquired sem1");

			long experiStartTs = System.currentTimeMillis();
			// set the start time to all the thread objects
			for (int i = 0; i < numThreads; i++) {
				// this.executorService.execute(new SubEventGen(this.iseg,
				// nestedList.get(i)));
				subEventGenArr[i].experiStartTime = experiStartTs;
				if (experimentDurationMillis > 0)
					subEventGenArr[i].experiDuration = experimentDurationMillis;
				this.executorService.execute(subEventGenArr[i]);
			}
			sem2.release(numThreads);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
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

			Semaphore sem1 = new Semaphore(0);

			Semaphore sem2 = new Semaphore(0);

			SubEventGen[] subEventGenArr = new SubEventGen[numThreads];
			for (int i = 0; i < numThreads; i++) {
				// this.executorService.execute(new SubEventGen(this.iseg,
				// nestedList.get(i)));
				subEventGenArr[i] = new SubEventGen(this.iseg, nestedList.get(i), sem1, sem2, this.rate);
				this.executorService.execute(subEventGenArr[i]);
			}

			sem1.acquire(numThreads);
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
			sem2.release(numThreads);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

class SubEventGen implements Runnable {
	ISyntheticEventGen iseg;
	TableClass eventList;
	Long experiStartTime; // in millis since epoch
	Semaphore sem1, sem2;
	Long experiDuration = 600000L;
	double delay = 0;

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

	@Override
	public void run() {
		sem1.release();
		try {
			sem2.acquire();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		List<List<String>> rows = this.eventList.getRows();
		int rowLen = rows.size();
		List<Long> timestamps = this.eventList.getTs();
		Long experiRestartTime = experiStartTime;
		boolean runOnce = (experiDuration < 0);
		System.out.println(this.getClass().getName() + " - runOnce: " + runOnce);
		long currentRuntime = 0;


		do {
			for (int i = 0; i < rowLen && (runOnce || (currentRuntime < experiDuration)); i++) {
				// Long deltaTs = timestamps.get(i);
				List<String> event = rows.get(i);
				Long currentTs = System.currentTimeMillis();
				// long delay = deltaTs - (currentTs - experiRestartTime); //
				// how long until this event should be sent?

				/*
				 * Delaying sending the event if we're working with old csv
				 * files does not make sense. Is data preparation required
				 * beforehand?
				 */

				// System.out.println(this.getClass().getName() + " - delay: " +
				// delay + " deltaTs " + deltaTs);

				/*
				 * Why sleep here ???? It seems delay ~= deltaTs, which we
				 * actually get from the csv file. It's a large value and hence
				 * the thread sleeps here !!! Commenting this sleep for now.
				 */

				/*
				 * if (delay > 10) { // sleep only if it is non-trivial time. We
				 * will catch up on sleep later.
				 * 
				 * }
				 */
				long start = System.nanoTime();
				while (start + delay >= System.nanoTime())
					;

				this.iseg.receive(event);

				/*
				 * Since not sleeping anymore, should not factor in the value of
				 * delay in currentRuntime calculation
				 */
				// ORIG: currentRuntime = (currentTs - experiStartTime) + delay;
				// // appox time since the experiment started

				currentRuntime = (long) ((currentTs - experiStartTime) + (delay / 1000000));
				// System.out.println(this.getClass().getName() + " - updated
				// current time: " + currentRuntime +
				// " Calculation: currentTs: " + Long.toString(currentTs) +
				// "expStartTime: " + Long.toString(experiStartTime));
			}

			experiRestartTime = System.currentTimeMillis();
		} while (!runOnce && (currentRuntime < experiDuration));

	}
}
