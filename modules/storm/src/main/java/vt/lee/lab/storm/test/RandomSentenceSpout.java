package vt.lee.lab.storm.test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import in.dream_lab.bm.stream_iot.storm.genevents.logging.BatchedFileLogging;
import in.dream_lab.bm.stream_iot.storm.spouts.BpTime;
import in.dream_lab.bm.stream_iot.storm.spouts.BpTimeIntervalMonitoringTask;

public class RandomSentenceSpout extends BaseRichSpout implements ISyntheticSentenceGenerator {

	private static final long serialVersionUID = 1L;
	SpoutOutputCollector _collector;
	Random _rand;
	BlockingQueue<String> eventQueue;
	String[] sentences = RandomSentences.sentences;
	SentenceEventGenerator sentenceEventGen;
	private int rate;
	private long expDuration;
	String spoutLogFile;
	BatchedFileLogging ba;
	long msgId;
	long startingMsgID;
	long numEvents;
	String outDir;
	
	boolean bpMonitor = false;
	boolean recordBp = false;
	Timer timer;
	BpTime bptime;

	public RandomSentenceSpout(String logFile, int rate, long expDuration, long numEvents) {
		this.rate = rate;
		this.expDuration = expDuration;
		this.spoutLogFile = logFile;
		this.numEvents = numEvents;
	}

	public RandomSentenceSpout(String logFile, int rate, long expDuration, long numEvents, String outDir) {
		this(logFile, rate, expDuration, numEvents);
		this.outDir = outDir;
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		_rand = new Random();
		BatchedFileLogging.writeToTemp(this, this.spoutLogFile);
		Random r = new Random();
		try {
			msgId = (long) (1 * Math.pow(10, 12) + (r.nextInt(1000) * Math.pow(10, 9)) + r.nextInt(10));
			startingMsgID = msgId;

		} catch (Exception e) {

			e.printStackTrace();
		}

		this.sentenceEventGen = new SentenceEventGenerator(this, this.rate, this.numEvents);
		this.eventQueue = new LinkedBlockingQueue<String>();
		this.sentenceEventGen.launch(RandomSentences.sentences, this.expDuration);
		
		bptime = new BpTime();
		ba = new BatchedFileLogging(this.spoutLogFile, context.getThisComponentId());
	}

	@Override
	public void nextTuple() {
		//System.out.println("nextTuple called");
		String snetence = this.eventQueue.poll();
		if (snetence == null || (msgId > (startingMsgID + numEvents)))
			return;

		msgId++;
		
		//System.out.println(msgId - startingMsgID);
		
		_collector.emit(new Values(Long.toString(msgId), snetence));
		
		// start monitoring backpressure
		if ((this.msgId == (this.startingMsgID + this.numEvents / 3) && !bpMonitor)) {
			bpMonitor = true;
			long window = 2000;
			BpTimeIntervalMonitoringTask bpTask = new BpTimeIntervalMonitoringTask(bptime, window, 5.0);
			timer = new Timer("BpIntervalTimer");
			timer.scheduleAtFixedRate(bpTask, 10, window);
		}

		// stop monitoring backpressure
		if ((this.msgId == (this.startingMsgID + (this.numEvents * 3) / 4) && bpMonitor))
			bpMonitor = false;

		if (this.msgId == this.startingMsgID + this.numEvents - 1) {
			String dir = spoutLogFile.substring(0, spoutLogFile.lastIndexOf("/") + 1);
			String filename = spoutLogFile.substring(spoutLogFile.lastIndexOf("/") + 1);
			filename = dir + "back_pressure-" + filename;
			writeBPTime(filename);

			timer.cancel();
		}
		
		/*
		try {
			ba.batchLogwriter(System.currentTimeMillis(), "MSGID," + msgId);
		} catch (Exception e) {
			e.printStackTrace();
		}
		*/
		/* skip logging first 1/3 of events to reach a stable condition */
		
		if (this.msgId > (this.startingMsgID + this.numEvents / 3)
				&& (this.msgId < (this.startingMsgID + (this.numEvents * 3) / 4))) {
			try {
				ba.batchLogwriter(System.currentTimeMillis(), "MSGID," + msgId);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("MSGID", "sentence"));
	}
	
	@Override
	public void ack(Object msgId) {
		if (bpMonitor) {
			recordBp = true;
			// bpStartTime = System.currentTimeMillis();
			bptime.setBpStartTime(System.currentTimeMillis());
		}
	}

	@Override
	public void fail(Object msgId) {
		if (recordBp) {
			// bpTotalTime = bpTotalTime + (System.currentTimeMillis() -
			// bpStartTime);
			bptime.updateBpCurrAccTime();
			recordBp = false;
		}
	}
	
	/*
	@Override
	public void ack(Object msgId) {
		System.out.println("Acker called");
	}

	@Override
	public void fail(Object msgId) {
		System.out.println("Failure called");
	}
	*/
	
	@Override
	public void receive(String event) {
		try {
			this.eventQueue.put(event);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void writeBPTime(String fileName) {
		BufferedWriter writer;
		long bpTime = 0;
		try {
			writer = new BufferedWriter(new FileWriter(fileName));

			bpTime = bptime.bpTotalAccTime;
			writer.write(Long.toString(bpTime));
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
