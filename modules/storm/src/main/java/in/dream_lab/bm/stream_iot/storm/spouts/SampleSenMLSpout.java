package in.dream_lab.bm.stream_iot.storm.spouts;

import in.dream_lab.bm.stream_iot.storm.genevents.EventGen;
import in.dream_lab.bm.stream_iot.storm.genevents.ISyntheticEventGen;
import in.dream_lab.bm.stream_iot.storm.genevents.logging.BatchedFileLogging;
import in.dream_lab.bm.stream_iot.storm.genevents.utils.GlobalConstants;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SampleSenMLSpout extends BaseRichSpout implements ISyntheticEventGen {
	SpoutOutputCollector _collector;
	EventGen eventGen;
	BlockingQueue<List<String>> eventQueue;
	String csvFileName;
	String outSpoutCSVLogFileName;
	String experiRunId;
	double scalingFactor;
	BatchedFileLogging ba;
	long msgId;
	int inputRate;
	long numEvents;
	long startingMsgId;
	boolean bpMonitor = false;
	long bpStartTime = 0;
	long bpTotalTime = 0;

	public SampleSenMLSpout() {
		// this.csvFileName = "/home/ubuntu/sample100_sense.csv";
		// System.out.println("Inside sample spout code");
		this.csvFileName = "/home/tarun/j2ee_workspace/eventGen-anshu/eventGen/bangalore.csv";
		this.scalingFactor = GlobalConstants.accFactor;
		// System.out.print("the output is as follows");
	}

	public SampleSenMLSpout(String csvFileName, String outSpoutCSVLogFileName, double scalingFactor,
			String experiRunId) {
		this.csvFileName = csvFileName;
		this.outSpoutCSVLogFileName = outSpoutCSVLogFileName;
		this.scalingFactor = scalingFactor;
		this.experiRunId = experiRunId;
	}

	public SampleSenMLSpout(String csvFileName, String outSpoutCSVLogFileName, double scalingFactor) {
		this(csvFileName, outSpoutCSVLogFileName, scalingFactor, "");
	}

	public SampleSenMLSpout(String csvFileName, String outSpoutCSVLogFileName, double scalingFactor, int inputRate) {
		this(csvFileName, outSpoutCSVLogFileName, scalingFactor, "");
		this.inputRate = inputRate;
	}

	public SampleSenMLSpout(String csvFileName, String outSpoutCSVLogFileName, double scalingFactor, int inputRate,
			long numEvents) {
		this(csvFileName, outSpoutCSVLogFileName, scalingFactor, "");
		this.inputRate = inputRate;
		this.numEvents = numEvents;
	}

	@Override
	public void nextTuple() {
		List<String> entry = this.eventQueue.poll(); // nextTuple should not
														// block!
		if (entry == null || (this.msgId > this.startingMsgId + this.numEvents))
			return;

		Values values = new Values();
		StringBuilder rowStringBuf = new StringBuilder();
		for (String s : entry) {
			rowStringBuf.append(",").append(s);
		}
		
		String rowString = rowStringBuf.toString().substring(1);
		String newRow = rowString.substring(rowString.indexOf(",") + 1);
		msgId++;
		values.add(Long.toString(msgId));
		values.add(newRow);

		this._collector.emit(values);

		if ((this.msgId > (this.startingMsgId + this.numEvents / 3) && !bpMonitor)) bpMonitor = true; // start monitoring backpressure
//		if ((this.msgId < (this.startingMsgId + (this.numEvents * 3) / 4) && bpMonitor)) bpMonitor = false; // stop monitoring backpressure

		if (this.msgId == this.startingMsgId + this.numEvents - 1) {
			String dir = outSpoutCSVLogFileName.substring(0, outSpoutCSVLogFileName.lastIndexOf("/")+1);
			String filename = outSpoutCSVLogFileName.substring(outSpoutCSVLogFileName.lastIndexOf("/")+1);
			filename = dir + "back_pressure-" + filename;
			writeBPTime(filename);
		}
		
		/* skip logging first 1/3 of events to reach a stable condition */
		if (this.msgId > (this.startingMsgId + this.numEvents / 3)
				&& (this.msgId < (this.startingMsgId + (this.numEvents * 3) / 4))) {
			try {
				ba.batchLogwriter(System.currentTimeMillis(), "MSGID," + msgId);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
		BatchedFileLogging.writeToTemp(this, this.outSpoutCSVLogFileName);
		Random r = new Random();
		try {
			msgId = (long) (1 * Math.pow(10, 12) + (r.nextInt(1000) * Math.pow(10, 9)) + r.nextInt(10));
			this.startingMsgId = msgId;
		} catch (Exception e) {
			e.printStackTrace();
		}
		_collector = collector;
		this.eventGen = new EventGen(this, this.scalingFactor, this.inputRate);
		this.eventQueue = new LinkedBlockingQueue<List<String>>();
		String uLogfilename = this.outSpoutCSVLogFileName;
		this.eventGen.launch(this.csvFileName, uLogfilename, -1, true); // Launch
																		// threads

		ba = new BatchedFileLogging(uLogfilename, context.getThisComponentId());
	}

	@Override
	public void ack(Object msgId) {
		System.out.println("Backpressure Observed");
		if (bpMonitor) {
			System.out.println("BPMONITOR: Backpressure Observed");
			bpStartTime = System.currentTimeMillis();
		}
	}

	@Override
	public void fail(Object msgId) {
		if (bpMonitor) {
			bpTotalTime = bpTotalTime + (System.currentTimeMillis() - bpStartTime);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("MSGID", "PAYLOAD"));
	}

	@Override
	public void receive(List<String> event) {
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
			if (bpTotalTime == 0) {
				if (bpStartTime != 0)
					bpTime = System.currentTimeMillis() - bpStartTime;
			} else
				bpTime = bpTotalTime;

			writer.write(Long.toString(bpTime));
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
