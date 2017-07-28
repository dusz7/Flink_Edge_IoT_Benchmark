package vt.lee.lab.storm.test;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import in.dream_lab.bm.stream_iot.storm.genevents.logging.BatchedFileLogging;

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

	public RandomSentenceSpout(String logFile, int rate, long expDuration) {
		this.rate = rate;
		this.expDuration = expDuration;
		this.spoutLogFile = logFile;
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		_rand = new Random();
		BatchedFileLogging.writeToTemp(this, this.spoutLogFile);
		Random r = new Random();
		try {
			msgId = (long) (1 * Math.pow(10, 12) + (r.nextInt(1000) * Math.pow(10, 9)) + r.nextInt(10));

		} catch (Exception e) {

			e.printStackTrace();
		}

		this.sentenceEventGen = new SentenceEventGenerator(this, this.rate);
		this.eventQueue = new LinkedBlockingQueue<String>();
		this.sentenceEventGen.launch(RandomSentences.sentences, this.expDuration);

		ba = new BatchedFileLogging(this.spoutLogFile, context.getThisComponentId());
	}

	@Override
	public void nextTuple() {
		String snetence = this.eventQueue.poll();
		if (snetence == null)
			return;

		msgId++;

		_collector.emit(new Values(Long.toString(msgId), snetence));
		try {
			ba.batchLogwriter(System.currentTimeMillis(), "MSGID," + msgId);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("MSGID", "sentence"));
	}

	@Override
	public void ack(Object msgId) {
		System.out.println("RandomSentenceSpout: ack method triggered (Backpressure was enabled). Input Rate = "
				+ this.rate + ". msgId = " + msgId.toString());
	}

	@Override
	public void fail(Object msgId) {
		System.out.println(
				"RandomSentenceSpout: fail method triggered (One or more receive queues reached high watermark). Input Rate = "
						+ this.rate + ". msgId = " + msgId.toString());
	}

	@Override
	public void receive(String event) {
		try {
			this.eventQueue.put(event);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
