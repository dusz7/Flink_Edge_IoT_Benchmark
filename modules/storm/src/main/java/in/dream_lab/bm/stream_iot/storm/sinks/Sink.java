package in.dream_lab.bm.stream_iot.storm.sinks;

import in.dream_lab.bm.stream_iot.storm.genevents.logging.BatchedFileLogging;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

/**
 * Created by anshushukla on 19/05/15.
 */
public class Sink extends BaseRichBolt {
	private static final Logger LOG = LoggerFactory.getLogger("APP");

	OutputCollector collector;
	private static Logger l;

	public static void initLogger(Logger l_) {
		l = l_;
	}

	private BatchedFileLogging ba;
	String csvFileNameOutSink; // Full path name of the file at the sink bolt

	public Sink(String csvFileNameOutSink) {
		Random ran = new Random();
		this.csvFileNameOutSink = csvFileNameOutSink;
		/*
		 * System.out.println(Thread.currentThread().getId() +
		 * Thread.currentThread().getName() + this.getClass().getName() +
		 * "SINK: Output log File Name: " + csvFileNameOutSink);
		 */
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = outputCollector;
		BatchedFileLogging.writeToTemp(this, this.csvFileNameOutSink);
		// ba=new BatchedFileLogging();
		setBa(new BatchedFileLogging(this.csvFileNameOutSink, topologyContext.getThisComponentId()));
		/*
		 * if (ba != null) System.out.println(Thread.currentThread().getId() +
		 * Thread.currentThread().getName() + this.getClass().getName() +
		 * "Created new BatchedFileLogging. " + ba.toString());
		 */
	}

	@Override
	public void execute(Tuple input) {
		String msgId = input.getStringByField("MSGID");

		try {
			getBa().batchLogwriter(System.currentTimeMillis(), msgId);
			// ba.batchLogwriter(System.currentTimeMillis(),msgId+","+exe_time);//addon
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

	}

	public BatchedFileLogging getBa() {
		return ba;
	}

	public void setBa(BatchedFileLogging ba) {
		this.ba = ba;
	}
}
