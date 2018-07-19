package vt.lee.lab.storm.test.configurable;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import in.dream_lab.bm.stream_iot.storm.genevents.logging.BatchedFileLogging;

public class ConfSink extends BaseRichBolt {
	private BatchedFileLogging ba;
	
	private final String csvFileNameOutSink; // Full path name of the file at the sink bolt
	private final long total;
	private long count = 0;
	
	public ConfSink(String csvFileNameOutSink, long total) {
		this.csvFileNameOutSink = csvFileNameOutSink;
		this.total = total;
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		BatchedFileLogging.writeToTemp(this, this.csvFileNameOutSink);
		ba = new BatchedFileLogging(this.csvFileNameOutSink, topologyContext.getThisComponentId());
	}

	@Override
	public void execute(Tuple input) {
		count++;
		
		// skip first third and last quarter
		/*
		if ((count < total / 3) || (count > (total * 3) / 4)) {
			return;	
		}
		*/
		
		String msgId = input.getStringByField("MSGID");
		String identifier = msgId + ",onepath";

		try {
			ba.batchLogwriter(System.currentTimeMillis(), identifier);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

	}
}
