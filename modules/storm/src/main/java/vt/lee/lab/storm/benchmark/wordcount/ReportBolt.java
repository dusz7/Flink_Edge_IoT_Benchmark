package vt.lee.lab.storm.benchmark.wordcount;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class ReportBolt extends BaseRichBolt {
	private Map<String, Long> counts = null;
	private Map<String, NumChecker> checker = null;
	private OutputCollector collector;
	
	public void prepare(Map config, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.counts = new HashMap<String, Long>();
		this.checker = new HashMap<String, NumChecker>();
	}
	public void execute(Tuple tuple) {
		String msgId = tuple.getStringByField("MSGID");
		String word = tuple.getStringByField("word");
		Integer num = tuple.getIntegerByField("num");
		Long count = tuple.getLongByField("count");
	
		if (!checker.containsKey(msgId))
			checker.put(msgId, new NumChecker(num));
		
		if (checker.get(msgId).inc()) {
			checker.remove(msgId);
			this.collector.emit(new Values(msgId));
		}
		
		this.counts.put(word, count);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("MSGID"));
	}
	
	private class NumChecker {
		private final int num;
		private int curr = 0;
		
		public NumChecker (Integer num) {
			this.num = num;
		}
		
		public boolean inc() {
			curr += 1;
			
			if (curr == num)
				return true;
			else 
				return false;
		}
	}
}
