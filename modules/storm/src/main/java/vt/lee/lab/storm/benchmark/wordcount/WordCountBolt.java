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

public class WordCountBolt extends BaseRichBolt{
	private OutputCollector collector;
	private HashMap<String, Long> counts = null;
	
	public void prepare(Map config, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.counts = new HashMap<String, Long>();
	}
	
	public void execute(Tuple tuple) {
		String msgId = tuple.getStringByField("MSGID");
		String word = tuple.getStringByField("word");
		Integer num = tuple.getIntegerByField("num");
		
		Long count = this.counts.get(word);
		if(count == null){
			count = 0L;
		}
		count++;
		this.counts.put(word, count);
		this.collector.emit(new Values(msgId, word, num, count));
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("MSGID", "word", "num", "count"));
	}
}
