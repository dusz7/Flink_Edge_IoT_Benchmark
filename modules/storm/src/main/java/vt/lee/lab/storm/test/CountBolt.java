package vt.lee.lab.storm.test;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class CountBolt extends BaseRichBolt {

	private OutputCollector collector;
	private Map<String, Integer> map;
	private int iter;

	public CountBolt(int iter) {
		this.iter = iter;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.map = new HashMap<String, Integer>();
	}

	@Override
	public void execute(Tuple input) {
		String word = input.getStringByField("word");
		String msgId = input.getStringByField("MSGID");
		if (!map.containsKey(word)) {
			map.put(word, 1);
		} else {
			while (this.iter-- >= 0)
				map.put(word, map.get(word) + 1);
		}
		this.collector.emit(new Values(msgId, word, map.get(word)));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("MSGID", "word", "count"));

	}

}
