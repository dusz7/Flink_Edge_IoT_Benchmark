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
		String sentence = input.getStringByField("sentence");
		String msgId = input.getStringByField("MSGID");
		int i = 0;
		while (i < this.iter) {
			String[] splitSentence = sentence.split(" ");
			for (String word : splitSentence) {

				if (!map.containsKey(word)) {
					map.put(word, 1);
				} else {
					map.put(word, map.get(word) + 1);
				}
			}
			i++;
		}
		this.collector.emit(new Values(msgId, sentence));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("MSGID", "sentence"));

	}

}
