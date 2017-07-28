package vt.lee.lab.storm.test.inbalance;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class InbalanceCountBolt extends BaseRichBolt {

	private OutputCollector collector;
	private Map<String, Long> map;
	private int iter;

	public InbalanceCountBolt(int iter) {
		this.iter = iter;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.map = new HashMap<String, Long>();
	}

	@Override
	public void execute(Tuple input) {
		String msgId = input.getStringByField("MSGID");
		String sentence = input.getStringByField("sentence");
		
		for (int i = 0; i < iter; i++) {
			String[] words = sentence.split(" ");
			for(String word : words){
				Long count = this.map.get(word);
				if(count == null){
					count = 0L;
				}
				count++;
				this.map.put(word, count);
			}
		}
		
		this.collector.emit(new Values(msgId, sentence));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("MSGID", "sentence"));

	}

}
