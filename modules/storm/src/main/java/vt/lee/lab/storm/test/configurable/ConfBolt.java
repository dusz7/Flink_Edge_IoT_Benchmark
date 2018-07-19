package vt.lee.lab.storm.test.configurable;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class ConfBolt extends BaseRichBolt {

	private OutputCollector collector;
	private Map<String, Long> map;
	private final int complexity;
	private final int inputRatio;
	private final int outputRatio;
	private int emitCount;

	public ConfBolt(int complexity, int inputRatio, int outputRatio) {
		this.complexity = complexity;
		this.inputRatio = inputRatio;
		this.outputRatio = outputRatio;
		this.emitCount = 0;
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
		
		for (int i = 0; i < complexity; i++) {
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
		
		if (inputRatio == outputRatio) {
			this.collector.emit(new Values(msgId, sentence));
		} else if (inputRatio < outputRatio){
			if (emitCount == inputRatio) {
				emitCount = 0;
			}
			emitCount++;
			
			this.collector.emit(new Values(msgId, sentence));
			if (emitCount == inputRatio) {
				for (int i = 0; i < outputRatio - inputRatio; i++) {
					this.collector.emit(new Values(msgId, sentence));
				}
			}
		} else {
			if (emitCount == inputRatio) {
				emitCount = 0;
			}
			emitCount++;
			
			if (emitCount <= outputRatio) {
				this.collector.emit(new Values(msgId, sentence));
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("MSGID", "sentence"));

	}

}
