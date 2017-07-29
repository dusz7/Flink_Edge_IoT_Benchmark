package vt.lee.lab.storm.test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SimpleWordCountToplogy {

	public static class WordSpout extends BaseRichSpout {
		SpoutOutputCollector _collector;
		Random _rand;

		String[] words = { "Hello", "World", "New", "Words" };

		@Override
		public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
			_collector = collector;
			_rand = new Random();
		}

		@Override
		public void nextTuple() {
			String word = words[_rand.nextInt(words.length)];
			_collector.emit(new Values(word));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));

		}
		
		@Override
		public void ack(Object msgId) {
			System.out.println("Acker called");
		}
		
		@Override
		public void fail(Object msgId) {
			System.out.println("Failure called");
		}
		
	}

	public static class WordCountBolt extends BaseRichBolt {

		private OutputCollector collector;
		private Map<String, Integer> map;

		@Override
		public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
			this.collector = collector;
			this.map = new HashMap<String, Integer>();
		}

		@Override
		public void execute(Tuple input) {
			String word = input.getStringByField("word");
			Integer count = 1;
			if (!map.containsKey(word)) {
				map.put(word, count);
			} else {
				count = map.get(word)+1;
				map.put(word, count);
			}
			this.collector.emit(new Values(word, count));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word", "count"));

		}

	}

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();

		Config config = new Config();
		config.setNumAckers(0);
		config.put(Config.TOPOLOGY_BACKPRESSURE_ENABLE, true);
		config.setDebug(true);

		builder.setSpout("word_spout", new WordSpout());
		builder.setBolt("count_bolt", new WordCountBolt()).shuffleGrouping("word_spout");

		StormTopology stormTopology = builder.createTopology();

		try {
			StormSubmitter.submitTopology("SimpleWordCountToplogy", config, stormTopology);
		} catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
