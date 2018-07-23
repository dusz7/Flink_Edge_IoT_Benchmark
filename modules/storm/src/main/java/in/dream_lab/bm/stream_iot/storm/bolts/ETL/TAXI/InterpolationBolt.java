package in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.aggregate.BlockWindowAverage;
import in.dream_lab.bm.stream_iot.tasks.filter.RangeFilterCheck;
import in.dream_lab.bm.stream_iot.tasks.statistics.Interpolation;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InterpolationBolt extends BaseRichBolt {

	private Properties p;

	public InterpolationBolt(Properties p_) {
		p = p_;

	}

	OutputCollector collector;
	private static Logger l;

	public static void initLogger(Logger l_) {
		l = l_;
	}

	Interpolation interpolationTask;
	Map<String, Interpolation> InterpolationMap;

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

		this.collector = outputCollector;
		initLogger(LoggerFactory.getLogger("APP"));

		interpolationTask = new Interpolation();

		interpolationTask.setup(l, p);
	}

	@Override
	public void execute(Tuple input) {
		//System.out.println("InterpolationBolt : " + Thread.currentThread().getId());

		String msgId = (String) input.getValueByField("MSGID");
		String sensorId = (String) input.getValueByField("SENSORID");
		String meta = (String) input.getValueByField("META");
		String obsType = (String) input.getValueByField("OBSTYPE");
		String obsVal = (String) input.getValueByField("OBSVAL");

		HashMap<String, String> map = new HashMap();
		map.put("SENSORID", sensorId);
		map.put(obsType, obsVal);
		Float res = interpolationTask.doTask(map);

		if (res == null) {
			Values values = new Values(msgId, sensorId, meta, obsType, obsVal); 
			//System.out.println(this.getClass().getName() + " - EMITS - " + values.toString());
			
			if (input.getLongByField("TIMESTAMP") > 0) {
				values.add(System.currentTimeMillis());
			} else {
				values.add(-1L);
			}
			
			Long spoutTimestamp = input.getLongByField("SPOUTTIMESTAMP");
    		if (spoutTimestamp > 0) {
    			values.add(spoutTimestamp);
    		} else {
    			values.add(-1L);
    		}
			
			collector.emit(values);
		}
		else {
			if (res != Float.MIN_VALUE) {
				Values values = new Values(msgId, sensorId, meta, obsType, res.toString()); 
				//System.out.println(this.getClass().getName() + " - EMITS - " + values.toString());
				
				if (input.getLongByField("TIMESTAMP") > 0) {
					values.add(System.currentTimeMillis());
				} else {
					values.add(-1L);
				}
				
				Long spoutTimestamp = input.getLongByField("SPOUTTIMESTAMP");
	    		if (spoutTimestamp > 0) {
	    			values.add(spoutTimestamp);
	    		} else {
	    			values.add(-1L);
	    		}
				
				collector.emit(values);

			} else {
				if (l.isWarnEnabled())
					l.warn("Error in Interpolation Bolt");
				throw new RuntimeException();
			}
		}
	}

	@Override
	public void cleanup() {
		interpolationTask.tearDown();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("MSGID", "SENSORID", "META", "OBSTYPE", "OBSVAL", "TIMESTAMP", "SPOUTTIMESTAMP"));
	}

}
