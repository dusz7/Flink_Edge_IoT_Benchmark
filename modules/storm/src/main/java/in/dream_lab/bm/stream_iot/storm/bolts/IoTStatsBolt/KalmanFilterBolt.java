package in.dream_lab.bm.stream_iot.storm.bolts.IoTStatsBolt;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.statistics.KalmanFilter;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KalmanFilterBolt extends BaseRichBolt {

	private Properties p;
	private ArrayList<String> useMsgList;

	public KalmanFilterBolt(Properties p_) {
		p = p_;
	}

	OutputCollector collector;
	private static Logger l;

	public static void initLogger(Logger l_) {
		l = l_;
	}

	Map<String, KalmanFilter> kmap; // kalmanFilter;
	// KalmanFilter kalmanFilter;

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

		this.collector = outputCollector;
		initLogger(LoggerFactory.getLogger("APP"));
		kmap = new HashMap<String, KalmanFilter>();
		String useMsgField = p.getProperty("STATISTICS.KALMAN_FILTER.USE_MSG_FIELDLIST");
		String[] msgField = useMsgField.split(",");
		useMsgList = new ArrayList<String>();
		for (String s : msgField) {
			useMsgList.add(s);
		}

		// kalmanFilter=new KalmanFilter();
		// kalmanFilter.setup(l,p);
	}

	// from bloom - outputFieldsDeclarer.declare(new
	// Fields("sensorMeta","sensorID","obsType","obsVal","MSGID"));

	@Override
	public void execute(Tuple input) {

		String msgId = input.getStringByField("MSGID");
		String sensorMeta = input.getStringByField("sensorMeta");
		String sensorID = input.getStringByField("sensorID");
		String obsType = input.getStringByField("obsType");
		String obsVal = input.getStringByField("obsVal");
		
		if (useMsgList.contains(obsType)) {

			String key = sensorID + obsType;

			KalmanFilter kalmanFilter = kmap.get(key);
			if (kalmanFilter == null) {
				kalmanFilter = new KalmanFilter();
				kalmanFilter.setup(l, p);
				kmap.put(key, kalmanFilter);
			}

			HashMap<String, String> map = new HashMap();
			map.put(AbstractTask.DEFAULT_KEY, obsVal);
			Float kalmanUpdatedVal = kalmanFilter.doTask(map);

			// if(l.isInfoEnabled())
			// System.out.println("INFO is enabled");
			//

			// if(l.isInfoEnabled())
			// l.info("TEST1:kalmanUpdatedVal-"+kalmanUpdatedVal);

			if (kalmanUpdatedVal != null) {
				Values values = new Values(sensorMeta, sensorID, obsType, kalmanUpdatedVal.toString(), msgId);
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
				
				values.add(input.getLongByField("CHAINSTAMP"));
				collector.emit(values);
			} else {
				// if (l.isWarnEnabled())
				//	l.warn("Error in KalmanFilterBolt and Val is -" + kalmanUpdatedVal);
				throw new RuntimeException();
			}
		}
	}

	@Override
	public void cleanup() {
		// kalmanFilter.tearDown();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("sensorMeta", "sensorID", "obsType", "kalmanUpdatedVal", "MSGID", "TIMESTAMP", "SPOUTTIMESTAMP", "CHAINSTAMP"));
	}

}