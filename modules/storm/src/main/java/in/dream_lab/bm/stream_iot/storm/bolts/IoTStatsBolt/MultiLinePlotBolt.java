package in.dream_lab.bm.stream_iot.storm.bolts.IoTStatsBolt;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.aggregate.AccumlatorTask;
import in.dream_lab.bm.stream_iot.tasks.io.ZipMultipleBufferTask;
import in.dream_lab.bm.stream_iot.tasks.utils.TimestampValue;
import in.dream_lab.bm.stream_iot.tasks.visualize.XChartMultiLinePlotTask;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author shilpa
 *
 */
public class MultiLinePlotBolt extends BaseRichBolt {

	private Properties p;

	public MultiLinePlotBolt(Properties p_) {
		p = p_;
	}

	OutputCollector collector;
	private static Logger l;

	public static void initLogger(Logger l_) {
		l = l_;
	}

	XChartMultiLinePlotTask plotTask;
	AccumlatorTask accumlatorTask;
	ZipMultipleBufferTask zipTask;

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

		this.collector = outputCollector;
		initLogger(LoggerFactory.getLogger("APP"));
		plotTask = new XChartMultiLinePlotTask();
		accumlatorTask = new AccumlatorTask();
		zipTask = new ZipMultipleBufferTask();
		accumlatorTask.setup(l, p);
		plotTask.setup(l, p);
		zipTask.setup(l, p);
	}

	@Override
	public void execute(Tuple input) {
		String msgId = input.getStringByField("MSGID");
		Map<String, String> map = new HashMap<String, String>();
		map.put("SENSORID", input.getStringByField("SENSORID"));
		map.put("OBSTYPE", input.getStringByField("OBSTYPE"));
		map.put("OBSVALUE", input.getStringByField("res"));
		map.put("META", input.getStringByField("META"));

/*		System.out.println(this.getClass().getName() + " INPUT: " + map.get("SENSORID") + " - " + map.get("OBSTYPE")
				+ " - " + map.get("OBSVALUE") + " - " + map.get("META"));
*/
		// call accumulator with tuple
		float res = accumlatorTask.doTaskLogic(map);
		//System.out.println(this.getClass().getName() + " - accumulator task result: " + res);

		if (res == 1.0f) { // finished accumulate
			try {
				// get accumulated values
				/*
				 * the returned map is a map from SensorID:Observation ->
				 * Map[Sensor -> Queue of timestamp values for the sensor]
				 * Timestamp value contains timestamp and observed value of the sensor
				 */
				Map<String, Map<String, Queue<TimestampValue>>> valuesMap = accumlatorTask.getLastResult();

				Set<Entry<String, Map<String, Queue<TimestampValue>>>> entrySet = valuesMap.entrySet();

				// For each type of accumulated observation
				for (Entry<String, Map<String, Queue<TimestampValue>>> entry : entrySet) {
					// send accumulated values for observation to plotting
					// routine, in-memory
					// and get an input stream with response
					Map<String, Queue<TimestampValue>> inputForPlotMap = entry.getValue();
					plotTask.doTaskLogic(inputForPlotMap);
					InputStream byteInputStream = plotTask.getLastResult();

					// send generated chart from input stream, and send to zip
					// task
					HashMap<String, InputStream> inputForZipMap = new HashMap<String, InputStream>();
					inputForZipMap.put(AbstractTask.DEFAULT_KEY, byteInputStream);

					float zipres = zipTask.doTask(inputForZipMap);

					//System.out.println(this.getClass().getName() + " - Zip result: " + zipres);

					// if zip is done batching one set of requests, send zip
					// path downstream
					if (zipres == 1.0f) {
						// emit the path sent as last result from zip task
						String path = zipTask.getLastResult();
						collector.emit(new Values(msgId, path));
						// FIXME: garbase collect zip file at destination, once
						// uploaded to blob
					}
				}
			} catch (Exception e) {
				l.error("Exception occured in MultiLinePlotBolt exceute method " + e.getMessage());
			}
		}
	}

	@Override
	public void cleanup() {
		// plotTask.tearDown();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("MSGID", "PATH"));
	}
}