package in.dream_lab.bm.stream_iot.storm.bolts.TRAIN.SYS;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import in.dream_lab.bm.stream_iot.tasks.io.AzureTableRangeQueryTaskSYS;

public class ReadTrainDataBolt extends BaseRichBolt {

	private static final long serialVersionUID = 100000000L;
	Properties p;
	OutputCollector collector;
	private static Logger l;
	private String TRAIN_DATA_FILE;
	Scanner scanner;
	String ROWKEYSTART;
	String ROWKEYEND;
	File f;

	public static void initLogger(Logger l_) {
		l = l_;
	}

	public ReadTrainDataBolt(Properties p_) {
		p = p_;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		initLogger(LoggerFactory.getLogger("APP"));
		TRAIN_DATA_FILE = (String) stormConf.get("TRAIN_DATA_FILE");
		f = new File(TRAIN_DATA_FILE);
		try {
			scanner = new Scanner(f);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple input) {
		String msgId = input.getStringByField("MSGID");
		ROWKEYSTART = input.getStringByField("ROWKEYSTART");
		ROWKEYEND = input.getStringByField("ROWKEYEND");

		long start = Long.parseLong(ROWKEYSTART);
		long end = Long.parseLong(ROWKEYEND);

		List<AzureTableRangeQueryTaskSYS.SYS_City> data = readData(start, end);

		//		System.out.println("[READ_TRAIN_DATA_BOLT: ] " + start + " - " + end + "Rows in the data = " + data.size());

		StringBuffer bf = new StringBuffer();
		// Loop through the results, displaying information about the entity
		for (AzureTableRangeQueryTaskSYS.SYS_City entity : data) {
			bf.append(entity.getTemperature()).append(",").append(entity.getHumidity()).append(",")
					.append(entity.getLight()).append(",").append(entity.getDust()).append(",")
					.append(entity.getAirquality_raw()).append("\n");

		}
		Values values = new Values(bf.toString(), msgId, ROWKEYEND);
		
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
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("TRAINDATA", "MSGID", "ROWKEYEND", "TIMESTAMP", "SPOUTTIMESTAMP", "CHAINSTAMP"));
	}

	private List<AzureTableRangeQueryTaskSYS.SYS_City> readData(long start, long end) {
		List<AzureTableRangeQueryTaskSYS.SYS_City> list = new ArrayList<>();
		int counter = 0;
		while (counter++ < start && scanner.hasNextLine())
			scanner.nextLine();
		while (scanner.hasNextLine() && counter < end) {
			String input = scanner.nextLine();
			list.add(AzureTableRangeQueryTaskSYS.SYS_City.parseString(input, counter));
			counter++;
		}
		if (!scanner.hasNextLine())
			try {
				scanner = new Scanner(f);
			} catch (IOException e) {
				e.printStackTrace();
			}
		return list;
	}
}
