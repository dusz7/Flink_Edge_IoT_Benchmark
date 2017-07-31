package in.dream_lab.bm.stream_iot.storm.sinks;

import org.apache.storm.tuple.Tuple;

public class EtlTopologySinkBolt extends Sink {

	public EtlTopologySinkBolt(String csvFileNameOutSink) {
		super(csvFileNameOutSink);
	}

	@Override
	public void execute(Tuple input) {
		String msgId = input.getStringByField("MSGID");
		String source = input.getSourceComponent();
		String identifier = msgId + "," + source;

		try {
			ba.batchLogwriter(System.currentTimeMillis(), identifier);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
