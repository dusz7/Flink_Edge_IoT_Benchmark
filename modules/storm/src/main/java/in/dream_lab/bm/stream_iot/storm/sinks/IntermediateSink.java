package in.dream_lab.bm.stream_iot.storm.sinks;

import org.apache.storm.tuple.Tuple;

public class IntermediateSink extends Sink {

	public IntermediateSink(String csvFileNameOutSink) {
		super(csvFileNameOutSink);
	}

	@Override
	public void execute(Tuple input) {
		String msgId = input.getStringByField("MSGID");
		String ts = input.getStringByField("TIMESTAMP");
		String source = input.getSourceComponent();
		String identifier = msgId + "," + source;

		try {
			getBa().batchLogwriter(Long.parseLong(ts), identifier);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
