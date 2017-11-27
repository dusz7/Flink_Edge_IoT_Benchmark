package in.dream_lab.bm.stream_iot.storm.spouts;

import java.util.TimerTask;

public class BpTimeIntervalMonitoringTask extends TimerTask {
	static int counter = 0;
	double threshold = 5.0;
	long window;

	BpTime bptime;

	public BpTimeIntervalMonitoringTask(BpTime bptime, long window, double threshold) {
		this.bptime = bptime;
		this.threshold = threshold;
		this.window = window;
	}

	@Override
	public void run() {
		// Case 1: BP continued from previous window to current. (bPActive =
		// true)
		// if backpressure is active at the start of this window, take diff of
		// bpstart with current as the time bp was active during
		// this window, if greater than T, update totalBP time. and set bpStart
		// time to current time.

		System.out.println("BP WINDOW:");
		if (bptime.bpActive) {
			System.out.println("BP ACTIVE");
			long bpActiveTime = System.currentTimeMillis() - bptime.bpStartTime + bptime.bpCurrAccTime;
			double pAge = (bpActiveTime / this.window) * 100;

			if (pAge >= threshold) {
				System.out.println("Active %age = " + pAge);

				bptime.bpTotalAccTime += bpActiveTime;
				bptime.bpCurrAccTime = 0;
				bptime.bpStartTime = System.currentTimeMillis();
			}
			System.out.println(bptime);
		} else {
			System.out.println("BP NOT ACTIVE");
			long bpActiveTime = bptime.bpCurrAccTime;
			double pAge = (bpActiveTime / this.window) * 100;

			if (pAge >= threshold) {
				System.out.println("Active %age = " + pAge);
				bptime.bpTotalAccTime += bpActiveTime;
				bptime.bpCurrAccTime = 0;
			}
			System.out.println(bptime);
		}
		// Case 2: BP started and stopped during this window. (bpActive = false)
		// in this case, bpstart will be zero.
	}
}
