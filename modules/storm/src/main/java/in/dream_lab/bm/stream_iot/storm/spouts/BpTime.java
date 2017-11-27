package in.dream_lab.bm.stream_iot.storm.spouts;

public class BpTime {
	long bpStartTime = 0;
	long bpCurrAccTime = 0;
	long bpTotalAccTime = 0;
	boolean bpActive = false;

	public void updateBpCurrAccTime() {
		System.out.println("BP Falling edge observed. Updating BP Current Time. Start time = " + this.bpStartTime
				+ ", Now current time = " + System.currentTimeMillis());

		this.bpCurrAccTime = this.bpCurrAccTime + (System.currentTimeMillis() - this.bpStartTime);
		this.bpActive = false;

		System.out.println("bp curr time = " + this.bpCurrAccTime);
	}

	/* setters */
	public void setBpStartTime(long bpStartTime) {
		this.bpStartTime = bpStartTime;
		this.bpActive = true;
		System.out.println("BP start time = " + this.bpStartTime);
	}

	public void setBpCurrAccTime(long bpCurrAccTime) {
		this.bpCurrAccTime = bpCurrAccTime;
	}

	public void setBpTotalAccTime(long bpTotalAccTime) {
		this.bpTotalAccTime = bpTotalAccTime;
	}

	/* getters */
	public long getBpTotalAccTime() {
		return bpTotalAccTime;
	}

	public long getBpCurrAccTime() {
		return bpCurrAccTime;
	}

	public long getBpStartTime() {
		return bpStartTime;
	}

	public String toString() {
		return this.bpStartTime + ", " + this.bpCurrAccTime + ", " + this.bpTotalAccTime;
	}
}
