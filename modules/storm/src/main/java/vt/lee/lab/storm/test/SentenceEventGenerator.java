package vt.lee.lab.storm.test;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SentenceEventGenerator {

	private ISyntheticSentenceGenerator issg;
	ExecutorService executorService;
	private int rate = 0;

	public SentenceEventGenerator(ISyntheticSentenceGenerator issg, int rate) {
		this.issg = issg;
		this.rate = rate;
	}

	public void launch(String[] sentences, long experimentDuration) {
		int numThreads = 1;
		this.executorService = Executors.newFixedThreadPool(numThreads);

		SentenceGenerator sentenceGenerator = new SentenceGenerator(this.issg, sentences, this.rate,
				experimentDuration);
		this.executorService.execute(sentenceGenerator);
	}

}

class SentenceGenerator implements Runnable {

	ISyntheticSentenceGenerator issg;
	Random _rand;
	String[] events;
	double delay = 0;
	long experimentDuration;
	long experiStartTime;

	public SentenceGenerator(ISyntheticSentenceGenerator issg, String[] events, int rate, long experimentDuration) {
		_rand = new Random();
		this.issg = issg;
		this.experimentDuration = experimentDuration;
		this.experiStartTime = System.currentTimeMillis();
		this.events = events;
		if (rate != 0) {
			this.delay = (1 / (double) rate) * 1000000000; /* delay in ns */
			System.out.println(Thread.currentThread().getName() + "Delay: " + this.delay);
		}
	}

	@Override
	public void run() {
		System.out.println("Running SentenceGenerator");
		long currentRuntime = 0;

		System.out.println("Going to generate events");
		System.out.println(this.delay);
		System.out.println(experimentDuration);
		System.out.println(experiStartTime);
		System.out.println(events);
		System.out.println(currentRuntime);

		do {
			System.out.println("SentenceGenerator: Start");
			System.out.println(currentRuntime);
			String event = this.events[_rand.nextInt(this.events.length)];
			System.out.println(event);
			Long currentTs = System.currentTimeMillis();
			System.out.println(currentTs);

			System.out.println("Adding sentence to spout's queue");
			this.issg.receive(event);

			long start = System.nanoTime();
			while (start + delay >= System.nanoTime())
				;

			currentRuntime = (long) ((currentTs - experiStartTime) + (delay / 1000000));

		} while (currentRuntime < experimentDuration);

	}

}
