package edu.mit.streamjit.impl.distributed.controller.HT;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.impl.distributed.common.Utils;
import edu.mit.streamjit.impl.distributed.controller.AppInstance;
import edu.mit.streamjit.util.Pair;

/**
 * @author sumanan
 * @since 7 Mar, 2016
 */
public class TPStatistics {

	// Lets initialize to keep throughput for 1000s.
	int initSize = 1000_000 / Options.throughputMeasurementPeriod;

	CircularBuffer cb = new CircularBuffer(initSize);

	private int startIdx = 0;
	private int endIdx = 0;
	private volatile double startAvg = 0.0;
	private int startAvgDur = 0;

	private boolean isDropStarted = false;
	private int dropStartIdx = 0;
	private int dropEndIdx = 0;

	private final FileWriter writer;

	final List<Drop> drops = new ArrayList<Drop>(Options.tuningRounds * 3);
	int totalReconfigs = 0;

	private final AtomicInteger zeroTP = new AtomicInteger(0);

	private int totalzeroTP = 0;

	/**
	 * No of running {@link AppInstance}s.
	 */
	private int appInstCount = 0;

	TPStatistics(String appName) {
		String fileName = "TPStatistics.txt";
		File f = new File(String.format("%s%s%s", appName, File.separator,
				fileName));
		boolean alreadyExists = f.exists();
		writer = Utils.fileWriter(appName, fileName, true);
		if (!alreadyExists) {
			write("####################################################################\n");
			write("cfg-Configuration, Avg:-Non-overlap average throughput, DT:- Down Time\n");
			write("O-Avg:-Overlap runs average throughput, Dur:-Avg measured duration\n");
			write("Detected drop is written in (dropAvg,dropPercentage,dropDuration) format\n");
			write("####################################################################\n\n");
			write("Cfg\t\tAvg\t\tDur\t\tO-Avg\t\tDur\t\tDT\n");
		}
		write("--------------------------------------------------------------\n");
	}

	void newThroughput(double throughput) {
		cb.store(throughput);
		checkDrop(throughput);
	}

	private void checkDrop(double throughput) {
		if (appInstCount == 2) {
			if (!isDropStarted && throughput < 0.8 * startAvg) {
				isDropStarted = true;
				dropStartIdx = cb.tail;
			} else if (isDropStarted && throughput > 0.8 * startAvg) {
				dropFinished();
			}
			if (throughput == 0)
				zeroTP.incrementAndGet();
		} else if (isDropStarted) {
			dropFinished();
		}
	}

	private void dropFinished() {
		isDropStarted = false;
		dropEndIdx = cb.tail;
		Pair<Double, Integer> p = averageTP(dropStartIdx, dropEndIdx);
		double dropAvg = p.first;
		Drop d = new Drop(startAvg, dropAvg, p.second);
		drops.add(d);
		String msg = d.toString();
		write(msg + "\t");
		System.out.println(msg);
	}

	public void cfgStarted(int appInstId) {
		totalReconfigs++;
		appInstCount++;
		startIdx = cb.tail;
		Pair<Double, Integer> p = averageTP(endIdx, startIdx);
		startAvg = p.first;
		startAvgDur = p.second;
	}

	public void cfgEnded(int appInstId) {
		appInstCount--;
		endIdx = cb.tail;
		Pair<Double, Integer> p = averageTP(startIdx, endIdx);
		int z = zeroTP.get();
		int downTime = z * Options.throughputMeasurementPeriod;
		totalzeroTP += z;
		zeroTP.set(0);

		if (appInstCount == 1) {
			write(String.format("\n%d\t\t%.2f\t\t%d\t\t%.2f\t\t%d\t\t%d\n",
					appInstId, startAvg, startAvgDur, p.first, p.second,
					downTime));
		} else
			write(String.format("\n%d\t\t%.2f\t\t%d\t\t\t\t\t\t%d\n",
					appInstId, p.first, p.second, downTime));
	}

	private Pair<Double, Integer> averageTP(int start, int end) {
		double tot = 0.0;
		double avg = 0.0;
		int dur = 0;

		if (start < end) {
			dur = (end - start) * Options.throughputMeasurementPeriod;
			for (int i = start; i < end; i++)
				tot += cb.data[i];
		} else {
			dur = (cb.data.length + end - start)
					* Options.throughputMeasurementPeriod;
			for (int i = 0; i < end; i++)
				tot += cb.data[i];
			for (int i = start; i < cb.data.length; i++)
				tot += cb.data[i];
		}
		avg = 1000 * tot / dur;
		cb.head = end;
		return new Pair<Double, Integer>(avg, dur);
	}

	private void totalStats() {
		double totDrop = 0;
		int totDropDuration = 0;
		double workLost = 0;
		for (Drop d : drops) {
			totDropDuration += d.dropDuration;
			totDrop += d.dropPercentage * d.dropDuration;
			workLost += ((d.startAvg - d.dropAvg) * d.dropDuration)
					/ d.startAvg;
		}
		double avgDropPercentage = totDrop / totDropDuration;
		double avgDropDuration = totDropDuration / totalReconfigs;
		double avgWorkLost = workLost / totalReconfigs;
		double avgDownTime = totalzeroTP * Options.throughputMeasurementPeriod
				/ totalReconfigs;
		write("\n-----------------------------------------\n");
		write(String
				.format("Average Drop Duration = %.2fms\n", avgDropDuration));
		write(String.format("Average Work Lost = %.2fms\n", avgWorkLost));
		write(String.format("Average Drop Percentage = %.2f\n",
				avgDropPercentage));
		write(String.format("Average Down Time = %.2fms\n", avgDownTime));
	}

	boolean write(String msg) {
		if (writer != null)
			try {
				writer.write(msg);
				return true;
			} catch (Exception e) {
			}
		return false;
	}

	void stop() {
		totalStats();
		if (writer != null)
			try {
				writer.flush();
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
	}

	/**
	 * Copied from http://www.java2s.com/Tutorial/Java/
	 * 0140__Collections/CircularBuffer.htm.
	 * 
	 * @author sumanan
	 * @since 7 Mar, 2016
	 */
	private static class CircularBuffer {
		double data[];
		volatile int head;
		volatile int tail;

		public CircularBuffer(int number) {
			data = new double[number];
			head = 0;
			tail = 0;
		}

		public boolean store(Double value) {
			if (!bufferFull()) {
				data[tail++] = value;
				if (tail == data.length) {
					tail = 0;
				}
				return true;
			} else {
				new IllegalStateException("Buffer is full").printStackTrace();
				return false;
			}
		}

		private boolean bufferFull() {
			if (tail + 1 == head) {
				return true;
			}
			if (tail == (data.length - 1) && head == 0) {
				return true;
			}
			return false;
		}
	}

	static class Drop {
		final double startAvg;
		final double dropAvg;
		final double dropPercentage;
		final int dropDuration;
		Drop(double startAvg, double dropAvg, int dropDuration) {
			this.startAvg = startAvg;
			this.dropAvg = dropAvg;
			this.dropDuration = dropDuration;
			this.dropPercentage = 100 - 100 * dropAvg / startAvg;
		}

		public String toString() {
			return String.format("(%.2f,%.2f,%d)", dropAvg, dropPercentage,
					dropDuration);
		}
	}
}