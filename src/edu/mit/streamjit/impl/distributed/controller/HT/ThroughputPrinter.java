package edu.mit.streamjit.impl.distributed.controller.HT;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import edu.mit.streamjit.impl.common.Counter;
import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.impl.distributed.common.Utils;
import edu.mit.streamjit.impl.distributed.controller.AppInstance;
import edu.mit.streamjit.tuner.EventTimeLogger;
import edu.mit.streamjit.util.Pair;

/**
 * Periodically prints the number of outputs received by a {@link Counter}.
 */
public class ThroughputPrinter {

	private final String appName;

	private final Counter counter;

	/**
	 * The no of outputs received at the end of last period.
	 */
	private int lastCount;

	/**
	 * {@link ScheduledExecutorService#scheduleAtFixedRate(Runnable, long, long, TimeUnit)}
	 * in not guaranteed to scheduled at fixed rate. We have to use the last
	 * called time for more accurate throughput calculation.
	 */
	private long lastNano;

	private boolean isPrevNoOutput;
	private long noOutputStartTime;

	/**
	 * If noOutput period is lesser than this value, the event won't get logged
	 * to {@link #eLogger}. Without this, {@link #eLogger} gets filled with
	 * several but tiny noOutput logs. This value is in milliseconds.
	 */
	private final int tolerablenoOutputPeriod = 500;

	private final EventTimeLogger eLogger;

	private FileWriter writer;

	private ScheduledExecutorService scheduledExecutorService;

	private RuntimeMXBean rb = ManagementFactory.getRuntimeMXBean();

	private final String fileName;

	/**
	 * If the {@link Options#throughputMeasurementPeriod} is in second range, We
	 * write the time values in second format in the time column of the output
	 * file, instead of milliseconds. This will make the throughput graph
	 * generation easier.
	 */
	private final boolean isTimeInSeconds;

	public final TPStatistics tpStatistics;

	ThroughputPrinter(Counter counter, String appName, EventTimeLogger eLogger,
			String cfgPrefix) {
		this(counter, appName, eLogger, cfgPrefix, "throughput.txt");
	}

	public ThroughputPrinter(Counter counter, String appName,
			EventTimeLogger eLogger, String cfgPrefix, String fileName) {
		this.counter = counter;
		this.appName = appName;
		this.eLogger = eLogger;
		this.fileName = fileName;
		this.isTimeInSeconds = Options.throughputMeasurementPeriod >= 1000;
		this.tpStatistics = new TPStatistics(appName);
		printThroughput(cfgPrefix);
	}

	private void printThroughput(String cfgPrefix) {
		if (Options.throughputMeasurementPeriod < 1)
			return;
		initWriter(cfgPrefix);
		lastCount = 0;
		lastNano = System.nanoTime();
		noOutputStartTime = lastNano;
		isPrevNoOutput = false;
		scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
		scheduledExecutorService.scheduleAtFixedRate(runnable(),
				Options.throughputMeasurementPeriod,
				Options.throughputMeasurementPeriod, TimeUnit.MILLISECONDS);
	}

	private Runnable runnable() {
		return new Runnable() {
			@Override
			public void run() {
				int currentCount = counter.count();
				long currentNano = System.nanoTime();
				int newOutputs = currentCount - lastCount;
				double throughput;
				if (newOutputs < 1) {
					throughput = 0;
					noOutput();
				} else {
					long duration = currentNano - lastNano;
					// number of items per second.
					throughput = (newOutputs * 1e9) / duration;
					if (isPrevNoOutput)
						outputReceived(currentNano);
				}
				lastCount = currentCount;
				lastNano = currentNano;
				newThroughput(currentCount, throughput);
				tpStatistics.newThroughput(throughput);

			}
		};
	}

	private void newThroughput(int currentCount, double throughput) {
		long upTime = rb.getUptime();
		if (isTimeInSeconds)
			upTime = upTime / 1000;
		String msg = String.format("%d\t\t%d\t\t%.2f\n", upTime, currentCount,
				throughput);
		try {
			writer.write(msg);
			// writer.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void outputReceived(long currentNano) {
		isPrevNoOutput = false;
		long noOutputPeriod = TimeUnit.MILLISECONDS.convert(currentNano
				- noOutputStartTime, TimeUnit.NANOSECONDS);
		if (eLogger != null && noOutputPeriod > tolerablenoOutputPeriod)
			eLogger.logEvent("noOutput", noOutputPeriod);
	}

	private void noOutput() {
		if (!isPrevNoOutput) {
			noOutputStartTime = lastNano;
			isPrevNoOutput = true;
		}
	}

	public void stop() {
		if (scheduledExecutorService != null)
			scheduledExecutorService.shutdown();
		if (writer != null)
			try {
				writer.flush();
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		tpStatistics.stop();
	}

	/**
	 * Creates/open a file with append == true. If the file created for the
	 * first time, writes the column headers.
	 * 
	 * @param cfgPrefix
	 */
	private void initWriter(String cfgPrefix) {
		File f = new File(String.format("%s%s%s", appName, File.separator,
				fileName));
		boolean alreadyExists = f.exists();
		writer = Utils.fileWriter(appName, fileName, true);
		if (!alreadyExists) {
			String timeUnit = isTimeInSeconds ? "s" : "ms";
			String colHeader = String.format(
					"Time(%s)\t\tCurrentCount\t\tThroughput(items/s)\n",
					timeUnit);
			write(colHeader);
		}
		write(String.format(
				"----------------------------%s----------------------------\n",
				cfgPrefix));
	}

	/**
	 * This method writes to the file in a non thread safe way. But this is
	 * enough to serve the purpose.
	 * <P>
	 * TODO: This method is just for debugging purpose, Remove this method and
	 * its usage later.
	 */
	boolean write(String msg) {
		if (writer != null)
			try {
				writer.write(msg);
				return true;
			} catch (Exception e) {
			}
		return false;
	}

	/**
	 * @author sumanan
	 * @since 7 Mar, 2016
	 */
	public static class TPStatistics {

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
				write("cfg-Configuration, Avg:-Non-overlap average throughput\n");
				write("O-Avg:-Overlap runs average throughput, Dur:-Avg measured duration\n");
				write("Detected drop is written in (dropAvg,dropPercentage,dropDuration) format\n");
				write("Cfg\t\tAvg\t\tDur\t\tO-Avg\t\tDur\n");
			}
			write("----------------------------------------------------------\n");
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
			} else if (isDropStarted) {
				dropFinished();
			}
		}

		private void dropFinished() {
			isDropStarted = false;
			dropEndIdx = cb.tail;
			Pair<Double, Integer> p = averageTP(dropStartIdx, dropEndIdx);
			double dropAvg = p.first;
			double dropPercentage = 100 - 100 * dropAvg / startAvg;
			Drop d = new Drop(startAvg, dropAvg, dropPercentage, p.second);
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

			if (appInstCount == 1) {
				write(String.format("\n%d\t\t%.2f\t\t%d\t\t%.2f\t\t%d\n",
						appInstId, startAvg, startAvgDur, p.first, p.second));
			} else
				write(String.format("\n%d\t\t%.2f\t\t%d\n", appInstId, p.first,
						p.second));
		}

		private Pair<Double, Integer> averageTP(int start, int end) {
			double tot = 0.0;
			double avg = 0.0;
			Integer dur = 0;

			if (start < end) {
				dur = (end - start);
				for (int i = start; i < end; i++)
					tot += cb.data[i];
				avg = tot / dur;
			} else {
				dur = (cb.data.length + end - start);
				for (int i = 0; i < end; i++)
					tot += cb.data[i];
				for (int i = start; i < cb.data.length; i++)
					tot += cb.data[i];
				avg = tot / dur;
			}
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
			write("\n-----------------------------------------\n");
			write(String.format("Average Drop Duration = %.2f\n",
					avgDropDuration));
			write(String.format("Average Work Lost = %.2f\n", avgWorkLost));
			write(String.format("Average Drop Percentage = %.2f\n",
					avgDropPercentage));
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
	}

	/**
	 * Copied from http://www.java2s.com/Tutorial/Java/
	 * 0140__Collections/CircularBuffer.htm.
	 * 
	 * @author sumanan
	 * @since 7 Mar, 2016
	 */
	static class CircularBuffer {
		private double data[];
		private volatile int head;
		private volatile int tail;

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
		Drop(double startAvg, double dropAvg, double dropPercentage,
				int dropDuration) {
			this.startAvg = startAvg;
			this.dropAvg = dropAvg;
			this.dropPercentage = dropPercentage;
			this.dropDuration = dropDuration;
		}

		public String toString() {
			return String.format("(%.2f,%.2f,%d)", dropAvg, dropPercentage,
					dropDuration);
		}
	}
}