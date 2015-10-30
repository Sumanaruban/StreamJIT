package edu.mit.streamjit.impl.distributed;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import edu.mit.streamjit.impl.common.Counter;
import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.impl.distributed.common.Utils;
import edu.mit.streamjit.tuner.EventTimeLogger;

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

	ThroughputPrinter(Counter counter, String appName, EventTimeLogger eLogger,
			String cfgPrefix) {
		this.counter = counter;
		this.appName = appName;
		this.eLogger = eLogger;
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
		scheduledExecutorService.scheduleAtFixedRate(
				new Runnable() {
					@Override
					public void run() {
						int currentCount = counter.count();
						long currentNano = System.nanoTime();
						int newOutputs = currentCount - lastCount;
						double throughput;
						if (newOutputs < 1) {
							throughput = 0;
							if (!isPrevNoOutput) {
								noOutputStartTime = lastNano;
								isPrevNoOutput = true;
							}
						} else {
							long duration = currentNano - lastNano;
							// number of items per second.
							throughput = (newOutputs * 1e9) / duration;
							if (isPrevNoOutput) {
								isPrevNoOutput = false;
								long noOutputPeriod = TimeUnit.MILLISECONDS
										.convert(currentNano
												- noOutputStartTime,
												TimeUnit.NANOSECONDS);
								if (noOutputPeriod > tolerablenoOutputPeriod)
									eLogger.logEvent("noOutput", noOutputPeriod);
							}
						}
						lastCount = currentCount;
						lastNano = currentNano;
						String msg = String.format("%d\t\t%d\t\t%.2f\n",
								rb.getUptime(), currentCount, throughput);
						try {
							writer.write(msg);
							// writer.flush();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}, Options.throughputMeasurementPeriod,
				Options.throughputMeasurementPeriod, TimeUnit.MILLISECONDS);
	}

	void stop() {
		if (scheduledExecutorService != null)
			scheduledExecutorService.shutdown();
		if (writer != null)
			try {
				writer.flush();
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
	}

	/**
	 * Creates/open a file with append == true. If the file created for the
	 * first time, writes the column headers.
	 * 
	 * @param cfgPrefix
	 */
	private void initWriter(String cfgPrefix) {
		String fileName = "throughput.txt";
		File f = new File(String.format("%s%s%s", appName, File.separator,
				fileName));
		boolean alreadyExists = f.exists();
		writer = Utils.fileWriter(appName, fileName, true);
		if (!alreadyExists) {
			String colHeader = "Time(ms)\t\tCurrentCount\t\tThroughput(items/s)\n";
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
}