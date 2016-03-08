package edu.mit.streamjit.impl.distributed.controller.HT;

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
		this.tpStatistics = new TPStatisticsImpl(appName);
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
}