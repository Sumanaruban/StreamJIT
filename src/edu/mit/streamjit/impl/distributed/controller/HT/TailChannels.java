package edu.mit.streamjit.impl.distributed.controller.HT;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Stopwatch;

import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.common.BufferWriteCounter;
import edu.mit.streamjit.impl.common.Counter;
import edu.mit.streamjit.impl.common.drainer.AbstractDrainer.DrainDataAction;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionProvider;
import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.impl.distributed.node.BlockingInputChannel;
import edu.mit.streamjit.tuner.EventTimeLogger;

public class TailChannels {

	private static class PerformanceLogger extends Thread {

		private AtomicBoolean stopFlag;

		private final String appName;

		private final TailChannel tailChannel;

		private PerformanceLogger(TailChannel tailChannel, String appName) {
			super("PerformanceLogger");
			stopFlag = new AtomicBoolean(false);
			this.appName = appName;
			this.tailChannel = tailChannel;
		}

		public void run() {
			int i = 0;
			FileWriter writer;
			try {
				writer = new FileWriter(String.format("%s%sFixedOutPut.txt",
						appName, File.separator));
			} catch (IOException e1) {
				e1.printStackTrace();
				return;
			}

			writeInitialInfo(writer);

			Long sum = 0l;

			while (++i < 10 && !stopFlag.get()) {
				try {
					Long time = tailChannel.getFixedOutputTime();

					sum += time;
					System.out.println("Execution time is " + time
							+ " milli seconds");

					writer.write(time.toString());
					writer.write('\n');
					writer.flush();
				} catch (InterruptedException | IOException e) {
					e.printStackTrace();
				}
			}
			try {
				writer.write("Average = " + sum / (i - 1));
				writer.write('\n');
				writer.flush();
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

			System.out.println("PerformanceLogger exits. App will run till "
					+ "inputdata exhausted.");
		}

		private void writeInitialInfo(FileWriter writer) {
			System.out.println(String.format(
					"PerformanceLogger starts to log the time to"
							+ " produce %d number of outputs",
					Options.outputCount));

			try {
				writer.write(String.format("GlobalConstants.outputCount = %d",
						Options.outputCount));
				writer.write('\n');
				writer.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void stopLogging() {
			stopFlag.set(true);
		}
	}

	private static abstract class AbstractBlockingTailChannel
			extends
				BlockingInputChannel implements TailChannel {

		protected final int skipCount;

		protected final int totalCount;

		private final Counter counter;

		/**
		 * {@link #releaseAndInitilize()} and {@link #reset()} methods set the
		 * {@link #count} value to 0. This variable copies the {@link #count}'s
		 * value just before resetting.
		 */
		private int countAtReset = 0;

		private final PerformanceLogger pLogger;

		private final ThroughputPrinter throughputPrinter;

		private final String cfgPrefix;

		protected final EventTimeLogger eLogger;

		protected abstract void releaseAndInitilize();

		/**
		 * @param buffer
		 * @param conProvider
		 * @param conInfo
		 * @param bufferTokenName
		 * @param debugLevel
		 * @param skipCount
		 *            : Skips this amount of output before evaluating the
		 *            running time. This is added to avoid the noise from init
		 *            schedule and the drain data. ( i.e., In order to get real
		 *            steady state execution time)
		 * @param steadyCount
		 *            : {@link #getFixedOutputTime()} calculates the time taken
		 *            to get this amount of outputs ( after skipping skipCount
		 *            number of outputs at the beginning).
		 */
		public AbstractBlockingTailChannel(BufferWriteCounter bwc,
				ConnectionProvider conProvider, ConnectionInfo conInfo,
				String bufferTokenName, int debugLevel, int skipCount,
				int steadyCount, String appName, String cfgPrefix,
				EventTimeLogger eLogger) {
			super(bwc, conProvider, conInfo, bufferTokenName, debugLevel);
			this.skipCount = skipCount;
			this.totalCount = steadyCount + skipCount;
			this.counter = bwc;
			countAtReset = 0;
			this.cfgPrefix = cfgPrefix;
			if (Options.run == 0) {
				// TODO: Leaks this object from the constructor. May cause
				// subtle bugs. Re-factor it.
				pLogger = new PerformanceLogger(this, appName);
				pLogger.start();
			} else
				pLogger = null;
			if (Options.throughputMeasurementPeriod > 0)
				throughputPrinter = new ThroughputPrinter(this, appName,
						eLogger, cfgPrefix);
			else
				throughputPrinter = null;
			this.eLogger = eLogger;
		}

		@Override
		public void stop(DrainDataAction drainDataAction) {
			super.stop(drainDataAction);
			if (pLogger != null) {
				releaseAndInitilize();
				pLogger.stopLogging();
			}
			if (throughputPrinter != null)
				throughputPrinter.stop();
		}

		@Override
		public int count() {
			// return count + countAtReset;
			return counter.count();
		}

		protected long normalizedTime(int count, long time) {
			if (count < skipCount)
				return -1;
			return (Options.outputCount * time) / (count - skipCount);
		}

		/**
		 * Opposite to the {@link #normalizedTime(long)}'s equation.
		 * <code>time=unnormalizedTime(normalizedTime(time))</code>
		 */
		protected long unnormalizedTime(long time) {
			return (long) (time * (((double) (totalCount - skipCount)) / Options.outputCount));
		}

		/**
		 * Logs the time reporting event.
		 * 
		 * TODO: This method is just for debugging purpose, Remove this method
		 * and its usage later.
		 */
		protected void reportingTime(long time) {
			if (throughputPrinter != null) {
				String msg = String.format(
						"Reporting-%s.cfg,time=%d,TotalCount=%d\n", cfgPrefix,
						time, countAfterReset());
				throughputPrinter.write(msg);
			}
		}

		protected int countAfterReset() {
			return counter.count() - countAtReset;
		}

		protected void countReset() {
			countAtReset = counter.count();
		}
	}

	public static final class BlockingTailChannel1
			extends
				AbstractBlockingTailChannel {

		private volatile CountDownLatch steadyLatch;

		private volatile CountDownLatch skipLatch;

		private boolean skipLatchUp;

		private boolean steadyLatchUp;

		/**
		 * @param buffer
		 * @param conProvider
		 * @param conInfo
		 * @param bufferTokenName
		 * @param debugLevel
		 *            For all above 5 parameters, see
		 *            {@link BlockingInputChannel#BlockingInputChannel(Buffer, ConnectionProvider, ConnectionInfo, String, int)}
		 * @param skipCount
		 *            : Skips this amount of output before evaluating the
		 *            running time. This is added to avoid the noise from init
		 *            schedule and the drain data. ( i.e., In order to get real
		 *            steady state execution time)
		 * @param steadyCount
		 *            : {@link #getFixedOutputTime()} calculates the time taken
		 *            to get this amount of outputs ( after skipping skipCount
		 *            number of outputs at the beginning).
		 */
		public BlockingTailChannel1(BufferWriteCounter bwc,
				ConnectionProvider conProvider, ConnectionInfo conInfo,
				String bufferTokenName, int debugLevel, int skipCount,
				int steadyCount, String appName, String cfgPrefix,
				EventTimeLogger eLogger) {
			super(bwc, conProvider, conInfo, bufferTokenName, debugLevel,
					skipCount, steadyCount, appName, cfgPrefix, eLogger);
			steadyLatch = new CountDownLatch(1);
			skipLatch = new CountDownLatch(1);
			this.skipLatchUp = true;
			this.steadyLatchUp = true;
		}

		@Override
		public void receiveData() {
			super.receiveData();
			if (count() == 1)
				eLogger.eEvent("initialization");

			if (skipLatchUp && countAfterReset() > skipCount) {
				skipLatch.countDown();
				skipLatchUp = false;
			}

			if (steadyLatchUp && countAfterReset() > totalCount) {
				steadyLatch.countDown();
				steadyLatchUp = false;
			}
		}

		/**
		 * Skips skipCount amount of output at the beginning and then calculates
		 * the time taken to get steadyCount amount of outputs. skipCount is
		 * added to avoid the noise from init schedule and the drain data. (
		 * i.e., In order to get real steady state execution time).
		 * 
		 * @return time in MILLISECONDS.
		 * @throws InterruptedException
		 */
		public long getFixedOutputTime() throws InterruptedException {
			releaseAndInitilize();
			skipLatch.await();
			Stopwatch stopwatch = Stopwatch.createStarted();
			steadyLatch.await();
			stopwatch.stop();
			long time = stopwatch.elapsed(TimeUnit.MILLISECONDS);
			int cnt = countAfterReset();
			reportingTime(time);
			return normalizedTime(cnt, time);
		}

		@Override
		public long getFixedOutputTime(long timeout)
				throws InterruptedException {
			if (timeout < 1)
				return getFixedOutputTime();

			timeout = unnormalizedTime(timeout);
			releaseAndInitilize();
			skipLatch.await();
			Stopwatch stopwatch = Stopwatch.createStarted();
			while (steadyLatch.getCount() > 0
					&& stopwatch.elapsed(TimeUnit.MILLISECONDS) < timeout) {
				Thread.sleep(100);
			}

			stopwatch.stop();
			long time = stopwatch.elapsed(TimeUnit.MILLISECONDS);
			int cnt = countAfterReset();
			reportingTime(time);
			return normalizedTime(cnt, time);
		}

		/**
		 * Releases all latches, and re-initializes the latches and counters.
		 */
		protected void releaseAndInitilize() {
			countReset();
			skipLatch.countDown();
			skipLatch = new CountDownLatch(1);
			skipLatchUp = true;
			steadyLatch.countDown();
			steadyLatch = new CountDownLatch(1);
			steadyLatchUp = true;
		}

		public void reset() {
			steadyLatch.countDown();
			skipLatch.countDown();
			countReset();
		}
	}

	public static final class BlockingTailChannel2
			extends
				AbstractBlockingTailChannel {

		private volatile CountDownLatch skipLatch;

		private boolean skipLatchUp;

		private final Stopwatch stopWatch;

		/**
		 * @param buffer
		 * @param conProvider
		 * @param conInfo
		 * @param bufferTokenName
		 * @param debugLevel
		 *            For all above 5 parameters, see
		 *            {@link BlockingInputChannel#BlockingInputChannel(Buffer, ConnectionProvider, ConnectionInfo, String, int)}
		 * @param skipCount
		 *            : Skips this amount of output before evaluating the
		 *            running time. This is added to avoid the noise from init
		 *            schedule and the drain data. ( i.e., In order to get real
		 *            steady state execution time)
		 * @param steadyCount
		 *            : {@link #getFixedOutputTime()} calculates the time taken
		 *            to get this amount of outputs ( after skipping skipCount
		 *            number of outputs at the beginning).
		 */
		public BlockingTailChannel2(BufferWriteCounter bwc,
				ConnectionProvider conProvider, ConnectionInfo conInfo,
				String bufferTokenName, int debugLevel, int skipCount,
				int steadyCount, String appName, String cfgPrefix,
				EventTimeLogger eLogger) {
			super(bwc, conProvider, conInfo, bufferTokenName, debugLevel,
					skipCount, steadyCount, appName, cfgPrefix, eLogger);
			stopWatch = Stopwatch.createUnstarted();
			skipLatch = new CountDownLatch(1);
			this.skipLatchUp = true;
		}

		@Override
		public void receiveData() {
			super.receiveData();
			if (count() == 1)
				eLogger.eEvent("initialization");

			if (skipLatchUp && countAfterReset() > skipCount) {
				skipLatch.countDown();
				skipLatchUp = false;
			}

			if (stopWatch.isRunning() && countAfterReset() > totalCount) {
				stopWatch.stop();
			}
		}

		/**
		 * Skips skipCount amount of output at the beginning and then calculates
		 * the time taken to get steadyCount amount of outputs. skipCount is
		 * added to avoid the noise from init schedule and the drain data. (
		 * i.e., In order to get real steady state execution time).
		 * 
		 * @return time in MILLISECONDS.
		 * @throws InterruptedException
		 */
		public long getFixedOutputTime() throws InterruptedException {
			releaseAndInitilize();
			skipLatch.await();
			stopWatch.start();
			while (stopWatch.isRunning())
				Thread.sleep(250);
			long time = stopWatch.elapsed(TimeUnit.MILLISECONDS);
			int cnt = countAfterReset();
			reportingTime(time);
			return normalizedTime(cnt, time);
		}

		@Override
		public long getFixedOutputTime(long timeout)
				throws InterruptedException {
			if (timeout < 1)
				return getFixedOutputTime();

			timeout = unnormalizedTime(timeout);
			releaseAndInitilize();
			skipLatch.await();
			stopWatch.start();
			while (stopWatch.isRunning()
					&& stopWatch.elapsed(TimeUnit.MILLISECONDS) < timeout) {
				Thread.sleep(250);
			}

			long time = stopWatch.elapsed(TimeUnit.MILLISECONDS);
			int cnt = countAfterReset();
			reportingTime(time);
			return normalizedTime(cnt, time);
		}

		/**
		 * Releases all latches, and re-initializes the latches and counters.
		 */
		protected void releaseAndInitilize() {
			countReset();
			skipLatch.countDown();
			skipLatch = new CountDownLatch(1);
			skipLatchUp = true;
			stopWatch.reset();
		}

		public void reset() {
			stopWatch.reset();
			skipLatch.countDown();
			countReset();
		}
	}

	public static final class BlockingTailChannel3
			extends
				AbstractBlockingTailChannel {

		private final Stopwatch stopWatch;

		private final long skipMills = Options.skipMills;

		private final long steadyMills = Options.steadyMills;

		/**
		 * If no output for a given period, reject the configuration.
		 */
		private final int noOutputTimeLimit = Options.noOutputTimeLimit;

		public BlockingTailChannel3(BufferWriteCounter bwc,
				ConnectionProvider conProvider, ConnectionInfo conInfo,
				String bufferTokenName, int debugLevel, int skipCount,
				int steadyCount, String appName, String cfgPrefix,
				EventTimeLogger eLogger) {
			super(bwc, conProvider, conInfo, bufferTokenName, debugLevel,
					skipCount, steadyCount, appName, cfgPrefix, eLogger);
			stopWatch = Stopwatch.createUnstarted();
		}

		@Override
		public void receiveData() {
			super.receiveData();
			if (count() == 1)
				eLogger.eEvent("initialization");
		}

		@Override
		public long getFixedOutputTime() throws InterruptedException {
			releaseAndInitilize();
			if (!waitForFirstOutput())
				return -1;
			stopWatch.start();
			while (stopWatch.elapsed(TimeUnit.MILLISECONDS) < skipMills)
				Thread.sleep(1);
			int startCount = countAfterReset();
			while (stopWatch.elapsed(TimeUnit.MILLISECONDS) < (skipMills + steadyMills))
				Thread.sleep(1);
			int endCount = countAfterReset();
			System.err.println(String.format("startCount=%d, endCount=%d",
					startCount, endCount));
			long time = fixedtime(endCount - startCount);
			reportingTime(time);
			return time;
		}

		private boolean waitForFirstOutput() throws InterruptedException {
			Stopwatch sw = Stopwatch.createStarted();
			while (countAfterReset() < 1
					&& sw.elapsed(TimeUnit.MILLISECONDS) < noOutputTimeLimit)
				Thread.sleep(1);
			return countAfterReset() > 0;
		}

		public long fixedtime(int cnt) {
			if (cnt < 1)
				return -1;
			int steadyCount = totalCount - skipCount;
			long time = (steadyMills * steadyCount) / cnt;
			return time;
		}

		@Override
		public long getFixedOutputTime(long timeout)
				throws InterruptedException {
			return getFixedOutputTime();
		}

		@Override
		public void reset() {
			stopWatch.reset();
			countReset();
		}

		@Override
		protected void releaseAndInitilize() {
			countReset();
			stopWatch.reset();
		}
	}
}