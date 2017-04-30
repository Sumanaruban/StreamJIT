package edu.mit.streamjit.util;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import edu.mit.streamjit.impl.distributed.common.Utils;

/**
 * Logs the time taken for each event for debugging and documentation purpose.
 * 
 * @author sumanan
 * @since 6 Mar, 2015
 */
public interface EventTimeLogger {

	/**
	 * Call this method at the beginning of an event.
	 * 
	 * @param eventName
	 */
	void bEvent(String eventName);

	/**
	 * Call this method at the end of an event. Measures and logs the elapsed
	 * time between {@link #bEvent(String)} call and this method call.
	 * 
	 * @param eventName
	 * @return elapsed time in milliseconds.
	 */
	long eEvent(String eventName);

	void bTuningRound(String cfgPrefix);
	void eTuningRound(String cfgPrefix);

	/**
	 * This method can be used to log extra messages. A newline must be added at
	 * the end of the @param message.
	 * 
	 * @param message
	 */
	void log(String message);

	/**
	 * Classes which measure the timings themselves may use this method to log
	 * the event time.
	 * 
	 * @param eventName
	 * @param elapsedMills
	 */
	void logEvent(String eventName, long elapsedMills);

	/**
	 * Logs nothing.
	 */
	public static class NoEventTimeLogger implements EventTimeLogger {

		@Override
		public void bEvent(String eventName) {
		}

		@Override
		public long eEvent(String eventName) {
			return 0;
		}

		@Override
		public void bTuningRound(String cfgPrefix) {
		}

		@Override
		public void eTuningRound(String cfgPrefix) {
		}

		@Override
		public void log(String message) {
		}

		@Override
		public void logEvent(String eventName, long elapsedMills) {
		}
	}

	public static class EventTimeLoggerImpl implements EventTimeLogger {

		private final OutputStreamWriter osWriter;
		private final Map<String, Event> events;
		final Ticker ticker;

		/**
		 * If this flag is <code>true</code>, {@link EventTimeLogger} logs the
		 * system's upTime in seconds; logs in milliseconds otherwise.
		 */
		private final boolean reportInSeconds;

		private RuntimeMXBean rb = ManagementFactory.getRuntimeMXBean();

		/**
		 * @param os
		 * @param needSynchronized
		 *            - Pass true if this EventTimeLoggerImpl object is going to
		 *            be used by multiple threads. EventTimeLoggerImpl use
		 *            {@link ConcurrentHashMap} to store events in true case;
		 *            uses {@link HashMap} otherwise.
		 * @param reportInSeconds
		 *            If this flag is <code>true</code>, {@link EventTimeLogger}
		 *            logs the system's upTime in seconds; logs in milliseconds
		 *            otherwise.
		 */
		public EventTimeLoggerImpl(OutputStream os, boolean needSynchronized,
				boolean reportInSeconds) {
			this(getOSWriter(os), needSynchronized, reportInSeconds);
		}

		/**
		 * 
		 * @param osWriter
		 * @param needSynchronized
		 *            - Pass true if this EventTimeLoggerImpl object is going to
		 *            be used by multiple threads. EventTimeLoggerImpl use
		 *            {@link ConcurrentHashMap} to store events in true case;
		 *            uses {@link HashMap} otherwise.
		 * @param reportInSeconds
		 *            If this flag is <code>true</code>, {@link EventTimeLogger}
		 *            logs the system's upTime in seconds; logs in milliseconds
		 *            otherwise.
		 */
		public EventTimeLoggerImpl(OutputStreamWriter osWriter,
				boolean needSynchronized, boolean reportInSeconds) {
			this.osWriter = osWriter;
			this.reportInSeconds = reportInSeconds;
			if (needSynchronized)
				this.events = new ConcurrentHashMap<>();
			else
				this.events = new HashMap<>();
			if (reportInSeconds)
				ticker = new MilliTicker();
			else
				ticker = new NanoTicker();
			write("Event\t\t\tUptime\t\telapsedtime\n");
			write("====================================================\n");
		}

		@Override
		public void bEvent(String eventName) {
			long time = ticker.time();
			if (events.containsKey(eventName)) {
				// new IllegalStateException(String.format(
				// "Event %s has already been started", eventName))
				// .printStackTrace();
			}
			Event e = new Event(eventName);
			e.startTime = time;
			events.put(eventName, e);
		}

		@Override
		public long eEvent(String eventName) {
			long time = ticker.time();
			Event e = events.remove(eventName);
			if (e == null) {
				new IllegalStateException(String.format(
						"Event %s has not started yet", eventName))
						.printStackTrace();
				return 0;
			}
			e.endTime = time;
			return log(e);
		}

		@Override
		public void bTuningRound(String cfgPrefix) {
			bEvent("tuningRound-" + cfgPrefix);
			write(String
					.format("----------------------------%s----------------------------\n",
							cfgPrefix));
		}

		@Override
		public void eTuningRound(String cfgPrefix) {
			eEvent("tuningRound-" + cfgPrefix);
		}

		private long log(Event e) {
			long uptime = rb.getUptime();
			long elapsedMills = TimeUnit.MILLISECONDS.convert(e.endTime
					- e.startTime, ticker.timeUnit);
			write(e.name, uptime, elapsedMills);
			return elapsedMills;
		}

		private void write(String eventName, long uptime, long elapsedMills) {
			if (reportInSeconds)
				uptime = uptime / 1000;
			write(String.format("%-22s\t%-12d\t%d\n", eventName, uptime,
					elapsedMills));
		}

		private void write(String msg) {
			if (osWriter != null)
				try {
					osWriter.write(msg);
					osWriter.flush();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}

		private static OutputStreamWriter getOSWriter(OutputStream os) {
			if (os == null)
				return null;
			return new OutputStreamWriter(os);
		}

		@Override
		public void log(String message) {
			write(message);
		}

		@Override
		public void logEvent(String eventName, long elapsedMills) {
			write(eventName, rb.getUptime(), elapsedMills);
		}
	}

	/**
	 * Writes the event time info in to appName/eventTime_[fileNameSuffix].txt
	 * file, where appName and fileNameSuffix are constructor arguments.
	 * 
	 */
	public static class FileEventTimeLogger extends EventTimeLoggerImpl {
		/**
		 * @param appName
		 * @param fileNameSuffix
		 * @param needSynchronized
		 *            - Pass true if this EventTimeLoggerImpl object is going to
		 *            be used by multiple threads. EventTimeLoggerImpl use
		 *            {@link ConcurrentHashMap} to store events in true case;
		 *            uses {@link HashMap} otherwise.
		 * @param reportInSeconds
		 *            If this flag is <code>true</code>, {@link EventTimeLogger}
		 *            logs the system's upTime in seconds; logs in milliseconds
		 *            otherwise.
		 */
		public FileEventTimeLogger(String appName, String fileNameSuffix,
				boolean needSynchronized, boolean reportInSeconds) {
			super(Utils.fileWriter(appName,
					String.format("eventTime_%s.txt", fileNameSuffix)),
					needSynchronized, reportInSeconds);
		}
	}

	/**
	 * Prints the event time info to the standard out.
	 */
	public static class PrintEventTimeLogger extends EventTimeLoggerImpl {
		/**
		 * @param needSynchronized
		 *            - Pass true if this EventTimeLoggerImpl object is going to
		 *            be used by multiple threads. EventTimeLoggerImpl use
		 *            {@link ConcurrentHashMap} to store events in true case;
		 *            uses {@link HashMap} otherwise.
		 * @param reportInSeconds
		 *            If this flag is <code>true</code>, {@link EventTimeLogger}
		 *            logs the system's upTime in seconds; logs in milliseconds
		 *            otherwise.
		 */
		public PrintEventTimeLogger(boolean needSynchronized,
				boolean reportInSeconds) {
			super(System.out, needSynchronized, reportInSeconds);
		}
	}

	/**
	 * This is just a decorator class of EventTimeLogger to log the events with
	 * a prefix.
	 * 
	 * @author sumanan
	 * @since 9 Dec, 2015
	 */
	public static class PrefixedEventTimeLogger implements EventTimeLogger {
		final EventTimeLogger eLogger;
		final String prefix;
		public PrefixedEventTimeLogger(EventTimeLogger eLogger, String prefix) {
			this.eLogger = eLogger;
			this.prefix = prefix;
		}

		public void bEvent(String eventName) {
			eLogger.bEvent(eventName(eventName));
		}

		public long eEvent(String eventName) {
			return eLogger.eEvent(eventName(eventName));
		}

		public void logEvent(String eventName, long elapsedMills) {
			eLogger.logEvent(eventName(eventName), elapsedMills);
		}

		private String eventName(String eventName) {
			return prefix + "-" + eventName;
		}

		@Override
		public void bTuningRound(String cfgPrefix) {
			eLogger.bTuningRound(cfgPrefix);
		}

		@Override
		public void eTuningRound(String cfgPrefix) {
			eLogger.eTuningRound(cfgPrefix);
		}

		@Override
		public void log(String message) {
			eLogger.log(message);
		}
	}

	static class Event {

		private final String name;

		long startTime;

		long endTime;

		Event(String name) {
			this.name = name;
		}
	}

	abstract class Ticker {
		final TimeUnit timeUnit;
		Ticker(TimeUnit timeUnit) {
			this.timeUnit = timeUnit;
		}

		abstract long time();
	}

	final class MilliTicker extends Ticker {

		MilliTicker() {
			super(TimeUnit.MILLISECONDS);
		}

		@Override
		long time() {
			return System.currentTimeMillis();
		}
	}

	final class NanoTicker extends Ticker {

		NanoTicker() {
			super(TimeUnit.NANOSECONDS);
		}

		@Override
		long time() {
			return System.nanoTime();
		}
	}
}
