package edu.mit.streamjit.tuner;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.HashMap;
import java.util.Map;
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
	 * Call this method at the end of an event.
	 * 
	 * @param eventName
	 */
	void eEvent(String eventName);

	void bTuningRound(String cfgPrefix);
	void eTuningRound();

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
		public void eEvent(String eventName) {
		}

		@Override
		public void bTuningRound(String cfgPrefix) {
		}

		@Override
		public void eTuningRound() {
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
		Map<String, Event> events;
		final Ticker ticker = new NanoTicker();

		private RuntimeMXBean rb = ManagementFactory.getRuntimeMXBean();

		public EventTimeLoggerImpl(OutputStream os) {
			this(getOSWriter(os));
		}

		public EventTimeLoggerImpl(OutputStreamWriter osWriter) {
			this.osWriter = osWriter;
			this.events = new HashMap<>();
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
		public void eEvent(String eventName) {
			long time = ticker.time();
			Event e = events.remove(eventName);
			if (e == null) {
				new IllegalStateException(String.format(
						"Event %s has not started yet", eventName))
						.printStackTrace();
				return;
			}
			e.endTime = time;
			log(e);
		}

		@Override
		public void bTuningRound(String cfgPrefix) {
			bEvent("tuningRound");
			write(String
					.format("----------------------------%s----------------------------\n",
							cfgPrefix));
		}

		@Override
		public void eTuningRound() {
			eEvent("tuningRound");
		}

		private void log(Event e) {
			long uptime = rb.getUptime();
			long elapsedMills = TimeUnit.MILLISECONDS.convert(e.endTime
					- e.startTime, ticker.timeUnit);
			write(e.name, uptime, elapsedMills);
		}

		private void write(String eventName, long uptime, long elapsedMills) {
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
		public FileEventTimeLogger(String appName, String fileNameSuffix) {
			super(Utils.fileWriter(appName,
					String.format("eventTime_%s.txt", fileNameSuffix)));
		}
	}

	/**
	 * Prints the event time info to the standard out.
	 */
	public static class PrintEventTimeLogger extends EventTimeLoggerImpl {
		public PrintEventTimeLogger() {
			super(System.out);
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
