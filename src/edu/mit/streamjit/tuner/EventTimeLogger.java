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
 * Logs the opentuner's method call times for debugging purpose.
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

	void bTuningRound(int round);
	void eTuningRound();

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
		public void bTuningRound(int round) {
		}

		@Override
		public void eTuningRound() {
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
			write("Method\t\t\tUptime\t\telapsedtime\n");
			write("====================================================\n");
		}

		@Override
		public void bEvent(String eventName) {
			long time = ticker.time();
			if (events.containsKey(eventName)) {
				throw new IllegalStateException(String.format(
						"Event %s has already started", eventName));
			}
			Event e = new Event(eventName);
			e.startTime = time;
			events.put(eventName, e);
		}

		@Override
		public void eEvent(String eventName) {
			long time = ticker.time();
			Event e = events.get(eventName);
			if (e == null) {
				String.format("Event %s has not started yet", eventName);
			}
			e.endTime = time;
			log(e);
			events.remove(eventName);
		}

		@Override
		public void bTuningRound(int round) {
			bEvent("tuningRound");
			write(String
					.format("----------------------------%d----------------------------\n",
							round));
		}

		@Override
		public void eTuningRound() {
			eEvent("tuningRound");

		}

		private void log(Event e) {
			long uptime = rb.getUptime();
			long elapsedMills = TimeUnit.MILLISECONDS.convert(e.endTime
					- e.startTime, ticker.timeUnit);
			write(String.format("%-22s\t%-12d\t%d\n", e.name, uptime,
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
	}

	/**
	 * Writes the method call time info to appName/onlineTuner.txt file.
	 */
	public static class FileEventTimeLogger extends EventTimeLoggerImpl {
		public FileEventTimeLogger(String appName) {
			super(Utils.fileWriter(appName, "onlineTuner.txt"));
		}
	}

	/**
	 * Prints the method call time info to the standard out.
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
