package edu.mit.streamjit.tuner;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Stopwatch;

import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.TimeLogger;
import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.impl.distributed.controller.ConfigurationManager;
import edu.mit.streamjit.impl.distributed.controller.ConfigurationManager.NewConfiguration;
import edu.mit.streamjit.impl.distributed.controller.StreamJitApp;
import edu.mit.streamjit.impl.distributed.controller.HT.ThroughputGraphGenerator;
import edu.mit.streamjit.util.ConfigurationUtils;
import edu.mit.streamjit.util.Pair;
import edu.mit.streamjit.util.TimeLogProcessor;
import edu.mit.streamjit.util.json.Jsonifiers;

/**
 * Online tuner does continues learning.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Oct 8, 2013
 */
public class OnlineTuner implements Runnable {
	private final OpenTuner tuner;
	private final StreamJitApp<?, ?> app;
	private final ConfigurationManager cfgManager;
	private final boolean needTermination;
	private final TimeLogger logger;
	private final ConfigurationPrognosticator prognosticator;
	private final EventTimeLogger mLogger;
	private final Reconfigurer configurer;
	private final Configuration defaultCfg;
	private long currentBestTime;
	private Configuration bestCfg;
	private final OpenTunerListener listener;

	public OnlineTuner(Reconfigurer configurer, boolean needTermination,
			Configuration defaultCfg) {
		this.configurer = configurer;
		this.app = configurer.app;
		this.cfgManager = configurer.cfgManager;
		this.tuner = new TCPTuner();
		this.needTermination = needTermination;
		this.logger = configurer.app.logger;
		this.prognosticator = configurer.prognosticator;
		this.mLogger = app.eLogger;
		this.currentBestTime = Integer.MAX_VALUE;
		this.listener = new OpenTunerListener(tuner, app.name, configurer);
		this.defaultCfg = defaultCfg;
	}

	@Override
	public void run() {
		if (Options.run == 1)
			tune();
		else
			System.err.println("Options.tune is not in tune mode.");
	}

	private void tune() {
		int round = 0;
		Stopwatch searchTimeSW = Stopwatch.createStarted();
		try {
			mLogger.bEvent("startTuner");
			startTuner();
			mLogger.eEvent("startTuner");
			Pair<Boolean, Integer> ret;

			System.out.println("New tune run.............");
			while (configurer.manager.getStatus() != AppStatus.STOPPED) {
				mLogger.bTuningRound(new Integer(++round).toString());
				mLogger.bEvent("serialcfg");
				NewConfiguration newconfig = listener.nextConfig();
				logger.logSearchTime(searchTimeSW
						.elapsed(TimeUnit.MILLISECONDS));
				if (newconfig == null) {
					System.err.println("OpenTuner closed unexpectly.");
					break;
				}

				// At the end of the tuning, Opentuner will send "Completed"
				// msg. This means no more tuning.
				/*
				 * if (cfgJson.equals("Completed")) {
				 * mLogger.bEvent("handleTermination"); handleTermination();
				 * mLogger.eEvent("handleTermination"); break; }
				 */

				mLogger.bEvent("newCfg");
				String cfgPrefix = ConfigurationUtils
						.getConfigPrefix(newconfig.configuration);

				System.out
						.println(String
								.format("---------------------Tuning-%s-------------------------",
										cfgPrefix));

				mLogger.eEvent("newCfg");
				mLogger.bEvent("reconfigure");
				ret = configurer.reconfigure(newconfig);
				mLogger.eEvent("reconfigure");
				long time;
				if (ret.second > 0)
					time = getTime();
				else
					time = ret.second;
				if (time > 1 && currentBestTime > time) {
					currentBestTime = time;
					bestCfg = newconfig.configuration;
				}
				logger.logRunTime(time);
				prognosticator.time(time);
				tuner.writeLine(String.format("%s:%s", cfgPrefix, new Double(
						time).toString()));
				searchTimeSW.reset();
				searchTimeSW.start();

				if (!ret.first) {
					tuner.writeLine("exit");
					break;
				}
				mLogger.bTuningRound(new Integer(round).toString());
			}

		} catch (IOException e) {
			e.printStackTrace();
			mLogger.bEvent("terminate");
			configurer.terminate();
			mLogger.eEvent("terminate");
		}
		mLogger.bEvent("tuningFinished");
		tuningFinished(round);
		mLogger.eEvent("tuningFinished");
	}

	private void startTuner() throws IOException {
		String relativeTunerPath = String.format(
				"lib%sopentuner%sstreamjit%sstreamjit2.py", File.separator,
				File.separator, File.separator);

		String absoluteTunerPath = String.format("%s%s%s",
				System.getProperty("user.dir"), File.separator,
				relativeTunerPath);

		tuner.startTuner(absoluteTunerPath, new File(app.name));

		tuner.writeLine("program");
		tuner.writeLine(app.name);

		tuner.writeLine("tunerounds");
		tuner.writeLine(new Integer(Options.tuningRounds).toString());

		tuner.writeLine("confg");
		tuner.writeLine(Jsonifiers.toJson(defaultCfg).toString());
		new Thread(listener).start();
	}

	private long getTime() {
		long timeout = Options.timeOut ? 2 * currentBestTime : 0;
		long time;
		time = configurer.getFixedOutputTime(timeout);
		if (time < 0)
			return time;
		if ((time - currentBestTime) < currentBestTime / 5) {
			long time1 = time;
			long time2 = configurer.getFixedOutputTime(timeout);
			if (time2 > 0)
				time = (time1 + time2) / 2;
			System.err.println(String.format(
					"Remeasurred...cbt=%d,avgt=%d,t1=%d,t2=%d",
					currentBestTime, time, time1, time2));
		}
		return time;
	}

	/**
	 * Just excerpted from run() method for better readability.
	 * 
	 * @throws IOException
	 */
	private void handleTermination() throws IOException {
		String finalConfg = tuner.readLine();
		System.out.println("Tuning finished");
		ConfigurationUtils.saveConfg(finalConfg, "final", app.name);
		Configuration finalcfg = Configuration.fromJson(finalConfg);
		finalcfg = ConfigurationUtils.addConfigPrefix(finalcfg, "final");
		verify();
		if (needTermination) {
			configurer.terminate();
		} else {
			NewConfiguration newconfig = configurer.newConfiguration(finalcfg);
			Pair<Boolean, Integer> ret = configurer.reconfigure(newconfig);
			if (ret.first && ret.second > 0)
				System.out
						.println("Application is running forever with the final configuration.");
			else {
				System.err.println("Invalid final configuration.");
				configurer.terminate();
			}
		}
	}

	private void verify() {
		List<Pair<String, Integer>> cfgPrefixes = new ArrayList<>();
		cfgPrefixes.add(new Pair<>("final", 0));
		cfgPrefixes.add(new Pair<>("hand", 0));
		new Verifier(configurer).verifyTuningTimes(cfgPrefixes);
	}

	private void tuningFinished(int round) {
		// TODO: seamless
		/*
		 * try { configurer.drainer.dumpDraindataStatistics(); } catch
		 * (IOException e) { e.printStackTrace(); }
		 */

		if (needTermination)
			configurer.terminate();

		try {
			TimeLogProcessor.summarize(app.name);
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (round < 25)
			try {
				ThroughputGraphGenerator.summarize(app.name);
			} catch (IOException e) {
				e.printStackTrace();
			}
	}

	private static class OpenTunerListener implements Runnable {
		private ConcurrentLinkedQueue<NewConfiguration> cfgQueue;
		private final AtomicBoolean stopFlag;
		private final AtomicBoolean openTunerStopped;
		private final OpenTuner tuner;
		private final String appName;
		private final Reconfigurer configurer;

		private OpenTunerListener(OpenTuner tuner, String appName,
				Reconfigurer configurer) {
			this.cfgQueue = new ConcurrentLinkedQueue<NewConfiguration>();
			this.stopFlag = new AtomicBoolean(false);
			this.openTunerStopped = new AtomicBoolean(false);
			this.tuner = tuner;
			this.appName = appName;
			this.configurer = configurer;
		}

		@Override
		public void run() {
			while (!stopFlag.get()) {
				try {
					String cfgJson = tuner.readLine();
					if (cfgJson == null) {
						openTunerStopped.set(true);
						stopFlag.set(true);
						break;
					} else if (cfgJson.equals("Completed")) {
						cfgJson = tuner.readLine();
					}
					NewConfiguration newcfg = newcfgJson(cfgJson);
					newConfiguration(newcfg);
				} catch (IOException e) {
					e.printStackTrace();
					stopFlag.set(true);
					openTunerStopped.set(true);
				}
			}
		}

		public NewConfiguration nextConfig() {
			NewConfiguration newCfg = null;
			if (openTunerStopped.get())
				return null;
			while ((newCfg = cfgQueue.poll()) == null)
				if (openTunerStopped.get())
					return null;
			return newCfg;
		}

		public void stop() {
			this.stopFlag.set(true);
		}

		private NewConfiguration newcfgJson(String cfgJson) {
			Configuration config = Configuration.fromJson(cfgJson);
			String cfgPrefix = ConfigurationUtils.getConfigPrefix(config);

			if (Options.saveAllConfigurations)
				ConfigurationUtils.saveConfg(cfgJson, cfgPrefix, appName);
			return configurer.newConfiguration(config);
		}

		private void newConfiguration(NewConfiguration newConfig) {
			if (newConfig.verificationPassed
					&& newConfig.isPrognosticationPassed())
				cfgQueue.offer(newConfig);
			else
				try {
					tuner.writeLine(String.format("%s:%s", ConfigurationUtils
							.getConfigPrefix(newConfig.configuration),
							new Double(-1).toString()));
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
	}
}