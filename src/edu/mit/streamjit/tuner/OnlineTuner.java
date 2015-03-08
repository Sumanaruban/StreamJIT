package edu.mit.streamjit.tuner;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.TimeLogger;
import edu.mit.streamjit.impl.distributed.ConfigurationManager;
import edu.mit.streamjit.impl.distributed.StreamJitApp;
import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.impl.distributed.common.Utils;
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
	private long currentBestTime;
	private Map<Integer, Configuration> bestCfgs;

	public OnlineTuner(Reconfigurer configurer, boolean needTermination) {
		this.configurer = configurer;
		this.app = configurer.app;
		this.cfgManager = configurer.cfgManager;
		this.tuner = new TCPTuner();
		this.needTermination = needTermination;
		this.logger = configurer.logger;
		this.prognosticator = configurer.prognosticator;
		this.mLogger = configurer.mLogger;
		this.currentBestTime = Integer.MAX_VALUE;
		this.bestCfgs = new HashMap<>();
	}

	@Override
	public void run() {
		if (Options.tune == 1)
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
				mLogger.bTuningRound(++round);
				mLogger.bEvent("serialcfg");
				String cfgJson = tuner.readLine();
				logger.logSearchTime(searchTimeSW
						.elapsed(TimeUnit.MILLISECONDS));
				if (cfgJson == null) {
					System.err.println("OpenTuner closed unexpectly.");
					break;
				}

				// At the end of the tuning, Opentuner will send "Completed"
				// msg. This means no more tuning.
				if (cfgJson.equals("Completed")) {
					mLogger.bEvent("handleTermination");
					handleTermination();
					mLogger.eEvent("handleTermination");
					break;
				}

				mLogger.bEvent("newCfg");
				Configuration config = newCfg(round, cfgJson);
				mLogger.eEvent("newCfg");
				mLogger.bEvent("reconfigure");
				ret = configurer.reconfigure(config);
				mLogger.eEvent("reconfigure");
				long time;
				if (ret.second > 0)
					time = getTime();
				else
					time = ret.second;
				if (time > 1 && currentBestTime > time) {
					currentBestTime = time;
					bestCfgs.put(dynCount, config);
				}
				logger.logRunTime(time);
				prognosticator.time(time);
				tuner.writeLine(new Double(time).toString());
				searchTimeSW.reset();
				searchTimeSW.start();

				if (!ret.first) {
					tuner.writeLine("exit");
					break;
				}
				mLogger.eTuningRound();
				endOfTuningRound(round);
				if (dynCount > 1) {
					System.err.println("DynTest over");
					break;
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
			mLogger.bEvent("terminate");
			configurer.terminate();
			mLogger.eEvent("terminate");
		}
		mLogger.bEvent("tuningFinished");
		tuningFinished();
		mLogger.eEvent("tuningFinished");
		summarize(round);
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
		tuner.writeLine(Jsonifiers.toJson(app.getConfiguration()).toString());
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
			Pair<Boolean, Integer> ret = configurer.reconfigure(finalcfg);
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
		Map<String, Integer> cfgPrefixes = new HashMap<>();
		cfgPrefixes.put("final", 0);
		cfgPrefixes.put("hand", 0);
		new Verifier(configurer).verifyTuningTimes(cfgPrefixes);
	}

	private Configuration newCfg(int round, String cfgJson) {
		String cfgPrefix = new Integer(round).toString();
		System.out.println(String.format(
				"---------------------%s-------------------------", cfgPrefix));
		logger.newConfiguration(cfgPrefix);
		Configuration config = Configuration.fromJson(cfgJson);
		config = ConfigurationUtils.addConfigPrefix(config, cfgPrefix);

		if (Options.saveAllConfigurations)
			ConfigurationUtils.saveConfg(cfgJson, cfgPrefix, app.name);
		return config;
	}

	private void tuningFinished() {
		try {
			configurer.drainer.dumpDraindataStatistics();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (needTermination)
			configurer.terminate();

		try {
			TimeLogProcessor.summarize(app.name);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private int dynCount = 0;
	private final int initialTuningCount = Options.initialTuningCount;
	private final int dynTuningCount = Options.dynTuningCount;
	private final int bestcfgMinutes = 3;

	/**
	 * Pausing condition of the online tuning.
	 */
	private boolean pauseTuning(int round) {
		if (round - cfgManager.rejectCount > initialTuningCount
				+ (dynCount * dynTuningCount)) {
			return true;
		}
		return false;
	}

	private void endOfTuningRound(int round) {
		if (pauseTuning(round)) {
			System.err.println("Configuring with the best cfg..");
			logger.newConfiguration("bestCfgs-" + dynCount);
			Pair<Boolean, Long> ret = reconfigure(bestCfgs.get(dynCount), 0);
			try {
				Thread.sleep(bestcfgMinutes * 60 * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.err.println(String.format("bestCfgs-%d, Runtime=%d",
					dynCount, ret.second));
			simulateDynamism();
			System.out.println("Going for dynamism tuning...");
		}
	}

	private void simulateDynamism() {
		System.err.println("simulateDynamism");
		cfgManager.nodeDown(1);
		dynCount++;
	}

	private void summarize(int round) {
		FileWriter writer = Utils.fileWriter(app.name, "dynamism.txt");
		try {
			writer.write(String.format("round=%d\n", round));
			writer.write(String.format("dynCount=%d\n", dynCount));
			writer.write(String.format("Rejected=%d\n", cfgManager.rejectCount));
			writer.close();
			for (Map.Entry<Integer, Configuration> bestcfg : bestCfgs
					.entrySet()) {
				ConfigurationUtils.saveConfg(bestcfg.getValue(), "best"
						+ bestcfg.getKey(), app.name);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}