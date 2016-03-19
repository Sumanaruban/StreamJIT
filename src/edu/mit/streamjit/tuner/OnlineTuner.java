package edu.mit.streamjit.tuner;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
	final DynamismTester dynamism = new DynamismTester();

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
			dynamism.init();
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
					bestCfgs.put(dynamism.dynCount, config);
				}
				logger.logRunTime(time);
				prognosticator.time(time);
				tuner.writeLine(new Double(dynamism.penalized(time)).toString());
				searchTimeSW.reset();
				searchTimeSW.start();

				if (!ret.first) {
					tuner.writeLine("exit");
					break;
				}
				mLogger.eTuningRound();
				dynamism.endOfTuningRound(round);
				if (dynamism.stopDyn) {
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
		dynamism.summarize(round);
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

	private class DynamismTester {
		/**
		 * Total dynamisms to simulate. After initialTuningCount a new dynamism
		 * will be simulated for every dynTuningCount of tuning rounds.
		 */
		private final int totalDyn = Options.dynType == 1 ? 1 : 3;
		/**
		 * No of dynamism simulated.
		 */
		private int dynCount = 0;
		private boolean stopDyn = false;
		private final int initialTuningCount = Options.initialTuningCount;
		private final int dynTuningCount = Options.dynTuningCount;
		private final int bestcfgMinutes = 3;
		private final boolean penalize = Options.penalize;

		public void init() {
			if (Options.dynType == 3) {
				for (int i = 1; i <= totalDyn; i++)
					cfgManager.nodeDown(i);
			}
		}

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
				Configuration bestCfg = bestCfgs.get(dynCount);
				String cfgPrefix = ConfigurationUtils.getConfigPrefix(bestCfg);
				String newcfgPrefix = String.format("EndDyn%d:%s", dynCount,
						cfgPrefix);
				bestCfg = ConfigurationUtils.addConfigPrefix(bestCfg,
						newcfgPrefix);
				runBestCfg(bestCfg);
				if (dynCount == totalDyn) {
					stopDyn = true;
					return;
				}
				simulateDynamism();
				newcfgPrefix = String.format("BeginDyn%d:%s", dynCount,
						cfgPrefix);
				bestCfg = ConfigurationUtils.addConfigPrefix(bestCfg,
						newcfgPrefix);
				runBestCfg(bestCfg);
				System.out.println("Going for dynamism tuning...");
			}
		}

		private void simulateDynamism() {
			System.err.println("simulateDynamism");
			switch (Options.dynType) {
				case 1 :
					blockCores();
					break;
				case 2 :
					blockNode();
					break;
				case 3 :
					unblockNode();
					break;
				default :
					blockNode();
					break;
			}
			currentBestTime = Integer.MAX_VALUE;
			dynCount++;
		}

		private void runBestCfg(Configuration config) {
			System.err.println("Configuring with the best cfg..");
			String cfgPrefix = ConfigurationUtils.getConfigPrefix(config);
			logger.newConfiguration(cfgPrefix);
			Pair<Boolean, Integer> ret = configurer.reconfigure(config);
			long time = ret.second;
			if (ret.second > 0) {
				time = configurer.getFixedOutputTime(0);
				System.out.println(String.format(
						"Going to sleep for %d minutes...", bestcfgMinutes));
				try {
					Thread.sleep(bestcfgMinutes * 60 * 1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			System.err.println(String.format("bestCfgs-%s, Runtime=%d",
					cfgPrefix, time));
		}

		int blockNode = 1;
		private void blockNode() {
			System.err.println(String.format("Blocking Node-%d...", blockNode));
			cfgManager.nodeDown(blockNode++);
		}

		private void unblockNode() {
			System.err.println(String.format("Unblocking Node-%d...",
					blockNode));
			cfgManager.nodeUp(blockNode++);
		}

		private void blockCores() {
			System.err.println("blockCores...");

			Set<Integer> cores = new HashSet<>();
			int noOfCorestoBlock = Options.maxNumCores;
			if (Options.blockCorePattern == 1 || Options.blockCorePattern == 2)
				noOfCorestoBlock = Options.maxNumCores / 2;

			for (int i = 0; i < noOfCorestoBlock; i++)
				cores.add(i);

			int noOfNodestoBlock = cfgManager.noOfMachines() - 1;
			if (Options.blockCorePattern == 2 || Options.blockCorePattern == 3)
				noOfNodestoBlock = noOfNodestoBlock / 2;

			for (int i = 1; i <= noOfNodestoBlock; i++)
				configurer.manager.blockCores(i, cores);
		}

		private void summarize(int round) {
			FileWriter writer = Utils.fileWriter(app.name, "dynamism.txt");
			try {
				writer.write(String.format("round=%d\n", round));
				writer.write(String.format("dynCount=%d\n", dynCount));
				writer.write(String.format("Rejected=%d\n",
						cfgManager.rejectCount));
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

		private long penalized(long time) {
			if (penalize) {
				long newtime = (long) ((1 + cfgManager.wrongParamCount * 0.1) * time);
				System.err.println(String.format(
						"Actual time=%d, Penalized time=%d\n", time, newtime));
				return newtime;
			}
			return time;
		}
	}
}