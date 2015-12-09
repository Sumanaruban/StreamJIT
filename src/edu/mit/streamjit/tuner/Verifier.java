package edu.mit.streamjit.tuner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.distributed.ConfigurationManager.NewConfiguration;
import edu.mit.streamjit.impl.distributed.ThroughputGraphGenerator;
import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.util.ConfigurationUtils;
import edu.mit.streamjit.util.Pair;
import edu.mit.streamjit.util.TimeLogProcessor;

public class Verifier implements Runnable {

	private final Reconfigurer configurer;
	private final String appName;

	public Verifier(Reconfigurer configurer) {
		this.configurer = configurer;
		this.appName = configurer.app.name;
	}

	public void verify() {
		Map<String, Integer> cfgPrefixes = cfgPrefixes(appName);
		verifyTuningTimes(cfgPrefixes);
		generateGraphs(cfgPrefixes);
	}

	private void generateGraphs(Map<String, Integer> cfgPrefixes) {
		if (Options.verificationCount > 50 || Options.evaluationCount > 50)
			try {
				TimeLogProcessor.processVerifycaionRun(appName, cfgPrefixes);
				TimeLogProcessor.summarize(appName);
			} catch (IOException e) {
				e.printStackTrace();
			}

		try {
			ThroughputGraphGenerator.summarize(appName);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * This method just picks a few configurations and re-run the app to ensure
	 * the time we reported to the opentuner is correct.
	 * 
	 * This method can be called after the completion of the tuning.
	 * 
	 * @param cfgPrefixes
	 *            map of cfgPrefixes and expected running time.
	 */
	void verifyTuningTimes(Map<String, Integer> cfgPrefixes) {
		try {
			FileWriter writer = writer();
			for (int i = 0; i < Options.verificationCount; i++) {
				for (Map.Entry<String, Integer> en : cfgPrefixes.entrySet()) {
					String prefix = en.getKey();
					Integer expectedRunningTime = en.getValue();
					String cfgName = String
							.format("%s_%s.cfg", prefix, appName);
					Configuration cfg = ConfigurationUtils.readConfiguration(
							appName, prefix);
					if (cfg == null) {
						System.err.println(String.format("No %s file exists",
								cfgName));
						continue;
					}
					cfg = ConfigurationUtils.addConfigPrefix(cfg, prefix);
					writer.write("----------------------------------------\n");
					writer.write(String.format("Configuration name = %s\n",
							cfgName));
					List<Long> runningTimes = evaluateConfig(cfg);
					processRunningTimes(runningTimes, expectedRunningTime,
							writer);
				}
			}
			writer.write("**************FINISHED**************\n\n");
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void processRunningTimes(List<Long> runningTimes,
			Integer expectedRunningTime, FileWriter writer) throws IOException {
		writer.write(String.format("Expected running time = %dms\n",
				expectedRunningTime));
		int correctEval = 0;
		double total = 0;
		for (int i = 0; i < runningTimes.size(); i++) {
			long runningTime = runningTimes.get(i);
			if (runningTime > 0) {
				correctEval++;
				total += runningTime;
			}
			writer.write(String.format("Evaluation %d = %dms\n", i + 1,
					runningTime));
		}
		double avg = total / correctEval;
		writer.write(String.format("Average running time = %.3fms\n", avg));
	}

	private FileWriter writer() throws IOException {
		FileWriter writer = new FileWriter(String.format("%s%sevaluation.txt",
				appName, File.separator, appName), true);
		writer.write("##########################################################");
		Properties prop = Options.getProperties();
		prop.store(writer, "");
		return writer;
	}

	public static Map<String, Integer> cfgPrefixes(String appName) {
		Map<String, Integer> cfgPrefixes = new HashMap<>();
		try {
			BufferedReader reader = new BufferedReader(new FileReader(
					String.format("%s%sverify.txt", appName, File.separator)));
			String line;
			while ((line = reader.readLine()) != null) {
				if (line.contains("="))
					process1(line, cfgPrefixes);
				else
					process2(line, cfgPrefixes);
			}
			reader.close();
		} catch (IOException e) {
		}
		return cfgPrefixes;
	}

	/**
	 * Processes the line that is generated by {@link TimeLogProcessor}.
	 */
	private static void process1(String line, Map<String, Integer> cfgPrefixes) {
		String[] arr = line.split("=");
		String cfgPrefix = arr[0].trim();
		int expectedTime = 0;
		if (arr.length > 1)
			try {
				expectedTime = Integer.parseInt(arr[1]);
			} catch (NumberFormatException ex) {
				System.err.println("NumberFormatException: " + arr[1]);
			}
		cfgPrefixes.put(cfgPrefix, expectedTime);
	}

	/**
	 * Processes manually entered lines in the verify.txt
	 */
	private static void process2(String line, Map<String, Integer> cfgPrefixes) {
		String[] arr = line.split(",");
		for (String s : arr) {
			cfgPrefixes.put(s.trim(), 0);
		}
	}

	/**
	 * Evaluates a configuration.
	 * 
	 * @param cfg
	 *            configuration that needs to be evaluated
	 */
	private List<Long> evaluateConfig(Configuration cfg) {
		String cfgPrefix = ConfigurationUtils.getConfigPrefix(cfg);
		System.out.println("Evaluating " + cfgPrefix);
		int count = Options.evaluationCount;
		List<Long> runningTime = new ArrayList<>(count);
		Pair<Boolean, Integer> ret;
		if (cfg != null) {
			for (int i = 0; i < count; i++) {
				long time;
				logBegining(cfgPrefix);
				NewConfiguration newconfig = configurer.newConfiguration(cfg);
				ret = configurer.reconfigure(newconfig);
				if (ret.second > 0)
					time = configurer.getFixedOutputTime(0);
				else {
					time = ret.second;
					System.err.println("Evaluation failed.ret.second="
							+ ret.second);
				}
				logEnding(time, cfgPrefix);
				runningTime.add(time);
			}
		} else {
			System.err.println("Null configuration\n");
		}
		return runningTime;
	}

	@Override
	public void run() {
		if (Options.tune == 2) {
			verify();
			configurer.terminate();
		} else
			System.err.println("Options.tune is not in the verify mode");
	}

	private void logBegining(String cfgPrefix) {
		configurer.mLogger.bTuningRound(cfgPrefix);
		configurer.mLogger.bEvent("serialcfg");
		configurer.logger.newConfiguration(cfgPrefix);
	}

	private void logEnding(long time, String cfgPrefix) {
		configurer.logger.logRunTime(time);
		configurer.prognosticator.time(time);
		configurer.mLogger.eTuningRound(cfgPrefix);
	}
}