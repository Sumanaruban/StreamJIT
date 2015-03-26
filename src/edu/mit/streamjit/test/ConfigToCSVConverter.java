package edu.mit.streamjit.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.FloatParameter;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.common.Configuration.Parameter;
import edu.mit.streamjit.impl.common.Configuration.SwitchParameter;
import edu.mit.streamjit.impl.distributed.common.Utils;
import edu.mit.streamjit.tuner.ConfigurationAnalyzer;
import edu.mit.streamjit.tuner.SqliteAdapter;
import edu.mit.streamjit.util.ConfigurationUtils;

/**
 * Converts configuration data and running time to CSV format for R analysis.
 * 
 * @author sumanan
 * @since 26 Mar, 2015
 */
public class ConfigToCSVConverter {
	public static void main(String[] args) throws IOException {
		String appName;
		try {
			appName = args[0];
		} catch (Exception ex) {
			appName = "NestedSplitJoinCore";
		}

		ConfigToCSVConverter csv = new ConfigToCSVConverter(appName);
		csv.startWrite();
	}

	private final String appName;
	private final Character delimiter = ',';

	public ConfigToCSVConverter(String appName) {
		this.appName = appName;
	}

	public void startWrite() throws IOException {
		FileWriter writer = Utils.fileWriter(appName, appName + ".csv");
		List<String> paramNameList = paramNameList(appName);
		Map<String, Integer> runningTime = processRunTime(appName);
		writeHeader(paramNameList, writer);
		for (int i = 1; i <= runningTime.size(); i++) {
			Configuration cfg = ConfigurationUtils
					.readConfiguration(appName, i);
			Integer time = runningTime.get(new Integer(i).toString());
			write(paramNameList, cfg, time, writer);
		}
		writer.flush();
		writer.close();
	}

	private void write(List<String> paramNameList, Configuration cfg,
			Integer time, FileWriter writer) throws IOException {
		Map<String, Parameter> paramMap = cfg.getParametersMap();
		for (String s : paramNameList) {
			Parameter p = paramMap.get(s);
			String stringval;
			if (p.getClass() == IntParameter.class) {
				Integer intval = ((IntParameter) p).getValue();
				stringval = intval.toString();
			} else if (p.getClass() == FloatParameter.class) {
				Float floatval = ((FloatParameter) p).getValue();
				stringval = floatval.toString();
			} else if (p.getClass() == SwitchParameter.class) {
				Object objval = ((SwitchParameter) p).getValue();
				stringval = objval.toString();
			} else {
				throw new IllegalStateException("No matched param class");
			}
			writer.write(stringval);
			writer.write(delimiter);
		}
		writer.write(time == null ? "-1" : time.toString());
		writer.write("\n");
	}

	private void writeHeader(List<String> paramNameList, FileWriter writer)
			throws IOException {
		for (String s : paramNameList) {
			writer.write(String.format("\"%s\"", s));
			writer.write(delimiter);
		}
		writer.write("time\n");
	}

	private List<String> paramNameList(String appName) {
		Configuration cfg = ConfigurationUtils.readConfiguration(appName, 1);
		Map<String, Parameter> paramMap = cfg.getParametersMap();
		List<String> paramNameList = new ArrayList<String>(paramMap.size());
		for (Map.Entry<String, Parameter> en : paramMap.entrySet()) {
			Parameter p = en.getValue();
			if (p.getClass() == IntParameter.class
					|| p.getClass() == FloatParameter.class
					|| p.getClass() == SwitchParameter.class) {
				paramNameList.add(en.getKey());
			}
		}
		return paramNameList;
	}

	private static void verifyTime(String appName) throws IOException {
		Map<String, Integer> runningTime = processRunTime(appName);
		SqliteAdapter adapter = ConfigurationAnalyzer.connectDB(appName);
		for (int i = 1; i < runningTime.size(); i++) {
			Integer time = runningTime.get(new Integer(i).toString());
			double dbTime = ConfigurationAnalyzer.getRunningTime(adapter, i);
			System.out.println(String.format("%d-%f\n", time, dbTime));
		}

	}

	private static Map<String, Integer> processRunTime(String appName)
			throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(
				tuneRunTimeFile(appName)));
		String line;
		String cfgPrefix = "Init";
		int i = 0;
		Map<String, Integer> ret = new HashMap<>(5000);
		while ((line = reader.readLine()) != null) {
			if (line.startsWith("----------------------------"))
				cfgPrefix = cfgString(line);
			else if (line.startsWith("Execution")) {
				String[] arr = line.split(" ");
				String time = arr[3].trim();
				time = time.substring(0, time.length() - 2);
				int val = Integer.parseInt(time);
				ret.put(cfgPrefix, val);
			}
		}
		reader.close();
		return ret;
	}

	private static String cfgString(String line) {
		String l = line.replace('-', ' ');
		return l.trim();
	}

	private static String tuneRunTimeFile(String appName) {
		String tuneDirPath = String.format("%s%stune", appName, File.separator);
		File tuneDir = new File(tuneDirPath);
		if (tuneDir.exists())
			return String
					.format("%s%srunTime.txt", tuneDirPath, File.separator);

		String origFilePath = String.format("%s%srunTime.txt.orig", appName,
				File.separator);
		File origFile = new File(origFilePath);
		if (origFile.exists())
			return origFilePath;

		return String.format("%s%srunTime.txt", appName, File.separator);
	}
}
