package edu.mit.streamjit.tuner;

import static com.google.common.base.Preconditions.checkState;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.Parameter;
import edu.mit.streamjit.impl.distributed.common.Utils;
import edu.mit.streamjit.tuner.ComparisionSummary.ParamClassSummary;
import edu.mit.streamjit.util.ConfigurationUtils;
import edu.mit.streamjit.util.TimeLogProcessor;

public class ConfigurationAnalyzer {

	public static void main(String[] args) throws IOException {
		String appName;
		try {
			appName = args[0];
		} catch (Exception ex) {
			appName = "FMRadioCore";
		}
		ConfigurationAnalyzer ca = new ConfigurationAnalyzer(appName);
		ca.Analyze();
		ca.Analyze2();
	}

	private final String appName;

	private boolean dbExists;

	private final FullParameterSummary fullParameterSummary;

	public ConfigurationAnalyzer(String appName) {
		dbExists = verifyPath2(ConfigurationUtils.configDir, appName);
		this.appName = appName;
		fullParameterSummary = new FullParameterSummary(appName);
	}

	private static SqliteAdapter connectDB(String appName) {
		String dbPath = String.format("%s%s%s", appName, File.separator,
				appName);
		SqliteAdapter sqlite = new SqliteAdapter();
		sqlite.connectDB(dbPath);
		return sqlite;
	}

	private static double getRunningTime(String appName, int round) {
		SqliteAdapter sqlite = connectDB(appName);
		return getRunningTime(sqlite, appName, round);
	}

	private static double getRunningTime(SqliteAdapter sqlite, String appName,
			int round) {
		ResultSet result = sqlite.executeQuery(String.format(
				"SELECT * FROM result WHERE id=%d", round));

		String runtime = "1000000000";
		try {
			runtime = result.getString("time");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		double val = Double.POSITIVE_INFINITY;
		try {
			val = Double.parseDouble(runtime);
		} catch (NumberFormatException e) {
		}
		return val;
	}

	private static int getTotalResults(SqliteAdapter sqlite) {
		ResultSet result = sqlite.executeQuery("SELECT COUNT(*) FROM result");

		String runtime = "0";
		try {
			runtime = result.getString("COUNT(*)");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return Integer.parseInt(runtime);
	}

	private static boolean verifyPath(String cfgDir, String appName) {
		String dbPath = String.format("%s%s%s", appName, File.separator,
				appName);
		File db = new File(dbPath);
		if (!db.exists())
			throw new IllegalStateException("No database file found in "
					+ dbPath);

		String dirPath = String.format("%s%s%s", appName, File.separator,
				cfgDir);
		File dir = new File(dirPath);
		if (!dir.exists())
			throw new IllegalStateException("No directory found in " + dirPath);

		return true;
	}

	private static boolean verifyPath2(String cfgDir, String appName) {
		boolean ret = true;
		String dbPath = String.format("%s%s%s", appName, File.separator,
				appName);
		File db = new File(dbPath);
		if (!db.exists()) {
			ret = false;
			System.err.println("No database file found in " + dbPath);
		}

		String dirPath = String.format("%s%s%s", appName, File.separator,
				cfgDir);
		File dir = new File(dirPath);
		if (!dir.exists())
			throw new IllegalStateException("No directory found in " + dirPath);

		return ret;
	}

	private void Analyze() throws IOException {
		SqliteAdapter sqlite = null;
		if (dbExists)
			sqlite = connectDB(appName);

		int maxTuneCount = 5000;
		if (dbExists)
			maxTuneCount = getTotalResults(sqlite);

		int start = 1;
		int end = maxTuneCount; // inclusive
		List<ComparisionSummary> comparitionSummaryList = new ArrayList<>();
		for (int i = start; i < end; i++) {
			double t1 = 0;
			double t2 = 0;
			if (dbExists) {
				t1 = getRunningTime(sqlite, appName, i);
				t2 = getRunningTime(sqlite, appName, i + 1);
			}

			if (needTocompare(t1, t2)) {
				Configuration cfg1 = ConfigurationUtils.readConfiguration(
						appName, i);
				Configuration cfg2 = ConfigurationUtils.readConfiguration(
						appName, i + 1);
				cfg1 = ConfigurationUtils.addConfigPrefix(cfg1,
						new Integer(i).toString());
				cfg2 = ConfigurationUtils.addConfigPrefix(cfg2, new Integer(
						i + 1).toString());
				if (cfg1 == null || cfg2 == null)
					continue;
				ComparisionSummary sum = ComparisionSummary.compare(cfg1, cfg2,
						t1, t2, fullParameterSummary);
				comparitionSummaryList.add(sum);
			}
		}
		File summaryDir = new File(String.format("%s%ssummary", appName,
				File.separator));
		Utils.createDir(summaryDir.getPath());
		String datafile = "cfgAnalizePrev.txt";
		printTable(comparitionSummaryList,
				Utils.fileWriter(summaryDir.getPath(), datafile));
		File plotFile = createPlotFile2(summaryDir, appName, datafile);
		TimeLogProcessor.plot(summaryDir, plotFile);
	}

	private void Analyze2() throws IOException {
		SqliteAdapter sqlite = null;
		if (dbExists)
			sqlite = connectDB(appName);

		int maxTuneCount = 5000;
		if (dbExists)
			maxTuneCount = getTotalResults(sqlite);

		int start = 1;
		int end = maxTuneCount; // inclusive
		List<ComparisionSummary> comparitionSummaryList = new ArrayList<>();
		double curBestTime = Double.MAX_VALUE;
		Configuration curBestConfig = null;
		for (int i = start; i < end; i++) {
			double t1 = 0;
			if (dbExists)
				t1 = getRunningTime(sqlite, appName, i);

			if (needTocompare(t1, curBestTime)) {
				System.out.println("Comparing..." + i);
				Configuration cfg1 = ConfigurationUtils.readConfiguration(
						appName, i);
				cfg1 = ConfigurationUtils.addConfigPrefix(cfg1,
						new Integer(i).toString());
				if (cfg1 != null && curBestConfig != null) {
					ComparisionSummary sum = ComparisionSummary.compare(
							curBestConfig, cfg1, curBestTime, t1,
							fullParameterSummary);
					comparitionSummaryList.add(sum);
				}

				if (curBestTime > t1) {
					curBestTime = t1;
					curBestConfig = cfg1;
					System.out.println(String.format("New best. %s, time-%.0f",
							ConfigurationUtils.getConfigPrefix(curBestConfig),
							curBestTime));
				}
			}
		}
		File summaryDir = new File(String.format("%s%ssummary", appName,
				File.separator));
		Utils.createDir(summaryDir.getPath());
		String datafile = "cfgAnalizeBest.txt";
		printTable(comparitionSummaryList,
				Utils.fileWriter(summaryDir.getPath(), datafile));
		File plotFile = createPlotFile2(summaryDir, appName, datafile);
		TimeLogProcessor.plot(summaryDir, plotFile);
	}

	private void print(List<ComparisionSummary> comparitionSummaryList,
			OutputStreamWriter osWriter) throws IOException {
		osWriter.write(String.format(
				"Total parameters in the configuration = %d\n",
				fullParameterSummary.totalCount));
		for (ComparisionSummary s : comparitionSummaryList) {
			osWriter.write("\n-------------------------------------------------------\n");
			osWriter.write(s + "\n");
			osWriter.write(String.format("t1=%.0fms, t2=%.0fms\n", s.t1, s.t2));
			osWriter.write(s.distanceSummary() + "\n");
			for (ParamClassSummary ps : s.ParamClassSummaryList())
				osWriter.write(ps + "\n");
		}
		osWriter.flush();
	}

	private void printTable(List<ComparisionSummary> comparitionSummaryList,
			OutputStreamWriter osWriter) throws IOException {
		writeHeader(osWriter);
		for (ComparisionSummary sum : comparitionSummaryList) {
			osWriter.write(String.format("\n%6s\t\t", sum.firstCfg));
			osWriter.write(String.format("%.2f\t\t", sum.distance()));
			osWriter.write(String.format("%.2f\t\t", sum.weightedDistance()));
			osWriter.write(String.format("%.2f\t\t", sum.normalizedDistance()));
			osWriter.write(String.format("%.2f\t\t",
					sum.weightedNormalizedDistance()));
			osWriter.write(String.format("%.5f\t\t",
					sum.distance(ParamType.MULTIPLIER)));
			osWriter.write(String.format("%.2f\t\t",
					sum.distance(ParamType.UNROLL_CORE)));
			osWriter.write(String.format("%.2f\t\t",
					sum.distance(ParamType.ALLOCATION_STRATEGY)));
			osWriter.write(String.format("%.2f\t\t",
					sum.distance(ParamType.INTERNAL_STORAGE_STRATEGY)));
			osWriter.write(String.format("%.2f\t\t",
					sum.distance(ParamType.PARTITION)));
			osWriter.write(String.format("%.2f\t\t",
					sum.distance(ParamType.UNBOXING_STRATEGY)));
			osWriter.write(String.format("%.2f\t\t",
					sum.distance(ParamType.REMOVAL_STRATEGY)));
			osWriter.write(String.format("%.2f\t\t",
					sum.distance(ParamType.FUSION_STRATEGY)));
			osWriter.write(String.format("%.2f\t\t", sum.t1));
			osWriter.write(String.format("%.2f\t\t", sum.t2));
			osWriter.write(String.format("%.2f", sum.t1 - sum.t2));
		}
		osWriter.close();
	}

	private static File createPlotFile(File dir, String appName)
			throws IOException {
		String title = TimeLogProcessor.getTitle(appName);
		boolean pdf = true;
		String dataFile = "cfgAnalize.txt";
		File plotfile = new File(dir, "cfgAnalize.plt");
		FileWriter writer = new FileWriter(plotfile, false);
		if (pdf) {
			writer.write("set terminal pdf enhanced color\n");
			writer.write(String.format("set output \"%scfgAnalize.pdf\"\n",
					title));
		} else {
			writer.write("set terminal postscript eps enhanced color\n");
			writer.write(String.format("set output \"%s.eps\"\n", title));
		}
		writer.write(String.format("set title \"%s\"\n", title));
		writer.write("set grid\n");
		writer.write("#set yrange [0:*]\n");
		writer.write("set xlabel \"Time Improvements(ms)\"\n");
		writer.write("set ylabel \"Distance\"\n");
		writer.write(String.format(
				"plot \"%s\" using 16:2 with points title \"Distance\"\n",
				dataFile));
		writer.write("set ylabel \"weightedDist\"\n");
		writer.write(String.format(
				"plot \"%s\" using 16:3 with points title \"weightedDist\"\n",
				dataFile));
		writer.write("set ylabel \"NormalizedDistance\"\n");
		writer.write(String
				.format("plot \"%s\" using 16:4 with points title \"NormalizedDistance\"\n",
						dataFile));
		writer.write("set ylabel \"weightedNormalizedDistance\"\n");
		writer.write(String
				.format("plot \"%s\" using 16:5 with points title \"weightedNormalizedDistance\"\n",
						dataFile));
		writer.write("set ylabel \"Multiplier\"\n");
		writer.write(String.format(
				"plot \"%s\" using 16:6 with points title \"Multiplier\"\n",
				dataFile));
		writer.write("set ylabel \"Unroll\"\n");
		writer.write(String.format(
				"plot \"%s\" using 16:7 with points title \"Unroll\"\n",
				dataFile));
		writer.write("set ylabel \"AllocationStrategy\"\n");
		writer.write(String
				.format("plot \"%s\" using 16:8 with points title \"AllocationStrategy\"\n",
						dataFile));
		writer.write("set ylabel \"InternalStorage\"\n");
		writer.write(String
				.format("plot \"%s\" using 16:9 with points title \"InternalStorage\"\n",
						dataFile));
		writer.write("set ylabel \"Partitioning\"\n");
		writer.write(String.format(
				"plot \"%s\" using 16:10 with points title \"Partitioning\"\n",
				dataFile));
		writer.write("set ylabel \"Unboxing\"\n");
		writer.write(String.format(
				"plot \"%s\" using 16:11 with points title \"Unboxing\"\n",
				dataFile));
		writer.write("set ylabel \"Removal\"\n");
		writer.write(String.format(
				"plot \"%s\" using 16:12 with points title \"Removal\"\n",
				dataFile));
		writer.write("set ylabel \"Fusion\"\n");
		writer.write(String.format(
				"plot \"%s\" using 16:13 with points title \"Fusion\"\n",
				dataFile));
		writer.close();
		return plotfile;
	}

	private static File createPlotFile2(File dir, String appName,
			String dataFile) throws IOException {
		String title = TimeLogProcessor.getTitle(appName);
		boolean pdf = true;
		String extensionRemoved = dataFile.split("\\.")[0];
		File plotfile = new File(dir, String.format("%s.plt", extensionRemoved));
		FileWriter writer = new FileWriter(plotfile, false);
		if (pdf) {
			writer.write("set terminal pdf enhanced color\n");
			writer.write(String.format("set output \"%s%s.pdf\"\n", title,
					extensionRemoved));
		} else {
			writer.write("set terminal postscript eps enhanced color\n");
			writer.write(String.format("set output \"%s.eps\"\n", title));
		}
		writer.write(String.format("set title \"%s\"\n", title));
		writer.write("set grid\n");
		writer.write("#set yrange [0:*]\n");
		writer.write("set ylabel \"Time Improvements(ms)\"\n");
		writer.write("set xlabel \"Distance\"\n");
		writer.write(String.format(
				"plot \"%s\" using 2:16 with points title \"Distance\"\n",
				dataFile));
		writer.write("set xlabel \"weightedDist\"\n");
		writer.write(String.format(
				"plot \"%s\" using 3:16 with points title \"weightedDist\"\n",
				dataFile));
		writer.write("set xlabel \"NormalizedDistance\"\n");
		writer.write(String
				.format("plot \"%s\" using 4:16 with points title \"NormalizedDistance\"\n",
						dataFile));
		writer.write("set xlabel \"weightedNormalizedDistance\"\n");
		writer.write(String
				.format("plot \"%s\" using 5:16 with points title \"weightedNormalizedDistance\"\n",
						dataFile));
		writer.write("set xlabel \"Multiplier\"\n");
		writer.write(String.format(
				"plot \"%s\" using 6:16 with points title \"Multiplier\"\n",
				dataFile));
		writer.write("set xlabel \"Unroll\"\n");
		writer.write(String.format(
				"plot \"%s\" using 7:16 with points title \"Unroll\"\n",
				dataFile));
		writer.write("set xlabel \"AllocationStrategy\"\n");
		writer.write(String
				.format("plot \"%s\" using 8:16 with points title \"AllocationStrategy\"\n",
						dataFile));
		writer.write("set xlabel \"InternalStorage\"\n");
		writer.write(String
				.format("plot \"%s\" using 9:16 with points title \"InternalStorage\"\n",
						dataFile));
		writer.write("set xlabel \"Partitioning\"\n");
		writer.write(String.format(
				"plot \"%s\" using 10:16 with points title \"Partitioning\"\n",
				dataFile));
		writer.write("set xlabel \"Unboxing\"\n");
		writer.write(String.format(
				"plot \"%s\" using 11:16 with points title \"Unboxing\"\n",
				dataFile));
		writer.write("set xlabel \"Removal\"\n");
		writer.write(String.format(
				"plot \"%s\" using 12:16 with points title \"Removal\"\n",
				dataFile));
		writer.write("set xlabel \"Fusion\"\n");
		writer.write(String.format(
				"plot \"%s\" using 13:16 with points title \"Fusion\"\n",
				dataFile));
		writer.close();
		return plotfile;
	}

	private static void writeHeader(OutputStreamWriter writer) {
		try {
			writer.write(String.format("%.7s", "cfgID"));
			writer.write("\t\t");
			writer.write(String.format("%.7s", "distance"));
			writer.write("\t\t");
			writer.write(String.format("%.7s", "weiDist"));
			writer.write("\t\t");
			writer.write(String.format("%.7s", "normDist"));
			writer.write("\t\t");
			writer.write(String.format("%.7s", "WeiNormDist"));
			writer.write("\t\t");
			writer.write(String.format("%.7s", "Mul"));
			writer.write("\t\t");
			writer.write(String.format("%.7s", "Unroll"));
			writer.write("\t\t");
			writer.write(String.format("%.7s", "Alocation"));
			writer.write("\t\t");
			writer.write(String.format("%.7s", "InternStorege"));
			writer.write("\t\t");
			writer.write(String.format("%.7s", "Partition"));
			writer.write("\t\t");
			writer.write(String.format("%.7s", "Unbox"));
			writer.write("\t\t");
			writer.write(String.format("%.7s", "Removal"));
			writer.write("\t\t");
			writer.write(String.format("%.7s", "Fusion"));
			writer.write("\t\t");
			writer.write(String.format("%.7s", "t1"));
			writer.write("\t\t");
			writer.write(String.format("%.7s", "t2"));
			writer.write("\t\t");
			writer.write(String.format("%.7s", "t1-t2"));
			// writer.write("\t\t");
			writer.flush();
		} catch (IOException e) {

		}
	}

	private boolean needTocompare(double t1, double t2) {
		if (needCompareAll)
			return true;

		final double diff = t1_t2;
		double min;
		double max;
		if (t2 > t1) {
			min = t1;
			max = t2;
		} else {
			min = t2;
			max = t1;
		}
		if (max == Double.POSITIVE_INFINITY)
			return false;
		if (max / min > diff)
			return true;
		return false;
	}

	public enum ParamType {

		PARTITION {
			@Override
			public ImmutableSet<String> variablePrefixList() {
				return ImmutableSet.of("worker");
			}

			@Override
			public int weight() {
				return PARTITIONW;
			}
		},
		REMOVAL_STRATEGY {
			@Override
			public ImmutableSet<String> variablePrefixList() {
				return ImmutableSet.of("remove");
			}

			@Override
			public int weight() {
				return REMOVAL_STRATEGYW;
			}
		},
		FUSION_STRATEGY {
			@Override
			public ImmutableSet<String> variablePrefixList() {
				return ImmutableSet.of("fuse");
			}

			@Override
			public int weight() {
				return FUSION_STRATEGYW;
			}
		},
		UNBOXING_STRATEGY {
			@Override
			public ImmutableSet<String> variablePrefixList() {
				return ImmutableSet.of("unbox");
			}

			@Override
			public int weight() {
				return UNBOXING_STRATEGYW;
			}
		},
		ALLOCATION_STRATEGY {
			@Override
			public ImmutableSet<String> variablePrefixList() {
				return ImmutableSet.of("Group");
			}

			@Override
			public int weight() {
				return ALLOCATION_STRATEGYW;
			}
		},
		INTERNAL_STORAGE_STRATEGY {
			@Override
			public ImmutableSet<String> variablePrefixList() {
				return ImmutableSet.of("InternalArrayish");
			}

			@Override
			public int weight() {
				return INTERNAL_STORAGE_STRATEGYW;
			}
		},
		// EXTERNAL_STORAGE_STRATEGY {
		// @Override
		// public ImmutableSet<String> variablePrefixList() {
		// return ImmutableSet.of("ExternalArrayish", "UseDoubleBuffers");
		// }
		// },
		MULTIPLIER {
			@Override
			public ImmutableSet<String> variablePrefixList() {
				return ImmutableSet.of("multiplier");
			}

			@Override
			public int weight() {
				return MULTIPLIERW;
			}
		},
		UNROLL_CORE {
			@Override
			public ImmutableSet<String> variablePrefixList() {
				return ImmutableSet.of("UnrollCore");
			}

			@Override
			public int weight() {
				return UNROLL_COREW;
			}
		};

		public abstract ImmutableSet<String> variablePrefixList();
		public abstract int weight();
	}

	/**
	 * Contains summary of a StreamJit application's full tuning parameters.
	 * Classifies the parameters into {@link ParamType}s and keep the counts.
	 */
	public static class FullParameterSummary {
		final int totalCount;
		final ImmutableMap<ParamType, Integer> parmTypeCount;

		public FullParameterSummary(String appName) {
			this(ConfigurationUtils.readConfiguration(appName, 1));
		}

		FullParameterSummary(Configuration config) {
			checkState(config != null, "Null configuration passed");
			Map<String, Parameter> paramMap = config.getParametersMap();
			totalCount = paramMap.size();
			parmTypeCount = count(paramMap);
		}

		private ImmutableMap<ParamType, Integer> count(
				Map<String, Parameter> paramMap) {
			Map<ParamType, Integer> localParmTypeCount = new HashMap<ParamType, Integer>();
			for (ParamType p : ParamType.values()) {
				localParmTypeCount.put(p, 0);
			}

			for (Parameter p : paramMap.values())
				classify(p, localParmTypeCount);

			return ImmutableMap.copyOf(localParmTypeCount);
		}

		void classify(Parameter param,
				Map<ParamType, Integer> localParmTypeCount) {
			for (ParamType p : ParamType.values()) {
				for (String prefix : p.variablePrefixList()) {
					if (param.getName().startsWith(prefix)) {
						int count = localParmTypeCount.get(p);
						localParmTypeCount.put(p, ++count);
						return;
					}
				}
			}
			// System.err.println("No matches to " + param.toString());
		}
	}

	final static int PARTITIONW;
	final static int REMOVAL_STRATEGYW;
	final static int FUSION_STRATEGYW;
	final static int UNBOXING_STRATEGYW;
	final static int ALLOCATION_STRATEGYW;
	final static int INTERNAL_STORAGE_STRATEGYW;
	final static int MULTIPLIERW;
	final static int UNROLL_COREW;
	final static boolean needCompareAll;
	final static double t1_t2;

	private static Properties loadProperties() {
		Properties prop = new Properties();
		InputStream input = null;
		try {
			input = new FileInputStream("cfgAnalyze.properties");
			prop.load(input);
		} catch (IOException ex) {
			System.err.println("Failed to load options.properties");
		}
		return prop;
	}

	static {
		Properties prop = loadProperties();
		PARTITIONW = Integer.parseInt(prop.getProperty("PARTITIONW"));
		REMOVAL_STRATEGYW = Integer.parseInt(prop
				.getProperty("REMOVAL_STRATEGYW"));
		FUSION_STRATEGYW = Integer.parseInt(prop
				.getProperty("FUSION_STRATEGYW"));
		UNBOXING_STRATEGYW = Integer.parseInt(prop
				.getProperty("UNBOXING_STRATEGYW"));
		ALLOCATION_STRATEGYW = Integer.parseInt(prop
				.getProperty("ALLOCATION_STRATEGYW"));
		INTERNAL_STORAGE_STRATEGYW = Integer.parseInt(prop
				.getProperty("INTERNAL_STORAGE_STRATEGYW"));
		MULTIPLIERW = Integer.parseInt(prop.getProperty("MULTIPLIERW"));
		UNROLL_COREW = Integer.parseInt(prop.getProperty("UNROLL_COREW"));
		needCompareAll = Boolean.parseBoolean(prop
				.getProperty("needCompareAll"));
		t1_t2 = Double.parseDouble(prop.getProperty("t1_t2"));
	}
}
