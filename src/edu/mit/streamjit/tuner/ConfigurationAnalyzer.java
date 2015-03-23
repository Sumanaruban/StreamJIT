package edu.mit.streamjit.tuner;

import static com.google.common.base.Preconditions.checkState;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.Parameter;
import edu.mit.streamjit.impl.distributed.common.Utils;
import edu.mit.streamjit.tuner.ComparisionSummary.ParamClassSummary;
import edu.mit.streamjit.util.ConfigurationUtils;

public class ConfigurationAnalyzer {

	public static void main(String[] args) throws IOException {
		ConfigurationAnalyzer ca = new ConfigurationAnalyzer("FMRadioCore");
		ca.Analyze();
	}

	private final String appName;

	private final FullParameterSummary fullParameterSummary;

	public ConfigurationAnalyzer(String appName) {
		verifyPath(ConfigurationUtils.configDir, appName);
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

	private void Analyze() throws IOException {
		SqliteAdapter sqlite = connectDB(appName);
		int maxTuneCount = getTotalResults(sqlite);
		int start = 1;
		int end = maxTuneCount; // inclusive
		List<ComparisionSummary> comparitionSummaryList = new ArrayList<>();
		for (int i = start; i < end; i++) {
			double t1 = getRunningTime(sqlite, appName, i);
			double t2 = getRunningTime(sqlite, appName, i + 1);
			if (needTocompare(t1, t2)) {
				Configuration cfg1 = ConfigurationUtils.readConfiguration(
						appName, i);
				Configuration cfg2 = ConfigurationUtils.readConfiguration(
						appName, i + 1);
				cfg1 = ConfigurationUtils.addConfigPrefix(cfg1,
						new Integer(i).toString());
				cfg2 = ConfigurationUtils.addConfigPrefix(cfg2, new Integer(
						i + 1).toString());
				ComparisionSummary sum = ComparisionSummary.compare(cfg1, cfg2,
						t1, t2, fullParameterSummary);
				comparitionSummaryList.add(sum);
			}
		}
		print(comparitionSummaryList,
				Utils.fileWriter(appName, "cfgAnalize.txt"));
	}

	private void print(List<ComparisionSummary> comparitionSummaryList,
			OutputStreamWriter osWriter) throws IOException {
		osWriter.write(String.format(
				"Total parameters in the configuration = %d\n",
				fullParameterSummary.totalCount));
		for (ComparisionSummary s : comparitionSummaryList) {
			osWriter.write("\n-------------------------------------------------------\n");
			osWriter.write(s + "\n");
			osWriter.write(String.format("t1=%f, t2=%f\n", s.t1, s.t2));
			osWriter.write(s.distanceSummary() + "\n");
			for (ParamClassSummary ps : s.ParamClassSummaryList())
				osWriter.write(ps + "\n");
		}
		osWriter.flush();
	}

	private boolean needTocompare(double t1, double t2) {
		final double diff = 1.2;
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
				return 1;
			}
		},
		REMOVAL_STRATEGY {
			@Override
			public ImmutableSet<String> variablePrefixList() {
				return ImmutableSet.of("remove");
			}

			@Override
			public int weight() {
				return 1;
			}
		},
		FUSION_STRATEGY {
			@Override
			public ImmutableSet<String> variablePrefixList() {
				return ImmutableSet.of("fuse");
			}

			@Override
			public int weight() {
				return 1;
			}
		},
		UNBOXING_STRATEGY {
			@Override
			public ImmutableSet<String> variablePrefixList() {
				return ImmutableSet.of("unbox");
			}

			@Override
			public int weight() {
				return 1;
			}
		},
		ALLOCATION_STRATEGY {
			@Override
			public ImmutableSet<String> variablePrefixList() {
				return ImmutableSet.of("Group");
			}

			@Override
			public int weight() {
				return 1;
			}
		},
		INTERNAL_STORAGE_STRATEGY {
			@Override
			public ImmutableSet<String> variablePrefixList() {
				return ImmutableSet.of("InternalArrayish");
			}

			@Override
			public int weight() {
				return 1;
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
				return 1;
			}
		},
		UNROLL_CORE {
			@Override
			public ImmutableSet<String> variablePrefixList() {
				return ImmutableSet.of("UnrollCore");
			}

			@Override
			public int weight() {
				return 1;
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
}
