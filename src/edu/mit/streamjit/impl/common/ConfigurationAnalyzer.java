package edu.mit.streamjit.impl.common;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.impl.common.Configuration.FloatParameter;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.common.Configuration.Parameter;
import edu.mit.streamjit.impl.common.Configuration.SwitchParameter;
import edu.mit.streamjit.tuner.SqliteAdapter;
import edu.mit.streamjit.util.ConfigurationUtils;

public class ConfigurationAnalyzer {

	public static void main(String[] args) throws IOException {
		ConfigurationAnalyzer ca = new ConfigurationAnalyzer(
				"NestedSplitJoinCore");
		ca.Analyze(100);
		// ca.compare(3, 4);

		// System.out.println(ca.getRunningTime("NestedSplitJoinCore", 3));
	}

	private final String cfgDir;

	private final String appName;

	List<Integer> bestConfigurations;

	/**
	 * Path of the directory which contains app's configuration in sub
	 * directory.
	 * 
	 * <pre>
	 * confgDirectory
	 * 			|
	 * 			------>appName1
	 * 			|
	 * 			------>appName2
	 * 			|
	 * 			------>
	 * </pre>
	 */

	public ConfigurationAnalyzer(String appName) {
		verifyPath(ConfigurationUtils.configDir, appName);
		bestConfigurations = new LinkedList<>();
		this.appName = appName;
		this.cfgDir = String.format("%s%s%s", appName, File.separator,
				ConfigurationUtils.configDir);
	}

	private static void compare(FloatParameter p1, FloatParameter p2) {
		float val1 = p1.getValue();
		float val2 = p2.getValue();
		if (val1 == val2)
			System.out.println(String.format("%s: p1 = p2. value = %f",
					p1.getName(), val1));
		if (val1 > val2)
			System.out.println(String.format("%s: p1 > p2. %f > %f",
					p1.getName(), val1, val2));
		else
			System.out.println(String.format("%s: p1 < p2. %f < %f",
					p1.getName(), val1, val2));
	}

	private void compare(Integer first, Integer second) {
		Configuration cfg1 = readcoConfiguration(first);
		Configuration cfg2 = readcoConfiguration(second);
		for (Entry<String, Parameter> en : cfg1.getParametersMap().entrySet()) {
			Parameter p1 = en.getValue();
			Parameter p2 = cfg2.getParameter(en.getKey());
			if (p2 == null)
				throw new IllegalStateException(String.format(
						"No parameter %s in configuration2", en.getKey()));
			if (p1.getClass() == Configuration.IntParameter.class)
				compare((IntParameter) p1, (IntParameter) p2);
			else if (p1.getClass() == Configuration.FloatParameter.class)
				compare((FloatParameter) p1, (FloatParameter) p2);
			else if (p1.getClass() == Configuration.SwitchParameter.class)
				compare((SwitchParameter<?>) p1, (SwitchParameter<?>) p2);
			else
				System.out.println(String.format(
						"Parameter class %s is not handled.", p1.getClass()
								.getName()));

		}
	}

	/*
	 * Any way to avoid code duplication in compare(IntParameter p1,
	 * IntParameter p2) and compare(FloatParameter p1, FloatParameter p2)?
	 */
	/**
	 * 
	 * @param p1
	 * @param p2
	 */
	private static void compare(IntParameter p1, IntParameter p2) {
		int val1 = p1.getValue();
		int val2 = p2.getValue();
		if (val1 == val2)
			System.out.println(String.format("%s: p1 = p2. value = %d",
					p1.getName(), val1));
		if (val1 > val2)
			System.out.println(String.format("%s: p1 > p2. %d > %d",
					p1.getName(), val1, val2));
		else
			System.out.println(String.format("%s: p1 < p2. %d < %d",
					p1.getName(), val1, val2));
	}

	private static <T1, T2> void compare(SwitchParameter<T1> p1,
			SwitchParameter<T2> p2) {
		Class<T1> type1 = p1.getGenericParameter();
		Class<T2> type2 = p2.getGenericParameter();
		assert type1 == type2;
		T1 val1 = p1.getValue();
		T2 val2 = p2.getValue();

		if (val1.equals(val2))
			System.out.println(String.format(
					"%s - same values - p1 = %s, p2 = %s. Universe:%s",
					p1.getName(), val1, val2, p1.getUniverse()));
		else
			System.out.println(String.format(
					"%s - different values - p1 = %s, p2 = %s. Universe:%s",
					p1.getName(), val1, val2, p1.getUniverse()));
	}

	private static SqliteAdapter connectDB(String appName) {
		SqliteAdapter sqlite = new SqliteAdapter();
		sqlite.connectDB(appName);
		return sqlite;
	}

	private static double getRunningTime(String appName, int round) {
		String dbPath = String.format("%s%s%s", appName, File.separator,
				appName);
		SqliteAdapter sqlite = connectDB(dbPath);
		ResultSet result = sqlite.executeQuery(String.format(
				"SELECT * FROM result WHERE id=%d", round));

		String runtime = "1000000000";
		try {
			runtime = result.getString("time");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return Double.parseDouble(runtime);
	}

	private int getTotalResults() {
		String dbPath = String.format("%s%s%s", appName, File.separator,
				appName);
		SqliteAdapter sqlite = connectDB(dbPath);
		ResultSet result = sqlite.executeQuery("SELECT COUNT(*) FROM result");

		String runtime = "0";
		try {
			runtime = result.getString("COUNT(*)");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return Integer.parseInt(runtime);
	}

	private Configuration readcoConfiguration(Integer cfgNo) {
		String cfg = String.format("%s%s%d_%s.cfg", cfgDir, File.separator,
				cfgNo, appName);
		return ConfigurationUtils.readConfiguration(cfg);
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
		int maxTuneCount = getTotalResults();
		int start = 1;
		int end = maxTuneCount;
		List<ComparisionSummary> comparitionSummaryList = new ArrayList<>();
		for (int i = start; i < end - 1; i++) {
			double t1 = getRunningTime(appName, i);
			double t2 = getRunningTime(appName, i + 1);
			if (needTocompare(t1, t2)) {
				// System.out.println("Comparing...");
				ComparisionSummary sum = new ComparisionSummary(i, i + 1, t1,
						t2);
				int diffCount = compare2(i, i + 1, sum);
				// System.out.println(diffCount);
				comparitionSummaryList.add(sum);
			}
		}
		print(comparitionSummaryList,
				edu.mit.streamjit.impl.distributed.common.Utils.fileWriter(
						appName, "cfgAnalize.txt"));
	}

	private void print(List<ComparisionSummary> comparitionSummaryList,
			OutputStreamWriter osWriter) throws IOException {
		ParamMapSummary paramMapSum = new ParamMapSummary();
		osWriter.write(String.format(
				"Total parameters in the configuration = %d\n",
				paramMapSum.totalCount));
		for (ComparisionSummary s : comparitionSummaryList) {
			List<ParamSummary> paramSummaryList = new ArrayList<>();
			osWriter.write("\n-------------------------------------------------------\n");
			osWriter.write(s + "\n");
			int totalDiffs = s.toatalDiffCount;
			double per1 = ((double) totalDiffs * 100) / paramMapSum.totalCount;
			osWriter.write(String.format(
					"TotalParams=%d,TotalDiffs=%d,Per=%f\n",
					paramMapSum.totalCount, totalDiffs, per1));
			for (ParamType p : ParamType.values()) {
				int totalCount = paramMapSum.parmTypeCount.get(p);
				int diffCount = s.diffCount.get(p);
				ParamSummary ps = new ParamSummary(p, totalCount, diffCount);
				paramSummaryList.add(ps);
				// System.out.println(ps);
			}
			Collections.sort(paramSummaryList);
			for (ParamSummary ps : paramSummaryList)
				osWriter.write(ps + "\n");
		}
		osWriter.flush();
	}

	private class ParamSummary implements Comparable<ParamSummary> {
		final ParamType p;
		final int totalCount;
		final int diffCount;
		final Double per;

		ParamSummary(ParamType p, int totalCount, int diffCount) {
			this.p = p;
			this.totalCount = totalCount;
			this.diffCount = diffCount;
			this.per = ((double) diffCount * 100) / totalCount;
		}

		public String toString() {
			return String.format("\t%s:tot=%d,diff=%d,per=%f", p, totalCount,
					diffCount, per);
		}

		@Override
		public int compareTo(ParamSummary o) {
			return o.per.compareTo(per);
		}
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
		if (max / min > diff)
			return true;
		return false;
	}

	private int compare2(Integer first, Integer second, ComparisionSummary sum) {
		Configuration cfg1 = readcoConfiguration(first);
		Configuration cfg2 = readcoConfiguration(second);
		int diffCount = 0;
		Map<String, Parameter> paramMap = cfg1.getParametersMap();
		// System.out.println("ParamMap size = " + paramMap.size());
		for (Entry<String, Parameter> en : paramMap.entrySet()) {
			Parameter p1 = en.getValue();
			Parameter p2 = cfg2.getParameter(en.getKey());
			if (p2 == null)
				throw new IllegalStateException(String.format(
						"No parameter %s in configuration2", en.getKey()));
			if (!p1.equals(p2)) {
				diffCount++;
				sum.diff(p1);
			}

		}
		sum.toatalDiffCount = diffCount;
		return diffCount;
	}

	public enum ParamType {

		PARTITION {
			@Override
			public ImmutableSet<String> variablePrefixList() {
				return ImmutableSet.of("worker");
			}
		},
		REMOVAL_STRATEGY {
			@Override
			public ImmutableSet<String> variablePrefixList() {
				return ImmutableSet.of("remove");
			}
		},
		FUSION_STRATEGY {
			@Override
			public ImmutableSet<String> variablePrefixList() {
				return ImmutableSet.of("fuse");
			}
		},
		UNBOXING_STRATEGY {
			@Override
			public ImmutableSet<String> variablePrefixList() {
				return ImmutableSet.of("unbox");
			}
		},
		ALLOCATION_STRATEGY {
			@Override
			public ImmutableSet<String> variablePrefixList() {
				return ImmutableSet.of("Group");
			}
		},
		INTERNAL_STORAGE_STRATEGY {
			@Override
			public ImmutableSet<String> variablePrefixList() {
				return ImmutableSet.of("InternalArrayish");
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
		},
		UNROLL_CORE {
			@Override
			public ImmutableSet<String> variablePrefixList() {
				return ImmutableSet.of("UnrollCore");
			}
		};
		public abstract ImmutableSet<String> variablePrefixList();
	}

	public class ParamMapSummary {
		int totalCount;
		final Map<ParamType, Integer> parmTypeCount;
		ParamMapSummary() {
			parmTypeCount = new HashMap<>();
			initilizeparmTypeCount();
			count();
		}

		private void initilizeparmTypeCount() {
			for (ParamType p : ParamType.values()) {
				parmTypeCount.put(p, 0);
			}
		}

		private void count() {
			Configuration cfg1 = readcoConfiguration(1);
			Map<String, Parameter> paramMap = cfg1.getParametersMap();
			totalCount = paramMap.size();
			for (Parameter p : paramMap.values())
				diff(p);
		}

		void diff(Parameter param) {
			for (ParamType p : ParamType.values()) {
				for (String prefix : p.variablePrefixList()) {
					if (param.getName().startsWith(prefix)) {
						int count = parmTypeCount.get(p);
						parmTypeCount.put(p, ++count);
						return;
					}
				}
			}
			// System.err.println("No matches to " + param.toString());
		}
	}

	private class ComparisionSummary {
		final int firstCfg;
		final int secondCfg;
		final double t1;
		final double t2;
		int toatalDiffCount = 0;
		Map<ParamType, Integer> diffCount;
		public ComparisionSummary(final int firstCfg, final int secondCfg,
				final double t1, final double t2) {
			this.firstCfg = firstCfg;
			this.secondCfg = secondCfg;
			this.t1 = t1;
			this.t2 = t2;
			initilizeDiffCount();
		}

		private void initilizeDiffCount() {
			diffCount = new HashMap<>();
			for (ParamType p : ParamType.values()) {
				diffCount.put(p, 0);
			}
		}

		void diff(Parameter param) {
			for (ParamType p : ParamType.values()) {
				for (String prefix : p.variablePrefixList()) {
					if (param.getName().startsWith(prefix)) {
						int count = diffCount.get(p);
						diffCount.put(p, ++count);
						return;
					}
				}
			}
			// System.err.println("No matches to " + param.toString());
		}

		@Override
		public String toString() {
			return String.format("Cfg1=%d, Cfg2=%d, t1=%f, t2=%f", firstCfg,
					secondCfg, t1, t2);
		}
	}
}
