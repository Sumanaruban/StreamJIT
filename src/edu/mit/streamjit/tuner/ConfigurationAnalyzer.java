package edu.mit.streamjit.tuner;

import static com.google.common.base.Preconditions.checkState;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.FloatParameter;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.common.Configuration.Parameter;
import edu.mit.streamjit.impl.common.Configuration.SwitchParameter;
import edu.mit.streamjit.impl.distributed.common.Utils;
import edu.mit.streamjit.util.ConfigurationUtils;
import edu.mit.streamjit.util.Pair;

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
				ComparisionSummary sum = new ComparisionSummary(i, i + 1, t1,
						t2);
				int diffCount = compare2(i, i + 1, sum);
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
			List<ParamClassSummary> paramSummaryList = new ArrayList<>();
			osWriter.write("\n-------------------------------------------------------\n");
			osWriter.write(s + "\n");
			int totalDiffs = s.toatalDiffCount;
			double per1 = ((double) totalDiffs * 100)
					/ fullParameterSummary.totalCount;
			osWriter.write(String.format(
					"TotalParams=%d,TotalDiffs=%d,Per=%f\n",
					fullParameterSummary.totalCount, totalDiffs, per1));
			for (ParamType p : ParamType.values()) {
				int totalCount = fullParameterSummary.parmTypeCount.get(p);
				int diffCount = s.diffCount.get(p);
				ParamClassSummary ps = new ParamClassSummary(p, totalCount,
						diffCount, s.normalizedDistant(p));
				paramSummaryList.add(ps);
				// System.out.println(ps);
			}
			Collections.sort(paramSummaryList);
			for (ParamClassSummary ps : paramSummaryList)
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

	private int compare2(Integer first, Integer second, ComparisionSummary sum) {
		Configuration cfg1 = ConfigurationUtils.readConfiguration(appName,
				first);
		Configuration cfg2 = ConfigurationUtils.readConfiguration(appName,
				second);
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
				sum.diff(p1, p2);
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

	public static class ParamClassSummary implements
			Comparable<ParamClassSummary> {
		final ParamType p;
		final int totalCount;
		final int diffCount;
		final Double per;
		final double distance;

		ParamClassSummary(ParamType p, int totalCount, int diffCount,
				double distance) {
			this.p = p;
			this.totalCount = totalCount;
			this.diffCount = diffCount;
			this.per = ((double) diffCount * 100) / totalCount;
			this.distance = distance;
		}

		public String toString() {
			return String.format("\t%s:tot=%d,diff=%d,per=%f,dist=%f", p,
					totalCount, diffCount, per, distance);
		}

		@Override
		public int compareTo(ParamClassSummary o) {
			return o.per.compareTo(per);
		}
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

	private class ComparisionSummary {
		final int firstCfg;
		final int secondCfg;
		final double t1;
		final double t2;
		int toatalDiffCount = 0;
		Map<ParamType, Integer> diffCount;
		Map<ParamType, ParameterClass> ParameterClassMap;
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
			ParameterClassMap = new HashMap<>();
			for (ParamType p : ParamType.values()) {
				diffCount.put(p, 0);
				ParameterClassMap.put(p, new ParameterClass(p));
			}
		}

		void diff(Parameter param1, Parameter param2) {
			checkState(param1.getName().equals(param1.getName()),
					"Different parameters");
			for (ParamType p : ParamType.values()) {
				for (String prefix : p.variablePrefixList()) {
					if (param1.getName().startsWith(prefix)) {
						int count = diffCount.get(p);
						diffCount.put(p, ++count);
						ParameterClass pc = ParameterClassMap.get(p);
						pc.addParameterPair(param1, param2);
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

		double getDistance(ParamType p) {
			ParameterClass pc = ParameterClassMap.get(p);
			return pc.distant();
		}

		double normalizedDistant(ParamType p) {
			ParameterClass pc = ParameterClassMap.get(p);
			return pc.normalizedDistant();
		}
	}

	private class ParameterClass {

		private final ParamType type;

		private final Set<Pair<Parameter, Parameter>> parameters;

		ParameterClass(ParamType type) {
			this.type = type;
			parameters = new HashSet<>();
		}

		void addParameterPair(Parameter p1, Parameter p2) {
			parameters.add(new Pair<>(p1, p2));
		}

		double normalizedDistant() {
			return distant() / fullParameterSummary.parmTypeCount.get(type);
		}

		double distant() {
			double dist = 0;
			for (Pair<Parameter, Parameter> pair : parameters) {
				if (pair.first.getClass() == IntParameter.class)
					dist += distant((IntParameter) pair.first,
							(IntParameter) pair.second);
				else if (pair.first.getClass() == FloatParameter.class)
					dist += distant((FloatParameter) pair.first,
							(FloatParameter) pair.second);
				else if (pair.first.getClass() == SwitchParameter.class)
					dist += distant((SwitchParameter) pair.first,
							(SwitchParameter) pair.second);
				else
					System.out.println("Not supported for the moment:"
							+ pair.first.getClass());
			}
			return dist;
		}

		private double distant(IntParameter first, IntParameter second) {
			double diff = Math.abs(first.getValue() - second.getValue());
			int range = first.getMax() - first.getMin();
			return diff / range;
		}

		private double distant(FloatParameter first, FloatParameter second) {
			double diff = Math.abs(first.getValue() - second.getValue());
			double range = first.getMax() - first.getMin();
			return diff / range;
		}

		private <T> double distant(SwitchParameter<T> first,
				SwitchParameter<T> second) {
			T val1 = first.getValue();
			T val2 = second.getValue();
			if (val1.equals(val2))
				return 0;
			else
				return 1;
		}
	}
}
