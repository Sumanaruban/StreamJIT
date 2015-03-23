package edu.mit.streamjit.tuner;

import static com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.ImmutableList;

import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.FloatParameter;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.common.Configuration.Parameter;
import edu.mit.streamjit.impl.common.Configuration.SwitchParameter;
import edu.mit.streamjit.tuner.ConfigurationAnalyzer.FullParameterSummary;
import edu.mit.streamjit.tuner.ConfigurationAnalyzer.ParamType;
import edu.mit.streamjit.util.ConfigurationUtils;
import edu.mit.streamjit.util.Pair;

public class ComparisionSummary {

	public static ComparisionSummary compare(Integer first, Integer second,
			double tfirst, double tsecond,
			FullParameterSummary fullParameterSummary, String appName) {
		ComparisionSummary sum = new ComparisionSummary(first, second, tfirst,
				tsecond, fullParameterSummary);
		Configuration cfg1 = ConfigurationUtils.readConfiguration(appName,
				first);
		Configuration cfg2 = ConfigurationUtils.readConfiguration(appName,
				second);
		int diffCount = 0;
		Map<String, Parameter> paramMap = cfg1.getParametersMap();
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
		return sum;
	}

	final FullParameterSummary fullParameterSummary;
	final int firstCfg;
	final int secondCfg;
	final double t1;
	final double t2;
	private int toatalDiffCount = 0;
	private Map<ParamType, Integer> diffCount;
	private Map<ParamType, ParameterClass> ParameterClassMap;

	private ComparisionSummary(final int firstCfg, final int secondCfg,
			final double t1, final double t2,
			FullParameterSummary fullParameterSummary) {
		this.firstCfg = firstCfg;
		this.secondCfg = secondCfg;
		this.t1 = t1;
		this.t2 = t2;
		this.fullParameterSummary = fullParameterSummary;
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

	private void diff(Parameter param1, Parameter param2) {
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

	public int diffCount(ParamType type) {
		return diffCount.get(type);
	}

	public int diffCount() {
		return toatalDiffCount;
	}

	public ParamClassSummary paramClassSummary(ParamType p) {
		int totalCount = fullParameterSummary.parmTypeCount.get(p);
		int diffCount = diffCount(p);
		ParamClassSummary ps = new ParamClassSummary(p, totalCount, diffCount,
				normalizedDistant(p));
		return ps;
	}

	public ImmutableList<ParamClassSummary> ParamClassSummaryList() {
		List<ParamClassSummary> paramSummaryList = new ArrayList<>();
		for (ParamType p : ParamType.values()) {
			paramSummaryList.add(paramClassSummary(p));
		}
		Collections.sort(paramSummaryList);
		return ImmutableList.copyOf(paramSummaryList);
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

	public static class ParamClassSummary implements
			Comparable<ParamClassSummary> {
		private final ParamType p;
		private final int totalCount;
		private final int diffCount;
		private final Double per;
		private final double distance;

		private ParamClassSummary(ParamType p, int totalCount, int diffCount,
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

		public ParamType paramType() {
			return p;
		}

		public int totalCount() {
			return totalCount;
		}

		public int diffCount() {
			return diffCount;
		}

		public Double percentage() {
			return per;
		}

		public double distance() {
			return distance;
		}
	}
}
