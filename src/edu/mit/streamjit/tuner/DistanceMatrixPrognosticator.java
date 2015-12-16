package edu.mit.streamjit.tuner;

import java.io.IOException;
import java.io.OutputStreamWriter;

import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.impl.distributed.common.Utils;
import edu.mit.streamjit.impl.distributed.controller.ConfigurationManager.NewConfiguration;
import edu.mit.streamjit.tuner.ConfigurationAnalyzer.FullParameterSummary;
import edu.mit.streamjit.tuner.ConfigurationAnalyzer.ParamType;

/**
 * Calculates a weighted distance between the current configuration and the
 * previous configuration and decides whether to accept or reject the current
 * configuration.
 * 
 * @author sumanan
 * @since 23 Mar, 2015
 */
public class DistanceMatrixPrognosticator implements
		ConfigurationPrognosticator {

	FullParameterSummary fullParameterSummary;
	private final OutputStreamWriter writer;

	Configuration prevConfig = null;
	Configuration curConfig = null;
	Configuration bestConfig = null;

	double prevConfigTime = -1;
	double curConfigTime = -1;
	double bestConfigTime = Integer.MAX_VALUE;

	ComparisionSummary prevCurSummary = null;
	ComparisionSummary bestCurSummary = null;

	/**
	 * If many {@link ConfigurationPrognosticator}s are used, only one should
	 * write the time to the writer.
	 */
	private final boolean needWriteTime;

	public DistanceMatrixPrognosticator(String appName) {
		this(Utils.fileWriter(appName, "DistanceMatrix.txt"), true);
	}

	public DistanceMatrixPrognosticator(OutputStreamWriter osWriter,
			boolean needWriteTime) {
		this.writer = osWriter;
		this.needWriteTime = needWriteTime;
		writeHeader(writer);
		if (needWriteTime)
			writeTimeHeader(osWriter);
	}

	@Override
	public boolean prognosticate(NewConfiguration newConfig) {
		if (newConfig.configuration == null)
			throw new IllegalArgumentException("Null Configuration");
		if (fullParameterSummary == null) {
			fullParameterSummary = new FullParameterSummary(
					newConfig.configuration);
			curConfig = newConfig.configuration;
			prevConfig = newConfig.configuration;
			bestConfig = newConfig.configuration;
			return true;
		}
		prevConfig = curConfig;
		prevConfigTime = curConfigTime;
		curConfig = newConfig.configuration;

		// prevCurSummary = ComparisionSummary.compare(prevConfig, curConfig,
		// fullParameterSummary);
		bestCurSummary = ComparisionSummary.compare(bestConfig, curConfig,
				fullParameterSummary);
		writeSummary(writer, bestCurSummary);
		return decide(bestCurSummary);
	}

	private boolean decide(ComparisionSummary sum) {
		StringBuilder s = new StringBuilder();
		boolean accept = true;

		if (sum.normalizedDistant(ParamType.MULTIPLIER) > 0.4) {
			s.append("1");
			accept = false;
		}
		if (sum.normalizedDistant(ParamType.UNROLL_CORE) > 0.4) {
			s.append("2");
			accept = false;
		}
		if (sum.normalizedDistant(ParamType.ALLOCATION_STRATEGY) > 0.4) {
			s.append("3");
			accept = false;
		}
		if (sum.normalizedDistant(ParamType.INTERNAL_STORAGE_STRATEGY) > 0.4) {
			s.append("4");
			accept = false;
		}
		if (sum.normalizedDistant(ParamType.PARTITION) > 0.4) {
			s.append("5");
			accept = false;
		}
		if (sum.normalizedDistant(ParamType.UNBOXING_STRATEGY) > 0.4) {
			s.append("6");
			accept = false;
		}
		if (sum.normalizedDistant(ParamType.REMOVAL_STRATEGY) > 0.4) {
			s.append("7");
			accept = false;
		}
		if (sum.normalizedDistant(ParamType.FUSION_STRATEGY) > 0.4) {
			s.append("8");
			accept = false;
		}

		try {
			writer.write(String.format("%s\t\t",
					accept ? "Acptd" : s.toString()));
		} catch (IOException e) {

		}
		return !Options.prognosticate || accept;
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
			writer.write(String.format("%.7s", "A/R")); // Accepted or Rejected.
			writer.write("\t\t");
			writer.flush();
		} catch (IOException e) {

		}
	}

	private static void writeSummary(OutputStreamWriter osWriter,
			ComparisionSummary sum) {
		if (sum == null)
			return;
		try {
			osWriter.write(String.format("%6s\t\t", sum.firstCfg));
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
		} catch (IOException e) {

		}
	}

	@Override
	public void time(double time) {
		curConfigTime = time;
		if (needWriteTime) {
			// writeTime(writer, prevConfigTime, curConfigTime);
			writeTime(writer, bestConfigTime, curConfigTime);
		}
		if (time > 0 && bestConfigTime > time) {
			bestConfig = curConfig;
			bestConfigTime = time;
		}
	}

	private static void writeTime(OutputStreamWriter osWriter, double t1,
			double t2) {
		try {
			osWriter.write(String.format("%.2f\t\t", t1));
			osWriter.write(String.format("%.2f\n", t2));
			osWriter.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void writeTimeHeader(OutputStreamWriter writer) {
		try {
			writer.write(String.format("%.7s", "t1"));
			writer.write("\t\t");
			writer.write(String.format("%.7s", "t2"));
			writer.write("\n");
			writer.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
