package edu.mit.streamjit.tuner;

import java.io.IOException;
import java.io.OutputStreamWriter;

import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.distributed.common.Utils;
import edu.mit.streamjit.tuner.ComparisionSummary.ParamClassSummary;
import edu.mit.streamjit.tuner.ConfigurationAnalyzer.FullParameterSummary;

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
	double bestConfigTime = -1;

	ComparisionSummary prevCurSummary = null;
	ComparisionSummary bestCurSummary = null;

	public DistanceMatrixPrognosticator(String appName) {
		this(Utils.fileWriter(appName, "DistanceMatrix.txt"));
	}

	public DistanceMatrixPrognosticator(OutputStreamWriter osWriter) {
		this.writer = osWriter;
	}

	@Override
	public boolean prognosticate(Configuration config) {
		if (config == null)
			throw new IllegalArgumentException("Null Configuration");
		if (fullParameterSummary == null) {
			fullParameterSummary = new FullParameterSummary(config);
			curConfig = config;
			prevConfig = config;
			bestConfig = config;
			writeHeader(writer, fullParameterSummary);
			return true;
		}
		prevConfig = curConfig;
		prevConfigTime = curConfigTime;
		curConfig = config;

		prevCurSummary = ComparisionSummary.compare(prevConfig, curConfig,
				fullParameterSummary);
		bestCurSummary = ComparisionSummary.compare(bestConfig, curConfig,
				fullParameterSummary);
		return true;
	}

	private static void writeHeader(OutputStreamWriter osWriter,
			FullParameterSummary fullParameterSummary) {
		try {
			osWriter.write(String.format(
					"Total parameters in the configuration = %d\n",
					fullParameterSummary.totalCount));
			osWriter.flush();
		} catch (IOException e) {

		}
	}

	private static void writeSummary(OutputStreamWriter osWriter,
			ComparisionSummary summary, double t1, double t2) {
		try {
			osWriter.write("\n-------------------------------------------------------\n");
			osWriter.write(summary + "\n");
			osWriter.write(String.format("t1=%.0fms, t2=%.0fms\n", t1, t2));
			osWriter.write(summary.distanceSummary() + "\n");
			for (ParamClassSummary ps : summary.ParamClassSummaryList())
				osWriter.write(ps + "\n");
			osWriter.flush();
		} catch (IOException e) {

		}
	}

	@Override
	public void time(double time) {
		curConfigTime = time;
		writeSummary(writer, prevCurSummary, prevConfigTime, curConfigTime);
		writeSummary(writer, bestCurSummary, bestConfigTime, curConfigTime);
		if (time > 0 && bestConfigTime > time) {
			bestConfig = curConfig;
			bestConfigTime = time;
		}
	}
}
