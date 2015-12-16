package edu.mit.streamjit.tuner;

import java.io.IOException;
import java.io.OutputStreamWriter;

import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.distributed.common.Utils;
import edu.mit.streamjit.impl.distributed.controller.ConfigurationManager.NewConfiguration;
import edu.mit.streamjit.tuner.ComparisionSummary.ParamClassSummary;
import edu.mit.streamjit.tuner.ConfigurationAnalyzer.FullParameterSummary;

/**
 * Calculates a weighted distance between the current configuration and the
 * previous configuration and decides whether to accept or reject the current
 * configuration.
 * 
 * [30-03-2015] Renamed to old. This one writes the status in a non tabular
 * format.
 * 
 * @author sumanan
 * @since 23 Mar, 2015
 */
public class DistanceMatrixPrognosticatorOld implements
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

	public DistanceMatrixPrognosticatorOld(String appName) {
		this(Utils.fileWriter(appName, "DistanceMatrix.txt"));
	}

	public DistanceMatrixPrognosticatorOld(OutputStreamWriter osWriter) {
		this.writer = osWriter;
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
			writeHeader(writer, fullParameterSummary);
			return true;
		}
		prevConfig = curConfig;
		prevConfigTime = curConfigTime;
		curConfig = newConfig.configuration;

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
		if (summary == null)
			return;
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
