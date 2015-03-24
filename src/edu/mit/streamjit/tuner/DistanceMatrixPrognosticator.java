package edu.mit.streamjit.tuner;

import java.io.OutputStreamWriter;

import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.distributed.common.Utils;
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

	Configuration prevConfig = null;
	double prevConfigTime = -1;
	FullParameterSummary fullParameterSummary;

	private final OutputStreamWriter writer;

	public DistanceMatrixPrognosticator(String appName) {
		this(Utils.fileWriter(appName, "DistanceMatrix.txt"));
	}

	public DistanceMatrixPrognosticator(OutputStreamWriter osWriter) {
		this.writer = osWriter;
	}

	@Override
	public boolean prognosticate(Configuration config) {
		if (fullParameterSummary == null) {
			fullParameterSummary = new FullParameterSummary(config);
			prevConfig = config;
			return true;
		}

		ComparisionSummary summary = ComparisionSummary.compare(config,
				prevConfig, fullParameterSummary);
		return false;
	}

	@Override
	public void time(double time) {
		prevConfigTime = time;
	}
}
