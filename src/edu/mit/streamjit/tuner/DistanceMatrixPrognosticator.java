package edu.mit.streamjit.tuner;

import edu.mit.streamjit.impl.common.Configuration;
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

	@Override
	public boolean prognosticate(Configuration config) {
		if (fullParameterSummary == null) {
			fullParameterSummary = new FullParameterSummary(config);
			prevConfig = config;
			return true;
		}

		ComparisionSummary summarry = ComparisionSummary.compare(config,
				prevConfig, fullParameterSummary);
		return false;
	}

	@Override
	public void time(double time) {
		prevConfigTime = time;
	}
}
