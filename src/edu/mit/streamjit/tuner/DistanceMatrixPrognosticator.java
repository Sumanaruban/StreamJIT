package edu.mit.streamjit.tuner;

import edu.mit.streamjit.impl.common.Configuration;

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

	@Override
	public boolean prognosticate(Configuration config) {
		return false;
	}

	@Override
	public void time(double time) {
	}
}
