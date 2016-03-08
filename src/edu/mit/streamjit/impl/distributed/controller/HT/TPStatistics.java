package edu.mit.streamjit.impl.distributed.controller.HT;

/**
 * @author sumanan
 * @since 8 Mar, 2016
 */
public interface TPStatistics {

	public void cfgStarted(int appInstId);

	public void cfgEnded(int appInstId);

	void stop();

	void newThroughput(double throughput);

}