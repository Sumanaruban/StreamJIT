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

	public static class EmptyTPStatistics implements TPStatistics {

		@Override
		public void cfgStarted(int appInstId) {
		}

		@Override
		public void cfgEnded(int appInstId) {
		}

		@Override
		public void stop() {
		}

		@Override
		public void newThroughput(double throughput) {
		}
	}
}