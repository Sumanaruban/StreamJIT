package edu.mit.streamjit.impl.common;

import edu.mit.streamjit.impl.distributed.ThroughputPrinter;

/**
 * Interface to count number of data items received or sent.
 * {@link ThroughputPrinter} needs this to measure the throughput of an
 * StreamJIT app.
 * 
 * @author sumanan
 * @since 30 Oct, 2015
 */
public interface Counter {

	/**
	 * @return Counter's current value.
	 */
	public int count();
}
