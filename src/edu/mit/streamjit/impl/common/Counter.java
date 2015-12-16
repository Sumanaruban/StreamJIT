package edu.mit.streamjit.impl.common;

import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.distributed.controller.HT.ThroughputPrinter;

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
	 * Counter's current value. A {@link Buffer} may be written and read. So the
	 * count value that is returned by this method can be either write count or
	 * read count, based on the implementation.
	 * 
	 * @return Counter's current value.
	 */
	public int count();
}
