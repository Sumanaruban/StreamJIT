package edu.mit.streamjit.impl.distributed.common;

import com.google.common.collect.ImmutableList;

import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionInfo;

/**
 * {@link BoundaryChannel} wraps a {@link Buffer} that crosses over the
 * machine(node) boundary. Potentially these buffers may depend on a I/O
 * communication method to send or receive data with the peer node. As
 * {@link BoundaryChannel}s are meant to run on a independent I/O thread, the
 * wrapped {@link Buffer} must be thread safe.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 28, 2013
 */
public interface BoundaryChannel {

	String name();

	/**
	 * @return {@link Runnable} that does all IO communication and send
	 *         data(stream tuples) to other node (or receive from other node).
	 */
	Runnable getRunnable();

	ImmutableList<Object> getUnprocessedData();

	Connection getConnection();

	ConnectionInfo getConnectionInfo();

	/**
	 * Interface that represents input channels.
	 */
	public interface BoundaryInputChannel extends BoundaryChannel {

		/**
		 * <p>
		 * No more data will be sent by corresponding
		 * {@link BoundaryOutputChannel}. So stop receiving.
		 * </p>
		 * <p>
		 * There may be data in middle, specifically in intermediate buffers
		 * like kernel's socket buffer. Its implementations responsibility to
		 * receive all data those are in middle and try to fill the actual
		 * buffer. But in some case, after Stop() is called, actual buffer might
		 * be full forever to write and there might be even more data in the
		 * intermediate kernel buffer. In this case, before exiting, extraBuffer
		 * should be filled with all unconsumed data in the kernel buffer.
		 * 
		 * <p>
		 * Based on the type argument, implementation may treat uncounsumed data
		 * differently
		 * <ol>
		 * <li>1 - No extraBuffer. Wait and push all received data in to the
		 * actual buffer. May be used at final draining.
		 * <li>2 - Create extra buffer and put all unconsumed data. This can be
		 * send to the controller as draindata. May be used at intermediate
		 * draining.
		 * <li>3 - Discard all unconsumed data. This is useful, if we don't care
		 * about the data while tuning for performance.
		 * </p>
		 * 
		 * @param type
		 *            : Can be 1, 2 or 3. rest are illegal.
		 */
		void stop(int type);

		/**
		 * Receive data from other node.
		 */
		void receiveData();

		/**
		 * @return unconsumed data after Stop() is called. Returning buffer may
		 *         or may not be thread safe. Or null also can be returned if
		 *         there is no data.
		 */
		Buffer getExtraBuffer();
	}

	/**
	 * Interface that represents output channels.
	 */
	public interface BoundaryOutputChannel extends BoundaryChannel {

		/**
		 * Stop sending. If isFinal is true, send all data in the buffer before
		 * stop. Else just stop and leave the buffer as it is. i.e., call
		 * stop(true) for final stop. call stop(false) for onlinetuning's
		 * intermediate stop.
		 * 
		 * @param isFinal
		 */
		void stop(boolean isFinal);

		/**
		 * Send data to other node.
		 */
		void sendData();
	}
}
