package edu.mit.streamjit.impl.distributed.node;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.Buffer;

/**
 * Provides dynamic buffer backed by a buffer implementation which is passed as
 * an argument to {@link #getBuffer(Class, List, int, int)} method. <b> Use a
 * {@link DynamicBufferManager} per blob ( i.e., one to one mapping between a
 * blob and a DynamicBufferManager). Do not use same instance of
 * {@link DynamicBufferManager} to create buffers for multiple blobs or multiple
 * {@link DynamicBufferManager}s to make buffer for a single blob. Doing this
 * will affect the deadlock detection algorithm and ultimately results either
 * infinite buffer growth or unsolvable deadlock. </b>
 * 
 * @author sumanan
 * @since Mar 10, 2014
 * 
 */
public final class DynamicBufferManager {

	/**
	 * inputs of the blob in which this object is bound to.
	 */
	private final Set<Token> inputs;

	/**
	 * outputs of the blob in which this object is bound to.
	 */
	private final Set<Token> outputs;

	/**
	 * keeps track of all buffers created for a particular blob. We need to
	 * track this list to determine whether there is an actual deadlock or this
	 * blob is faster than all down blobs.
	 */
	List<Buffer> buffers;

	public DynamicBufferManager(Blob blob) {
		inputs = blob.getInputs();
		outputs = blob.getOutputs();
		buffers = new ArrayList<>();
	}

	/**
	 * Makes and returns a dynamic buffer backed by an instance of bufferClass.
	 * Passed bufferClass ( a concrete implementation of {@link Buffer}) must
	 * have an constructor which takes the capacity of the new buffer as an
	 * argument.
	 * 
	 * @param Name
	 *            : Name for this buffer. Just for documentation purpose.
	 *            Token.toString() may be passed where the token is a token of
	 *            the input/output edge of a blob.
	 * @param bufferClass
	 *            : Any concrete implementation of {@link Buffer}.
	 * @param initialArguments
	 *            : Constructor arguments. : Initial capacity of the buffer.
	 * @param capacityPos
	 *            : the position of size parameter in the bufferClass.
	 * @return : A dynamic buffer backed by an instance of bufferClass.
	 */
	public Buffer getBuffer(Token t, Class<? extends Buffer> bufferClass,
			List<?> initialArguments, int initialCapacity, int capacityPos) {

		if (!(outputs.contains(t) || inputs.contains(t)))
			throw new IllegalAccessError(
					String.format(
							"%s is not related to the blob in which this dynamic buffer manager is bound to",
							t.toString()));;

		Buffer buf = new DynamicBuffer(this, t.toString(), bufferClass,
				initialArguments, initialCapacity, capacityPos);
		buffers.add(buf);
		return buf;
	}
}
