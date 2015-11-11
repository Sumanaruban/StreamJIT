package edu.mit.streamjit.impl.common;

import edu.mit.streamjit.impl.blob.Buffer;

/**
 * Counts number of items written into the {@link #buffer}. Reading from the
 * {@link #buffer} has no effect in counting.
 * 
 * @author sumanan
 * @since 30 Oct, 2015
 */
public class BufferWriteCounter implements Buffer, Counter {

	private final Buffer buffer;
	private int count;

	/**
	 * Instead of making the variable count volatile, lets do a piggyback
	 * synchronization.
	 */
	private volatile boolean piggybackSync = true;

	public BufferWriteCounter(Buffer buffer) {
		this.buffer = buffer;
	}

	@Override
	public int count() {
		piggybackSync = !piggybackSync;
		return count;
	}

	@Override
	public Object read() {
		return buffer.read();
	}

	@Override
	public int read(Object[] data, int offset, int length) {
		return buffer.read(data, offset, length);
	}

	@Override
	public boolean readAll(Object[] data) {
		return buffer.readAll(data);
	}

	@Override
	public boolean readAll(Object[] data, int offset) {
		return buffer.readAll(data, offset);
	}

	@Override
	public boolean write(Object t) {
		boolean ret = buffer.write(t);
		if (ret)
			count++;
		return ret;
	}

	@Override
	public int write(Object[] data, int offset, int length) {
		int written = buffer.write(data, offset, length);
		count += written;
		return written;
	}

	@Override
	public int size() {
		return buffer.size();
	}

	@Override
	public int capacity() {
		return buffer.capacity();
	}
}
