package edu.mit.streamjit.impl.common;

import edu.mit.streamjit.impl.blob.Buffer;

/**
 * Counts number of items read from the {@link #buffer}. Writing into the
 * {@link #buffer} has no effect in counting.
 *
 * @author sumanan
 * @since 11 Nov, 2015
 */
public class BufferReadCounter implements Buffer, Counter {

	private final Buffer buffer;
	private int count;

	/**
	 * Instead of making the variable count volatile, lets do a piggyback
	 * synchronization.
	 */
	private volatile boolean piggybackSync = true;

	public BufferReadCounter(Buffer buffer) {
		this.buffer = buffer;
	}

	@Override
	public int count() {
		piggybackSync = !piggybackSync;
		return count;
	}

	@Override
	public Object read() {
		Object o = buffer.read();
		if (o != null)
			count++;
		return o;
	}

	@Override
	public int read(Object[] data, int offset, int length) {
		int read = buffer.read(data, offset, length);
		count += read;
		return read;
	}

	@Override
	public boolean readAll(Object[] data) {
		boolean ret = buffer.readAll(data);
		if (ret)
			count += data.length;
		return ret;
	}

	@Override
	public boolean readAll(Object[] data, int offset) {
		boolean ret = buffer.readAll(data, offset);
		if (ret)
			count += (data.length - offset);
		return ret;
	}

	@Override
	public boolean write(Object t) {
		return buffer.write(t);
	}

	@Override
	public int write(Object[] data, int offset, int length) {
		return buffer.write(data, offset, length);
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
