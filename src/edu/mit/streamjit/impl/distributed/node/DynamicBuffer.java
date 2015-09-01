package edu.mit.streamjit.impl.distributed.node;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.util.ConstructorSupplier;
import edu.mit.streamjit.util.ReflectionUtils;

/**
 * Dynamically increases supplied buffer capacity in order to avoid dead locks.
 * Actually creates a new instance of the supplied buffer and copy the data from
 * old buffer to new buffer. A decorator pattern for {@link Buffer}.
 * 
 * <p>
 * Determining whether buffer fullness is due to deadlock situation or the
 * current blob is executing on a faster node than the down stream blob is
 * little tricky.
 * </p>
 * 
 * <p>
 * TODO: {@link ConstructorSupplier} can be reused here to instantiate the
 * buffer instances if we make {@link ConstructorSupplier}.arguments not final.
 * </p>
 * 
 * <p>
 * TODO: Possible performance bug during read() due to volatile buffer variable
 * and the need for acquire readLock for every single reading. Any way to
 * improve this???. splitjoin1 show 30-40% performance overhead when uses
 * {@link DynamicBuffer}.
 * 
 * @author sumanan
 * @since Mar 10, 2014
 * 
 */
class DynamicBuffer implements Buffer {

	/**
	 * 
	 */
	private final DynamicBufferManager dynamicBufferManager;

	private final String name;

	/**
	 * Minimum time gap between the last successful write and the current time
	 * in order to consider the option of doubling the buffer
	 */
	private final long gap;

	/**
	 * Every successful write operation should update this time.
	 */
	private long lastWrittenTime;

	/**
	 * When the algorithm detects there are some progress ( May be after some
	 * expansions), this flag is set to stop any future expansions. This is to
	 * prevent infinity buffer growth.
	 */
	private boolean expandable;

	/**
	 * Read lock should be acquired at every single read where as write lock
	 * only when switching the buffer from old to new.
	 */
	private ReadWriteLock rwlock;

	private final List<?> initialArguments;
	private final int initialCapacity;
	private final int capacityPos;
	private final Constructor<? extends Buffer> cons;

	/**
	 * TODO: Volatileness may severely affects the reading performance.
	 */
	private volatile Buffer buffer;

	/**
	 * If this buffer used as drain buffer and if the down blob is drained due
	 * to the DrainDeadlockHandler's call, we might get a deadlock situation
	 * when expandable become false and the buffer is full. Throws exception if
	 * that case occurred.
	 */
	private final boolean isDrainBuffer;

	DynamicBuffer(DynamicBufferManager dynamicBufferManager, String name,
			Class<? extends Buffer> bufferClass, List<?> initialArguments,
			int initialCapacity, int capacityPos) {
		this(dynamicBufferManager, name, constructor(bufferClass,
				initialArguments), initialArguments, initialCapacity,
				capacityPos);
	}

	public DynamicBuffer(String name, Class<? extends Buffer> bufferClass,
			List<?> initialArguments, int initialCapacity, int capacityPos) {
		this(null, name, bufferClass, initialArguments, initialCapacity,
				capacityPos);
	}

	public DynamicBuffer(String name, Constructor<? extends Buffer> cons,
			List<?> initialArguments, int initialCapacity, int capacityPos) {
		this(null, name, cons, initialArguments, initialCapacity, capacityPos);
	}

	DynamicBuffer(DynamicBufferManager dynamicBufferManager, String name,
			Constructor<? extends Buffer> cons, List<?> initialArguments,
			int initialCapacity, int capacityPos) {
		this.dynamicBufferManager = dynamicBufferManager;
		this.name = name;
		this.initialArguments = initialArguments;
		this.initialCapacity = initialCapacity;
		this.capacityPos = capacityPos;
		this.isDrainBuffer = (dynamicBufferManager == null);
		this.cons = cons;
		this.buffer = getNewBuffer(initialCapacity);
		this.gap = 2_000_000_000; // 2s
		expandable = true;
		rwlock = new ReentrantReadWriteLock();
		lastWrittenTime = 0;
	}

	private List<?> getArguments(int newCapacity) {
		List<Object> newArgs = new ArrayList<>(initialArguments.size());
		for (int i = 0; i < initialArguments.size(); i++) {
			if (i == capacityPos)
				newArgs.add(newCapacity);
			else
				newArgs.add(initialArguments.get(i));
		}
		return newArgs;
	}

	private Buffer getNewBuffer(int newCapacity) {
		Buffer buffer;
		try {
			buffer = cons.newInstance(getArguments(newCapacity).toArray());
			return buffer;
		} catch (InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public Object read() {
		rwlock.readLock().lock();
		Object o = buffer.read();
		rwlock.readLock().unlock();
		return o;
	}

	@Override
	public int read(Object[] data, int offset, int length) {
		rwlock.readLock().lock();
		int ret = buffer.read(data, offset, length);
		rwlock.readLock().unlock();
		return ret;
	}

	@Override
	public boolean readAll(Object[] data) {
		rwlock.readLock().lock();
		boolean ret = buffer.readAll(data);
		rwlock.readLock().unlock();
		return ret;
	}

	@Override
	public boolean readAll(Object[] data, int offset) {
		rwlock.readLock().lock();
		boolean ret = buffer.readAll(data, offset);
		rwlock.readLock().unlock();
		return ret;
	}

	@Override
	public boolean write(Object t) {
		boolean ret = buffer.write(t);
		if (!ret)
			writeFailed();
		else if (lastWrittenTime != 0)
			lastWrittenTime = 0;
		return ret;
	}

	@Override
	public int write(Object[] data, int offset, int length) {
		int written = buffer.write(data, offset, length);
		if (written == 0)
			writeFailed();
		else if (lastWrittenTime != 0)
			lastWrittenTime = 0;
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

	private void writeFailed() {
		if (areAllFull())
			return;

		if (!expandable) {
			if (isDrainBuffer)
				throw new IllegalStateException(String.format(
						"writeFailed, but the buffer cannot be expanded. Buffer size is %d. "
								+ "possible deadlock.", size()));
			return;
		}

		if (lastWrittenTime == 0) {
			lastWrittenTime = System.nanoTime();
			return;
		}

		if (System.nanoTime() - lastWrittenTime > gap && expandable) {
			doubleBuffer();
		}
	}
	private boolean areAllFull() {
		if (dynamicBufferManager == null)
			return false;
		for (Buffer b : this.dynamicBufferManager.buffers) {
			if (b.size() != b.capacity())
				return false;
		}
		return true;
	}

	private void doubleBuffer() {
		int newCapacity = 2 * buffer.capacity();
		if (newCapacity > 1024 * initialCapacity) {
			expandable = false;
			return;
		}
		System.out
				.println(String
						.format("%s : Doubling the buffer: initialCapacity - %d, newCapacity - %d",
								name, initialCapacity, newCapacity));
		Buffer newBuf = getNewBuffer(newCapacity);
		rwlock.writeLock().lock();
		final int size = buffer.size();
		// TODO: copying is done one by one. Any block level copying?
		for (int i = 0; i < size; i++) {
			newBuf.write(buffer.read());
		}

		if (buffer.size() != 0) {
			throw new IllegalStateException(
					"Buffter is not empty after copying all data");
		}
		this.buffer = newBuf;
		lastWrittenTime = 0;
		rwlock.writeLock().unlock();
	}

	private static Constructor<? extends Buffer> constructor(
			Class<? extends Buffer> bufferClass, List<?> initialArguments) {
		Constructor<? extends Buffer> con = null;
		try {
			con = ReflectionUtils
					.findConstructor(bufferClass, initialArguments);
		} catch (NoSuchMethodException e1) {
			e1.printStackTrace();
		}
		return con;
	}
}