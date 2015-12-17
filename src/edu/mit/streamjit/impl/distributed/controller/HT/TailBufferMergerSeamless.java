package edu.mit.streamjit.impl.distributed.controller.HT;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.ConcurrentArrayBuffer;

/**
 * @author sumanan
 * @since 17 Dec, 2015
 */
public abstract class TailBufferMergerSeamless implements TailBufferMerger {

	/**
	 * TODO: Determine this buffer size correctly.
	 */
	protected static final int bufSize = 100_000;

	protected final BufferProvider1 bufProvider;

	public TailBufferMergerSeamless() {
		bufProvider = new BufferProvider1();
	}

	@Override
	public BufferProvider bufferProvider() {
		return bufProvider;
	}

	class BufferProvider1 implements BufferProvider {

		private final Queue<Buffer> freeBufferQueue;

		BufferProvider1() {
			freeBufferQueue = createFreeBufferQueue();
		}

		private Queue<Buffer> createFreeBufferQueue() {
			Queue<Buffer> q = new ArrayBlockingQueue<>(2);
			q.add(new ConcurrentArrayBuffer(bufSize));
			q.add(new ConcurrentArrayBuffer(bufSize));
			return q;
		}

		@Override
		public Buffer newBuffer() {
			Buffer b = freeBufferQueue.poll();
			if (b == null)
				throw new IllegalStateException("freeBufferQueue is empty.");
			return b;
		}

		public void reclaimedBuffer(Buffer buf) {
			freeBufferQueue.add(buf);
		}
	}
}
