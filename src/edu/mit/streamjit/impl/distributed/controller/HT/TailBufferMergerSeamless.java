package edu.mit.streamjit.impl.distributed.controller.HT;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import edu.mit.streamjit.api.Output;
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

	/**
	 * Final output buffer that is created from {@link Output}<O> output.
	 */
	protected final Buffer tailBuffer;

	/**
	 * {@link Buffer}'s bulk reading and writing interface methods expect an
	 * Object array to be passed.
	 */
	protected final Object[] intermediateArray = new Object[bufSize];

	protected volatile boolean stopCalled;

	protected final CountDownLatch latch = new CountDownLatch(1);

	protected volatile boolean merge;

	protected Buffer prevBuf;
	protected Buffer curBuf;
	protected Buffer nextBuf;

	protected final Map<Buffer, AppInstBufInfo> appInstBufInfos;

	public TailBufferMergerSeamless(Buffer tailBuffer) {
		this.tailBuffer = tailBuffer;
		this.stopCalled = false;
		bufProvider = new BufferProvider1();
		this.appInstBufInfos = new ConcurrentHashMap<>();
	}

	@Override
	public BufferProvider bufferProvider() {
		return bufProvider;
	}

	public void stop() {
		stopCalled = true;
	}

	@Override
	public void newAppInst(HeadTail ht, int skipCount) {
		Buffer b = ht.tailBuffer;
		AppInstBufInfo a = new AppInstBufInfo(ht.appInstId, ht, skipCount);
		appInstBufInfos.put(b, a);
		if (curBuf == null) {
			curBuf = b;
			latch.countDown();
		} else {
			if (nextBuf != null)
				throw new IllegalStateException("nextBuf == null expected.");
			nextBuf = b;
		}
	}

	/**
	 * TODO: We can do few of assertion checks in side this method. But I'm
	 * avoiding this in order to keep the method simple and elegant. If any bug
	 * occurs, do assertion checks to ensure the
	 * {@link Buffer#read(Object[], int, int)}'s and
	 * {@link Buffer#write(Object[], int, int)}'s return values are as expected.
	 * 
	 * @param readBuffer
	 */
	protected void copyToTailBuffer(final Buffer readBuffer) {
		int size = Math.min(readBuffer.size(), intermediateArray.length);
		readBuffer.read(intermediateArray, 0, size);
		int written = 0;
		while (written < size) {
			written += tailBuffer.write(intermediateArray, written, size
					- written);
			// TODO: just for debugging. Remove this later.
			if (written != size)
				System.err
						.println("TailBufferMerger.copyToTailBuffer: Unexpected.");
		}
	}

	/**
	 * wait for curBuf to be set.
	 */
	protected void waitForCurBuf() {
		try {
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	static class BufferProvider1 implements BufferProvider {

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

	static class AppInstBufInfo {
		final HeadTail ht;
		public final int skipCount;

		AppInstBufInfo(int appInstId, HeadTail ht, int skipCount) {
			this.ht = ht;
			this.skipCount = skipCount;
		}

		Buffer tailBuf() {
			return ht.tailBuffer;
		}

		int appInstId() {
			return ht.appInstId;
		}
	}
}
