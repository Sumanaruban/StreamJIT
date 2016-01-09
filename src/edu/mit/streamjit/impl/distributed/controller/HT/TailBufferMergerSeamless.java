package edu.mit.streamjit.impl.distributed.controller.HT;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;

import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.ConcurrentArrayBuffer;
import edu.mit.streamjit.tuner.EventTimeLogger;

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

	protected int duplicateOutputIndex = 0;

	protected final Phaser switchBufPhaser = new Phaser();

	protected abstract void merge();

	/**
	 * This method will be called by TailBufferMergerSeamless to inform its sub
	 * classes about the termination. Sub classes must release all resources
	 * that they acquired when this method is called.
	 */
	protected abstract void stoping();

	private final EventTimeLogger eLogger;

	protected static final boolean debug = false;

	public TailBufferMergerSeamless(Buffer tailBuffer, EventTimeLogger eLogger) {
		this.tailBuffer = tailBuffer;
		this.stopCalled = false;
		bufProvider = new BufferProvider1();
		this.appInstBufInfos = new ConcurrentHashMap<>();
		switchBufPhaser.bulkRegister(2);
		this.eLogger = eLogger;
	}

	public Runnable getRunnable() {
		return new Runnable() {
			@Override
			public void run() {
				waitForCurBuf();
				while (!stopCalled) {
					copyToTailBuffer(curBuf);
					if (merge) {
						eLogger.bEvent("merge time");
						merge();
						eLogger.eEvent("merge time");
					}
				}
				stoping();
			}
		};
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

	public void appInstStopped(int appInstId) {
		// TODO: Busy waiting. Consider using phaser.
		// while (merge);
		switchBufPhaser.arriveAndAwaitAdvance();
		if (prevBuf == null)
			throw new IllegalStateException("prevBuf != null expected.");

		AppInstBufInfo a = appInstBufInfos.get(prevBuf);
		if (a == null)
			throw new IllegalStateException("No AppInstance found for prevBuf");

		if (a.appInstId() != appInstId)
			throw new IllegalStateException(
					String.format(
							"AppInstIds mismatch. ID of the prevBuf = %d, ID passed = %d.",
							a.appInstId(), appInstId));

		int redudantOutput = a.ht.tailCounter.count() - duplicateOutputIndex;
		event(String.format("skipCount = %d, redudantOutput = %d", a.skipCount,
				redudantOutput));
		appInstBufInfos.remove(prevBuf);
		bufProvider.reclaimedBuffer(a.tailBuf());
		prevBuf = null;
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

	protected void switchBuffers(int skipCount) {
		if (debug)
			event("swb");
		if (skipCount > 0)
			skip(nextBuf, skipCount);
		prevBuf = curBuf;
		curBuf = nextBuf;
		nextBuf = null;
		merge = false;
		switchBufPhaser.arrive();
	}

	protected void copyFully(final Buffer readBuffer) {
		while (readBuffer.size() > 0)
			copyToTailBuffer(readBuffer);
	}

	protected void skip(final Buffer readBuffer, int skipCount) {
		int expected = skipCount;
		int min1, min2, readBufSize;
		if (debug)
			System.err.println(String.format(
					"Skip: BufferSize=%d, skipCount=%d", readBuffer.size(),
					skipCount));
		while (expected > 0) {
			readBufSize = readBuffer.size();
			if (readBufSize == 0) {
				try {
					Thread.sleep(50);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				continue;
			}
			min1 = Math.min(readBufSize, expected);
			min2 = Math.min(min1, intermediateArray.length);
			expected -= readBuffer.read(intermediateArray, 0, min2);
		}

		if (expected != 0)
			throw new IllegalStateException(
					String.format(
							"expected = %d. The variable expected must be 0.",
							expected));
	}

	protected void event(String eventName) {
		eLogger.logEvent(eventName, 0);
	}

	static class BufferProvider1 implements BufferProvider {

		private final Queue<Buffer> freeBufferQueue;

		private final Object[] intArray = new Object[bufSize];

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
			if (debug)
				System.err.println("reclaimedBuffer Buf Size is = "
						+ buf.size());
			while (buf.size() > 0) {
				buf.read(intArray, 0, intArray.length);
			}
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
