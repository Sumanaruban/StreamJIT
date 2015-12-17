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

/**
 * {@link TailBufferMerger} for seam-less reconfiguration process. Always skips
 * skioCount amount of data from the new graph and switch to the new graph only
 * after the old graph is drained.
 * 
 * @author sumanan
 * @since 28 Oct, 2015
 */
public class TailBufferMergerStateless implements TailBufferMerger {

	/**
	 * TODO: Determine this buffer size correctly.
	 */
	private static final int bufSize = 100_000;

	/**
	 * Final output buffer that is created from {@link Output}<O> output.
	 */
	private final Buffer tailBuffer;

	private Buffer prevBuf;
	private Buffer curBuf;
	private Buffer nextBuf;

	/**
	 * {@link Buffer}'s bulk reading and writing interface methods expect an
	 * Object array to be passed.
	 */
	private final Object[] intermediateArray = new Object[bufSize];

	private volatile boolean stopCalled;

	private volatile boolean merge;

	private final CountDownLatch latch = new CountDownLatch(1);

	private final Phaser switchBufPhaser = new Phaser();

	private final Map<Buffer, AppInstBufInfo> appInstBufInfos;

	private final BufferProvider1 bufProvider;

	public TailBufferMergerStateless(Buffer tailBuffer) {
		this.tailBuffer = tailBuffer;
		this.stopCalled = false;
		this.appInstBufInfos = new ConcurrentHashMap<>();
		switchBufPhaser.bulkRegister(2);
		bufProvider = new BufferProvider1();
	}

	public Runnable getRunnable() {
		return new Runnable() {
			@Override
			public void run() {
				waitForCurBuf();
				while (!stopCalled) {
					copyToTailBuffer(curBuf);
					if (merge) {
						switchBuffers();
					}
				}
			}
		};
	}

	/**
	 * wait for curBuf to be set.
	 */
	private void waitForCurBuf() {
		try {
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void newAppInst(HeadTail ht, int skipCount) {
		Buffer b = ht.tailBuffer;
		AppInstBufInfo a = new AppInstBufInfo(ht.appInstId, b, skipCount);
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

		if (a.appInstId != appInstId)
			throw new IllegalStateException(
					String.format(
							"AppInstIds mismatch. ID of the prevBuf = %d, ID passed = %d.",
							a.appInstId, appInstId));

		appInstBufInfos.remove(prevBuf);
		bufProvider.reclaimedBuffer(a.buf);
		prevBuf = null;
	}

	private void switchBuffers() {
		if (prevBuf != null)
			throw new IllegalStateException("prevBuf == null expected.");
		if (nextBuf == null)
			throw new IllegalStateException("nextBuf != null expected.");
		AppInstBufInfo a = appInstBufInfos.get(nextBuf);
		copyFully(curBuf);
		skip(a.buf, a.skipCount);
		prevBuf = curBuf;
		curBuf = nextBuf;
		nextBuf = null;
		merge = false;
		switchBufPhaser.arrive();
	}

	private void copyFully(final Buffer readBuffer) {
		while (readBuffer.size() > 0)
			copyToTailBuffer(readBuffer);
	}

	public void startMerge() {
		if (merge)
			throw new IllegalStateException("merge==false expected.");
		merge = true;
	}

	public void stop() {
		stopCalled = true;
	}

	@Override
	public BufferProvider bufferProvider() {
		return bufProvider;
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
	private void copyToTailBuffer(final Buffer readBuffer) {
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

	private void skip(final Buffer readBuffer, int skipCount) {
		int expected = skipCount;
		int min1, min2, readBufSize;
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

	private static class AppInstBufInfo {
		private final int appInstId;
		private final Buffer buf;
		private final int skipCount;

		AppInstBufInfo(int appInstId, Buffer buf, int skipCount) {
			this.appInstId = appInstId;
			this.buf = buf;
			this.skipCount = skipCount;
		}
	}

	private class BufferProvider1 implements BufferProvider {

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