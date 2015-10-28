package edu.mit.streamjit.impl.distributed;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.ConcurrentArrayBuffer;

/**
 * Reads output from two {@link AppInstance}s, merge them, and pass it to user.
 * 
 * Actually {@link AppInstanceManager} maintains {@link TailChannel}, that
 * writes the output of corresponding {@link AppInstance} to its output buffer.
 * 
 * @author sumanan
 * @since 28 Oct, 2015
 */
public class TailBufferMergerStateless {

	/**
	 * TODO: Determine this buffer size correctly.
	 */
	private final int bufSize = 100_000;

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

	private volatile boolean switchBuf;

	private final Queue<Buffer> freeBufferQueue;

	private final Map<Buffer, AppInstBufInfo> appInstBufInfos;

	TailBufferMergerStateless(Buffer tailBuffer) {
		this.tailBuffer = tailBuffer;
		this.stopCalled = false;
		this.appInstBufInfos = new ConcurrentHashMap<>();
		freeBufferQueue = createFreeBufferQueue();
	}

	private Queue<Buffer> createFreeBufferQueue() {
		Queue<Buffer> q = new ArrayBlockingQueue<>(2);
		q.add(new ConcurrentArrayBuffer(bufSize));
		q.add(new ConcurrentArrayBuffer(bufSize));
		return q;
	}

	public Runnable getRunnable() {
		return new Runnable() {
			@Override
			public void run() {
				while (!stopCalled) {
					copyToTailBuffer(curBuf);
					if (switchBuf) {
						switchBuffers();
					}
				}
			}
		};
	}

	public Buffer registerAppInst(int appInstId, int skipCount) {
		Buffer b = freeBufferQueue.poll();
		if (b == null)
			throw new IllegalStateException("freeBufferQueue is empty.");
		AppInstBufInfo a = new AppInstBufInfo(appInstId, b, skipCount);
		appInstBufInfos.put(b, a);
		if (curBuf == null)
			curBuf = b;
		else {
			if (nextBuf != null)
				throw new IllegalStateException("nextBuf == null expected.");
			nextBuf = b;
		}
		return b;
	}

	public void unregisterAppInst(int appInstId) {
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

		freeBufferQueue.add(a.buf);
		appInstBufInfos.remove(prevBuf);
		prevBuf = null;
	}

	private void switchBuffers() {
		if (prevBuf != null)
			throw new IllegalStateException("prevBuf == null expected.");
		if (nextBuf == null)
			throw new IllegalStateException("nextBuf != null expected.");
		AppInstBufInfo a = appInstBufInfos.get(nextBuf);
		skip(a.buf, a.skipCount);
		prevBuf = curBuf;
		curBuf = nextBuf;
		nextBuf = null;
		switchBuf = false;
	}

	public void switchBuf(int appInstId) {
		if (switchBuf)
			throw new IllegalStateException("switchBuf==false expected.");
		switchBuf = true;
	}

	public void stop() {
		stopCalled = true;
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
		int min1, min2;
		while (expected > 0) {
			min1 = Math.min(readBuffer.size(), expected);
			min2 = Math.min(min1, intermediateArray.length);
			expected -= readBuffer.read(intermediateArray, 0, min2);
		}

		if (expected != 0)
			throw new IllegalStateException(String.format(
					"expected = %d. variable expected must be 0.", expected));
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
}
