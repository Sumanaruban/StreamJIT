package edu.mit.streamjit.impl.distributed.controller.HT;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Phaser;

import edu.mit.streamjit.impl.blob.Buffer;

/**
 * {@link TailBufferMerger} for seam-less reconfiguration process. Always skips
 * skioCount amount of data from the new graph and switch to the new graph only
 * after the old graph is drained.
 * 
 * @author sumanan
 * @since 28 Oct, 2015
 */
public class TailBufferMergerStateless extends TailBufferMergerSeamless {

	private Buffer prevBuf;
	private Buffer curBuf;
	private Buffer nextBuf;

	private final Phaser switchBufPhaser = new Phaser();

	private final Map<Buffer, AppInstBufInfo> appInstBufInfos;

	public TailBufferMergerStateless(Buffer tailBuffer) {
		super(tailBuffer);
		this.appInstBufInfos = new ConcurrentHashMap<>();
		switchBufPhaser.bulkRegister(2);
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
}
