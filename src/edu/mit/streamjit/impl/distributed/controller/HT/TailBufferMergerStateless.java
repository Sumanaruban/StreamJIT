package edu.mit.streamjit.impl.distributed.controller.HT;

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

	private final Phaser switchBufPhaser = new Phaser();

	public TailBufferMergerStateless(Buffer tailBuffer) {
		super(tailBuffer);
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

		appInstBufInfos.remove(prevBuf);
		bufProvider.reclaimedBuffer(a.tailBuf());
		prevBuf = null;
	}

	public void startMerge() {
		if (merge)
			throw new IllegalStateException("merge==false expected.");
		merge = true;
	}

	public void startMerge(int duplicateOutputIndex) {
	}

	private void switchBuffers() {
		if (prevBuf != null)
			throw new IllegalStateException("prevBuf == null expected.");
		if (nextBuf == null)
			throw new IllegalStateException("nextBuf != null expected.");
		AppInstBufInfo a = appInstBufInfos.get(nextBuf);
		copyFully(curBuf);
		skip(a.tailBuf(), a.skipCount);
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
}
