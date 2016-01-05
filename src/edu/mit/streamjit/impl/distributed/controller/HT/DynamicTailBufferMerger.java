package edu.mit.streamjit.impl.distributed.controller.HT;

import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.common.Counter;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryOutputChannel;
import edu.mit.streamjit.tuner.EventTimeLogger;

/**
 * {@link TailBufferMerger} that performs dynamic switching. That is, skips to
 * new graph whenever it catches the old graph's output.
 * 
 * @author sumanan
 * @since 12 Nov, 2015
 */
public class DynamicTailBufferMerger extends TailBufferMergerSeamless {

	private int duplicateOutputIndex;

	private BoundaryOutputChannel hcSeamless;

	public DynamicTailBufferMerger(Buffer tailBuffer, EventTimeLogger eLogger) {
		super(tailBuffer, eLogger);
	}

	public void startMerge() {
	}

	public void startMerge(int duplicateOutputIndex,
			BoundaryOutputChannel hcSeamless) {
		if (debug) {
			event("m..");
			System.err.println(String.format("nextBufSize=%d", nextBuf.size()));
		}
		if (merge)
			throw new IllegalStateException("merge==false expected.");
		this.duplicateOutputIndex = duplicateOutputIndex;
		this.hcSeamless = hcSeamless;
		merge = true;
	}

	@Override
	protected void merge() {
		twoWayMerge();
	}

	private void twoWayMerge() {
		if (debug)
			event("twm");
		if (prevBuf != null)
			throw new IllegalStateException("prevBuf == null expected.");
		if (nextBuf == null)
			throw new IllegalStateException("nextBuf != null expected.");
		AppInstBufInfo curInfo = appInstBufInfos.get(curBuf);
		AppInstBufInfo nextInfo = appInstBufInfos.get(nextBuf);
		if (debug)
			System.err.println(String.format(
					"curAppInstCount - %d, duplicateOutputIndex - %d ",
					curInfo.ht.tailCounter.count(), duplicateOutputIndex));
		copyNonDuplicateOutput(curInfo.ht.tailCounter);
		if (debug)
			System.err
					.println(String
							.format(" copyNonDuplicateOutput curAppInstCount - %d, duplicateOutputIndex - %d ",
									curInfo.ht.tailCounter.count(),
									duplicateOutputIndex));
		int curDupData = curInfo.ht.tailCounter.count() - duplicateOutputIndex;
		while ((nextInfo.ht.tailCounter.count() <= curDupData) && !stopCalled) {
			copyToTailBuffer(curBuf);
			curDupData = curInfo.ht.tailCounter.count() - duplicateOutputIndex;
		}
		hcSeamless.stop(false);
		switchBuffers(curDupData);
	}

	private void copyNonDuplicateOutput(Counter tailCounter) {
		while ((tailCounter.count() - duplicateOutputIndex < 0) && !stopCalled)
			copyToTailBuffer(curBuf);
	}
}
