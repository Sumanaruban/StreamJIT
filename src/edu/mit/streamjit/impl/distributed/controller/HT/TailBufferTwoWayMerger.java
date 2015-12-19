package edu.mit.streamjit.impl.distributed.controller.HT;

import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.common.Counter;
import edu.mit.streamjit.tuner.EventTimeLogger;

public class TailBufferTwoWayMerger extends TailBufferMergerSeamless {

	private int duplicateOutputIndex;

	public TailBufferTwoWayMerger(Buffer tailBuffer, EventTimeLogger eLogger) {
		super(tailBuffer, eLogger);
	}

	public void startMerge() {
	}

	public void startMerge(int duplicateOutputIndex) {
		if (debug) {
			event("m..");
			System.err.println(String.format("nextBufSize=%d", nextBuf.size()));
		}
		if (merge)
			throw new IllegalStateException("merge==false expected.");
		this.duplicateOutputIndex = duplicateOutputIndex;
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
		switchBuffers(nextInfo.skipCount - curDupData);
	}

	private void copyNonDuplicateOutput(Counter tailCounter) {
		while ((tailCounter.count() - duplicateOutputIndex < 0) && !stopCalled)
			copyToTailBuffer(curBuf);
	}
}
