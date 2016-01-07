package edu.mit.streamjit.impl.distributed.controller.HT;

import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.tuner.EventTimeLogger;

/**
 * {@link TailBufferMerger} that performs static switching. That is, always
 * skips skioCount amount of data from the new graph and switch to the new graph
 * only after the old graph is drained.
 * 
 * @author sumanan
 * @since 28 Oct, 2015
 */
public class StaticTailBufferMerger extends TailBufferMergerSeamless {

	public StaticTailBufferMerger(Buffer tailBuffer, EventTimeLogger eLogger) {
		super(tailBuffer, eLogger);
	}

	public void startMerge() {
		if (merge)
			throw new IllegalStateException("merge==false expected.");
		merge = true;
	}

	public void startMerge(int duplicateOutputIndex,
			HeadChannelSeamless hcSeamless) {
		this.duplicateOutputIndex = duplicateOutputIndex;
	}

	@Override
	protected void merge() {
		if (prevBuf != null)
			throw new IllegalStateException("prevBuf == null expected.");
		if (nextBuf == null)
			throw new IllegalStateException("nextBuf != null expected.");
		AppInstBufInfo a = appInstBufInfos.get(nextBuf);
		copyFully(curBuf);
		switchBuffers(a.skipCount);
	}

	@Override
	protected void stoping() {
	}
}
