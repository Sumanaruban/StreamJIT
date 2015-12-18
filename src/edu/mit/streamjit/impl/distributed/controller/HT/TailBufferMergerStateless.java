package edu.mit.streamjit.impl.distributed.controller.HT;

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

	public TailBufferMergerStateless(Buffer tailBuffer) {
		super(tailBuffer);
	}

	public void startMerge() {
		if (merge)
			throw new IllegalStateException("merge==false expected.");
		merge = true;
	}

	public void startMerge(int duplicateOutputIndex) {
	}

	@Override
	protected void merge() {
		switchBuffers();
	}

	private void switchBuffers() {
		if (prevBuf != null)
			throw new IllegalStateException("prevBuf == null expected.");
		if (nextBuf == null)
			throw new IllegalStateException("nextBuf != null expected.");
		AppInstBufInfo a = appInstBufInfos.get(nextBuf);
		switchBuffers(a.skipCount);
	}
}
