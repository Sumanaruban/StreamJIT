package edu.mit.streamjit.impl.distributed.controller;

import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.impl.blob.Buffer;

/**
 * {@link TailBufferMerger} for pause-resume reconfiguration process.
 * 
 * @author sumanan
 * @since 28 Oct, 2015
 */
public class TailBufferMergerPauseResume implements TailBufferMerger {

	/**
	 * Final output buffer that is created from {@link Output}<O> output.
	 */
	private final Buffer tailBuffer;

	public TailBufferMergerPauseResume(Buffer tailBuffer) {
		this.tailBuffer = tailBuffer;
	}

	@Override
	public Runnable getRunnable() {
		return new Runnable() {
			@Override
			public void run() {
			}
		};
	}

	@Override
	public void stop() {
	}

	@Override
	public Buffer registerAppInst(int appInstId, int skipCount) {
		if (skipCount != 0)
			throw new IllegalStateException(
					String.format(
							"skipCount=0 expected for PauseResume reconfiguration. Received skipCount=%d",
							skipCount));
		return tailBuffer;
	}

	@Override
	public void unregisterAppInst(int appInstId) {
	}

	@Override
	public void startMerge() {
	}
}
