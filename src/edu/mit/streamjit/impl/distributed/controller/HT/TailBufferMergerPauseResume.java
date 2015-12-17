package edu.mit.streamjit.impl.distributed.controller.HT;

import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.impl.blob.Buffer;

/**
 * {@link TailBufferMerger} for pause-resume reconfiguration process.
 * 
 * @author sumanan
 * @since 28 Oct, 2015
 */
public class TailBufferMergerPauseResume implements TailBufferMerger {

	private final BufferProvider bufProvider;

	public TailBufferMergerPauseResume(Buffer tailBuffer) {
		bufProvider = new BufferProvider1(tailBuffer);
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
	public void newAppInst(HeadTail ht, int skipCount) {
		if (skipCount != 0)
			throw new IllegalStateException(
					String.format(
							"skipCount=0 expected for PauseResume reconfiguration. Received skipCount=%d",
							skipCount));
	}

	@Override
	public void appInstStopped(int appInstId) {
	}

	@Override
	public void startMerge() {
	}

	@Override
	public void startMerge(int duplicateOutputIndex) {
	}

	@Override
	public BufferProvider bufferProvider() {
		return bufProvider;
	}

	private class BufferProvider1 implements BufferProvider {

		/**
		 * Final output buffer that is created from {@link Output}<O> output.
		 */
		private final Buffer tailBuffer;

		BufferProvider1(Buffer tailBuffer) {
			this.tailBuffer = tailBuffer;
		}

		@Override
		public Buffer newBuffer() {
			return tailBuffer;
		}
	}
}
