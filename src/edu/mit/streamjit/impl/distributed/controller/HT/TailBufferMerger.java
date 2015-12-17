package edu.mit.streamjit.impl.distributed.controller.HT;

import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.distributed.controller.AppInstance;
import edu.mit.streamjit.impl.distributed.controller.AppInstanceManager;
import edu.mit.streamjit.impl.distributed.controller.StreamJitAppManager;

/**
 * Interface to merge two different {@link AppInstance}s' outputs together and
 * pass it to user.
 * <p>
 * Actually {@link AppInstanceManager} maintains {@link TailChannel}, that
 * writes the output of corresponding {@link AppInstance} to its output buffer.
 * {@link AppInstanceManager}s must use the {@link Buffer} given by this
 * interface for their {@link TailChannel}s.
 * 
 * @author sumanan
 * @since 28 Oct, 2015
 */
public interface TailBufferMerger {

	/**
	 * @return {@link Runnable} that do all data merging.
	 */
	Runnable getRunnable();

	/**
	 * Call this method to safely stop the {@link Runnable} that was returned by
	 * {@link #getRunnable()}.
	 */
	public void stop();

	/**
	 * At every reconfiguration, {@link StreamJitAppManager} must register the
	 * new {@link AppInstance} with {@link TailBufferMerger}.
	 * 
	 * @param ht
	 *            {@link HeadTail} info of the new {@link AppInstance}
	 * @param skipCount
	 *            Number of outputs of the {@link AppInstance} (with
	 *            id=appInstId) that should be discarded.
	 */
	public void newAppInst(HeadTail ht, int skipCount);

	/**
	 * Once an {@link AppInstance} is drained, {@link StreamJitAppManager} must
	 * call this method to release all allocated resources.
	 * 
	 * @param appInstId
	 */
	public void appInstStopped(int appInstId);

	/**
	 * When 2 {@link AppInstance}s are registered, this method can be called to
	 * inform {@link TailBufferMerger} to merge the outputs from both
	 * {@link AppInstance}s. How the outputs are merged depends on the
	 * {@link TailBufferMerger}'s implementation.
	 */
	public void startMerge();

	public BufferProvider bufferProvider();

	public interface BufferProvider {

		Buffer newBuffer();
	}
}
