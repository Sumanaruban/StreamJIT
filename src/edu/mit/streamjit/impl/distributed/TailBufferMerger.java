package edu.mit.streamjit.impl.distributed;

import edu.mit.streamjit.impl.blob.Buffer;

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
	 * new {@link AppInstance} with {@link TailBufferMerger} and get a new
	 * {@link Buffer} for that particular {@link AppInstance}'s
	 * {@link TailChannel}.
	 * 
	 * @param appInstId
	 *            id of the {@link AppInstance}.
	 * @param skipCount
	 *            Number of outputs of the {@link AppInstance} (with
	 *            id=appInstId) that should be discarded.
	 * @return A {@link Buffer} that must be used by {@link AppInstance}s for
	 *         their {@link TailChannel}s.
	 */
	public Buffer registerAppInst(int appInstId, int skipCount);

	/**
	 * Once an {@link AppInstance} is drained, {@link StreamJitAppManager} must
	 * unregister it from {@link TailBufferMerger} in order to release the
	 * allocated {@link Buffer}.
	 * 
	 * @param appInstId
	 */
	public void unregisterAppInst(int appInstId);

	/**
	 * When 2 {@link AppInstance}s are registered, this method can be called to
	 * inform {@link TailBufferMerger} to merge the outputs from both
	 * {@link AppInstance}s. How the outputs are merged depends on the
	 * {@link TailBufferMerger}'s implementation.
	 */
	public void startMerge();
}
