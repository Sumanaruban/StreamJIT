package edu.mit.streamjit.impl.distributed;

import edu.mit.streamjit.impl.blob.Buffer;

/**
 * Interface to merge two different {@link AppInstance}s' outputs together.
 * <p>
 * {@link AppInstance}s must use the {@link Buffer} given by this interface for
 * their {@link TailChannel}s.
 * 
 * @author sumanan
 * @since 28 Oct, 2015
 */
public interface TailBufferMerger {

	/**
	 * @return {@link Runnable} that to all data merging.
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
	 * @param skipCount
	 *            Number of outputs of the {@link AppInstance} (with
	 *            id=appInstId) that should be discarded
	 * @return A {@link Buffer} that must be used by {@link AppInstance}s for
	 *         their {@link TailChannel}s.
	 */
	public Buffer registerAppInst(int appInstId, int skipCount);

	/**
	 * Once an {@link AppInstance} is drained, {@link StreamJitAppManager} must
	 * unregister it in order to release the allocated {@link Buffer}.
	 * 
	 * @param appInstId
	 */
	public void unregisterAppInst(int appInstId);

	/**
	 * When 2 {@link AppInstance}s are registered, {@link StreamJitAppManager}
	 * can call this method to switch from one {@link AppInstance}'s output to
	 * another.
	 */
	public void switchBuf();
}
