package edu.mit.streamjit.impl.distributed.node;

import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.distributed.node.BlobExecuter.BlobThread2;

/**
 * Assigns CPU cores to {@link BlobThread2}s. {@link BlobThread2}s are expected
 * to set their processor affinity which is given by
 * {@link AffinityManager#getAffinity(Token, int)} before start running.
 * 
 * @author sumanan
 * @since 4 Feb, 2015
 */
public interface AffinityManager {

	/**
	 * @param blobID
	 * @param coreCode
	 * @return Set of CPU cores that is assigned the blob's coreCode.
	 */
	ImmutableSet<Integer> getAffinity(Token blobID, int coreCode);
}
