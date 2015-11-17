package edu.mit.streamjit.impl.distributed.node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.impl.blob.Blob.Token;

/**
 * Various implementations of the interface {@link AffinityManager}.
 * 
 * @author sumanan
 * @since 4 Feb, 2015
 */
public class AffinityManagers {

	/**
	 * This is an empty {@link AffinityManager}.
	 * {@link #getAffinity(Token, int)} always returns null.
	 * 
	 * @author sumanan
	 * @since 4 Feb, 2015
	 */
	public static class EmptyAffinityManager implements AffinityManager {

		@Override
		public ImmutableSet<Integer> getAffinity(Token blobID, int coreCode) {
			return null;
		}
	}

	/**
	 * Allocates only one core per blob. All threads of that blob must use that
	 * single core.
	 * 
	 * @author sumanan
	 * @since 17 Nov, 2015
	 */
	public static class OneCoreAffinityManager implements AffinityManager {

		int i = 0;

		Map<Token, Integer> blobCoreMap = new HashMap<>();
		@Override
		public ImmutableSet<Integer> getAffinity(Token blobID, int coreCode) {
			if (!blobCoreMap.containsKey(blobID))
				blobCoreMap.put(blobID, i++);
			return ImmutableSet.of(blobCoreMap.get(blobID));
		}
	}

	/**
	 * Equally divide the available cores to the blobs. Threads of a blob share
	 * all cores that are allocated to that blob.
	 * 
	 * @author sumanan
	 * @since 17 Nov, 2015
	 */
	public static class EqualAffinityManager implements AffinityManager {

		private final int totalBlobs;

		private final int totalCores = Runtime.getRuntime()
				.availableProcessors();

		Map<Token, List<Integer>> blobCoreMap = new HashMap<>();

		int currentCoreToAllocate = 0;

		final int coresPerBlob;

		int reminder;

		EqualAffinityManager(int totalBlobs) {
			if (totalBlobs == 0)
				totalBlobs = 1;
			this.totalBlobs = totalBlobs;
			coresPerBlob = totalCores / totalBlobs;
			reminder = totalCores % totalBlobs;
		}

		@Override
		public ImmutableSet<Integer> getAffinity(Token blobID, int coreCode) {
			if (!blobCoreMap.containsKey(blobID))
				blobCoreMap.put(blobID, newAllocation());
			return ImmutableSet.copyOf(blobCoreMap.get(blobID));
		}

		private List<Integer> newAllocation() {
			if (totalBlobs < totalCores) {
				List<Integer> l = new ArrayList<>();
				for (int i = 0; i < coresPerBlob; i++)
					l.add(currentCoreToAllocate++);
				if (reminder > 0) {
					l.add(currentCoreToAllocate++);
					reminder--;
				}
				return l;
			} else {
				int c = currentCoreToAllocate++ % totalCores;
				return ImmutableList.of(c);
			}
		}
	}
}
