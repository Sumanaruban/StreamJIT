package edu.mit.streamjit.impl.distributed.node;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;

import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.distributed.common.Machine;
import edu.mit.streamjit.impl.distributed.common.Utils;

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

		int currentCoreToAllocate = 0;

		private final int totalCores = Runtime.getRuntime()
				.availableProcessors();

		Map<Token, Integer> blobCoreMap = new HashMap<>();
		@Override
		public ImmutableSet<Integer> getAffinity(Token blobID, int coreCode) {
			if (!blobCoreMap.containsKey(blobID)) {
				int c = currentCoreToAllocate++ % totalCores;
				blobCoreMap.put(blobID, c);
			}
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

	public static class CoreCodeAffinityManager implements AffinityManager {

		final ImmutableSet<Blob> blobSet;

		final ImmutableTable<Token, Integer, Integer> assignmentTable;

		final int totalThreads;

		CoreCodeAffinityManager(ImmutableSet<Blob> blobSet) {
			this.blobSet = blobSet;
			totalThreads = totalThreads(blobSet);
			assignmentTable = assign();
		}

		@Override
		public ImmutableSet<Integer> getAffinity(Token blobID, int coreCode) {
			return ImmutableSet.of(assignmentTable.get(blobID, coreCode));
		}

		private int totalThreads(ImmutableSet<Blob> blobSet) {
			int threads = 0;
			for (Blob b : blobSet)
				threads += b.getCoreCount();
			return threads;
		}

		private ImmutableTable<Token, Integer, Integer> assign() {
			Map<Blob, Integer> coresPerBlob = coresPerBlob();
			return null;
		}

		private Map<Blob, Integer> coresPerBlob() {
			Map<Blob, Integer> coresPerBlob = new HashMap<>();
			int allocatedCores = 0;
			for (Blob b : blobSet) {
				int coreCode = b.getCoreCount();
				int p = Math.max(1, (coreCode * Machine.physicalCores)
						/ totalThreads);
				coresPerBlob.put(b, p);
				allocatedCores += p;
			}
			int remainingCores = Machine.physicalCores - allocatedCores;
			assignRemainingCores(coresPerBlob, remainingCores);
			return coresPerBlob;
		}

		private void assignRemainingCores(Map<Blob, Integer> coresPerBlob,
				int remainingCores) {
			List<Blob> blobList = new ArrayList<>(blobSet);
			Collections.sort(
					blobList,
					(b1, b2) -> Integer.compare(b1.getCoreCount(),
							b2.getCoreCount()));
			for (Blob b : blobList) {
				if (remainingCores == 0)
					break;
				int p = coresPerBlob.remove(b);
				coresPerBlob.put(b, p + 1);
				remainingCores--;
			}
		}

		private ImmutableTable<Token, Integer, Integer> assignment(
				Map<Blob, Integer> coresPerBlob) {
			int core = 0;
			ImmutableTable.Builder<Token, Integer, Integer> builder = ImmutableTable
					.builder();
			for (Blob b : blobSet) {
				Token blobId = Utils.getBlobID(b);
				int cores = coresPerBlob.get(b);
				int coreCodes = b.getCoreCount();

			}
			return builder.build();
		}

		Map<Blob, Set<Integer>> coresForBlob(Map<Blob, Integer> coresPerBlob) {
			int core = 0;
			return null;
		}

		/**
		 * If hyper threading is enabled, Linux first enumerates all physical
		 * cores without considering the hyper threading and then enumerates the
		 * hyper threaded (virtual) cores. E.g, On Lanka05, a machine which has
		 * two sockets, 12 cores per socket, and hyper threading enabled, lists
		 * the cores as follows
		 * <ol>
		 * <li>node0 CPU(s): 0-11,24-35
		 * <li>node1 CPU(s): 12-23,36-47
		 * </ol>
		 * 
		 * @param physicalCoreID
		 * @return
		 */
		private int getCorrespondingVirtualCoreID(int physicalCoreID) {
			if (physicalCoreID >= Machine.physicalCores)
				throw new IllegalArgumentException(String.format(
						"physicalCoreID(%d) >= Machine.physicalCores(%d)",
						physicalCoreID, Machine.physicalCores));
			if (Machine.isHTEnabled)
				return physicalCoreID + Machine.physicalCores;
			return physicalCoreID;
		}

		/**
		 * Uses only physical cores. Dedicated physical core for each coreCode
		 * of each blob.
		 * 
		 * @return
		 */
		private ImmutableTable<Token, Integer, Integer> assignment1() {
			System.out.println("Core Assignment 1");
			int core = 0;
			ImmutableTable.Builder<Token, Integer, Integer> builder = ImmutableTable
					.builder();
			for (Blob b : blobSet) {
				Token blobId = Utils.getBlobID(b);
				for (int coreCode = 0; coreCode < b.getCoreCount(); coreCode++) {
					builder.put(blobId, coreCode, core++);
				}
			}

			if (core > Machine.physicalCores)
				throw new IllegalStateException(
						String.format(
								"Assignment 1 : Assigned cores(%d) > Machine.physicalCores(%d)",
								core, Machine.physicalCores));
			return builder.build();
		}
	}
}
