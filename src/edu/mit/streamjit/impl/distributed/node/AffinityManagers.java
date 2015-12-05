package edu.mit.streamjit.impl.distributed.node;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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

		final Set<Blob> blobSet;

		final ImmutableTable<Blob, Integer, Integer> assignmentTable;

		final int totalThreads;

		/**
		 * Free cores in each socket.
		 */
		final Map<Integer, Integer> freeCoresMap;

		CoreCodeAffinityManager(Set<Blob> blobSet) {
			this.blobSet = blobSet;
			freeCoresMap = freeCores();
			totalThreads = totalThreads(blobSet);
			assignmentTable = assign();
			printTable(assignmentTable);
		}

		@Override
		public ImmutableSet<Integer> getAffinity(Token blobID, int coreCode) {
			return ImmutableSet.of(assignmentTable.get(blobID, coreCode));
		}

		private int totalThreads(Set<Blob> blobSet) {
			int threads = 0;
			for (Blob b : blobSet)
				threads += b.getCoreCount();
			return threads;
		}

		private ImmutableTable<Blob, Integer, Integer> assign() {
			Map<Blob, Integer> coresPerBlob = coresPerBlob();
			Map<Blob, Set<Integer>> coresForBlob = coresForBlob(coresPerBlob);

			ImmutableTable.Builder<Blob, Integer, Integer> tBuilder = ImmutableTable
					.builder();
			for (Map.Entry<Blob, Set<Integer>> en : coresForBlob.entrySet()) {
				Blob b = en.getKey();
				List<Integer> cores = new ArrayList<Integer>(en.getValue());
				for (int i = 0; i < b.getCoreCount(); i++) {
					int coreIndex = i % cores.size();
					int round = i / cores.size();
					int coreID = round % 2 == 0 ? cores.get(coreIndex)
							: getCorrespondingVirtualCoreID(cores
									.get(coreIndex));
					tBuilder.put(b, i, coreID);
				}
			}
			return tBuilder.build();
		}

		Map<Integer, Integer> freeCores() {
			Map<Integer, Integer> m = new HashMap<Integer, Integer>(
					Machine.sockets);
			for (int i = 0; i < Machine.sockets; i++)
				m.put(i, Machine.coresPerSocket);
			return m;
		}

		private Map<Blob, Integer> coresPerBlob() {
			int free = currentFreeCores();
			if (blobSet.size() >= free)
				return OneCorePerBlob();
			else if (totalThreads > free)
				return proportionalAllocation();
			else
				return FullAllocation();
		}

		/**
		 * Use this allocation if the total blobs are greater or equal than the
		 * total physical cores.
		 * 
		 * @return
		 */
		private Map<Blob, Integer> OneCorePerBlob() {
			Map<Blob, Integer> coresPerBlob = new HashMap<>();
			for (Blob b : blobSet) {
				coresPerBlob.put(b, 1);
			}
			return coresPerBlob;
		}

		/**
		 * Use this allocation if the total physical cores are greater or equal
		 * than the total required cores.
		 * 
		 * @return
		 */
		private Map<Blob, Integer> FullAllocation() {
			Map<Blob, Integer> coresPerBlob = new HashMap<>();
			for (Blob b : blobSet) {
				coresPerBlob.put(b, b.getCoreCount());
			}
			return coresPerBlob;
		}

		/**
		 * Use this allocation if the total physical cores are lesser than the
		 * total required cores.
		 * 
		 * @return
		 */
		private Map<Blob, Integer> proportionalAllocation() {
			Map<Blob, Integer> coresPerBlob = new HashMap<>();
			Set<Blob> unsatisfiedBlobs = new HashSet<>();
			int allocatedCores = 0;
			for (Blob b : blobSet) {
				int coreCode = b.getCoreCount();
				int p = Math.max(1, (coreCode * Machine.physicalCores)
						/ totalThreads);
				coresPerBlob.put(b, p);
				allocatedCores += p;
				if (coreCode > p)
					unsatisfiedBlobs.add(b);
			}
			int remainingCores = Machine.physicalCores - allocatedCores;
			assignRemainingCores(coresPerBlob, remainingCores, unsatisfiedBlobs);
			return coresPerBlob;
		}

		private void assignRemainingCores(Map<Blob, Integer> coresPerBlob,
				int remainingCores, Set<Blob> unsatisfiedBlobs) {
			if (remainingCores < 1)
				return;
			List<Blob> blobList = new ArrayList<>(unsatisfiedBlobs);
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
			Map<Blob, Set<Integer>> socketBlobAssignment = new HashMap<>();

			for (int i = 0; i < Machine.sockets; i++) {
				for (int j = Machine.coresPerSocket; j > 0; j--) {
					List<Map<Blob, Integer>> subsets = new ArrayList<>();
					List<Map.Entry<Blob, Integer>> list = new ArrayList<>(
							coresPerBlob.entrySet());
					ss(list, list.size(), new HashMap<>(), j, subsets, true);
					if (subsets.size() > 0) {
						Set<Blob> blobs = subsets.get(0).keySet();
						socketBlobAssignment(i, subsets.get(0),
								socketBlobAssignment);
						for (Blob b : blobs)
							coresPerBlob.remove(b);
						break;
					}
				}
			}

			if (coresPerBlob.size() != 0)
				breakBlobs(coresPerBlob, socketBlobAssignment);
			return socketBlobAssignment;
		}

		private void reInializeFreeCoreMap() {
			for (int i = 0; i < Machine.sockets; i++)
				freeCoresMap.put(i, Machine.coresPerSocket);
		}

		private void breakBlobs(Map<Blob, Integer> coresPerBlob,
				Map<Blob, Set<Integer>> socketBlobAssignment) {
			for (Map.Entry<Blob, Integer> en : coresPerBlob.entrySet()) {
				if (currentFreeCores() == 0)
					reInializeFreeCoreMap();
				for (int i = en.getValue();; i++) {
					if (i > 5000)
						System.out.println("dddddddddddddddddddd");
					List<Map<Integer, Integer>> subsets = new ArrayList<>();
					List<Map.Entry<Integer, Integer>> list = new ArrayList<>(
							freeCoresMap.entrySet());
					ss(list, list.size(), new HashMap<>(), i, subsets, false);
					if (subsets.size() > 0) {
						subsets.sort((m1, m2) -> Integer.compare(m1.size(),
								m2.size()));
						Map<Integer, Integer> m = subsets.get(0);
						int required = en.getValue();
						Set<Integer> s = new HashSet<Integer>();
						for (Map.Entry<Integer, Integer> e : m.entrySet()) {
							int a = Math.min(required, e.getValue());
							s.addAll(cores(e.getKey(), a));
							required -= a;
						}
						socketBlobAssignment.put(en.getKey(), s);
						break;
					}
				}
			}
		}

		int currentFreeCores() {
			int free = 0;
			for (int i : freeCoresMap.values())
				free += i;
			return free;
		}

		int requiredCores(Map<Blob, Integer> coresPerBlob) {
			int req = 0;
			for (int i : coresPerBlob.values())
				req += i;
			return req;
		}

		void socketBlobAssignment(int socket, Map<Blob, Integer> subset,
				Map<Blob, Set<Integer>> socketBlobAssignment) {
			int totalJobs = 0;
			for (int i : subset.values())
				totalJobs += i;
			if (totalJobs > Machine.coresPerSocket)
				throw new IllegalStateException(
						String.format(
								"Total jobs(%d) assigned to the processor(%d) is greater than the available cores(%d)",
								totalJobs, socket, Machine.coresPerSocket));

			for (Map.Entry<Blob, Integer> en : subset.entrySet()) {
				socketBlobAssignment.put(en.getKey(),
						cores(socket, en.getValue()));
			}
		}

		Set<Integer> cores(int socket, int noOfCores) {
			int socketStart = socket * Machine.coresPerSocket;
			int freeCores = freeCoresMap.get(socket);
			if (noOfCores > freeCores)
				throw new IllegalStateException(String.format(
						"Socket(%d): noOfCores(%d) > availableFreeCores(%d)",
						socket, noOfCores, freeCores));
			Set<Integer> s = new HashSet<>();
			int freeCoreStart = Machine.coresPerSocket - freeCores;
			for (int i = 0; i < noOfCores; i++)
				s.add(socketStart + freeCoreStart++);
			freeCoresMap.replace(socket, freeCores - noOfCores);
			return s;
		}

		/**
		 * Finds subsetsums. The original code was copied from
		 * http://www.edufyme.com/code/?id=45c48cce2e2d7fbdea1afc51c7c6ad26.
		 * 
		 * I added an additional parameter (boolean firstSubSet) to stop
		 * exploring too many subsets. If the flag is true, it returns only the
		 * very first subset.
		 * 
		 */
		static <T> boolean ss(List<Map.Entry<T, Integer>> list, int n,
				Map<T, Integer> subset, int sum, List<Map<T, Integer>> subsets,
				boolean firstSubSet) {

			if (sum == 0) {
				// printAns(subset);
				subsets.add(subset);
				return firstSubSet;
			}

			if (n == 0)
				return false;

			boolean ret;
			if (list.get(n - 1).getValue() <= sum) {
				ret = ss(list, n - 1, new HashMap<>(subset), sum, subsets,
						firstSubSet);
				if (ret)
					return true;
				Map.Entry<T, Integer> en = list.get(n - 1);
				subset.put(en.getKey(), en.getValue());
				ret = ss(list, n - 1, new HashMap<>(subset),
						sum - en.getValue(), subsets, firstSubSet);
				if (ret)
					return true;
			} else {
				ret = ss(list, n - 1, new HashMap<>(subset), sum, subsets,
						firstSubSet);
				if (ret)
					return true;
			}
			return false;
		}

		/**
		 * If hyper threading is enabled, Linux first enumerates all physical
		 * cores without considering the hyper threading and then enumerates the
		 * hyper threaded (virtual) cores. E.g, On Lanka05 (it has two sockets,
		 * 12 cores per socket, and hyper threading enabled), Linux lists the
		 * cores as follows
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

		static <T> void printAns(Map<T, Integer> subset) {
			for (Map.Entry i : subset.entrySet())
				System.out.print(i.getValue() + " ");
			System.out.println();
		}
	}

	static void printTable(
			ImmutableTable<Blob, Integer, Integer> assignmentTable) {
		for (Blob b : assignmentTable.rowKeySet()) {
			System.out.print(b + " :-");
			for (int i = 0; i < b.getCoreCount(); i++) {
				System.out.print(assignmentTable.get(b, i) + " ");
			}
			System.out.println();

		}
	}
}
