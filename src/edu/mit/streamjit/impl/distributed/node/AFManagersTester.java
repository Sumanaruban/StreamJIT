package edu.mit.streamjit.impl.distributed.node;

import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.distributed.common.Machine;

/**
 * @author sumanan
 * @since 5 Dec, 2015
 */
public class AFManagersTester {

	static int maxBlobs = 10;

	static int maxCoreCode = Machine.coresPerSocket;

	static Random r = new Random();

	public static void main(String[] args) {
		int i = 1000;
		while (i-- > 0) {
			Set<Blob> bSet = randomBlobSet();
			AffinityManager af = new AffinityManagers.CoreCodeAffinityManager(
					bSet);
		}
	}

	private static Set<Blob> blobSet() {
		// int[] threads = { 6, 5, 8, 11, 5, 6, 9, 7, 11, 1, 16, 10, 8, 10 };
		int[] threads = { 1, 2, 5, 3, 2 };
		Set<Blob> s = new HashSet<>(threads.length);
		for (int i = 0; i < threads.length; i++)
			s.add(new DBlob(threads[i], i));
		return s;
	}

	private static Set<Blob> randomBlobSet() {
		int blobs = 3 + r.nextInt(maxBlobs);
		Set<Blob> s = new HashSet<>(blobs);
		for (int i = 0; i < blobs; i++)
			s.add(new DBlob(r.nextInt(maxCoreCode + 1), i));
		return s;
	}

	private static class DBlob implements Blob {
		final int coreCodes;
		final String name;
		DBlob(int coreCodes, int blobID) {
			this.coreCodes = coreCodes;
			name = "Blob - " + blobID;
			System.out.println(name + " coreCodes=" + coreCodes);
		}

		@Override
		public String toString() {
			return name;
		}

		@Override
		public Set<Worker<?, ?>> getWorkers() {
			return null;
		}

		@Override
		public Set<Token> getInputs() {
			return null;
		}

		@Override
		public Set<Token> getOutputs() {
			return null;
		}

		@Override
		public int getMinimumBufferCapacity(Token token) {
			return 0;
		}

		@Override
		public int getMinimumSteadyBufferCapacity(Token token) {
			return 0;
		}

		@Override
		public int getMinimumInitBufferCapacity(Token token) {
			return 0;
		}

		@Override
		public void installBuffers(Map<Token, Buffer> buffers) {

		}

		@Override
		public int getCoreCount() {
			return coreCodes;
		}

		@Override
		public Runnable getCoreCode(int core) {
			return null;
		}

		@Override
		public void drain(Runnable callback) {

		}

		@Override
		public void insertDrainData(DrainData initialState)
				throws IllegalStateException {
		}

		@Override
		public DrainData getDrainData() {
			return null;
		}

		@Override
		public ExecutionStatistics getExecutionStatistics() {
			return null;
		}
	}
}
