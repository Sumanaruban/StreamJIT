package edu.mit.streamjit.impl.distributed.node;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.Buffer;

/**
 * {@link BlobsManagerImpl} refactored and its buffer management related inner
 * classes have been moved to this file.
 * 
 * @author sumanan
 * @since 10 Oct, 2015
 */
public class BufferManagementUtils {

	/**
	 * Handles another type of deadlock which occurs when draining. A Down blob,
	 * that has more than one upper blob, cannot progress because some of its
	 * upper blobs are drained and hence no input on the corresponding input
	 * channels, and other upper blobs blocked at their output channels as the
	 * down blob is no more consuming data. So those non-drained upper blobs are
	 * going to stuck forever at their output channels and the down blob will
	 * not receive DODrain command from the controller.
	 * 
	 * This class just discard the buffer contents so that blocked blobs can
	 * progress.
	 * 
	 * See the Deadlock 5.
	 * 
	 * @author sumanan
	 * 
	 */
	static class BufferCleaner extends Thread {

		final BlobsManagerImpl blobsManagerImpl;

		final AtomicBoolean run;

		final boolean needToCopyDrainData;

		final Map<Token, List<Object[]>> newlocalBufferMap;

		BufferCleaner(BlobsManagerImpl blobsManagerImpl,
				boolean needToCopyDrainData) {
			super("BufferCleaner");
			this.blobsManagerImpl = blobsManagerImpl;
			System.out.println("Buffer Cleaner : needToCopyDrainData == "
					+ needToCopyDrainData);
			this.run = new AtomicBoolean(true);
			this.needToCopyDrainData = needToCopyDrainData;
			if (needToCopyDrainData)
				newlocalBufferMap = new HashMap<>();
			else
				newlocalBufferMap = null;
		}

		public void run() {
			try {
				Thread.sleep(60000);
			} catch (InterruptedException e) {
				return;
			}

			System.out.println("BufferCleaner is going to clean buffers...");
			boolean areAllDrained = false;

			while (run.get()) {
				if (needToCopyDrainData)
					areAllDrained = copyLocalBuffers();
				else
					areAllDrained = cleanAllBuffers();

				if (areAllDrained)
					break;

				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					break;
				}
			}
		}

		/**
		 * Go through all blocked blobs and clean all input and output buffers.
		 * This method is useful when we don't care about the drain data.
		 * 
		 * @return true iff there is no blocked blobs, i.e., all blobs have
		 *         completed the draining.
		 */
		private boolean cleanAllBuffers() {
			boolean areAllDrained = true;
			for (BlobExecuter be : blobsManagerImpl.blobExecuters.values()) {
				if (be.drainer.drainState == 1 || be.drainer.drainState == 2) {
					// System.out.println(be.blobID + " is not drained");
					areAllDrained = false;
					for (Token t : be.blob.getOutputs()) {
						Buffer b = be.bufferMap.get(t);
						clean(b, t);
					}

					for (Token t : be.blob.getInputs()) {
						Buffer b = be.bufferMap.get(t);
						clean(b, t);
					}
				}
			}
			return areAllDrained;
		}

		private void clean(Buffer b, Token t) {
			int size = b.size();
			if (size == 0)
				return;
			System.out.println(String.format(
					"Buffer %s has %d data. Going to clean it", t.toString(),
					size));
			Object[] obArray = new Object[size];
			b.readAll(obArray);
		}

		/**
		 * Copy only the local buffers into a new large buffer to make the
		 * blocked blob to progress. This copied buffer can be sent to
		 * controller as a drain data.
		 */
		private boolean copyLocalBuffers() {
			ImmutableMap<Token, LocalBuffer> localBufferMap = blobsManagerImpl.bufferManager
					.localBufferMap();
			boolean areAllDrained = true;
			for (BlobExecuter be : blobsManagerImpl.blobExecuters.values()) {
				if (be.drainer.drainState == 1 || be.drainer.drainState == 2) {
					// System.out.println(be.blobID + " is not drained");
					areAllDrained = false;
					for (Token t : be.blob.getOutputs()) {
						if (localBufferMap.containsKey(t)) {
							Buffer b = be.bufferMap.get(t);
							copy(b, t);
						}
					}
				}
			}
			return areAllDrained;
		}

		private void copy(Buffer b, Token t) {
			int size = b.size();
			if (size == 0)
				return;

			if (!newlocalBufferMap.containsKey(t)) {
				newlocalBufferMap.put(t, new LinkedList<Object[]>());
			}

			List<Object[]> list = newlocalBufferMap.get(t);
			Object[] bufArray = new Object[size];
			b.readAll(bufArray);
			assert b.size() == 0 : String.format(
					"buffer size is %d. But 0 is expected", b.size());
			list.add(bufArray);
		}

		public void stopit() {
			this.run.set(false);
			this.interrupt();
		}

		public Object[] copiedBuffer(Token t) {
			assert needToCopyDrainData : "BufferCleaner is not in buffer copy mode";
			copy(blobsManagerImpl.bufferManager.localBufferMap().get(t), t);
			List<Object[]> list = newlocalBufferMap.get(t);
			if (list == null)
				return new Object[0];
			else if (list.size() == 0)
				return new Object[0];
			else if (list.size() == 1)
				return list.get(0);

			int size = 0;
			for (Object[] array : list) {
				size += array.length;
			}

			int destPos = 0;
			Object[] mergedArray = new Object[size];
			for (Object[] array : list) {
				System.arraycopy(array, 0, mergedArray, destPos, array.length);
				destPos += array.length;
			}
			return mergedArray;
		}
	}

}
