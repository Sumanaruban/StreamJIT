package edu.mit.streamjit.impl.distributed.node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.ConcurrentArrayBuffer;
import edu.mit.streamjit.impl.distributed.common.CompilationInfo;
import edu.mit.streamjit.impl.distributed.common.SNMessageElement;
import edu.mit.streamjit.impl.distributed.common.SNMessageElement.SNMessageElementHolder;
import edu.mit.streamjit.impl.distributed.controller.BufferSizeCalc;
import edu.mit.streamjit.impl.distributed.node.LocalBuffer.ConcurrentArrayLocalBuffer;
import edu.mit.streamjit.impl.distributed.node.LocalBuffer.LocalBuffer1;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;

/**
 * {@link BlobsManager} will use the services from {@link BufferManager}.
 * Implementation of this interface is expected to do two tasks
 * <ol>
 * <li>Calculates buffer sizes.
 * <li>Create local buffers.
 * </ol>
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 28, 2014
 * 
 */
public interface BufferManager {

	void initialise();

	/**
	 * Second initialization. If the buffer sizes are computed by controller and
	 * send back to the {@link StreamNode}s, this method can be called with the
	 * minimum input buffer size requirement.
	 * 
	 * @param minInputBufSizes
	 */
	void initialise2(Map<Token, Integer> minInputBufSizes);

	ImmutableSet<Token> localTokens();

	ImmutableSet<Token> outputTokens();

	ImmutableSet<Token> inputTokens();

	/**
	 * @return buffer sizes of each local and boundary channels. Returns
	 *         <code>null</code> if the buffer sizes are not calculated yet.
	 *         {@link #isbufferSizesReady()} tells whether the buffer sizes are
	 *         calculated or not.
	 */
	ImmutableMap<Token, Integer> bufferSizes();

	/**
	 * @return <code>true</code> iff buffer sizes are calculated.
	 */
	boolean isbufferSizesReady();

	/**
	 * @return local buffers if buffer sizes are calculated. Otherwise returns
	 *         null.
	 */
	ImmutableMap<Token, LocalBuffer> localBufferMap();

	public static abstract class AbstractBufferManager implements BufferManager {

		protected final Set<Blob> blobSet;

		protected final ImmutableSet<Token> localTokens;

		protected final ImmutableSet<Token> globalInputTokens;

		protected final ImmutableSet<Token> globalOutputTokens;

		protected boolean isbufferSizesReady;

		protected ImmutableMap<Token, Integer> bufferSizes;

		ImmutableMap<Token, LocalBuffer> localBufferMap;

		public AbstractBufferManager(Set<Blob> blobSet) {
			this.blobSet = blobSet;

			Set<Token> inputTokens = new HashSet<>();
			Set<Token> outputTokens = new HashSet<>();
			for (Blob b : blobSet) {
				inputTokens.addAll(b.getInputs());
				outputTokens.addAll(b.getOutputs());
			}

			localTokens = ImmutableSet.copyOf(Sets.intersection(inputTokens,
					outputTokens));
			globalInputTokens = ImmutableSet.copyOf(Sets.difference(
					inputTokens, localTokens));
			globalOutputTokens = ImmutableSet.copyOf(Sets.difference(
					outputTokens, localTokens));

			isbufferSizesReady = false;
			bufferSizes = null;
			localBufferMap = null;
		}

		@Override
		public ImmutableSet<Token> localTokens() {
			return localTokens;
		}

		@Override
		public ImmutableSet<Token> outputTokens() {
			return globalOutputTokens;
		}

		@Override
		public ImmutableSet<Token> inputTokens() {
			return globalInputTokens;
		}

		@Override
		public ImmutableMap<Token, Integer> bufferSizes() {
			return bufferSizes;
		}

		@Override
		public boolean isbufferSizesReady() {
			return isbufferSizesReady;
		}

		@Override
		public ImmutableMap<Token, LocalBuffer> localBufferMap() {
			return localBufferMap;
		}

		protected final void createLocalBuffers() {
			ImmutableMap.Builder<Token, LocalBuffer> bufferMapBuilder = ImmutableMap
					.<Token, LocalBuffer> builder();
			for (Token t : localTokens) {
				int bufSize = bufferSizes.get(t);
				bufferMapBuilder
						.put(t, concurrentArrayLocalBuffer1(t, bufSize));
			}
			localBufferMap = bufferMapBuilder.build();
		}

		protected final LocalBuffer1 concurrentArrayLocalBuffer1(Token t,
				int bufSize) {
			List<Object> args = new ArrayList<>(1);
			args.add(bufSize);
			return new LocalBuffer1(t.toString(), ConcurrentArrayBuffer.class,
					args, bufSize, 0);
		}

		protected final LocalBuffer concurrentArrayLocalBuffer(Token t,
				int bufSize) {
			return new ConcurrentArrayLocalBuffer(bufSize);
		}

		/**
		 * Just introduced to avoid code duplication.
		 */
		protected void addBufferSize(Token t, int minSize,
				ImmutableMap.Builder<Token, Integer> bufferSizeMapBuilder) {
			// TODO: Just to increase the performance. Change it later
			int bufSize = Math.max(1000, minSize);
			// System.out.println("Buffer size of " + t.toString() + " is " +
			// bufSize);
			bufferSizeMapBuilder.put(t, bufSize);
		}
	}

	/**
	 * Calculates buffer sizes locally at {@link StreamNode} side. No central
	 * calculation involved.
	 */
	public static class SNLocalBufferManager extends AbstractBufferManager {
		public SNLocalBufferManager(Set<Blob> blobSet) {
			super(blobSet);
		}

		@Override
		public void initialise() {
			bufferSizes = calculateBufferSizes(blobSet);
			createLocalBuffers();
			isbufferSizesReady = true;
		}

		@Override
		public void initialise2(Map<Token, Integer> minInputBufSizes) {
			throw new java.lang.Error(
					"initialise2() is not supposed to be called");
		}

		// TODO: Buffer sizes, including head and tail buffers, must be
		// optimised. consider adding some tuning factor
		private ImmutableMap<Token, Integer> calculateBufferSizes(
				Set<Blob> blobSet) {
			ImmutableMap.Builder<Token, Integer> bufferSizeMapBuilder = ImmutableMap
					.<Token, Integer> builder();

			Map<Token, Integer> minInputBufCapaciy = new HashMap<>();
			Map<Token, Integer> minOutputBufCapaciy = new HashMap<>();

			for (Blob b : blobSet) {
				Set<Blob.Token> inputs = b.getInputs();
				for (Token t : inputs) {
					minInputBufCapaciy.put(t, b.getMinimumBufferCapacity(t));
				}

				Set<Blob.Token> outputs = b.getOutputs();
				for (Token t : outputs) {
					minOutputBufCapaciy.put(t, b.getMinimumBufferCapacity(t));
				}
			}

			Set<Token> localTokens = Sets.intersection(
					minInputBufCapaciy.keySet(), minOutputBufCapaciy.keySet());
			Set<Token> globalInputTokens = Sets.difference(
					minInputBufCapaciy.keySet(), localTokens);
			Set<Token> globalOutputTokens = Sets.difference(
					minOutputBufCapaciy.keySet(), localTokens);

			for (Token t : localTokens) {
				int bufSize = Math.max(minInputBufCapaciy.get(t),
						minOutputBufCapaciy.get(t));
				addBufferSize(t, bufSize, bufferSizeMapBuilder);
			}

			for (Token t : globalInputTokens) {
				int bufSize = minInputBufCapaciy.get(t);
				addBufferSize(t, bufSize, bufferSizeMapBuilder);
			}

			for (Token t : globalOutputTokens) {
				int bufSize = minOutputBufCapaciy.get(t);
				addBufferSize(t, bufSize, bufferSizeMapBuilder);
			}
			return bufferSizeMapBuilder.build();
		}
	}

	/**
	 * {@link Controller} gathers the minimum buffer information from all
	 * {@link StreamNode}s, calculates the appropriate buffer sizes, and send
	 * the buffer sizes back to {@link StreamNode}s.
	 */
	public static class GlobalBufferManager extends AbstractBufferManager {

		private final StreamNode streamNode;

		private final int appInstId;

		public GlobalBufferManager(Set<Blob> blobSet, StreamNode streamNode,
				int appInstId) {
			super(blobSet);
			this.streamNode = streamNode;
			this.appInstId = appInstId;
		}

		@Override
		public void initialise() {
			sendBuffersizes();
		}

		@Override
		public void initialise2(Map<Token, Integer> minInputBufSizes) {
			bufferSizes = calculateBufferSizes(blobSet, minInputBufSizes);
			createLocalBuffers();
			isbufferSizesReady = true;
		}

		/**
		 * Sends all blob's minimum buffer capacity requirement to the
		 * {@link Controller}. Controller decides suitable buffer capacity of
		 * each buffer in order to avoid deadlock and sends the final values
		 * back to the blobs.
		 */
		private void sendBuffersizes() {
			ImmutableMap.Builder<Token, Integer> minInitInputBufCapaciyBuilder = new ImmutableMap.Builder<>();
			ImmutableMap.Builder<Token, Integer> minInitOutputBufCapaciyBuilder = new ImmutableMap.Builder<>();
			ImmutableMap.Builder<Token, Integer> minSteadyInputBufCapacityBuilder = new ImmutableMap.Builder<>();
			ImmutableMap.Builder<Token, Integer> minSteadyOutputBufCapacityBuilder = new ImmutableMap.Builder<>();
			for (Blob b : blobSet) {
				for (Token t : b.getInputs()) {
					minInitInputBufCapaciyBuilder.put(t,
							b.getMinimumInitBufferCapacity(t));
					minSteadyInputBufCapacityBuilder.put(t,
							b.getMinimumSteadyBufferCapacity(t));
				}

				for (Token t : b.getOutputs()) {
					minInitOutputBufCapaciyBuilder.put(t,
							b.getMinimumInitBufferCapacity(t));
					minSteadyOutputBufCapacityBuilder.put(t,
							b.getMinimumSteadyBufferCapacity(t));
				}
			}

			SNMessageElement bufSizes = new CompilationInfo.BufferSizes(
					streamNode.getNodeID(),
					minInitInputBufCapaciyBuilder.build(),
					minInitOutputBufCapaciyBuilder.build(),
					minSteadyInputBufCapacityBuilder.build(),
					minSteadyOutputBufCapacityBuilder.build());

			try {
				streamNode.controllerConnection
						.writeObject(new SNMessageElementHolder(bufSizes, appInstId));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		// TODO: Buffer sizes, including head and tail buffers, must be
		// optimized. Consider adding some tuning factor.
		//
		// [6 Feb, 2015] Sometimes buffer sizes cause performance problems. This
		// method uses BufferSizeCalc#scaledSize() to ensures the output buffer
		// size is at least factor*steadyOutput.
		private ImmutableMap<Token, Integer> calculateBufferSizes(
				Set<Blob> blobSet, Map<Token, Integer> finalMinInputCapacity) {

			ImmutableMap.Builder<Token, Integer> bufferSizeMapBuilder = ImmutableMap
					.builder();

			Map<Token, Integer> minInputBufCapaciy = new HashMap<>();
			Map<Token, Integer> minOutputBufCapaciy = new HashMap<>();
			Map<Token, Integer> minInitInputBufCapacity = new HashMap<>();
			Map<Token, Integer> minInitOutputBufCapacity = new HashMap<>();
			Map<Token, Integer> minSteadyInputBufCapacity = new HashMap<>();
			Map<Token, Integer> minSteadyOutputBufCapacity = new HashMap<>();

			for (Blob b : blobSet) {
				Set<Blob.Token> inputs = b.getInputs();
				for (Token t : inputs) {
					minInputBufCapaciy.put(t, b.getMinimumBufferCapacity(t));
					minInitInputBufCapacity.put(t,
							b.getMinimumInitBufferCapacity(t));
					minSteadyInputBufCapacity.put(t,
							b.getMinimumSteadyBufferCapacity(t));
				}

				Set<Blob.Token> outputs = b.getOutputs();
				for (Token t : outputs) {
					minOutputBufCapaciy.put(t, b.getMinimumBufferCapacity(t));
					minInitOutputBufCapacity.put(t,
							b.getMinimumInitBufferCapacity(t));
					minSteadyOutputBufCapacity.put(t,
							b.getMinimumSteadyBufferCapacity(t));
				}
			}

			Set<Token> localTokens = Sets.intersection(
					minInputBufCapaciy.keySet(), minOutputBufCapaciy.keySet());
			Set<Token> globalInputTokens = Sets.difference(
					minInputBufCapaciy.keySet(), localTokens);
			Set<Token> globalOutputTokens = Sets.difference(
					minOutputBufCapaciy.keySet(), localTokens);

			for (Token t : localTokens) {
				int localbufSize = minInputBufCapaciy.get(t);
				int finalbufSize = finalMinInputCapacity.get(t);
				assert finalbufSize >= localbufSize : "The final buffer capacity send by the controller must always be >= to the blob's minimum requirement.";
				int outbufSize = BufferSizeCalc.scaledSize(t,
						minInitOutputBufCapacity, minSteadyOutputBufCapacity);

				// TODO: doubling the local buffer sizes. Without this deadlock
				// occurred when draining. Need to find out exact reason. See
				// StreamJit/Deadlock/deadlock2 folder.
				addBufferSize(t, 2 * (outbufSize + finalbufSize),
						bufferSizeMapBuilder);
			}

			for (Token t : globalInputTokens) {
				int inbufSize = BufferSizeCalc.scaledSize(t,
						minInitInputBufCapacity, minSteadyInputBufCapacity);
				if (t.isOverallInput()) {
					addBufferSize(t, inbufSize, bufferSizeMapBuilder);
					continue;
				}

				int finalbufSize = finalMinInputCapacity.get(t);
				assert finalbufSize >= inbufSize : "The final buffer capacity send by the controller must always be >= to the blob's minimum requirement.";
				addBufferSize(t, finalbufSize, bufferSizeMapBuilder);
			}

			for (Token t : globalOutputTokens) {
				int outbufSize = BufferSizeCalc.scaledSize(t,
						minInitOutputBufCapacity, minSteadyOutputBufCapacity);
				addBufferSize(t, outbufSize, bufferSizeMapBuilder);
			}
			return bufferSizeMapBuilder.build();
		}
	}
}
