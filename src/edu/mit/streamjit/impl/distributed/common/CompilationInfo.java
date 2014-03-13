package edu.mit.streamjit.impl.distributed.common;

import com.google.common.collect.ImmutableMap;
import edu.mit.streamjit.impl.blob.Blob.Token;

/**
 * @author Sumanan sumanan@mit.edu
 * @since Mar 1, 2014
 */
public abstract class CompilationInfo implements SNMessageElement {

	private static final long serialVersionUID = 1L;

	@Override
	public void accept(SNMessageVisitor visitor) {
		visitor.visit(this);
	}

	public abstract void process(CompilationInfoProcessor cip);

	/**
	 * Buffer sizes
	 */
	public static final class BufferSizes extends CompilationInfo {
		private static final long serialVersionUID = 1L;

		public final int machineID;
		public final ImmutableMap<Token, Integer> minInitInputBufCapacity;
		public final ImmutableMap<Token, Integer> minInitOutputBufCapacity;
		public final ImmutableMap<Token, Integer> minSteadyInputBufCapacity;
		public final ImmutableMap<Token, Integer> minSteadyOutputBufCapacity;

		public BufferSizes(final int machineID,
				final ImmutableMap<Token, Integer> minInitInputBufCapacity,
				final ImmutableMap<Token, Integer> minInitOutputBufCapacity,
				final ImmutableMap<Token, Integer> minSteadyInputBufCapacity,
				final ImmutableMap<Token, Integer> minSteadyOutputBufCapacity) {
			this.machineID = machineID;
			this.minInitInputBufCapacity = minInitInputBufCapacity;
			this.minInitOutputBufCapacity = minInitOutputBufCapacity;
			this.minSteadyInputBufCapacity = minSteadyInputBufCapacity;
			this.minSteadyOutputBufCapacity = minSteadyOutputBufCapacity;
		}

		@Override
		public void process(CompilationInfoProcessor cip) {
			cip.process(this);
		}
	}

	public interface CompilationInfoProcessor {
		public void process(BufferSizes bufferSizes);
	}
}
