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
		public final ImmutableMap<Token, Integer> minInputBufCapacity;
		public final ImmutableMap<Token, Integer> minOutputBufCapacity;

		public BufferSizes(final int machineID,
				final ImmutableMap<Token, Integer> minInputBufCapacity,
				final ImmutableMap<Token, Integer> minOutputBufCapacity) {
			this.machineID = machineID;
			this.minInputBufCapacity = minInputBufCapacity;
			this.minOutputBufCapacity = minOutputBufCapacity;
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
