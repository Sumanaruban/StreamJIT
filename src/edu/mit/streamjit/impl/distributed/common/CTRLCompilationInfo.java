package edu.mit.streamjit.impl.distributed.common;

import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.impl.blob.Blob.Token;

public abstract class CTRLCompilationInfo implements CTRLRMessageElement {

	private static final long serialVersionUID = 1L;

	public abstract void process(CTRLCompilationInfoProcessor cip);

	@Override
	public void accept(CTRLRMessageVisitor visitor) {
		visitor.visit(this);
	}

	/**
	 * Buffer sizes
	 */
	public static final class FinalBufferSizes extends CTRLCompilationInfo {
		private static final long serialVersionUID = 1L;
		public final ImmutableMap<Token, Integer> minInputBufCapacity;

		public FinalBufferSizes(
				final ImmutableMap<Token, Integer> minInputBufCapacity) {
			this.minInputBufCapacity = minInputBufCapacity;
		}

		@Override
		public void process(CTRLCompilationInfoProcessor cip) {
			cip.process(this);
		}
	}

	public interface CTRLCompilationInfoProcessor {
		public void process(FinalBufferSizes finalBufferSizes);
	}
}
