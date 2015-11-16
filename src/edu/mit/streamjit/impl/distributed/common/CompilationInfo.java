package edu.mit.streamjit.impl.distributed.common;

import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.DrainData;

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

	public static final class InitScheduleCompleted extends CompilationInfo {
		private static final long serialVersionUID = 1L;

		public final Token blobID;
		public final long timeMills;

		public InitScheduleCompleted(Token blobID, long timeMills) {
			this.blobID = blobID;
			this.timeMills = timeMills;
		}

		@Override
		public void process(CompilationInfoProcessor cip) {
			cip.process(this);
		}
	}

	public static final class DrainDataSizes extends CompilationInfo {
		private static final long serialVersionUID = 1L;

		public final int machineID;

		public final ImmutableMap<Token, Integer> ddSizes;

		public DrainDataSizes(int machineID,
				ImmutableMap<Token, Integer> ddSizes) {
			this.machineID = machineID;
			this.ddSizes = ddSizes;
		}

		@Override
		public void process(CompilationInfoProcessor cip) {
			cip.process(this);
		}
	}

	public static final class State extends CompilationInfo {
		private static final long serialVersionUID = 1L;

		public final Token blobID;
		public final DrainData drainData;

		public State(Token blobID, DrainData drainData) {
			this.blobID = blobID;
			this.drainData = drainData;
		}

		@Override
		public void process(CompilationInfoProcessor cip) {
			cip.process(this);
		}
	}

	public interface CompilationInfoProcessor {
		public void process(BufferSizes bufferSizes);
		public void process(InitScheduleCompleted initScheduleCompleted);
		public void process(DrainDataSizes ddSizes);
		public void process(State state);
	}
}
