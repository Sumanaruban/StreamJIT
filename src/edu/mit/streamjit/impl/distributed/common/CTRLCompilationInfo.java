package edu.mit.streamjit.impl.distributed.common;

import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.DrainData;

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

	public static final class InitSchedule extends CTRLCompilationInfo {

		private static final long serialVersionUID = 1L;
		public final ImmutableMap<Token, Integer> steadyRunCount;

		public InitSchedule(ImmutableMap<Token, Integer> steadyRunCount) {
			this.steadyRunCount = steadyRunCount;
		}

		@Override
		public void process(CTRLCompilationInfoProcessor cip) {
			cip.process(this);
		}
	}

	public interface CTRLCompilationInfoProcessor {
		public void process(FinalBufferSizes finalBufferSizes);
		public void process(InitSchedule InitSchedule);
		public void process(ConfigurationString2DD cfgDD);
	}

	/**
	 * ConfigurationString2-DrainData
	 * 
	 * @author sumanan
	 * @since 27 Aug, 2015
	 */
	public static final class ConfigurationString2DD
			extends
				CTRLCompilationInfo {
		private static final long serialVersionUID = 1L;
		public final DrainData drainData;

		public ConfigurationString2DD(DrainData drainData) {
			this.drainData = drainData;
		}

		@Override
		public void process(CTRLCompilationInfoProcessor cip) {
			cip.process(this);
		}
	}
}
