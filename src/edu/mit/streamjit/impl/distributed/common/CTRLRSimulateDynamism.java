package edu.mit.streamjit.impl.distributed.common;

import java.util.Set;

/**
 * Block or UnBlock some hardware resources in order to tune a StreamJit
 * application for dynamic hardware resource changes.
 * 
 * @author sumanan
 * @since 8 Mar, 2015
 */
public abstract class CTRLRSimulateDynamism implements CTRLRMessageElement {

	private static final long serialVersionUID = 1L;

	public abstract void process(CTRLRDynamismProcessor dynProcessor);

	@Override
	public void accept(CTRLRMessageVisitor visitor) {
		visitor.visit(this);
	}

	public static final class BlockCores extends CTRLRSimulateDynamism {
		private static final long serialVersionUID = 1L;
		public final Set<Integer> coreSet;

		public BlockCores(Set<Integer> coreSet) {
			this.coreSet = coreSet;
		}

		@Override
		public void process(CTRLRDynamismProcessor dynProcessor) {
			dynProcessor.process(this);
		}
	}

	public static final class UnblockCores extends CTRLRSimulateDynamism {
		private static final long serialVersionUID = 1L;
		public final Set<Integer> coreSet;

		public UnblockCores(Set<Integer> coreSet) {
			this.coreSet = coreSet;
		}

		@Override
		public void process(CTRLRDynamismProcessor dynProcessor) {
			dynProcessor.process(this);
		}
	}

	public interface CTRLRDynamismProcessor {
		public void process(BlockCores blockCores);
		public void process(UnblockCores unblockCores);
	}
}
