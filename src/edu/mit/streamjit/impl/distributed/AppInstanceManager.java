package edu.mit.streamjit.impl.distributed;

import java.util.concurrent.CountDownLatch;

import edu.mit.streamjit.impl.common.TimeLogger;
import edu.mit.streamjit.impl.common.drainer.AbstractDrainer;
import edu.mit.streamjit.impl.distributed.common.AppStatus.AppStatusProcessor;
import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.Drained;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.SNDrainProcessor;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.SNDrainedData;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;
import edu.mit.streamjit.impl.distributed.runtimer.DistributedDrainer;

/**
 * This class is responsible to manage an {@link AppInstance} including
 * starting, stopping and draining it.
 * 
 * @author sumanan
 * @since 22 Jul, 2015
 */
public class AppInstanceManager {

	private final AppInstance appInst;
	final AbstractDrainer drainer;
	private final StreamJitAppManager appManager;
	AppStatusProcessorImpl apStsPro;
	SNDrainProcessorImpl dp;

	AppInstanceManager(AppInstance appInst, TimeLogger logger,
			StreamJitAppManager appManager) {
		this.appInst = appInst;
		// TODO:
		// Read this. Don't let the "this" reference escape during construction
		// http://www.ibm.com/developerworks/java/library/j-jtp0618/
		this.drainer = new DistributedDrainer(appInst, logger, this);
		this.appManager = appManager;
		this.apStsPro = new AppStatusProcessorImpl(appManager.noOfnodes);
		this.dp = new SNDrainProcessorImpl(drainer);
	}

	public AppStatusProcessor appStatusProcessor() {
		return apStsPro;
	}

	public SNDrainProcessor drainProcessor() {
		return dp;
	}

	/**
	 * {@link AppStatusProcessor} at {@link Controller} side.
	 * 
	 * @author Sumanan sumanan@mit.edu
	 * @since Aug 11, 2013
	 */
	class AppStatusProcessorImpl implements AppStatusProcessor {

		boolean compilationError;

		private CountDownLatch compileLatch;

		volatile boolean error;

		private final int noOfnodes;

		private AppStatusProcessorImpl(int noOfnodes) {
			this.noOfnodes = noOfnodes;
		}

		@Override
		public void processCOMPILATION_ERROR() {
			System.err.println("Compilation error");
			this.compilationError = true;
			compileLatch.countDown();
		}

		@Override
		public void processCOMPILED() {
			compileLatch.countDown();
		}

		@Override
		public void processERROR() {
			this.error = true;
			// This will release the OpenTuner thread which is waiting for fixed
			// output.
			appManager.tailChannel.reset();
		}

		@Override
		public void processNO_APP() {
		}

		@Override
		public void processNOT_STARTED() {
		}

		@Override
		public void processRUNNING() {
		}

		@Override
		public void processSTOPPED() {
		}

		void reset() {
			compileLatch = new CountDownLatch(noOfnodes);
			this.compilationError = false;
			this.error = false;
		}

		boolean waitForCompilation() {
			try {
				compileLatch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return !this.compilationError;
		}
	}

	/**
	 * {@link DrainProcessor} at {@link Controller} side.
	 * 
	 * @author Sumanan sumanan@mit.edu
	 * @since Aug 11, 2013
	 */
	class SNDrainProcessorImpl implements SNDrainProcessor {

		AbstractDrainer drainer;

		public SNDrainProcessorImpl(AbstractDrainer drainer) {
			this.drainer = drainer;
		}

		@Override
		public void process(Drained drained) {
			drainer.drained(drained.blobID);
		}

		@Override
		public void process(SNDrainedData snDrainedData) {
			if (Options.useDrainData)
				drainer.newSNDrainData(snDrainedData);
		}
	}
}