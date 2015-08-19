package edu.mit.streamjit.impl.distributed;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.common.TimeLogger;
import edu.mit.streamjit.impl.common.drainer.AbstractDrainer;
import edu.mit.streamjit.impl.distributed.BufferSizeCalc.GraphSchedule;
import edu.mit.streamjit.impl.distributed.common.AppStatus.AppStatusProcessor;
import edu.mit.streamjit.impl.distributed.common.CTRLCompilationInfo;
import edu.mit.streamjit.impl.distributed.common.CTRLRMessageElement;
import edu.mit.streamjit.impl.distributed.common.CTRLRMessageElement.CTRLRMessageElementHolder;
import edu.mit.streamjit.impl.distributed.common.CompilationInfo.BufferSizes;
import edu.mit.streamjit.impl.distributed.common.CompilationInfo.CompilationInfoProcessor;
import edu.mit.streamjit.impl.distributed.common.CompilationInfo.InitScheduleCompleted;
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
	CompilationInfoProcessorImpl ciP;

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
		this.ciP = new CompilationInfoProcessorImpl(appManager.noOfnodes);
	}

	public AppStatusProcessor appStatusProcessor() {
		return apStsPro;
	}

	public SNDrainProcessor drainProcessor() {
		return dp;
	}

	public CompilationInfoProcessor compilationInfoProcessor() {
		return ciP;
	}

	GraphSchedule graphSchedule;
	void sendDeadlockfreeBufSizes() {
		ciP.waitforBufSizes();
		if (!apStsPro.compilationError) {
			graphSchedule = BufferSizeCalc.finalInputBufSizes(ciP.bufSizes,
					appInst);
			ImmutableMap<Token, Integer> finalInputBuf = graphSchedule.bufferSizes;
			CTRLRMessageElement me = new CTRLCompilationInfo.FinalBufferSizes(
					finalInputBuf);
			appManager.controller
					.sendToAll(new CTRLRMessageElementHolder(me, 1));
		}
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

	/**
	 * Added on [2014-03-01]
	 * 
	 * @author sumanan
	 * 
	 */
	class CompilationInfoProcessorImpl implements CompilationInfoProcessor {

		private Map<Integer, BufferSizes> bufSizes;

		private final int noOfnodes;
		private CountDownLatch bufSizeLatch;

		@Override
		public void process(BufferSizes bufferSizes) {
			bufSizes.put(bufferSizes.machineID, bufferSizes);
			bufSizeLatch.countDown();
		}

		private CompilationInfoProcessorImpl(int noOfnodes) {
			this.noOfnodes = noOfnodes;
		}

		void reset() {
			bufSizes = new ConcurrentHashMap<>();
			bufSizeLatch = new CountDownLatch(noOfnodes);
		}

		private void waitforBufSizes() {
			try {
				bufSizeLatch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			for (Integer nodeID : appManager.controller.getAllNodeIDs()) {
				if (!bufSizes.containsKey(nodeID)) {
					throw new AssertionError(
							"Not all Stream nodes have sent the buffer size info");
				}
			}
		}

		private volatile CountDownLatch initScheduleLatch;
		@Override
		public void process(InitScheduleCompleted initScheduleCompleted) {
			appInst.app.eLogger.logEvent(String.format("InitSchedule-%s",
					initScheduleCompleted.blobID),
					initScheduleCompleted.timeMills);
			initScheduleLatch.countDown();
		}

		private void waitforInitSchedule() {
			try {
				initScheduleLatch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}