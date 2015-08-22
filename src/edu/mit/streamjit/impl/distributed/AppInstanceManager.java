package edu.mit.streamjit.impl.distributed;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.common.TimeLogger;
import edu.mit.streamjit.impl.common.drainer.AbstractDrainer;
import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.AppStatus.AppStatusProcessor;
import edu.mit.streamjit.impl.distributed.common.CTRLCompilationInfo;
import edu.mit.streamjit.impl.distributed.common.CTRLRMessageElement;
import edu.mit.streamjit.impl.distributed.common.CTRLRMessageElement.CTRLRMessageElementHolder;
import edu.mit.streamjit.impl.distributed.common.CompilationInfo;
import edu.mit.streamjit.impl.distributed.common.CompilationInfo.BufferSizes;
import edu.mit.streamjit.impl.distributed.common.CompilationInfo.CompilationInfoProcessor;
import edu.mit.streamjit.impl.distributed.common.Error;
import edu.mit.streamjit.impl.distributed.common.NodeInfo;
import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.Drained;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.SNDrainProcessor;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.SNDrainedData;
import edu.mit.streamjit.impl.distributed.common.SNException;
import edu.mit.streamjit.impl.distributed.common.SNMessageVisitor;
import edu.mit.streamjit.impl.distributed.common.SNTimeInfo;
import edu.mit.streamjit.impl.distributed.common.SystemInfo;
import edu.mit.streamjit.impl.distributed.profiler.SNProfileElement;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;

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
	final StreamJitAppManager appManager;
	AppStatusProcessorImpl apStsPro;
	SNDrainProcessorImpl dp;
	CompilationInfoProcessorImpl ciP;
	public final SNMessageVisitor mv;

	AppInstanceManager(AppInstance appInst, TimeLogger logger,
			StreamJitAppManager appManager) {
		this.appInst = appInst;
		this.appManager = appManager;
		// TODO:
		// Read this. Don't let the "this" reference escape during construction
		// http://www.ibm.com/developerworks/java/library/j-jtp0618/
		this.drainer = new DistributedDrainer(appInst, logger, this);
		this.apStsPro = new AppStatusProcessorImpl(appManager.noOfnodes);
		this.dp = new SNDrainProcessorImpl(drainer);
		this.ciP = new CompilationInfoProcessorImpl(appManager.noOfnodes);
		this.mv = new SNMessageVisitorImpl();
	}

	public int appInstId() {
		return appInst.id;
	}

	void sendDeadlockfreeBufSizes() {
		ciP.waitforBufSizes();
		if (!apStsPro.compilationError) {
			ImmutableMap<Token, Integer> finalInputBuf = BufferSizeCalc
					.finalInputBufSizes(ciP.bufSizes, appInst);
			CTRLRMessageElement me = new CTRLCompilationInfo.FinalBufferSizes(
					finalInputBuf);
			appManager.controller.sendToAll(new CTRLRMessageElementHolder(me,
					appInst.id));
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

		private AppStatusProcessorImpl(int noOfnodes) {
			compileLatch = new CountDownLatch(noOfnodes);
			this.compilationError = false;
			this.error = false;
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
				drainer.drainDataHandler.newSNDrainData(snDrainedData);
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

		private CountDownLatch bufSizeLatch;

		@Override
		public void process(BufferSizes bufferSizes) {
			bufSizes.put(bufferSizes.machineID, bufferSizes);
			bufSizeLatch.countDown();
		}

		private CompilationInfoProcessorImpl(int noOfnodes) {
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
	}

	/**
	 * @author Sumanan sumanan@mit.edu
	 * @since May 20, 2013
	 */
	private class SNMessageVisitorImpl implements SNMessageVisitor {

		@Override
		public void visit(Error error) {
			error.process(appManager.errorProcessor());
		}

		@Override
		public void visit(AppStatus appStatus) {
			appStatus.process(apStsPro);
		}

		@Override
		public void visit(SNDrainElement snDrainElement) {
			snDrainElement.process(dp);
		}

		@Override
		public void visit(SNException snException) {
			snException.process(appManager.exceptionProcessor());
		}

		@Override
		public void visit(SNTimeInfo timeInfo) {
			timeInfo.process(appManager.timeInfoProcessor());
		}

		@Override
		public void visit(CompilationInfo compilationInfo) {
			compilationInfo.process(ciP);
		}

		@Override
		public void visit(SNProfileElement snProfileElement) {
			snProfileElement.process(appManager.getProfiler());
		}

		@Override
		public void visit(SystemInfo systemInfo) {
			throw new UnsupportedOperationException(
					"AppInstanceManager's SNMessageVisitor does not process SystemInfo."
							+ " StreamNodeAgent's SNMessageVisitor must be called.");
		}

		@Override
		public void visit(NodeInfo nodeInfo) {
			throw new UnsupportedOperationException(
					"AppInstanceManager's SNMessageVisitor does not process NodeInfo."
							+ " StreamNodeAgent's SNMessageVisitor must be called.");
		}
	}
}