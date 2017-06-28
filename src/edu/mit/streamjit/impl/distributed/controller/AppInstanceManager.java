package edu.mit.streamjit.impl.distributed.controller;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.TimeLogger;
import edu.mit.streamjit.impl.common.drainer.AbstractDrainer;
import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.AppStatus.AppStatusProcessor;
import edu.mit.streamjit.impl.distributed.common.CTRLCompilationInfo;
import edu.mit.streamjit.impl.distributed.common.CTRLCompilationInfo.InitSchedule;
import edu.mit.streamjit.impl.distributed.common.CTRLRMessageElement;
import edu.mit.streamjit.impl.distributed.common.CTRLRMessageElement.CTRLRMessageElementHolder;
import edu.mit.streamjit.impl.distributed.common.Command;
import edu.mit.streamjit.impl.distributed.common.CompilationInfo;
import edu.mit.streamjit.impl.distributed.common.CompilationInfo.BufferSizes;
import edu.mit.streamjit.impl.distributed.common.CompilationInfo.CompilationInfoProcessor;
import edu.mit.streamjit.impl.distributed.common.CompilationInfo.DrainDataSizes;
import edu.mit.streamjit.impl.distributed.common.CompilationInfo.InitScheduleCompleted;
import edu.mit.streamjit.impl.distributed.common.CompilationInfo.State;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.Error;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.MiscCtrlElements.NewConInfo;
import edu.mit.streamjit.impl.distributed.common.NodeInfo;
import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.Drained;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.SNDrainProcessor;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.SNDrainedData;
import edu.mit.streamjit.impl.distributed.common.SNException;
import edu.mit.streamjit.impl.distributed.common.SNException.AddressBindException;
import edu.mit.streamjit.impl.distributed.common.SNException.SNExceptionProcessor;
import edu.mit.streamjit.impl.distributed.common.SNMessageVisitor;
import edu.mit.streamjit.impl.distributed.common.SNTimeInfo;
import edu.mit.streamjit.impl.distributed.common.SystemInfo;
import edu.mit.streamjit.impl.distributed.controller.BufferSizeCalc.GraphSchedule;
import edu.mit.streamjit.impl.distributed.node.StreamNode;
import edu.mit.streamjit.impl.distributed.profiler.SNProfileElement;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;
import edu.mit.streamjit.util.CollectionUtils;
import edu.mit.streamjit.util.ConfigurationUtils;
import edu.mit.streamjit.util.DrainDataUtils;
import edu.mit.streamjit.util.EventTimeLogger;
import edu.mit.streamjit.util.EventTimeLogger.PrefixedEventTimeLogger;

/**
 * This class is responsible to manage an {@link AppInstance} including
 * starting, stopping and draining it.
 * 
 * @author sumanan
 * @since 22 Jul, 2015
 */
public class AppInstanceManager {

	final AppInstance appInst;
	final AbstractDrainer drainer;
	final StreamJitAppManager appManager;
	final TimeLogger logger;
	AppStatusProcessorImpl apStsPro;
	SNDrainProcessorImpl dp;
	CompilationInfoProcessorImpl ciP;
	private final SNExceptionProcessorImpl exP;
	public final SNMessageVisitor mv;
	boolean isRunning = false;

	private ImmutableMap<Token, Integer> drainDataSizes = null;

	private DrainData states = null;

	/**
	 * TODO: An AppInstance can have three states about compilation:
	 * <ol>
	 * <li>Not compiled.
	 * <li>Compilation failed due to compilation errors.
	 * <li>Compilation successful.
	 * </ol>
	 * <p>
	 * This boolean flag distinguishes the third, which is Compilation
	 * successful, from the rest.
	 */
	boolean isCompiled = false;

	private GraphSchedule graphSchedule;

	Map<Token, ConnectionInfo> conInfoMap;

	final HeadTailHandler headTailHandler;

	private final CountDownLatch latch = new CountDownLatch(1);

	public final EventTimeLogger eLogger;

	AppInstanceManager(AppInstance appInst, StreamJitAppManager appManager) {
		this.appInst = appInst;
		this.appManager = appManager;
		this.logger = appInst.app.logger;
		this.eLogger = new PrefixedEventTimeLogger(appInst.app.eLogger,
				new Integer(appInst.id).toString());
		// TODO:
		// Read this. Don't let the "this" reference escape during construction
		// http://www.ibm.com/developerworks/java/library/j-jtp0618/
		this.drainer = new DistributedDrainer(appInst, logger, this);
		this.apStsPro = new AppStatusProcessorImpl(appManager.noOfnodes);
		this.dp = new SNDrainProcessorImpl(drainer);
		this.ciP = new CompilationInfoProcessorImpl(appManager.noOfnodes);
		this.mv = new SNMessageVisitorImpl();
		this.exP = new SNExceptionProcessorImpl();
		headTailHandler = new HeadTailHandler(appManager.controller,
				appInst.app);
	}

	public int appInstId() {
		return appInst.id;
	}

	public void drainingStarted(boolean isFinal) {
		eLogger.bEvent("draining");
		headTailHandler.stopHead(isFinal);
	}

	public void drainingFinished(boolean isFinal) {
		headTailHandler.waitToStopHead();
		headTailHandler.stopTail(isFinal);
		headTailHandler.waitToStopTail();
		// conInfoMap = null; Bug fix. We shouldn't use prevAIM's connections to
		// the currentAIM. At the SN side, multiple threads (prevAIM's and
		// curAIM's threads) may try to access the same connection, which will
		// throw IO exception and ultimately crashes the connection.
		appManager.drainingFinished(isFinal, this);

		long time = eLogger.eEvent("draining");
		System.out.println(String.format(
				"%s: Draining Finished. Draining time = %dms.", toString(),
				time));
		isRunning = false;
		eLogger.eEvent("totalRun");
		if (!isFinal)
			appInst.app.eLogger.eEvent("overlap"); // Not this.eLogger. Bcoz it
													// is
													// PrefixedEventTimeLogger.
	}

	/**
	 * Performs intermediate draining.
	 * 
	 * @return <code>true</code> iff the draining is success or the application
	 *         is not running currently.
	 */
	public boolean intermediateDraining() {
		boolean ret = true;
		if (isRunning) {
			ret = drainer.drainIntermediate();
			if (Options.useDrainData && Options.dumpDrainData) {
				String cfgPrefix = ConfigurationUtils
						.getConfigPrefix(appInst.configuration);
				DrainData dd = appInst.drainData;
				DrainDataUtils.dumpDrainData(dd, appInst.app.name, cfgPrefix);
			}
		}
		if (ret)
			appManager.removeAIM(appInstId());
		return ret;
	}

	/**
	 * Performs the steps that need to be done after the blobs are created.
	 * Specifically, sends deadlock free buffer sizes.
	 * 
	 * @return <code>true</code> iff the compilation process is success.
	 */
	boolean postCompilation() {
		eLogger.bEvent("postCompilation");
		sendDeadlockfreeBufSizes();
		if (apStsPro.compilationError)
			isCompiled = false;
		else
			isCompiled = apStsPro.waitForCompilation();
		eLogger.eEvent("compilation");
		logger.compilationFinished(isCompiled, "");
		eLogger.eEvent("postCompilation");
		return isCompiled;
	}

	void startChannels() {
		headTailHandler.startHead();
		headTailHandler.startTail();
		appManager.controller.sendToAll(new CTRLRMessageElementHolder(
				Command.START_CHANNELS, appInstId()));
	}

	void start() {
		if (isRunning)
			throw new IllegalStateException(String.format(
					"AppInstance %d is already running.", appInst.id));
		appManager.controller.sendToAll(new CTRLRMessageElementHolder(
				Command.START, appInstId()));
		isRunning = true;
		System.out.println(String
				.format("%s has started to run...", toString()));
		eLogger.bEvent("totalRun");
		appInst.app.eLogger.bEvent("overlap"); // Not this.eLogger. Bcoz it is
												// PrefixedEventTimeLogger.
	}

	void runInitSchedule() {
		ImmutableMap<Token, Integer> steadyRunCount = graphSchedule.steadyRunCountDuringInit;
		ciP.initScheduleLatch = new CountDownLatch(steadyRunCount.size());
		appManager.controller.sendToAll(new CTRLRMessageElementHolder(
				new InitSchedule(steadyRunCount), appInstId()));
		ciP.waitforInitSchedule();
	}

	void sendDeadlockfreeBufSizes() {
		ciP.waitforBufSizes();
		if (!apStsPro.compilationError) {
			graphSchedule = BufferSizeCalc.finalInputBufSizes(ciP.bufSizes,
					appInst);
			ImmutableMap<Token, Integer> finalInputBuf = graphSchedule.bufferSizes;
			CTRLRMessageElement me = new CTRLCompilationInfo.FinalBufferSizes(
					finalInputBuf);
			appManager.controller.sendToAll(new CTRLRMessageElementHolder(me,
					appInst.id));
		}
	}

	public String toString() {
		return String.format("AppInstanceManager-%d", appInst.id);
	}

	String dynamicCfg(Collection<ConnectionInfo> connectionsInUse) {
		Configuration.Builder builder = appInst.getDynamicConfiguration();
		addConInfoMap(builder, connectionsInUse);
		Configuration cfg = builder.build();
		return cfg.toJson();
	}

	/**
	 * Builds a new {@link ConnectionInfo} map based on this {@link AppInstance}
	 * 's partition information and adds it to the @param builder.
	 * 
	 * @param builder
	 * @param connectionsInUse
	 */
	void addConInfoMap(Configuration.Builder builder,
			Collection<ConnectionInfo> connectionsInUse) {
		conInfoMap = appManager.conManager.conInfoMap(
				appInst.getConfiguration(), appInst.partitionsMachineMap,
				connectionsInUse, appInst.app.source, appInst.app.sink);
		builder.putExtraData(GlobalConstants.CONINFOMAP, conInfoMap);
	}

	void requestDDsizes() {
		CTRLRMessageElement me = new CTRLCompilationInfo.DDSizes();
		appManager.controller.sendToAll(new CTRLRMessageElementHolder(me,
				appInstId()));
	}

	/**
	 * TODO: Like {@link #getState()}, drainDataSizes building task could be
	 * delegated to SN receiver threads.
	 * 
	 * @return
	 */
	ImmutableMap<Token, Integer> getDDsizes() {
		if (drainDataSizes == null) {
			ciP.waitforDDSizes();

			List<ImmutableMap<Token, Integer>> sizeList = new ArrayList<>();
			for (DrainDataSizes dds : ciP.ddSizes.values()) {
				sizeList.add(dds.ddSizes);
			}

			drainDataSizes = CollectionUtils.union((key, value) -> {
				if (key.equals(appInst.app.tailToken))
					return 0;
				int size = 0;
				for (Integer i : value)
					size += i;
				return size;
			}, sizeList);
		}
		return drainDataSizes;
	}

	void requestState() {
		throw new IllegalStateException("Method not implemented");
	}

	DrainData getState() {
		ciP.waitforStates();
		return states;
	}

	void releaseAllResources() {
		latch.countDown();
	}

	void waitToStop() {
		Stopwatch s = Stopwatch.createStarted();
		try {
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println(String.format("AIM-%d. waitToStop time = %dms",
				appInstId(), s.elapsed(TimeUnit.MILLISECONDS)));
	}

	public GraphSchedule graphSchedule() {
		return graphSchedule;
	}

	/**
	 * Sends the message @param me to the {@link StreamNode} on which the blob
	 * with id=@param blobID is running.
	 * 
	 * @param blobID
	 * @param me
	 */
	public void sendToBlob(Token blobID, CTRLRMessageElement me) {
		if (!appInst.blobtoMachineMap.containsKey(blobID))
			throw new IllegalArgumentException(blobID
					+ " not found in the blobtoMachineMap");
		int nodeID = appInst.blobtoMachineMap.get(blobID);
		appManager.controller.send(nodeID, new CTRLRMessageElementHolder(me,
				appInstId()));
	}

	/**
	 * @param me
	 */
	public void sendToAll(CTRLRMessageElement me) {
		appManager.controller.sendToAll(new CTRLRMessageElementHolder(me,
				appInstId()));
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
			headTailHandler.tailChannel.reset();
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

		private CompilationInfoProcessorImpl(int noOfnodes) {
			bufSizes = new ConcurrentHashMap<>();
			bufSizeLatch = new CountDownLatch(noOfnodes);
			ddSizes = new ConcurrentHashMap<>();
			ddSizesLatch = new CountDownLatch(noOfnodes);
			stateMap = new ConcurrentHashMap<>();
			stateLatch = new CountDownLatch(appInst.blobGraph.getBlobIds()
					.size());
		}

		// BufferSizes related variables and methods.
		private Map<Integer, BufferSizes> bufSizes;
		private CountDownLatch bufSizeLatch;

		@Override
		public void process(BufferSizes bufferSizes) {
			bufSizes.put(bufferSizes.machineID, bufferSizes);
			bufSizeLatch.countDown();
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

		// InitScheduleCompleted related variables and methods.
		private volatile CountDownLatch initScheduleLatch;

		@Override
		public void process(InitScheduleCompleted initScheduleCompleted) {
			eLogger.logEvent(String.format("InitSchedule-%s",
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

		// DrainDataSizes related variables and methods.
		private Map<Integer, DrainDataSizes> ddSizes;
		private CountDownLatch ddSizesLatch;

		@Override
		public void process(DrainDataSizes ddSizes) {
			this.ddSizes.put(ddSizes.machineID, ddSizes);
			ddSizesLatch.countDown();
		}

		private void waitforDDSizes() {
			try {
				ddSizesLatch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			for (Integer nodeID : appManager.controller.getAllNodeIDs()) {
				if (!ddSizes.containsKey(nodeID)) {
					throw new AssertionError(
							"Not all Stream nodes have sent the buffer size info");
				}
			}
		}

		// State related variables and methods.
		private Map<Token, DrainData> stateMap;
		private CountDownLatch stateLatch;

		@Override
		public synchronized void process(State state) {
			this.stateMap.put(state.blobID, state.drainData);
			if (states == null)
				states = state.drainData;
			else
				states = states.merge(state.drainData);
			stateLatch.countDown();
		}

		private void waitforStates() {
			try {
				stateLatch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			for (Token blobID : appInst.blobGraph.getBlobIds()) {
				if (!stateMap.containsKey(blobID)) {
					throw new AssertionError(
							"Not all blobs have sent their state");
				}
			}
		}
	}

	private class SNExceptionProcessorImpl implements SNExceptionProcessor {

		private final Object abExLock = new Object();

		private Set<ConnectionInfo> exConInfos;

		private SNExceptionProcessorImpl() {
			exConInfos = new HashSet<>();
		}

		@Override
		public void process(AddressBindException abEx) {
			synchronized (abExLock) {
				if (exConInfos.contains(abEx.conInfo)) {
					System.out
							.println("AddressBindException : Already handled...");
					return;
				}

				Token t = null;
				for (Map.Entry<Token, ConnectionInfo> entry : conInfoMap
						.entrySet()) {
					if (abEx.conInfo.equals(entry.getValue())) {
						t = entry.getKey();
						break;
					}
				}

				if (t == null) {
					throw new IllegalArgumentException(
							"Illegal TCP connection - " + abEx.conInfo);
				}

				ConnectionInfo coninfo = appManager.conManager
						.replaceConInfo(abEx.conInfo);

				exConInfos.add(abEx.conInfo);

				CTRLRMessageElement msg = new NewConInfo(coninfo, t);

				// TODO: seamless. correct appInstId must be passed.
				appManager.controller.send(coninfo.getSrcID(),
						new CTRLRMessageElementHolder(msg, 1));
				appManager.controller.send(coninfo.getDstID(),
						new CTRLRMessageElementHolder(msg, 1));
			}
		}

		@Override
		public void process(SNException ex) {
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
			snException.process(exP);
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