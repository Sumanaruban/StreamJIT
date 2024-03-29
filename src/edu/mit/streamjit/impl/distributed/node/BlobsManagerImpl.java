/*
 * Copyright (c) 2013-2014 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package edu.mit.streamjit.impl.distributed.node;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.common.drainer.AbstractDrainer.DrainDataAction;
import edu.mit.streamjit.impl.compiler2.Compiler2BlobHost;
import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryInputChannel;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryOutputChannel;
import edu.mit.streamjit.impl.distributed.common.CTRLCompilationInfo.CTRLCompilationInfoProcessor;
import edu.mit.streamjit.impl.distributed.common.CTRLCompilationInfo.DDSizes;
import edu.mit.streamjit.impl.distributed.common.CTRLCompilationInfo.FinalBufferSizes;
import edu.mit.streamjit.impl.distributed.common.CTRLCompilationInfo.InitSchedule;
import edu.mit.streamjit.impl.distributed.common.CTRLCompilationInfo.InitialState;
import edu.mit.streamjit.impl.distributed.common.CTRLCompilationInfo.RequestState;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.CTRLRDrainProcessor;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.DoDrain;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.DrainDataRequest;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.ReduceCore;
import edu.mit.streamjit.impl.distributed.common.Command.CommandProcessor;
import edu.mit.streamjit.impl.distributed.common.CompilationInfo.DrainDataSizes;
import edu.mit.streamjit.impl.distributed.common.CompilationInfo.State;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionProvider;
import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.impl.distributed.common.SNMessageElement;
import edu.mit.streamjit.impl.distributed.common.SNMessageElement.SNMessageElementHolder;
import edu.mit.streamjit.impl.distributed.common.Utils;
import edu.mit.streamjit.impl.distributed.node.BufferManagementUtils.BlobsBufferStatus;
import edu.mit.streamjit.impl.distributed.node.BufferManagementUtils.BufferCleaner;
import edu.mit.streamjit.impl.distributed.node.BufferManagementUtils.MonitorBuffers;
import edu.mit.streamjit.impl.distributed.node.BufferManager.GlobalBufferManager;
import edu.mit.streamjit.impl.distributed.profiler.SNProfileElement;
import edu.mit.streamjit.impl.distributed.profiler.StreamNodeProfiler;
import edu.mit.streamjit.util.CollectionUtils;

/**
 * {@link BlobsManagerImpl} responsible to run all {@link Blob}s those are
 * assigned to the {@link StreamNode}.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
public class BlobsManagerImpl implements BlobsManager {

	Map<Token, BlobExecuter> blobExecuters;

	final BufferManager bufferManager;

	private final CommandProcessor cmdProcessor;

	private final CTRLCompilationInfoProcessor compInfoProcessor;

	private final Map<Token, ConnectionInfo> conInfoMap;

	private final ConnectionProvider conProvider;

	volatile BufferCleaner bufferCleaner = null;

	private final CTRLRDrainProcessor drainProcessor;

	MonitorBuffers monBufs = null;

	final StreamNode streamNode;

	/**
	 * if true {@link BufferCleaner} will be used to unlock the draining time
	 * dead lock. Otherwise dynamic buffer will be used for local buffers to
	 * handled drain time data growth.
	 */
	final boolean useBufferCleaner = false;

	/**
	 * if true {@link MonitorBuffers} will be started to log the buffer sizes
	 * periodically.
	 */
	private final boolean monitorBuffers = false;

	final String appName;

	private ImmutableSet<StreamNodeProfiler> profilers;

	final AffinityManager affinityManager;

	public final int appInstId;

	/**
	 * {@link Starter} type.
	 */
	private final int starterType;

	private final String cfgPrefix;

	public BlobsManagerImpl(ImmutableSet<Blob> blobSet,
			Map<Token, ConnectionInfo> conInfoMap, StreamNode streamNode,
			ConnectionProvider conProvider, String appName, int appInstId,
			int starterType, String cfgPrefix) {
		this.conInfoMap = conInfoMap;
		this.streamNode = streamNode;
		this.conProvider = conProvider;
		this.appInstId = appInstId;
		this.starterType = starterType;
		this.cfgPrefix = cfgPrefix;
		this.appName = appName;

		this.cmdProcessor = new CommandProcessorImpl();
		this.drainProcessor = new CTRLRDrainProcessorImpl();
		this.compInfoProcessor = new CTRLCompilationInfoProcessorImpl(blobSet);
		this.bufferManager = new GlobalBufferManager(blobSet, streamNode,
				appInstId);
		this.affinityManager = affinityManager(blobSet);
		bufferManager.initialise();
		if (bufferManager.isbufferSizesReady())
			createBEs(blobSet);
	}

	private AffinityManager affinityManager(Set<Blob> blobSet) {
		switch (Options.AffinityManager) {
			case 0 :
				return new AffinityManagers.EmptyAffinityManager();
			case 1 :
				return new AffinityManagers.OneCoreAffinityManager();
			case 2 :
				return new AffinityManagers.EqualAffinityManager(blobSet.size());
			case 4 :
				return new AffinityManagers.AllParallelAffinityManager(blobSet);
			case 5 :
				return new AffinityManagers.FileAffinityManager(appName,
						new Integer(appInstId).toString(),
						streamNode.getNodeID());
			default : {
				AffinityManagers.CoreCodeAffinityManager am = new AffinityManagers.CoreCodeAffinityManager(
						blobSet);
				// am.dumpAffinityTable(appName,
				// new Integer(appInstId).toString(),
				// streamNode.getNodeID());
				return am;
			}
		}
	}
	/**
	 * Drain the blob identified by the token.
	 */
	public void drain(Token blobID, DrainDataAction drainDataAction) {
		BlobExecuter be = getBE(blobID);
		if (Options.doDraininNewThread)
			be.drainer.doDrain(drainDataAction,
					drainDataAction != DrainDataAction.DISCARD);
		else
			be.drainer.doDrain(drainDataAction);
	}

	public CommandProcessor getCommandProcessor() {
		return cmdProcessor;
	}

	public CTRLRDrainProcessor getDrainProcessor() {
		return drainProcessor;
	}

	public CTRLCompilationInfoProcessor getCompilationInfoProcessor() {
		return compInfoProcessor;
	}

	private BlobExecuter getBE(Token blobID) {
		for (BlobExecuter be : blobExecuters.values()) {
			if (be.getBlobID().equals(blobID))
				return be;
		}
		throw new IllegalArgumentException(String.format(
				"No blob with blobID %s", blobID));
	}

	public void reqDrainedData(Set<Token> blobSet) {
		throw new UnsupportedOperationException(
				"Method reqDrainedData not implemented");
	}

	private final Object drainedLastBlobLock = new Object();
	private boolean drainedLastBlobActionsDone = false;
	/**
	 * Each {@link BlobDrainer} must call this method after its {@link Blob} has
	 * been drained.
	 * <p>
	 * Multiple {@link BlobDrainer}s may call this method at the same time and
	 * conclude themselves as the last blob. In order to avoid that situation,
	 * we need to do the last blob actions in a synchronized block.
	 */
	void drainedLastBlobActions() {
		for (BlobExecuter be : this.blobExecuters.values()) {
			if (be.drainer.drainState < 4) {
				return;
			}
		}

		synchronized (drainedLastBlobLock) {
			if (drainedLastBlobActionsDone)
				return;

			if (this.monBufs != null)
				this.monBufs.stopMonitoring();

			if (this.bufferCleaner != null)
				this.bufferCleaner.stopit();

			unRegisterMe();
			this.streamNode.eventTimeLogger.eTuningRound(cfgPrefix);
			drainedLastBlobActionsDone = true;
		}
	}

	void unRegisterMe() {
		ExecutorService executorService = Executors.newSingleThreadExecutor();
		executorService.submit(() -> streamNode
				.unRegisterMessageVisitor(appInstId));
		executorService.shutdown();
	}

	private final Object doDrainLastBlobLock = new Object();
	private boolean doDrainLastBlobActionsDone = false;
	/**
	 * Each {@link BlobDrainer} must call this method after its {@link Blob} has
	 * been called for doDrain.
	 * <p>
	 * Multiple {@link BlobDrainer}s may call this method at the same time and
	 * conclude themselves as the last blob. In order to avoid that situation,
	 * we need to do the last blob actions in a synchronized block.
	 */
	void doDrainLastBlobActions(BlobDrainer bd) {
		if (!useBufferCleaner || bd.drainDataAction == DrainDataAction.FINISH)
			return;

		for (BlobExecuter be : blobExecuters.values()) {
			if (be.drainer.drainState == 0)
				return;
		}

		synchronized (doDrainLastBlobLock) {
			if (doDrainLastBlobActionsDone)
				return;

			if (bufferCleaner == null) {
				// System.out.println("****Starting BufferCleaner***");
				bufferCleaner = new BufferCleaner(this,
						bd.drainDataAction == DrainDataAction.SEND_BACK);
				bufferCleaner.start();
			}
			doDrainLastBlobActionsDone = true;
		}
	}

	/**
	 * Start and execute the blobs. This function should be responsible to
	 * manage all CPU and I/O threads those are related to the {@link Blob}s.
	 */
	public void start() {
		for (BlobExecuter be : blobExecuters.values())
			be.starter.start();

		if (monitorBuffers && monBufs == null) {
			// System.out.println("Creating new MonitorBuffers");
			monBufs = new MonitorBuffers(this);
			monBufs.start();
		}
	}

	public void runInitSchedule(InitSchedule initSchedule) {
		for (BlobExecuter be : blobExecuters.values()) {
			Token blobID = be.blobID;
			int steadyRunCount = initSchedule.steadyRunCount.get(blobID);
			be.starter.runInitSchedule(steadyRunCount);
		}
	}

	void startChannels() {
		for (BlobExecuter be : blobExecuters.values())
			be.starter.startChannels();
	}

	/**
	 * Stop all {@link Blob}s if running. No effect if a {@link Blob} is already
	 * stopped.
	 */
	public void stop() {
		if (blobExecuters != null)
			for (BlobExecuter be : blobExecuters.values())
				be.stop();

		if (monBufs != null)
			monBufs.stopMonitoring();

		if (bufferCleaner != null)
			bufferCleaner.stopit();
	}

	@Override
	public Set<StreamNodeProfiler> profilers() {
		if (profilers == null) {
			StreamNodeProfiler snp = new BufferProfiler();
			profilers = ImmutableSet.of(snp);
		}
		return profilers;
	}

	private void createBEs(ImmutableSet<Blob> blobSet) {
		assert bufferManager.isbufferSizesReady() : "Buffer sizes must be available to create BlobExecuters.";
		blobExecuters = new HashMap<>();
		Set<Token> locaTokens = bufferManager.localTokens();
		ImmutableMap<Token, Integer> bufferSizesMap = bufferManager
				.bufferSizes();
		for (Blob b : blobSet) {
			Token t = Utils.getBlobID(b);
			ImmutableMap<Token, BoundaryInputChannel> inputChannels = createInputChannels(
					Sets.difference(b.getInputs(), locaTokens), bufferSizesMap);
			ImmutableMap<Token, BoundaryOutputChannel> outputChannels = createOutputChannels(
					Sets.difference(b.getOutputs(), locaTokens), bufferSizesMap);
			blobExecuters.put(t, new BlobExecuter(this, t, b, inputChannels,
					outputChannels, starterType));
		}
	}

	private ImmutableMap<Token, BoundaryInputChannel> createInputChannels(
			Set<Token> inputTokens, ImmutableMap<Token, Integer> bufferMap) {
		ImmutableMap.Builder<Token, BoundaryInputChannel> inputChannelMap = new ImmutableMap.Builder<>();
		for (Token t : inputTokens) {
			ConnectionInfo conInfo = conInfoMap.get(t);
			inputChannelMap.put(t, conInfo.inputChannel(
					String.format("%s-%d", t.toString(), appInstId),
					bufferMap.get(t), conProvider));
		}
		return inputChannelMap.build();
	}

	private ImmutableMap<Token, BoundaryOutputChannel> createOutputChannels(
			Set<Token> outputTokens, ImmutableMap<Token, Integer> bufferMap) {
		ImmutableMap.Builder<Token, BoundaryOutputChannel> outputChannelMap = new ImmutableMap.Builder<>();
		for (Token t : outputTokens) {
			ConnectionInfo conInfo = conInfoMap.get(t);
			outputChannelMap.put(t, conInfo.outputChannel(
					String.format("%s-%d", t.toString(), appInstId),
					bufferMap.get(t), conProvider));
		}
		return outputChannelMap.build();
	}

	void sendToController(SNMessageElement me) {
		try {
			streamNode.controllerConnection
					.writeObject(new SNMessageElementHolder(me, appInstId));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param coreUsage
	 *            in percentage
	 */
	void updateAffinity(int coreUsage) {
		int totalProcessors = Runtime.getRuntime().availableProcessors();
		int totalCores = 1;
		switch (coreUsage) {
			case 50 :
				totalProcessors = totalProcessors / 2;
				break;
			case 25 :
				totalProcessors = totalProcessors / 4;
				break;
			default :
				totalProcessors = 1;
		}
		AffinityManager am = new AffinityManagers.EqualAffinityManager(
				blobExecuters.size(), totalCores);
		for (BlobExecuter be : blobExecuters.values())
			be.updateAffinity(am);
	}

	/**
	 * {@link CommandProcessor} at {@link StreamNode} side.
	 * 
	 * @author Sumanan sumanan@mit.edu
	 * @since May 27, 2013
	 */
	private class CommandProcessorImpl implements CommandProcessor {

		@Override
		public void processSTART() {
			start();
			System.out.println("StraemJit app is running...");
			logMemoryStatus();
		}

		public void logMemoryStatus() {
			DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			Calendar cal = Calendar.getInstance();
			long heapMaxSize = Runtime.getRuntime().maxMemory();
			long heapSize = Runtime.getRuntime().totalMemory();
			long heapFreeSize = Runtime.getRuntime().freeMemory();
			int MEGABYTE = 1024 * 1024;
			streamNode.eventTimeLogger.log(dateFormat.format(cal.getTime())
					+ "\n");
			streamNode.eventTimeLogger.log(String.format(
					"heapMaxSize = %dMB\n",
					heapMaxSize
							/ MEGABYTE));
			streamNode.eventTimeLogger.log(String
					.format("heapSize = %dMB\n", heapSize / MEGABYTE));
			streamNode.eventTimeLogger.log(String.format(
					"heapFreeSize = %dMB\n",
					heapFreeSize
							/ MEGABYTE));
		}

		@Override
		public void processSTOP() {
			stop();
			System.out.println("StraemJit app stopped...");
			sendToController(AppStatus.STOPPED);
		}

		@Override
		public void processSTART_CHANNELS() {
			startChannels();
			System.out.println("Establishing communication channels...");
		}
	}

	/**
	 * Implementation of {@link DrainProcessor} at {@link StreamNode} side. All
	 * appropriate response logic to successfully perform the draining is
	 * implemented here.
	 * 
	 * @author Sumanan sumanan@mit.edu
	 * @since Jul 30, 2013
	 */
	private class CTRLRDrainProcessorImpl implements CTRLRDrainProcessor {

		@Override
		public void process(DoDrain drain) {
			drain(drain.blobID, drain.drainDataAction);
		}

		@Override
		public void process(DrainDataRequest drnDataReq) {
			System.err.println("Not expected in current situation");
			// reqDrainedData(drnDataReq.blobsSet);
		}

		@Override
		public void process(ReduceCore reduceCore) {
			updateAffinity(reduceCore.coreUsage);
		}
	}

	private class CTRLCompilationInfoProcessorImpl implements
			CTRLCompilationInfoProcessor {

		private final ImmutableSet<Blob> blobSet;

		private CTRLCompilationInfoProcessorImpl(ImmutableSet<Blob> blobSet) {
			this.blobSet = blobSet;
		}

		@Override
		public void process(FinalBufferSizes finalBufferSizes) {
			// System.out.println("Processing FinalBufferSizes");
			bufferManager.initialise2(finalBufferSizes.minInputBufCapacity);
			assert bufferManager.isbufferSizesReady() == true : "bufferSizes are not ready";
			createBEs(blobSet);
			sendToController(AppStatus.COMPILED);
		}

		@Override
		public void process(InitSchedule initSchedule) {
			runInitSchedule(initSchedule);
		}

		@Override
		public void process(InitialState initialState) {
			insertDrainData(initialState.drainData);
		}

		@Override
		public void process(DDSizes ddSizes) {
			List<ImmutableMap<Token, Integer>> sizeList = new ArrayList<>();
			for (BlobExecuter be : blobExecuters.values()) {
				sizeList.add(((Compiler2BlobHost) be.blob).getDDSizes());
			}

			ImmutableMap<Token, Integer> sizeMap = CollectionUtils.union((key,
					value) -> {
				int size = 0;
				for (Integer i : value)
					size += i;
				return size;
			}, sizeList);

			sendToController(new DrainDataSizes(streamNode.getNodeID(), sizeMap));
		}

		@Override
		public void process(RequestState requestState) {
			Token blobID = requestState.blobID;
			BlobExecuter be = getBE(blobID);
			((Compiler2BlobHost) be.blob).requestState(
					requestState.sendStateAt, new StateCallback(blobID));
		}
	}

	public class BufferProfiler implements StreamNodeProfiler {

		final BlobsBufferStatus bbs = new BlobsBufferStatus(
				BlobsManagerImpl.this);

		@Override
		public SNProfileElement profile() {
			return bbs.snBufferStatusData();
		}
	}

	public void insertDrainData(DrainData initialState) {
		for (BlobExecuter be : blobExecuters.values())
			be.blob.insertDrainData(initialState);
	}

	public class StateCallback {
		private final Token blobID;

		StateCallback(Token blobID) {
			this.blobID = blobID;
		}

		public void sendState(DrainData state) {
			sendToController(new State(blobID, state));
		}
	}
}
