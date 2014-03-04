package edu.mit.streamjit.impl.distributed;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.common.AbstractDrainer;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.CTRLCompilationInfo;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement;
import edu.mit.streamjit.impl.distributed.common.CTRLRMessageElement;
import edu.mit.streamjit.impl.distributed.common.Command;
import edu.mit.streamjit.impl.distributed.common.CompilationInfo.BufferSizes;
import edu.mit.streamjit.impl.distributed.common.CompilationInfo.CompilationInfoProcessor;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.AppStatus.AppStatusProcessor;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryInputChannel;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryOutputChannel;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString.ConfigurationStringProcessor.ConfigType;
import edu.mit.streamjit.impl.distributed.common.Error.ErrorProcessor;
import edu.mit.streamjit.impl.distributed.common.MiscCtrlElements.NewConInfo;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.Drained;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.DrainedData;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.SNDrainProcessor;
import edu.mit.streamjit.impl.distributed.common.SNException;
import edu.mit.streamjit.impl.distributed.common.SNException.AddressBindException;
import edu.mit.streamjit.impl.distributed.common.SNException.SNExceptionProcessor;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionInfo;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;

/**
 * @author Sumanan sumanan@mit.edu
 * @since Oct 30, 2013
 */
public class StreamJitAppManager {

	private SNDrainProcessorImpl dp = null;

	private SNExceptionProcessorImpl exP = null;

	private ErrorProcessor ep = null;

	private AppStatusProcessorImpl apStsPro = null;

	private CompilationInfoProcessorImpl ciP = null;

	private final Controller controller;

	private final StreamJitApp app;

	private final ConfigurationManager cfgManager;

	private boolean isRunning;

	/**
	 * A {@link BoundaryOutputChannel} for the head of the stream graph. If the
	 * first {@link Worker} happened to fall outside the {@link Controller}, we
	 * need to push the {@link CompiledStream}.offer() data to the first
	 * {@link Worker} of the streamgraph.
	 */
	private BoundaryOutputChannel headChannel;

	/**
	 * A {@link BoundaryInputChannel} for the tail of the whole stream graph. If
	 * the sink {@link Worker} happened to fall outside the {@link Controller},
	 * we need to pull the sink's output in to the {@link Controller} in order
	 * to make {@link CompiledStream} .pull() to work.
	 */
	private TailChannel tailChannel;

	private Thread headThread;

	private Thread tailThread;

	private volatile AppStatus status;

	Map<Token, TCPConnectionInfo> conInfoMap;

	public StreamJitAppManager(Controller controller, StreamJitApp app,
			ConfigurationManager cfgManager) {
		int noOfnodes = controller.getAllNodeIDs().size();
		this.controller = controller;
		this.app = app;
		this.cfgManager = cfgManager;
		this.status = AppStatus.NOT_STARTED;
		this.exP = new SNExceptionProcessorImpl();
		this.ep = new ErrorProcessorImpl();
		this.apStsPro = new AppStatusProcessorImpl(noOfnodes);
		this.ciP = new CompilationInfoProcessorImpl(noOfnodes);
		controller.registerManager(this);
		controller.newApp(cfgManager.getStaticConfiguration()); // TODO: Find a
																// good calling
																// place.
		isRunning = false;
	}

	public boolean reconfigure() {
		reset();
		Configuration.Builder builder = Configuration.builder(cfgManager
				.getDynamicConfiguration());

		Map<Token, Map.Entry<Integer, Integer>> tokenMachineMap = new HashMap<>();
		Map<Token, Integer> portIdMap = new HashMap<>();

		conInfoMap = controller.buildConInfoMap(app.partitionsMachineMap,
				app.source, app.sink);

		builder.putExtraData(GlobalConstants.TOKEN_MACHINE_MAP, tokenMachineMap)
				.putExtraData(GlobalConstants.PORTID_MAP, portIdMap);

		builder.putExtraData(GlobalConstants.CONINFOMAP, conInfoMap);

		Configuration cfg = builder.build();
		String jsonStirng = cfg.toJson();

		ImmutableMap<Integer, DrainData> drainDataMap = app.getDrainData();

		for (int nodeID : controller.getAllNodeIDs()) {
			ConfigurationString json = new ConfigurationString(jsonStirng,
					ConfigType.DYNAMIC, drainDataMap.get(nodeID));
			controller.send(nodeID, json);
		}

		setupHeadTail(conInfoMap, app.bufferMap,
				Token.createOverallInputToken(app.source),
				Token.createOverallOutputToken(app.sink));

		ciP.waitforBufSizes();
		sendNewbufSizes();

		boolean isCompiled = apStsPro.waitForCompilation();

		if (isCompiled) {
			start();
			isRunning = true;
		} else {
			isRunning = false;
		}

		return isRunning;
	}

	/**
	 * Setup the headchannel and tailchannel.
	 * 
	 * @param cfg
	 * @param bufferMap
	 * @param headToken
	 * @param tailToken
	 */
	private void setupHeadTail(Map<Token, TCPConnectionInfo> conInfoMap,
			ImmutableMap<Token, Buffer> bufferMap, Token headToken,
			Token tailToken) {

		TCPConnectionInfo headconInfo = conInfoMap.get(headToken);
		assert headconInfo != null : "No head connection info exists in conInfoMap";
		assert headconInfo.getSrcID() == controller.controllerNodeID
				|| headconInfo.getDstID() == controller.controllerNodeID : "Head channel should start from the controller. "
				+ headconInfo;

		if (!bufferMap.containsKey(headToken))
			throw new IllegalArgumentException(
					"No head buffer in the passed bufferMap.");

		headChannel = new HeadChannel(bufferMap.get(headToken),
				controller.getConProvider(), headconInfo, "headChannel - "
						+ headToken.toString(), 0);

		TCPConnectionInfo tailconInfo = conInfoMap.get(tailToken);
		assert tailconInfo != null : "No tail connection info exists in conInfoMap";
		assert tailconInfo.getSrcID() == controller.controllerNodeID
				|| tailconInfo.getDstID() == controller.controllerNodeID : "Tail channel should ends at the controller. "
				+ tailconInfo;

		if (!bufferMap.containsKey(tailToken))
			throw new IllegalArgumentException(
					"No tail buffer in the passed bufferMap.");

		tailChannel = new TailChannel(bufferMap.get(tailToken),
				controller.getConProvider(), tailconInfo, "tailChannel - "
						+ tailToken.toString(), 0, GlobalConstants.outputCount);
	}

	/**
	 * Start the execution of the StreamJit application.
	 */
	private void start() {
		if (isRunning)
			throw new IllegalStateException("Application is already running.");

		if (headChannel != null) {
			headThread = new Thread(headChannel.getRunnable(),
					headChannel.name());
			headThread.start();
		}

		controller.sendToAll(Command.START);

		if (tailChannel != null) {
			tailChannel.reset();
			tailThread = new Thread(tailChannel.getRunnable(),
					tailChannel.name());
			tailThread.start();
		}
	}

	public boolean isRunning() {
		return isRunning;
	}

	public void drainingStarted(boolean isFinal) {
		if (headChannel != null) {
			headChannel.stop(isFinal);
			try {
				headThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public void drain(Token blobID, boolean isFinal) {
		// System.out.println("Drain requested to blob " + blobID);
		if (!app.blobtoMachineMap.containsKey(blobID))
			throw new IllegalArgumentException(blobID
					+ " not found in the blobtoMachineMap");
		int nodeID = app.blobtoMachineMap.get(blobID);
		controller
				.send(nodeID, new CTRLRDrainElement.DoDrain(blobID, !isFinal));
	}

	public void drainingFinished(boolean isFinal) {
		System.out.println("App Manager : Draining Finished...");
		if (tailChannel != null) {
			if (isFinal)
				tailChannel.stop(1);
			else if (GlobalConstants.useDrainData)
				tailChannel.stop(2);
			else
				tailChannel.stop(3);
			try {
				tailThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		if (isFinal) {
			this.status = AppStatus.STOPPED;
			controller.closeAll();
		}
		isRunning = false;
	}

	public void awaitForFixInput() throws InterruptedException {
		tailChannel.awaitForFixInput();
	}

	public void setDrainer(AbstractDrainer drainer) {
		assert dp == null : "SNDrainProcessor has already been set";
		this.dp = new SNDrainProcessorImpl(drainer);
	}

	public SNDrainProcessor drainProcessor() {
		return dp;
	}

	public SNExceptionProcessor exceptionProcessor() {
		return exP;
	}

	public ErrorProcessor errorProcessor() {
		return ep;
	}

	public AppStatusProcessor appStatusProcessor() {
		return apStsPro;
	}

	public CompilationInfoProcessor compilationInfoProcessor() {
		return ciP;
	}

	public AppStatus getStatus() {
		return status;
	}

	private void reset() {
		exP.exConInfos = new HashSet<>();
		apStsPro.reset();
		ciP.reset();
	}

	public void stop() {
		this.status = AppStatus.STOPPED;
		tailChannel.reset();
		controller.closeAll();
		dp.drainer.stop();
	}

	/**
	 * Calculates the input buffer sizes to avoid deadlocks. Added on
	 * [2014-03-01]
	 */
	private void sendNewbufSizes() {
		Map<Token, Integer> minInputBufCapacity = new HashMap<>();
		Map<Token, Integer> minOutputBufCapacity = new HashMap<>();
		ImmutableMap.Builder<Token, Integer> finalInputBufCapacity = new ImmutableMap.Builder<>();
		Map<Token, Integer> IORatio = new HashMap<>();

		for (BufferSizes b : ciP.bufSizes.values()) {
			minInputBufCapacity.putAll(b.minInputBufCapacity);
			minOutputBufCapacity.putAll(b.minOutputBufCapacity);
		}
		System.out.println("minInputBufCapacity requirement");
		for (Map.Entry<Token, Integer> en : minInputBufCapacity.entrySet()) {
			System.out.println(en.getKey() + " - " + en.getValue());
		}
		System.out.println("minOutputBufCapacity requirement");
		for (Map.Entry<Token, Integer> en : minOutputBufCapacity.entrySet()) {
			System.out.println(en.getKey() + " - " + en.getValue());
		}

		for (Token t : minInputBufCapacity.keySet()) {
			if (t.isOverallInput())
				continue;
			int outSize = minOutputBufCapacity.get(t);
			int inSize = minInputBufCapacity.get(t);
			IORatio.put(t, (int) Math.ceil(((double) inSize) / outSize));
		}

		for (Token blob : app.blobGraph.getBlobIds()) {
			int mul = 1;
			Set<Token> outputs = app.blobGraph.getOutputs(blob);
			for (Token out : outputs) {
				if (out.isOverallOutput())
					continue;
				mul = Math.max(mul, IORatio.get(out));
			}
			System.out.println("Multiplication factor of blob "
					+ blob.toString() + " is " + mul);
			for (Token out : outputs) {
				if (out.isOverallOutput())
					continue;
				int outSize = minOutputBufCapacity.get(out);
				int inSize = minInputBufCapacity.get(out);
				int newInSize = Math.max(outSize * mul, inSize);
				finalInputBufCapacity.put(out, newInSize);
			}
		}

		ImmutableMap<Token, Integer> finalInputBuf = finalInputBufCapacity
				.build();

		System.out.println("finalInputBufCapacity");
		for (Map.Entry<Token, Integer> en : finalInputBuf.entrySet()) {
			System.out.println(en.getKey() + " - " + en.getValue());
		}

		CTRLRMessageElement me = new CTRLCompilationInfo.FinalBufferSizes(
				finalInputBuf);
		controller.sendToAll(me);
	}

	/**
	 * {@link DrainProcessor} at {@link Controller} side.
	 * 
	 * @author Sumanan sumanan@mit.edu
	 * @since Aug 11, 2013
	 */
	private class SNDrainProcessorImpl implements SNDrainProcessor {

		AbstractDrainer drainer;

		public SNDrainProcessorImpl(AbstractDrainer drainer) {
			this.drainer = drainer;
		}

		@Override
		public void process(Drained drained) {
			drainer.drained(drained.blobID);
		}

		@Override
		public void process(DrainedData drainedData) {
			if (GlobalConstants.useDrainData)
				drainer.newDrainData(drainedData);
		}
	}

	private class SNExceptionProcessorImpl implements SNExceptionProcessor {

		private final Object abExLock = new Object();

		private Set<TCPConnectionInfo> exConInfos;

		private SNExceptionProcessorImpl() {
			exConInfos = new HashSet<>();
		}

		@Override
		public void process(SNException ex) {
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
				for (Map.Entry<Token, TCPConnectionInfo> entry : conInfoMap
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

				TCPConnectionInfo coninfo = controller
						.getNewTCPConInfo(abEx.conInfo);

				exConInfos.add(abEx.conInfo);

				CTRLRMessageElement msg = new NewConInfo(coninfo, t);
				controller.send(coninfo.getSrcID(), msg);
				controller.send(coninfo.getDstID(), msg);
			}
		}
	}

	/**
	 * {@link ErrorProcessor} at {@link Controller} side.
	 * 
	 * @author Sumanan sumanan@mit.edu
	 * @since Aug 11, 2013
	 */
	private class ErrorProcessorImpl implements ErrorProcessor {

		@Override
		public void processFILE_NOT_FOUND() {
			System.err
					.println("No application jar file in streamNode. Terminating...");
			stop();
		}

		@Override
		public void processWORKER_NOT_FOUND() {
			System.err
					.println("No top level class in the jar file. Terminating...");
			stop();
		}
	}

	/**
	 * {@link AppStatusProcessor} at {@link Controller} side.
	 * 
	 * @author Sumanan sumanan@mit.edu
	 * @since Aug 11, 2013
	 */
	private class AppStatusProcessorImpl implements AppStatusProcessor {

		private CountDownLatch compileLatch;

		private boolean compilationError;

		private final int noOfnodes;

		private AppStatusProcessorImpl(int noOfnodes) {
			this.noOfnodes = noOfnodes;
		}

		@Override
		public void processRUNNING() {
		}

		@Override
		public void processSTOPPED() {
		}

		@Override
		public void processERROR() {
		}

		@Override
		public void processNOT_STARTED() {
		}

		@Override
		public void processNO_APP() {
		}

		@Override
		public void processCOMPILED() {
			compileLatch.countDown();
		}

		@Override
		public void processCOMPILATION_ERROR() {
			System.err.println("Compilation error");
			this.compilationError = true;
			compileLatch.countDown();
		}

		private void reset() {
			compileLatch = new CountDownLatch(noOfnodes);
			this.compilationError = false;
		}

		private boolean waitForCompilation() {
			try {
				compileLatch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return !this.compilationError;
		}
	}

	/**
	 * Added on [2014-03-01]
	 * 
	 * @author sumanan
	 * 
	 */
	private class CompilationInfoProcessorImpl
			implements
				CompilationInfoProcessor {

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

		private void reset() {
			bufSizes = new ConcurrentHashMap<>();
			bufSizeLatch = new CountDownLatch(noOfnodes);
		}

		private void waitforBufSizes() {
			try {
				bufSizeLatch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			for (Integer nodeID : controller.getAllNodeIDs()) {
				if (!bufSizes.containsKey(nodeID)) {
					throw new AssertionError(
							"Not all Stream nodes have sent the buffer size info");
				}
			}
		}
	}
}
