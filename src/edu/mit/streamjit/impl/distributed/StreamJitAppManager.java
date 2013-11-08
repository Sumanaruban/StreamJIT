package edu.mit.streamjit.impl.distributed;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.common.AbstractDrainer;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement;
import edu.mit.streamjit.impl.distributed.common.CTRLRMessageElement;
import edu.mit.streamjit.impl.distributed.common.Command;
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
import edu.mit.streamjit.impl.distributed.common.SNException.MakeBlobException;
import edu.mit.streamjit.impl.distributed.common.SNException.SNExceptionProcessor;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionInfo;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;

public class StreamJitAppManager {

	private SNDrainProcessor dp = null;

	private SNExceptionProcessorImpl exP = null;

	private ErrorProcessor ep = null;

	private AppStatusProcessor ap = null;

	private final Controller controller;

	private final StreamJitApp app;

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

	public StreamJitAppManager(Controller controller, StreamJitApp app) {
		this.controller = controller;
		this.app = app;
		this.status = AppStatus.NOT_STARTED;
		this.exP = new SNExceptionProcessorImpl();
		this.ep = new ErrorProcessorImpl();
		this.ap = new AppStatusProcessorImpl();
		controller.registerManager(this);
		controller.newApp(app); // TODO: Find a good calling place.
	}

	public void reconfigure() {
		reset();
		Configuration.Builder builder = Configuration.builder(app
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

		start();
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
						+ tailToken.toString(), 0, 10000);
	}

	/**
	 * Start the execution of the StreamJit application.
	 */
	private void start() {
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
			tailChannel.stop(isFinal);
			try {
				tailThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		if (isFinal) {
			this.status = AppStatus.STOPPED;
			tailChannel.reset();
			controller.closeAll();
		}
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
		return ap;
	}

	public AppStatus getStatus() {
		return status;
	}

	private void reset() {
		exP.exConInfos = new HashSet<>();
	}

	private void stop() {

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

		@Override
		public void process(MakeBlobException mbEx) {

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
		}

		@Override
		public void processWORKER_NOT_FOUND() {
		}

		@Override
		public void processBLOB_NOT_FOUND() {
		}
	}

	/**
	 * {@link AppStatusProcessor} at {@link Controller} side.
	 * 
	 * @author Sumanan sumanan@mit.edu
	 * @since Aug 11, 2013
	 */
	private class AppStatusProcessorImpl implements AppStatusProcessor {

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
	}
}
