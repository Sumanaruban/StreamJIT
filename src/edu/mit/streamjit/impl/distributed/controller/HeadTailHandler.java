package edu.mit.streamjit.impl.distributed.controller;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.common.BufferWriteCounter;
import edu.mit.streamjit.impl.common.Counter;
import edu.mit.streamjit.impl.common.drainer.BlobGraph;
import edu.mit.streamjit.impl.distributed.common.AsyncTCPConnection.AsyncTCPConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryInputChannel;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryOutputChannel;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionProvider;
import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionInfo;
import edu.mit.streamjit.impl.distributed.controller.HT.HeadChannelSeamless;
import edu.mit.streamjit.impl.distributed.controller.HT.HeadChannels;
import edu.mit.streamjit.impl.distributed.controller.HT.HeadTail;
import edu.mit.streamjit.impl.distributed.controller.HT.TailChannel;
import edu.mit.streamjit.impl.distributed.controller.HT.TailChannels;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;
import edu.mit.streamjit.tuner.EventTimeLogger;
import edu.mit.streamjit.util.ConfigurationUtils;

/**
 * {@link StreamJitAppManager} refactored and its Head, Tail channel related
 * methods have been moved here. This class can be made as an inner class of
 * {@link StreamJitAppManager}.
 * 
 * @author sumanan
 * @since 20 Oct, 2015
 */
class HeadTailHandler {

	final Controller controller;

	final StreamJitApp<?, ?> app;

	/**
	 * A {@link BoundaryOutputChannel} for the head of the stream graph. If the
	 * first {@link Worker} happened to fall outside the {@link Controller}, we
	 * need to push the {@link CompiledStream}.offer() data to the first
	 * {@link Worker} of the streamgraph.
	 */
	BoundaryOutputChannel headChannel;

	private Thread headThread;

	/**
	 * A {@link BoundaryInputChannel} for the tail of the whole stream graph. If
	 * the sink {@link Worker} happened to fall outside the {@link Controller},
	 * we need to pull the sink's output in to the {@link Controller} in order
	 * to make {@link CompiledStream} .pull() to work.
	 */
	TailChannel tailChannel;

	private Thread tailThread;

	private HeadTail headTail;

	public HeadTailHandler(Controller controller, StreamJitApp<?, ?> app) {
		this.controller = controller;
		this.app = app;
	}

	/**
	 * Setup the headchannel and tailchannel.
	 * 
	 * [15-12-2015] TODO: needSeamless flag was added as a quick hack. Remove
	 * this flag and refactor this class in a OOP/OOD way.
	 * 
	 * @param cfg
	 * @param bufferMap
	 */
	void setupHeadTail(ImmutableMap<Token, Buffer> bufferMap,
			AppInstanceManager aim, boolean needSeamless) {
		Map<Token, ConnectionInfo> conInfoMap = aim.conInfoMap;
		int multiplier = aim.appInst.multiplier;
		ConnectionInfo tailconInfo = conInfoMap.get(app.tailToken);
		assert tailconInfo != null : "No tail connection info exists in conInfoMap";
		assert tailconInfo.getSrcID() == controller.controllerNodeID
				|| tailconInfo.getDstID() == controller.controllerNodeID : "Tail channel should ends at the controller. "
				+ tailconInfo;

		if (!bufferMap.containsKey(app.tailToken))
			throw new IllegalArgumentException(
					"No tail buffer in the passed bufferMap.");

		HeadTail.Builder builder = HeadTail.builder();
		builder.appInstId(aim.appInstId());
		int skipCount = Math.max(Options.outputCount, multiplier * 5);
		Buffer tb = bufferMap.get(app.tailToken);
		BufferWriteCounter bc = new BufferWriteCounter(tb);
		builder.tailBuffer(tb);
		builder.tailCounter(bc);
		tailChannel = tailChannel(bc, tailconInfo, skipCount, aim.appInst,
				aim.eLogger);
		setHead(conInfoMap, bufferMap, aim, bc, needSeamless);
		headTail = builder.build();
	}

	TailChannel tailChannel(BufferWriteCounter buffer, ConnectionInfo conInfo,
			int skipCount, AppInstance appinst, EventTimeLogger eLogger) {
		String appName = app.name;
		int steadyCount = Options.outputCount;
		int debugLevel = 0;
		String bufferTokenName = String.format("TC-%s - %d",
				app.tailToken.toString(), appinst.id);
		ConnectionProvider conProvider = controller.getConProvider();
		String cfgPrefix = ConfigurationUtils.getConfigPrefix(appinst
				.getConfiguration());
		switch (Options.tailChannel) {
			case 1 :
				return new TailChannels.BlockingTailChannel1(buffer,
						conProvider, conInfo, bufferTokenName, debugLevel,
						skipCount, steadyCount, appName, cfgPrefix, eLogger);
			case 3 :
				return new TailChannels.BlockingTailChannel3(buffer,
						conProvider, conInfo, bufferTokenName, debugLevel,
						skipCount, steadyCount, appName, cfgPrefix, eLogger);
			default :
				return new TailChannels.BlockingTailChannel2(buffer,
						conProvider, conInfo, bufferTokenName, debugLevel,
						skipCount, steadyCount, appName, cfgPrefix, eLogger);
		}
	}

	private void setHead(Map<Token, ConnectionInfo> conInfoMap,
			ImmutableMap<Token, Buffer> bufferMap, AppInstanceManager aim,
			Counter tailCounter, boolean needSeamless) {
		ConnectionInfo headconInfo = conInfoMap.get(app.headToken);
		assert headconInfo != null : "No head connection info exists in conInfoMap";
		assert headconInfo.getSrcID() == controller.controllerNodeID
				|| headconInfo.getDstID() == controller.controllerNodeID : "Head channel should start from the controller. "
				+ headconInfo;

		if (!bufferMap.containsKey(app.headToken))
			throw new IllegalArgumentException(
					"No head buffer in the passed bufferMap.");

		Buffer b = bufferMap.get(app.headToken);
		ConnectionProvider c = controller.getConProvider();
		String name = String.format("HC-%s - %d", app.headToken.toString(),
				aim.appInst.id);

		if (needSeamless)
			headChannel = new HeadChannelSeamless(b, c, headconInfo, name,
					aim.eLogger, tailCounter, aim);
		else {
			if (headconInfo instanceof TCPConnectionInfo)
				headChannel = new HeadChannels.TCPHeadChannel(b, c, headconInfo,
						name, 0, aim.eLogger);
			else if (headconInfo instanceof AsyncTCPConnectionInfo)
				headChannel = new HeadChannels.AsyncHeadChannel(b, c,
						headconInfo, name, 0, aim.eLogger);
			else
				throw new IllegalStateException(
						"Head ConnectionInfo doesn't match");
		}
	}

	void startHead() {
		if (headChannel != null) {
			headThread = new Thread(headChannel.getRunnable(),
					headChannel.name());
			headThread.start();
		}
	}

	void stopHead(boolean isFinal) {
		if (headChannel != null) {
			headChannel.stop(isFinal);
		}
	}

	void waitToStopHead() {
		if (headChannel != null) {
			try {
				headThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	void startTail() {
		if (tailChannel != null) {
			tailThread = new Thread(tailChannel.getRunnable(),
					tailChannel.name());
			tailThread.start();
		}
	}

	void stopTail(boolean isFinal) {
		if (tailChannel != null)
			tailChannel.stop(BlobGraph.ddAction(isFinal));
	}

	void waitToStopTail() {
		if (tailChannel != null) {
			try {
				tailThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	HeadChannelSeamless headChannelSeamless() {
		return (HeadChannelSeamless) headChannel;
	}

	public HeadTail headTail() {
		return headTail;
	}
}