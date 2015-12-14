package edu.mit.streamjit.impl.distributed.node;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.CompilationInfo;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString.ConfigurationProcessor;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.SNMessageElement;
import edu.mit.streamjit.impl.distributed.common.SNMessageElement.SNMessageElementHolder;
import edu.mit.streamjit.impl.distributed.common.Utils;
import edu.mit.streamjit.impl.distributed.node.BlobCreator.CreationLogic;
import edu.mit.streamjit.impl.distributed.node.BlobCreator.DrainDataCreationLogic;
import edu.mit.streamjit.impl.distributed.node.BlobCreator.InitDataSizeCreationLogic;
import edu.mit.streamjit.util.ConfigurationUtils;
import edu.mit.streamjit.util.json.Jsonifiers;

/**
 * {@link ConfigurationProcessor} at {@link StreamNode} side.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
public class ConfigurationProcessorImpl implements ConfigurationProcessor {

	private StreamNode streamNode;

	private SNStreamJitApp app;

	private BlobCreator blobCreator;

	public ConfigurationProcessorImpl(StreamNode streamNode) {
		this.streamNode = streamNode;
	}

	@Override
	public void process(String json, ConfigType type, DrainData drainData) {
		if (type == ConfigType.STATIC) {
			processStaticCfg(json);
		} else {
			processDynamicCfg(json, drainData);
		}
	}

	private void processStaticCfg(String json) {
		if (app == null) {
			app = new SNStreamJitApp(json, streamNode);
			blobCreator = new BlobCreator(app, streamNode);
		} else
			throw new IllegalStateException(
					"Multiple static configurations received.");
	}

	private void processDynamicCfg(String json, DrainData drainData) {
		Configuration cfg = Jsonifiers.fromJson(json, Configuration.class);
		CreationLogic creationLogic = creationLogic(cfg, drainData);
		compile(cfg, creationLogic);
	}

	CreationLogic creationLogic(Configuration dyncfg, DrainData drainData) {
		Configuration blobConfigs = dyncfg.getSubconfiguration("blobConfigs");
		CreationLogic creationLogic = new DrainDataCreationLogic(drainData,
				blobConfigs);
		return creationLogic;
	}
	CreationLogic creationLogic(Configuration dyncfg,
			ImmutableMap<Token, Integer> initialDrainDataBufferSizes) {
		Configuration blobConfigs = dyncfg.getSubconfiguration("blobConfigs");
		CreationLogic creationLogic = new InitDataSizeCreationLogic(
				initialDrainDataBufferSizes, blobConfigs);
		return creationLogic;
	}

	/**
	 * Send empty buffer sizes if compilation error occurred. If we didn't send
	 * this, Controller would be waiting forever at
	 * CompilationInfoProcessorImpl.waitforBufSizes().
	 */
	private void sendEmptyBuffersizes(int appInstId) {
		ImmutableMap.Builder<Token, Integer> minInitInputBufCapaciyBuilder = new ImmutableMap.Builder<>();
		ImmutableMap.Builder<Token, Integer> minInitOutputBufCapaciyBuilder = new ImmutableMap.Builder<>();
		ImmutableMap.Builder<Token, Integer> minSteadyInputBufCapacityBuilder = new ImmutableMap.Builder<>();
		ImmutableMap.Builder<Token, Integer> minSteadyOutputBufCapacityBuilder = new ImmutableMap.Builder<>();

		SNMessageElement bufSizes = new CompilationInfo.BufferSizes(
				streamNode.getNodeID(), minInitInputBufCapaciyBuilder.build(),
				minInitOutputBufCapaciyBuilder.build(),
				minSteadyInputBufCapacityBuilder.build(),
				minSteadyOutputBufCapacityBuilder.build());

		try {
			streamNode.controllerConnection
					.writeObject(new SNMessageElementHolder(bufSizes, appInstId));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	void newTuningRound(ImmutableSet<Blob> blobSet, String cfgPrefix) {
		streamNode.eventTimeLogger.bTuningRound(cfgPrefix);
		final boolean printPartionsToEventLogger = false;
		if (printPartionsToEventLogger && blobSet != null)
			for (Blob b : blobSet) {
				StringBuilder sb = new StringBuilder("Blob-");
				sb.append(Utils.getBlobID(b));
				sb.append("-");
				for (Worker<?, ?> w : b.getWorkers()) {
					sb.append(Workers.getIdentifier(w));
					sb.append(",");
				}
				sb.append('\n');
				streamNode.eventTimeLogger.log(sb.toString());
			}
	}

	private void compile(Configuration cfg, CreationLogic creationLogic) {
		int appInstId = (int) cfg.getExtraData("appInstId");
		String cfgPrefix = ConfigurationUtils.getConfigPrefix(cfg
				.getSubconfiguration("blobConfigs"));
		ImmutableSet<Blob> blobSet = blobCreator.getBlobs(cfg, creationLogic,
				appInstId);
		System.out
				.println(String
						.format("-------------------------New Configuration - %d-------------------------\n",
								appInstId));
		if (blobSet != null) {
			Map<Token, ConnectionInfo> conInfoMap = (Map<Token, ConnectionInfo>) cfg
					.getExtraData(GlobalConstants.CONINFOMAP);

			BlobsManagerImpl bm = new BlobsManagerImpl(blobSet, conInfoMap,
					streamNode, app.conProvider, app.appName, appInstId,
					app.starterType, cfgPrefix);
			CTRLRMessageVisitorImpl mv = new CTRLRMessageVisitorImpl(
					streamNode, bm, appInstId);
			streamNode.registerMessageVisitor(mv);
		} else {
			try {
				streamNode.controllerConnection
						.writeObject(new SNMessageElementHolder(
								AppStatus.COMPILATION_ERROR, appInstId));
				sendEmptyBuffersizes(appInstId);
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println("Couldn't get the blobset....");
		}
		newTuningRound(blobSet, cfgPrefix);
	}

	/*
	 * (non-Javadoc) We need to call the compile() method in a new thread in
	 * order to increase the responsiveness of the SN thread. Otherwise, the
	 * CTRLCompilationInfo.RequestState message send by the controller will be
	 * delivered to the blobs very lately. By the time blobs receive the
	 * RequestState message, they would be several iterations ahead from the
	 * state requested count.
	 */
	@Override
	public void process(String json, ConfigType type,
			ImmutableMap<Token, Integer> initialDrainDataBufferSizes) {
		Configuration cfg = Jsonifiers.fromJson(json, Configuration.class);
		CreationLogic creationLogic = creationLogic(cfg,
				initialDrainDataBufferSizes);
		new Thread(() -> compile(cfg, creationLogic)).start();
	}
}
