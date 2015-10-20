package edu.mit.streamjit.impl.distributed.node;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.PartitionParameter;
import edu.mit.streamjit.impl.common.Configuration.PartitionParameter.BlobSpecifier;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.CompilationInfo;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString.ConfigurationProcessor;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.SNMessageElement;
import edu.mit.streamjit.impl.distributed.common.SNTimeInfo.CompilationTime;
import edu.mit.streamjit.impl.distributed.common.Utils;
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
		if (app == null)
			app = new SNStreamJitApp(json, streamNode);
		else
			throw new IllegalStateException(
					"Multiple static configurations received.");
	}

	private void processDynamicCfg(String json, DrainData drainData) {
		System.out
				.println("------------------------------------------------------------");
		System.out.println("New Configuration.....");
		streamNode.releaseOldBM();
		Configuration cfg = Jsonifiers.fromJson(json, Configuration.class);
		ImmutableSet<Blob> blobSet = getBlobs(cfg, drainData);
		if (blobSet != null) {
			Map<Token, ConnectionInfo> conInfoMap = (Map<Token, ConnectionInfo>) cfg
					.getExtraData(GlobalConstants.CONINFOMAP);

			streamNode
					.setBlobsManager(new BlobsManagerImpl(blobSet, conInfoMap,
							streamNode, app.conProvider, app.topLevelClass));
		} else {
			try {
				streamNode.controllerConnection
						.writeObject(AppStatus.COMPILATION_ERROR);
				sendEmptyBuffersizes();
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println("Couldn't get the blobset....");
		}
		newTuningRound(blobSet, ConfigurationUtils.getConfigPrefix(cfg
				.getSubconfiguration("blobConfigs")));
	}

	private ImmutableSet<Blob> getBlobs(Configuration dyncfg,
			DrainData drainData) {

		PartitionParameter partParam = dyncfg.getParameter(
				GlobalConstants.PARTITION, PartitionParameter.class);
		if (partParam == null)
			throw new IllegalArgumentException(
					"Partition parameter is not available in the received configuraion");

		if (app.streamGraph != null) {
			List<BlobSpecifier> blobList = partParam
					.getBlobsOnMachine(streamNode.getNodeID());

			ImmutableSet.Builder<Blob> blobSet = ImmutableSet.builder();

			if (blobList == null)
				return blobSet.build();

			Configuration blobConfigs = dyncfg
					.getSubconfiguration("blobConfigs");
			return blobset1(blobSet, blobList, drainData, blobConfigs,
					app.source);

		} else
			return null;
	}

	private void sendCompilationTime(Stopwatch sw, Token blobID) {
		sw.stop();
		CompilationTime ct = new CompilationTime(blobID,
				sw.elapsed(TimeUnit.MILLISECONDS));
		try {
			streamNode.controllerConnection.writeObject(ct);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Compiles the blobs in serial.
	 */
	private ImmutableSet<Blob> blobset(ImmutableSet.Builder<Blob> blobSet,
			List<BlobSpecifier> blobList, DrainData drainData,
			Configuration blobConfigs, Worker<?, ?> source) {
		for (BlobSpecifier bs : blobList) {
			Set<Integer> workIdentifiers = bs.getWorkerIdentifiers();
			ImmutableSet<Worker<?, ?>> workerset = bs.getWorkers(source);
			try {
				BlobFactory bf = bs.getBlobFactory();
				int maxCores = bs.getCores();
				Stopwatch sw = Stopwatch.createStarted();
				DrainData dd = drainData == null ? null : drainData
						.subset(workIdentifiers);
				Blob b = bf.makeBlob(workerset, blobConfigs, maxCores, dd);
				sendCompilationTime(sw, Utils.getblobID(workerset));
				blobSet.add(b);
			} catch (Exception ex) {
				ex.printStackTrace();
				return null;
			} catch (OutOfMemoryError er) {
				Utils.printOutOfMemory();
				return null;
			}
			// // DEBUG MSG
			// if (!GlobalConstants.singleNodeOnline)
			// System.out.println(String.format(
			// "A new blob with workers %s has been created.",
			// workIdentifiers.toString()));
		}
		System.out.println("All blobs have been created");
		return blobSet.build();
	}

	/**
	 * Compiles the blobs in parallel.
	 */
	private ImmutableSet<Blob> blobset1(ImmutableSet.Builder<Blob> blobSet,
			List<BlobSpecifier> blobList, DrainData drainData,
			Configuration blobConfigs, Worker<?, ?> source) {
		Set<Future<Blob>> futures = new HashSet<>();
		ExecutorService executerSevce = Executors.newFixedThreadPool(blobList
				.size());

		for (BlobSpecifier bs : blobList) {
			MakeBlob mb = new MakeBlob(bs, drainData, blobConfigs, source);
			Future<Blob> f = executerSevce.submit(mb);
			futures.add(f);
		}

		executerSevce.shutdown();

		while (!executerSevce.isTerminated()) {
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		for (Future<Blob> f : futures) {
			Blob b;
			try {
				b = f.get();
				if (b == null)
					return null;
				blobSet.add(b);
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}

		System.out.println("All blobs have been created");
		return blobSet.build();
	}

	/**
	 * Send empty buffer sizes if compilation error occurred. If we didn't send
	 * this, Controller would be waiting forever at
	 * CompilationInfoProcessorImpl.waitforBufSizes().
	 */
	private void sendEmptyBuffersizes() {
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
			streamNode.controllerConnection.writeObject(bufSizes);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private class MakeBlob implements Callable<Blob> {
		private final BlobSpecifier bs;
		private final DrainData drainData;
		private final Configuration blobConfigs;
		private final Worker<?, ?> source;

		private MakeBlob(BlobSpecifier bs, DrainData drainData,
				Configuration blobConfigs, Worker<?, ?> source) {
			this.bs = bs;
			this.drainData = drainData;
			this.blobConfigs = blobConfigs;
			this.source = source;
		}

		@Override
		public Blob call() throws Exception {
			Blob b = null;
			Set<Integer> workIdentifiers = bs.getWorkerIdentifiers();
			ImmutableSet<Worker<?, ?>> workerset = bs.getWorkers(source);
			try {
				BlobFactory bf = bs.getBlobFactory();
				int maxCores = bs.getCores();
				Stopwatch sw = Stopwatch.createStarted();
				DrainData dd = drainData == null ? null : drainData
						.subset(workIdentifiers);
				b = bf.makeBlob(workerset, blobConfigs, maxCores, dd);
				sendCompilationTime(sw, Utils.getblobID(workerset));
			} catch (Exception ex) {
				ex.printStackTrace();
			} catch (OutOfMemoryError er) {
				Utils.printOutOfMemory();
			}
			// DEBUG MSG
			// if (!GlobalConstants.singleNodeOnline && b != null)
			// System.out.println(String.format(
			// "A new blob with workers %s has been created.",
			// workIdentifiers.toString()));
			return b;
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
}
