package edu.mit.streamjit.impl.distributed.node;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.PartitionParameter;
import edu.mit.streamjit.impl.common.Configuration.PartitionParameter.BlobSpecifier;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.SNTimeInfo.CompilationTime;
import edu.mit.streamjit.impl.distributed.common.Utils;

/**
 * {@link ConfigurationProcessorImpl} refactored and all blob creation methods
 * have been moved to here. Alternatively, we can make this class as an inner
 * class of {@link ConfigurationProcessorImpl}.
 * 
 * @author sumanan
 * @since 21 Oct, 2015
 */
public class BlobCreator {

	private final SNStreamJitApp app;
	private final StreamNode streamNode;

	BlobCreator(SNStreamJitApp app, StreamNode streamNode) {
		this.app = app;
		this.streamNode = streamNode;
	}

	public ImmutableSet<Blob> getBlobs(Configuration dyncfg,
			CreationLogic creationLogic) {

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

			return blobset1(blobSet, blobList, creationLogic, app.source);

		} else
			return null;
	}

	/**
	 * Compiles the blobs in parallel.
	 */
	private ImmutableSet<Blob> blobset1(ImmutableSet.Builder<Blob> blobSet,
			List<BlobSpecifier> blobList, CreationLogic creationLogic,
			Worker<?, ?> source) {
		Set<Future<Blob>> futures = new HashSet<>();
		ExecutorService executerSevce = Executors.newFixedThreadPool(blobList
				.size());

		for (BlobSpecifier bs : blobList) {
			MakeBlob mb = new MakeBlob(bs, source, creationLogic);
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

	private class MakeBlob implements Callable<Blob> {
		private final BlobSpecifier bs;
		private final CreationLogic creationLogic;
		private final Worker<?, ?> source;

		private MakeBlob(BlobSpecifier bs, Worker<?, ?> source,
				CreationLogic creationLogic) {
			this.bs = bs;
			this.source = source;
			this.creationLogic = creationLogic;
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
				b = creationLogic.create(bf, workerset, maxCores,
						workIdentifiers);
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

	/**
	 * This interface is to create {@link Blob}s in different ways. Currently,
	 * we only have
	 * {@link BlobFactory#makeBlob(Set, Configuration, int, DrainData)} to make
	 * blobs.
	 * 
	 * @author sumanan
	 * @since 21 Oct, 2015
	 */
	interface CreationLogic {
		public Blob create(BlobFactory bf,
				ImmutableSet<Worker<?, ?>> workerset, int maxCores,
				Set<Integer> workIdentifiers);
	}

	/**
	 * Uses {@link BlobFactory#makeBlob(Set, Configuration, int, DrainData)} to
	 * make blobs.
	 * 
	 * @author sumanan
	 * @since 21 Oct, 2015
	 */
	static class DrainDataCreationLogic implements CreationLogic {
		private final DrainData drainData;
		private final Configuration blobConfigs;

		DrainDataCreationLogic(DrainData drainData, Configuration blobConfigs) {
			this.drainData = drainData;
			this.blobConfigs = blobConfigs;
		}

		public Blob create(BlobFactory bf,
				ImmutableSet<Worker<?, ?>> workerset, int maxCores,
				Set<Integer> workIdentifiers) {
			DrainData dd = drainData == null ? null : drainData
					.subset(workIdentifiers);
			return bf.makeBlob(workerset, blobConfigs, maxCores, dd);
		}
	}
}
