package edu.mit.streamjit.impl.distributed.node;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.Buffers;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.common.drainer.AbstractDrainer.DrainDataAction;
import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryInputChannel;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryOutputChannel;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannelManager.BoundaryInputChannelManager;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannelManager.BoundaryOutputChannelManager;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannelManager.InputChannelManager;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannelManager.OutputChannelManager;
import edu.mit.streamjit.impl.distributed.common.Connection;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;
import edu.mit.streamjit.tuner.EventTimeLogger;
import edu.mit.streamjit.tuner.EventTimeLogger.PrefixedEventTimeLogger;
import edu.mit.streamjit.util.affinity.Affinity;

/**
 * This class was an inner class of {@link BlobsManagerImpl}. I have re factored
 * {@link BlobsManagerImpl} and moved this class a new file.
 * 
 * @author sumanan
 * @since 4 Feb, 2015
 */
class BlobExecuter {

	/**
	 * 
	 */
	final BlobsManagerImpl blobsManagerImpl;

	Blob blob;

	final Token blobID;

	final Set<BlobThread2> blobThreads;

	/**
	 * Buffers for all input and output edges of the {@link #blob}.
	 */
	ImmutableMap<Token, Buffer> bufferMap;

	ImmutableMap<Token, LocalBuffer> outputLocalBuffers;

	/**
	 * This flag will be set to true if an exception thrown by the core code of
	 * the {@link Blob}. Any exception occurred in a blob's corecode will be
	 * informed to {@link Controller} to halt the application. See the
	 * {@link BlobThread2}.
	 */
	AtomicBoolean crashed;

	final BoundaryInputChannelManager inChnlManager;

	final BoundaryOutputChannelManager outChnlManager;

	final Starter starter;

	final EventTimeLogger eLogger;

	final BlobDrainer drainer;

	BlobExecuter(BlobsManagerImpl blobsManagerImpl, Token t, Blob blob,
			ImmutableMap<Token, BoundaryInputChannel> inputChannels,
			ImmutableMap<Token, BoundaryOutputChannel> outputChannels,
			int starterType) {
		this.blobsManagerImpl = blobsManagerImpl;
		this.blobID = t;
		this.blob = blob;
		this.eLogger = eventTimeLogger();
		this.crashed = new AtomicBoolean(false);
		this.blobThreads = new HashSet<>();
		assert blob.getInputs().containsAll(inputChannels.keySet());
		assert blob.getOutputs().containsAll(outputChannels.keySet());
		this.inChnlManager = new InputChannelManager(inputChannels);
		this.outChnlManager = new OutputChannelManager(outputChannels);

		String baseName = getName(blob);
		for (int i = 0; i < blob.getCoreCount(); i++) {
			String name = String.format("%s-%d-%d", baseName,
					blobsManagerImpl.appInstId, i);
			blobThreads
					.add(new BlobThread2(blob.getCoreCode(i), this, name,
							blobsManagerImpl.affinityManager.getAffinity(t, i),
							i == 0));
		}

		if (blobThreads.size() < 1)
			throw new IllegalStateException("No blobs to execute");

		this.starter = starter(starterType);
		this.drainer = new BlobDrainer(this);
	}

	public Token getBlobID() {
		return blobID;
	}

	private EventTimeLogger eventTimeLogger() {
		return new PrefixedEventTimeLogger(
				blobsManagerImpl.streamNode.eventTimeLogger,
				blobsManagerImpl.appInstId + "-" + blobID);
	}

	/**
	 * Gets buffer from {@link BoundaryChannel}s and builds bufferMap. The
	 * bufferMap will contain all input and output edges of the {@link #blob}.
	 * 
	 * Note that, Some {@link BoundaryChannel}s (e.g.,
	 * {@link AsyncOutputChannel}) create {@link Buffer}s after establishing
	 * {@link Connection} with other end. So this method must be called after
	 * establishing all IO connections.
	 * {@link InputChannelManager#waitToStart()} and
	 * {@link OutputChannelManager#waitToStart()} ensure that the IO connections
	 * are successfully established.
	 * 
	 * @return Buffer map which contains {@link Buffers} for all input and
	 *         output edges of the {@link #blob}.
	 */
	ImmutableMap<Token, Buffer> buildBufferMap() {
		ImmutableMap.Builder<Token, Buffer> bufferMapBuilder = ImmutableMap
				.builder();
		ImmutableMap.Builder<Token, LocalBuffer> outputLocalBufferBuilder = ImmutableMap
				.builder();
		ImmutableMap<Token, LocalBuffer> localBufferMap = this.blobsManagerImpl.bufferManager
				.localBufferMap();
		ImmutableMap<Token, BoundaryInputChannel> inputChannels = inChnlManager
				.inputChannelsMap();
		ImmutableMap<Token, BoundaryOutputChannel> outputChannels = outChnlManager
				.outputChannelsMap();

		for (Token t : blob.getInputs()) {
			if (localBufferMap.containsKey(t)) {
				assert !inputChannels.containsKey(t) : "Same channels is exists in both localBuffer and inputChannel";
				bufferMapBuilder.put(t, localBufferMap.get(t));
			} else if (inputChannels.containsKey(t)) {
				BoundaryInputChannel chnl = inputChannels.get(t);
				bufferMapBuilder.put(t, chnl.getBuffer());
			} else {
				throw new AssertionError(String.format(
						"No Buffer for input channel %s ", t));
			}
		}

		for (Token t : blob.getOutputs()) {
			if (localBufferMap.containsKey(t)) {
				assert !outputChannels.containsKey(t) : "Same channels is exists in both localBuffer and outputChannel";
				LocalBuffer buf = localBufferMap.get(t);
				bufferMapBuilder.put(t, buf);
				outputLocalBufferBuilder.put(t, buf);
			} else if (outputChannels.containsKey(t)) {
				BoundaryOutputChannel chnl = outputChannels.get(t);
				bufferMapBuilder.put(t, chnl.getBuffer());
			} else {
				throw new AssertionError(String.format(
						"No Buffer for output channel %s ", t));
			}
		}
		outputLocalBuffers = outputLocalBufferBuilder.build();
		return bufferMapBuilder.build();
	}

	/**
	 * Returns a name for thread.
	 * 
	 * @param blob
	 * @return
	 */
	private String getName(Blob blob) {
		StringBuilder sb = new StringBuilder("Workers-");
		int limit = 0;
		for (Worker<?, ?> w : blob.getWorkers()) {
			sb.append(Workers.getIdentifier(w));
			sb.append(",");
			if (++limit > 5)
				break;
		}
		return sb.toString();
	}

	void startChannels() {
		outChnlManager.start();
		inChnlManager.start();
	}

	void stop() {
		inChnlManager.stop(DrainDataAction.FINISH);
		outChnlManager.stop(true);

		for (Thread t : blobThreads) {
			try {
				t.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		eLogger.bEvent("inChnlManager.waitToStop");
		inChnlManager.waitToStop();
		eLogger.eEvent("inChnlManager.waitToStop");
		eLogger.bEvent("outChnlManager.waitToStop");
		outChnlManager.waitToStop();
		eLogger.eEvent("outChnlManager.waitToStop");

		if (this.blobsManagerImpl.monBufs != null)
			this.blobsManagerImpl.monBufs.stopMonitoring();
		if (drainer.executorService != null
				&& !drainer.executorService.isTerminated())
			drainer.executorService.shutdownNow();
	}

	void updateAffinity(AffinityManager am) {
		int i = 0;
		for (BlobThread2 t : blobThreads)
			t.updateAffinity(am.getAffinity(blobID, i++));
	}

	Starter starter(int starterType) {
		if (starterType == 1)
			return new Starter1(this);
		else
			return new Starter2(this);
	}

	final class BlobThread2 extends Thread {

		private Set<Integer> cores;

		private final BlobExecuter be;

		final Runnable coreCode;

		volatile int stopping = 0;

		volatile boolean setAffinity = false;

		final boolean logTime;

		BlobThread2(Runnable coreCode, BlobExecuter be, String name,
				Set<Integer> cores, boolean logTime) {
			super(name);
			this.coreCode = coreCode;
			this.be = be;
			this.cores = cores;
			this.logTime = logTime;
		}

		public void requestStop() {
			stopping = 2;
		}

		@Override
		public void run() {
			setAffinity();
			try {
				starter.initScheduleRun(this);
				if (logTime)
					logFiringTime();
				while (true) {
					if (stopping == 0)
						coreCode.run();
					else
						break;

					if (setAffinity)
						setAffinity();
				}
			} catch (Error | Exception e) {
				System.out.println(Thread.currentThread().getName()
						+ " crashed...");
				if (be.crashed.compareAndSet(false, true)) {
					e.printStackTrace();
					if (drainer.drainState == 1 || drainer.drainState == 2)
						drainer.drained();
					else if (drainer.drainState == 0) {
						blobsManagerImpl.sendToController(AppStatus.ERROR);
					}
				}
			}
		}

		private void setAffinity() {
			if (cores != null && cores.size() > 0)
				Affinity.setThreadAffinity(cores);
			setAffinity = false;
		}

		private void logFiringTime() {
			int meassureCount = 5;
			// The very first coreCode.run() executes initCode which is single
			// threaded and very much slower than steadyCode. With initCode,
			// lets skip another few steadyCode executions before begin the
			// measurement.
			for (int i = 0; i < 10; i++) {
				if (stopping == 2)
					break;
				coreCode.run();
			}

			Stopwatch sw = Stopwatch.createStarted();
			for (int i = 0; i < meassureCount; i++) {
				if (stopping == 2)
					break;
				coreCode.run();
			}
			if (stopping != 2) {
				long time = sw.elapsed(TimeUnit.MILLISECONDS);
				long avgMills = time / meassureCount;
				eLogger.logEvent("-firing", avgMills);
			}
		}

		private void updateAffinity(Set<Integer> cores) {
			this.cores = cores;
			setAffinity = true;
		}
	}
}
