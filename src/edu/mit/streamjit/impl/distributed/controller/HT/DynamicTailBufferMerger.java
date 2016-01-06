package edu.mit.streamjit.impl.distributed.controller.HT;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.common.Counter;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement;
import edu.mit.streamjit.tuner.EventTimeLogger;

/**
 * {@link TailBufferMerger} that performs dynamic switching. That is, skips to
 * new graph whenever it catches the old graph's output.
 * 
 * @author sumanan
 * @since 12 Nov, 2015
 */
public class DynamicTailBufferMerger extends TailBufferMergerSeamless {

	private int duplicateOutputIndex;

	private HeadChannelSeamless hcSeamless;

	final ExecutorService executerSevce;

	// TODO:Explore using Future.cancel() to stop the trimResources().
	private volatile boolean stopTrimming = false;

	Future<Void> trimResourceFuture;

	public DynamicTailBufferMerger(Buffer tailBuffer, EventTimeLogger eLogger,
			boolean resourceTrimming) {
		super(tailBuffer, eLogger);
		if (resourceTrimming)
			executerSevce = Executors.newSingleThreadExecutor();
		else
			executerSevce = null;
	}

	public void startMerge() {
	}

	public void startMerge(int duplicateOutputIndex,
			HeadChannelSeamless hcSeamless) {
		if (debug) {
			event("m..");
			System.err.println(String.format("nextBufSize=%d", nextBuf.size()));
		}
		if (merge)
			throw new IllegalStateException("merge==false expected.");
		this.duplicateOutputIndex = duplicateOutputIndex;
		this.hcSeamless = hcSeamless;
		merge = true;
	}

	@Override
	protected void merge() {
		stopTrimming = false;
		if (debug)
			event("twm");
		if (prevBuf != null)
			throw new IllegalStateException("prevBuf == null expected.");
		if (nextBuf == null)
			throw new IllegalStateException("nextBuf != null expected.");
		AppInstBufInfo curInfo = appInstBufInfos.get(curBuf);
		AppInstBufInfo nextInfo = appInstBufInfos.get(nextBuf);
		if (debug)
			System.err.println(String.format(
					"curAppInstCount - %d, duplicateOutputIndex - %d ",
					curInfo.ht.tailCounter.count(), duplicateOutputIndex));
		copyNonDuplicateOutput(curInfo.ht.tailCounter);
		if (debug)
			System.err
					.println(String
							.format(" copyNonDuplicateOutput curAppInstCount - %d, duplicateOutputIndex - %d ",
									curInfo.ht.tailCounter.count(),
									duplicateOutputIndex));
		int curDupData = curInfo.ht.tailCounter.count() - duplicateOutputIndex;
		trimResources();
		while ((nextInfo.ht.tailCounter.count() <= curDupData) && !stopCalled) {
			copyToTailBuffer(curBuf);
			curDupData = curInfo.ht.tailCounter.count() - duplicateOutputIndex;
		}
		stopTrimming = true;
		hcSeamless.stop(false);
		switchBuffers(curDupData);
	}

	private void copyNonDuplicateOutput(Counter tailCounter) {
		while ((tailCounter.count() - duplicateOutputIndex < 0) && !stopCalled)
			copyToTailBuffer(curBuf);
	}

	private void trimResources() {
		if (executerSevce != null) {
			if (trimResourceFuture != null)
				if (!trimResourceFuture.isDone()) {
					throw new IllegalStateException(
							"trimResourceFuture.isDone() == true");
				}
			trimResourceFuture = executerSevce.submit(trimResourceCallable());
		}
	}

	private Callable<Void> trimResourceCallable() {
		return new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				sleep(5);
				if (stopTrimming)
					return null;
				hcSeamless.aim.sendToAll(new CTRLRDrainElement.ReduceCore(50));

				sleep(5);
				if (stopTrimming)
					return null;
				hcSeamless.aim.sendToAll(new CTRLRDrainElement.ReduceCore(25));

				sleep(5);
				if (stopTrimming)
					return null;
				hcSeamless.aim.sendToAll(new CTRLRDrainElement.ReduceCore(1));
				return null;
			}

			void sleep(int mills) {
				try {
					Thread.sleep(mills);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		};
	}

	@Override
	protected void stoping() {
		if (executerSevce != null)
			executerSevce.shutdownNow();
		System.out.println("DynamicTailBufferMerger: stoping.");
	}
}
