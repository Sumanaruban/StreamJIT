package edu.mit.streamjit.impl.distributed.controller.HT;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.common.Counter;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement;
import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.tuner.EventTimeLogger;

/**
 * {@link TailBufferMerger} that performs dynamic switching. That is, skips to
 * new graph whenever it catches the old graph's output.
 * 
 * @author sumanan
 * @since 12 Nov, 2015
 */
public class DynamicTailBufferMerger extends TailBufferMergerSeamless {

	private HeadChannelSeamless hcSeamless;

	final ExecutorService executerSevce;

	// TODO:Explore using Future.cancel() to stop the trimResources().
	private volatile boolean stopTrimming = false;

	Future<Void> trimResourceFuture;

	private volatile boolean timeout = false;

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
		int skipped = 0;
		int nextCount = 0;
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
		Stopwatch sw = Stopwatch.createStarted();
		copyNonDuplicateOutput(curInfo.ht.tailCounter);
		event(String.format("copy NonDuplicate Output time is %dms",
				sw.elapsed(TimeUnit.MILLISECONDS)));
		if (debug)
			System.err
					.println(String
							.format(" copyNonDuplicateOutput curAppInstCount - %d, duplicateOutputIndex - %d ",
									curInfo.ht.tailCounter.count(),
									duplicateOutputIndex));
		int curDupData = curInfo.ht.tailCounter.count() - duplicateOutputIndex;
		trimResources();
		timeout = false;
		while (((nextCount = nextInfo.ht.tailCounter.count()) <= curDupData)
				&& !stopCalled && !timeout) {
			copyToTailBuffer(curBuf);
			curDupData = curInfo.ht.tailCounter.count() - duplicateOutputIndex;
			if (nextBuf.size() == nextBuf.capacity()) {
				int s = Math.min(curDupData - skipped, nextBuf.size());
				skip(nextBuf, s);
				skipped += s;
			}
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if (debug)
				System.out.println(String.format(
						"curDupData = %d, nextCount = %d", curDupData,
						nextCount));
		}
		stopTrimming = true;
		hcSeamless.stop(false);
		switchBuffers(curDupData - skipped);
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
		// Just temporarily using Options.blobToNodeRatio to change the sleep
		// time. Remove this later.
		int sleepTime = Options.blobToNodeRatio;
		return new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				sleep(sleepTime);
				if (stopTrimming)
					return null;
				hcSeamless.aim.sendToAll(new CTRLRDrainElement.ReduceCore(50));
				System.out.println("Resource trimmed to half");

				sleep(sleepTime);
				if (stopTrimming)
					return null;
				hcSeamless.aim.sendToAll(new CTRLRDrainElement.ReduceCore(25));
				System.out.println("Resource trimmed to quater");

				sleep(sleepTime);
				if (stopTrimming)
					return null;
				hcSeamless.aim.sendToAll(new CTRLRDrainElement.ReduceCore(1));
				System.out.println("Resource trimmed to 1 core");
				sleep(sleepTime);
				timeout = true;
				System.err.println("DynamicTailBufMerge Time Out........");
				event("TOut");
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
