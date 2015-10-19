package edu.mit.streamjit.impl.distributed.node;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;

import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.ExecutionStatistics;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryInputChannel;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryOutputChannel;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannelManager.InputChannelManager;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.DrainType;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.SNDrainedData;
import edu.mit.streamjit.impl.distributed.common.SNMessageElement;
import edu.mit.streamjit.impl.distributed.common.SNMessageElement.SNMessageElementHolder;
import edu.mit.streamjit.impl.distributed.common.SNTimeInfo;
import edu.mit.streamjit.impl.distributed.node.BlobExecuter.BlobThread2;

/**
 * All draining related methods from {@link BlobExecuter} class have been moved
 * here. This class and {@link BlobExecuter} refers each other. This class is
 * could be made as an inner class of {@link BlobExecuter}.
 * <p>
 * Warning: This class refers {@link BlobExecuter}'s fields frequently as all
 * methods of this class used to be the methods of {@link BlobExecuter} before
 * this refactoring.
 * 
 * @author sumanan
 * @since 9 Oct, 2015
 */
class BlobDrainer {

	final BlobExecuter be;
	volatile int drainState;
	DrainType drainType;
	/**
	 * ExecutorService to call doDrain in a new thread.
	 */
	ExecutorService executorService = null;

	Blob blob;

	final BlobsManagerImpl blobsManagerImpl;

	BlobDrainer(BlobExecuter be) {
		this.be = be;
		drainState = 0;
		this.blob = be.blob;
		this.blobsManagerImpl = be.blobsManagerImpl;
	}

	/**
	 * The actual {@link #doDrain(DrainType)} method calls
	 * {@link InputChannelManager#waitToStop()}, which is a blocking call. This
	 * may cause deadlock situation in some cases if the main {@link StreamNode}
	 * thread calls {@link #doDrain(DrainType)} method. Calling
	 * {@link #doDrain(DrainType)} in a new thread is always safer.
	 * 
	 * @param drainType
	 * @param inNewThread
	 */
	void doDrain(DrainType drainType, boolean inNewThread) {
		if (inNewThread) {
			executorService = Executors.newSingleThreadExecutor();
			executorService.submit(() -> doDrain(drainType));
			executorService.shutdown();
		} else
			doDrain(drainType);
	}

	void doDrain(DrainType drainType) {
		// System.out.println("Blob " + blobID + "is doDrain");
		this.drainType = drainType;

		/*
		 * [2-9-2015] If this blob crashed during steady run, BlobThread sends
		 * Appstatus.Error message to the controller. However, if the the
		 * intermediate draining has already been started at the controller
		 * level and some other upper blobs are draining, the error message has
		 * no effect. In that case, this blob will also receive doDrain()
		 * command. So lets call drained(). If we do not call the drained(), the
		 * controller will wait at awaitDrainedIntrmdiate() forever.
		 */
		if (be.crashed.get() && drainState == 0)
			drained();

		drainState = 1;
		be.bEvent("inChnlManager.waitToStop");
		be.inChnlManager.stop(drainType);
		// TODO: [2014-03-14] I commented following line to avoid one dead
		// lock case when draining. Deadlock 5 and 6.
		// [2014-09-17] Lets waitToStop() if drain data is required.
		if (drainType != DrainType.DISCARD)
			be.inChnlManager.waitToStop();
		be.eEvent("inChnlManager.waitToStop");

		for (LocalBuffer buf : be.outputLocalBuffers.values()) {
			buf.drainingStarted(drainType);
		}

		if (this.blob != null) {
			DrainCallback dcb = new DrainCallback();
			drainState = 2;
			this.blob.drain(dcb);
		}
		// System.out.println("Blob " + blobID +
		// "this.blob.drain(dcb); passed");
		blobsManagerImpl.doDrainLastBlobActions(this);
	}

	void drained() {
		// System.out.println("Blob " + blobID + "drained at beg");
		if (drainState < 3)
			drainState = 3;
		else
			return;

		for (BlobThread2 bt : be.blobThreads) {
			bt.requestStop();
		}

		be.bEvent("outChnlManager.waitToStop");
		be.outChnlManager.stop(drainType == DrainType.FINAL);
		be.outChnlManager.waitToStop();
		be.eEvent("outChnlManager.waitToStop");

		if (drainState > 3)
			return;

		drainState = 4;
		SNMessageElement drained = new SNDrainElement.Drained(be.blobID);
		try {
			blobsManagerImpl.streamNode.controllerConnection
					.writeObject(new SNMessageElementHolder(drained,
							blobsManagerImpl.appInstId));
		} catch (IOException e) {
			e.printStackTrace();
		}
		// System.out.println("Blob " + blobID + "is drained at mid");

		if (drainType != DrainType.DISCARD) {
			SNMessageElement me;
			if (be.crashed.get())
				me = getEmptyDrainData();
			else
				me = getSNDrainData();

			try {
				blobsManagerImpl.streamNode.controllerConnection
						.writeObject(new SNMessageElementHolder(me,
								blobsManagerImpl.appInstId));
				// System.out.println(blobID + " DrainData has been sent");
				drainState = 6;

			} catch (IOException e) {
				e.printStackTrace();
			}
			// System.out.println("**********************************");
		}

		be.blob = null;
		this.blob = null;
		blobsManagerImpl.drainedLastBlobActions();
		// printDrainedStatus();
	}

	private SNDrainedData getSNDrainData() {
		if (this.blob == null)
			return getEmptyDrainData();

		DrainData dd = blob.getDrainData();
		drainState = 5;
		// DrainDataUtils.printDrainDataStats(dd);

		ImmutableMap.Builder<Token, ImmutableList<Object>> inputDataBuilder = new ImmutableMap.Builder<>();
		ImmutableMap.Builder<Token, ImmutableList<Object>> outputDataBuilder = new ImmutableMap.Builder<>();

		ImmutableMap<Token, BoundaryInputChannel> inputChannels = be.inChnlManager
				.inputChannelsMap();

		be.bEvent("inChnlManager.waitToStop");
		// In a proper system the following line should be called inside
		// doDrain(), just after inChnlManager.stop(). Read the comment
		// in doDrain().
		be.inChnlManager.waitToStop();
		be.eEvent("inChnlManager.waitToStop");

		for (Token t : blob.getInputs()) {
			if (inputChannels.containsKey(t)) {
				BoundaryChannel chanl = inputChannels.get(t);
				ImmutableList<Object> draindata = chanl.getUnprocessedData();
				// if (draindata.size() > 0)
				// System.out.println(String.format("From %s - %d",
				// chanl.name(), draindata.size()));
				inputDataBuilder.put(t, draindata);
			}

			else {
				unprocessedDataFromLocalBuffer(inputDataBuilder, t);
			}
		}

		ImmutableMap<Token, BoundaryOutputChannel> outputChannels = be.outChnlManager
				.outputChannelsMap();
		for (Token t : blob.getOutputs()) {
			if (outputChannels.containsKey(t)) {
				BoundaryChannel chanl = outputChannels.get(t);
				ImmutableList<Object> draindata = chanl.getUnprocessedData();
				// if (draindata.size() > 0)
				// System.out.println(String.format("From %s - %d",
				// chanl.name(), draindata.size()));
				outputDataBuilder.put(t, draindata);
			}
		}

		return new SNDrainElement.SNDrainedData(be.blobID, dd,
				inputDataBuilder.build(), outputDataBuilder.build());
	}

	// TODO: Unnecessary data copy. Optimise this.
	private void unprocessedDataFromLocalBuffer(
			ImmutableMap.Builder<Token, ImmutableList<Object>> inputDataBuilder,
			Token t) {
		Object[] bufArray;
		if (blobsManagerImpl.bufferCleaner == null) {
			Buffer buf = be.bufferMap.get(t);
			bufArray = new Object[buf.size()];
			buf.readAll(bufArray);
			assert buf.size() == 0 : String.format(
					"buffer size is %d. But 0 is expected", buf.size());
		} else {
			bufArray = blobsManagerImpl.bufferCleaner.copiedBuffer(t);
		}
		// if (bufArray.length > 0)
		// System.out.println(String.format("From LocalBuffer: %s - %d",
		// t, bufArray.length));
		inputDataBuilder.put(t, ImmutableList.copyOf(bufArray));
	}

	private SNDrainedData getEmptyDrainData() {
		drainState = 5;
		ImmutableMap.Builder<Token, ImmutableList<Object>> inputDataBuilder = new ImmutableMap.Builder<>();
		ImmutableMap.Builder<Token, ImmutableList<Object>> outputDataBuilder = new ImmutableMap.Builder<>();
		ImmutableMap.Builder<Token, ImmutableList<Object>> dataBuilder = ImmutableMap
				.builder();
		ImmutableTable.Builder<Integer, String, Object> stateBuilder = ImmutableTable
				.builder();
		DrainData dd = new DrainData(dataBuilder.build(), stateBuilder.build());
		return new SNDrainElement.SNDrainedData(be.blobID, dd,
				inputDataBuilder.build(), outputDataBuilder.build());
	}

	/**
	 * Just to added for debugging purpose.
	 */
	synchronized void printDrainedStatus() {
		System.out.println("****************************************");
		for (BlobExecuter be : blobsManagerImpl.blobExecuters.values()) {
			switch (be.drainer.drainState) {
				case 0 :
					System.out.println(String.format("%s - No Drain Called",
							be.blobID));
					break;
				case 1 :
					System.out.println(String.format("%s - Drain Called",
							be.blobID));
					break;
				case 2 :
					System.out.println(String.format(
							"%s - Drain Passed to Interpreter", be.blobID));
					break;
				case 3 :
					System.out.println(String.format(
							"%s - Returned from Interpreter", be.blobID));
					break;
				case 4 :
					System.out.println(String.format(
							"%s - Draining Completed. All threads stopped.",
							be.blobID));
					break;
				case 5 :
					System.out.println(String.format(
							"%s - Processing Drain data", be.blobID));
					break;
				case 6 :
					System.out.println(String.format("%s - Draindata sent",
							be.blobID));
					break;
			}
		}
		System.out.println("****************************************");
	}

	class DrainCallback implements Runnable {

		private final Stopwatch sw;

		DrainCallback() {
			sw = Stopwatch.createStarted();
		}

		private void updateDrainTime() {
			sw.stop();
			long time = sw.elapsed(TimeUnit.MILLISECONDS);
			be.logEvent("-draining", time);
			try {
				blobsManagerImpl.streamNode.controllerConnection
						.writeObject(new SNMessageElementHolder(
								new SNTimeInfo.DrainingTime(be.blobID, time),
								blobsManagerImpl.appInstId));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void logBlobExecutionStatistics() {
			ExecutionStatistics es = blob.getExecutionStatistics();
			be.eventTimeLogger.log(String.format("%-22s\t%-12d\t%d\n",
					be.blobID + "-initTime", 0, es.initTime));
			be.eventTimeLogger.log(String.format("%-22s\t%-12d\t%d\n",
					be.blobID + "-adjustTime", 0, es.adjustTime));
			be.eventTimeLogger.log(String.format("%-22s\t%-12d\t%d\n",
					be.blobID + "-adjustCount", 0, es.adjustCount));
			be.eventTimeLogger.log(String.format("%-22s\t%-12d\t%d\n",
					be.blobID + "-drainTime", 0, es.drainTime));
		}

		@Override
		public void run() {
			logBlobExecutionStatistics();
			updateDrainTime();
			drained();
		}
	}
}