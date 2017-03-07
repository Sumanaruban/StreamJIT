package edu.mit.streamjit.impl.distributed.controller.HT;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.common.Counter;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryOutputChannel;
import edu.mit.streamjit.impl.distributed.common.CTRLCompilationInfo;
import edu.mit.streamjit.impl.distributed.common.CTRLRMessageElement;
import edu.mit.streamjit.impl.distributed.common.Connection;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionProvider;
import edu.mit.streamjit.impl.distributed.controller.AppInstanceManager;
import edu.mit.streamjit.impl.distributed.controller.BufferSizeCalc.GraphSchedule;
import edu.mit.streamjit.impl.distributed.controller.HT.DuplicateDataHandler.DuplicateArrayContainer;
import edu.mit.streamjit.tuner.EventTimeLogger;

/**
 * 
 * @author sumanan
 * @since 12 Nov, 2015
 */
public class HeadChannelSeamless implements BoundaryOutputChannel, Counter {

	private static final int duplicationFiring = 500;

	private Connection connection;

	final Buffer readBuffer;

	final int dataLength = 10000;
	final Object[] data = new Object[dataLength];

	private volatile int stopCalled;
	private final EventTimeLogger eLogger;

	volatile boolean canWrite;

	int count;

	GraphSchedule graphSchedule;

	final Counter tailCounter;

	final AppInstanceManager aim;

	private final int debugLevel = 0;

	private final ConnectionProvider conProvider;

	private final ConnectionInfo conInfo;

	private final String name;

	HeadChannelSeamless next;

	int duplicationCount;

	/**
	 * As the result of input duplication, the current graph and the next graph
	 * will generate duplicate output as well. This index tells the duplicate
	 * output's starting point.
	 */
	int duplicateOutputIndex;

	private final TailBufferMerger tbMerger;

	private double firingRate = 10;

	public static final int fcTimeGap = 3; // 10s

	private final Duplicator duplicator;

	private int limittedFcTimeGap = fcTimeGap;

	private Stopwatch asw;

	private volatile CountDownLatch duplicationLatch;

	public HeadChannelSeamless(Buffer buffer, ConnectionProvider conProvider,
			ConnectionInfo conInfo, String bufferTokenName,
			EventTimeLogger eLogger, Counter tailCounter,
			AppInstanceManager aim, TailBufferMerger tbMerger) {
		name = "HeadChannelSeamless " + bufferTokenName;
		this.conProvider = conProvider;
		this.conInfo = conInfo;
		stopCalled = 0;
		readBuffer = buffer;
		this.eLogger = eLogger;
		count = 0;
		canWrite = false;
		this.tailCounter = tailCounter;
		this.aim = aim;
		this.tbMerger = tbMerger;
		this.duplicator = new Duplicator2();
	}

	public Runnable getRunnable() {
		return new Runnable() {
			@Override
			public void run() {
				eLogger.bEvent("initialization");
				makeConnection();
				graphSchedule = aim.graphSchedule();
				canWrite = true;
				duplicator.initialDuplication();
				sendData();
				if (stopCalled == 4)
					limittedSend();
				if (stopCalled == 1)
					duplicator.finalDuplication(duplicationCount);
				else if (stopCalled == 2) {
					reqState();
					duplicator.finalDuplication(duplicationCount);
				}
				// if (stopCalled == 1 || stopCalled == 2)
				new DrainerThread().start();
				closeConnection();
			}
		};
	}

	public void waitToStart() {
		duplicator.waitToStart();
	}

	public void sendData() {
		int read = 1;
		int size = data.length;
		do {
			read = readBuffer.read(data, 0, size);
			send(data, read);
			flowControl(fcTimeGap);
			size = Math.min(data.length,
					(int) (firingRate * graphSchedule.steadyIn));
		} while (stopCalled == 0);
		sendRemining();
	}

	private void limittedSend() {
		int read = 1;
		double fr = firingRate;
		System.out.println(String.format(
				"limittedSend: firingRate=%f, limittedFcTimeGap=%d  ",
				firingRate, limittedFcTimeGap));
		while (stopCalled == 4) {
			int size = Math.min(data.length,
					(int) (fr * graphSchedule.steadyIn));
			read = readBuffer.read(data, 0, size);
			send(data, read);
			flowControl(limittedFcTimeGap);
		}
		sendRemining();
	}

	private void send(final Object[] data, int items) {
		int totalWritten = 0;
		int written;
		try {
			while (totalWritten < items) {
				written = connection.writeObjects(data, totalWritten, items
						- totalWritten);
				totalWritten += written;
				if (written == 0) {
					try {
						// TODO: Verify this sleep time.
						Thread.sleep(500);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		count += totalWritten;
	}

	private int send(final Object[] data, int items, int rate) {
		int totalWritten = 0;
		int written;
		int itemsToSend = 0;
		try {
			while (totalWritten < items && stopCalled != 3) {
				Thread.sleep(950);
				itemsToSend = Math.min(items - totalWritten, rate);
				written = connection.writeObjects(data, totalWritten,
						itemsToSend);
				totalWritten += written;
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		count += totalWritten;
		return totalWritten;
	}

	private void sendRemining() {
		int reminder = (count - graphSchedule.totalInDuringInit)
				% graphSchedule.steadyIn;
		int residue = graphSchedule.steadyIn - reminder;
		send(residue);
	}

	private void send(int itemsToSend) {
		if (itemsToSend < 0)
			throw new IllegalStateException("Items < 0. Items = " + itemsToSend);
		int itemsToRead;
		int itemsSent = 0;
		while (itemsSent < itemsToSend) {
			itemsToRead = Math.min(data.length, itemsToSend - itemsSent);
			int read = readBuffer.read(data, 0, itemsToRead);
			send(data, read);
			itemsSent += read;
		}
	}

	private void flowControl(int timeGap) {
		Stopwatch sw = null;
		if (debugLevel > 0)
			sw = Stopwatch.createStarted();
		int expectedFiring = expectedFiring();
		int currentFiring = 0;
		int fcFiringGap = (int) (firingRate * timeGap);
		while ((expectedFiring - (currentFiring = currentFiring()) > fcFiringGap)
				&& stopCalled != 3) {
			long sleepMills = timeGap * 300; // 30% of time gap.
			try {
				Thread.sleep(sleepMills);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			firingRate = firingRate(currentFiring() - currentFiring, sleepMills);
			fcFiringGap = (int) (firingRate * timeGap);

			if (debugLevel > 1)
				System.out
						.println(String
								.format("flowControl : expectedFiring - %d, currentFiring = %d",
										expectedFiring, currentFiring));
		}

		if (debugLevel > 1) {
			System.out.println(String.format(
					"flowControl : expectedFiring - %d, currentFiring = %d",
					expectedFiring, currentFiring));
			System.out.println("flowControl Over................");
		}
		if (sw != null)
			System.out
					.println(String.format(
							"%d-FLOW CONTROL time is %dms. timeGap is %ds",
							aim.appInstId(), sw.elapsed(TimeUnit.MILLISECONDS),
							timeGap));
	}

	private int expectedFiring() {
		int firing = (count - graphSchedule.totalInDuringInit)
				/ graphSchedule.steadyIn;
		return firing;
	}

	private int expectedOutput() {
		int out = expectedFiring() * graphSchedule.steadyOut
				+ graphSchedule.totalOutDuringInit;
		return out;
	}

	private int currentFiring() {
		int firing = (tailCounter.count() - graphSchedule.totalOutDuringInit)
				/ graphSchedule.steadyOut;
		return firing;
	}

	double firingRate(int firing, long timeDuration) {
		double firingRate = (double) firing * 1000 / timeDuration;
		if (debugLevel > 0) {
			System.err.println("firingRate = " + firingRate);
		}
		return firingRate;
	}

	public void duplicationEnabled() {
		duplicator.duplicationEnabled();
	}

	public void duplicate(HeadChannelSeamless next) {
		this.duplicationCount = Integer.MAX_VALUE;
		this.next = next;
		asw = Stopwatch.createStarted();
		this.stopCalled = 1;
	}

	public void duplicateAndStop(int duplicationCount, HeadChannelSeamless next) {
		this.duplicationCount = duplicationCount;
		this.next = next;
		asw = Stopwatch.createStarted();
		this.stopCalled = 1;
	}

	public void reqStateAndDuplicate(HeadChannelSeamless next) {
		this.duplicationCount = Integer.MAX_VALUE;
		this.next = next;
		asw = Stopwatch.createStarted();
		this.stopCalled = 2;
	}

	public void reqStateDuplicateAndStop(int duplicationCount,
			HeadChannelSeamless next) {
		this.duplicationCount = duplicationCount;
		this.next = next;
		asw = Stopwatch.createStarted();
		this.stopCalled = 2;
	}

	private void reqState() {
		int reqStateAt = requestState();
		int items = reqStateAt * graphSchedule.steadyIn
				+ graphSchedule.totalInDuringInit - count;
		send(items);
	}

	/**
	 * TODO: Replace this polling with a {@link CountDownLatch}.
	 */
	private void waitToWrite() {
		while (!canWrite) {
			System.out
					.println("Wait to duplicate...Next Head is not ready yet");
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public void limitSend(int fcTimeGap) {
		System.err.println("limitSend..limitSend..limitSend");
		this.limittedFcTimeGap = fcTimeGap;
		this.stopCalled = 4;
	}

	protected void fillUnprocessedData() {
		throw new Error("Method not implemented");
	}

	public void stop() {
		this.stopCalled = 3;
	}

	private int requestState() {
		int i = expectedFiring();
		int reqStateAt = i + (int) (firingRate * 5);
		for (Map.Entry<Token, Integer> en : graphSchedule.steadyRunCount
				.entrySet()) {
			Token blobID = en.getKey();
			int steadyRun = en.getValue();
			int steadyDuringInit = graphSchedule.steadyRunCountDuringInit
					.get(blobID);
			CTRLRMessageElement me = new CTRLCompilationInfo.RequestState(
					blobID, reqStateAt * steadyRun + steadyDuringInit);
			aim.sendToBlob(blobID, me);
		}
		return reqStateAt;
	}

	private void waitForDuplication() {
		if (duplicationLatch != null)
			try {
				duplicationLatch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	}

	public static int duplicationFiring() {
		return duplicationFiring;
	}

	private void makeConnection() {
		if (connection == null || !connection.isStillConnected()) {
			try {
				connection = conProvider.getConnection(conInfo);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void closeConnection() {
		try {
			connection.softClose();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public String name() {
		return name;
	}

	@Override
	public ImmutableList<Object> getUnprocessedData() {
		return null;
	}

	@Override
	public Connection getConnection() {
		return null;
	}

	@Override
	public ConnectionInfo getConnectionInfo() {
		return null;
	}

	@Override
	public Buffer getBuffer() {
		return null;
	}

	@Override
	public void stop(boolean isFinal) {
		stop();
	}

	class DrainerThread extends Thread {
		DrainerThread() {
			super(String.format("DrainerThread - %d", aim.appInstId()));
		}

		public void run() {
			aim.intermediateDraining();
		}
	}

	@Override
	public int count() {
		return count;
	}

	private interface Duplicator {
		void initialDuplication();
		void waitToStart();
		void duplicationEnabled();
		void prevDupCompleted();
		void finalDuplication(int duplicationCount);
	}

	private class Duplicator1 implements Duplicator {

		@Override
		public void initialDuplication() {
			waitForDuplication();
		}

		@Override
		public void duplicationEnabled() {
			duplicationLatch = new CountDownLatch(1);
		}

		@Override
		public void prevDupCompleted() {
			duplicationLatch.countDown();
		}

		@Override
		public void finalDuplication(int duplicationCount) {
			flowControl(3);
			duplicateOutputIndex = expectedOutput();
			tbMerger.startMerge(duplicateOutputIndex, HeadChannelSeamless.this);
			int itemsToRead;
			int itemsDuplicated = 0;
			while (itemsDuplicated < duplicationCount && stopCalled != 3) {
				itemsToRead = Math.min(data.length, duplicationCount
						- itemsDuplicated);
				int read = readBuffer.read(data, 0, itemsToRead);
				next.waitToWrite();
				next.send(data, read);
				send(data, read);
				itemsDuplicated += read;
				flowControl(3);
				// next.flowControl(3);
			}
			next.duplicator.prevDupCompleted();
		}

		@Override
		public void waitToStart() {
		}
	}

	private class Duplicator2 implements Duplicator {

		final DuplicateDataHandler dupDataHandler;

		Duplicator2() {
			dupDataHandler = DuplicateDataHandler.createInstance(readBuffer);
		}

		@Override
		public void initialDuplication() {
			waitForDuplication();
			System.out.println(aim.appInstId() + "-Starting to inject data");
			for (int i = 0; i < DuplicateDataHandler.totalContainers; i++) {
				if (stopCalled == 0 || stopCalled == 4) {
					DuplicateArrayContainer container = dupDataHandler
							.container(i);
					if (!container.hasData.get())
						container.fillContainer();
					send(container.data, container.filledDataSize);
					flowControl(fcTimeGap);
					container.readCompleted();
				}
			}
			dupDataHandler.duplicationCompleted();
		}

		@Override
		public void duplicationEnabled() {
			duplicationLatch = new CountDownLatch(1);
		}

		@Override
		public void prevDupCompleted() {
			duplicationLatch.countDown();
		}

		@Override
		public void finalDuplication(int duplicationCount) {
			next.duplicator.prevDupCompleted();
			System.out.println("Time between dup requested and dup started = "
					+ asw.elapsed(TimeUnit.MILLISECONDS) + "ms.");
			int rate = Math.max((int) (firingRate / 3), 1);
			int itemRate = rate * graphSchedule.steadyIn;
			Stopwatch sw = Stopwatch.createStarted();
			limitedDuplicate(duplicationCount, itemRate);
			System.out.println("limitedDuplicate time is "
					+ sw.elapsed(TimeUnit.MILLISECONDS) + "ms");
		}

		private void limitedDuplicate(int duplicationCount, int rate) {
			System.out.println("limitedDuplication. Items per sec = " + rate);
			duplicateOutputIndex = expectedOutput();
			tbMerger.startMerge(duplicateOutputIndex, HeadChannelSeamless.this);
			int itemsToSend = 0;
			int itemsDuplicated = 0;
			for (int i = 0; i < DuplicateDataHandler.totalContainers; i++) {
				if (itemsDuplicated < duplicationCount && stopCalled != 3) {
					DuplicateArrayContainer container = dupDataHandler
							.container(i);
					if (!container.hasData.get())
						container.fillContainer();
					itemsToSend = Math.min(container.filledDataSize,
							duplicationCount - itemsDuplicated);
					int send = send(container.data, itemsToSend, rate);
					itemsDuplicated += send;
					container.readCompleted();
				}
			}
			if (itemsDuplicated < duplicationCount && stopCalled != 3) {
				System.err
						.println("ERROR: DuplicateDataHandler.totalContainers is not enough");
			}
			dupDataHandler.duplicationCompleted();
		}

		@Override
		public void waitToStart() {
			Stopwatch s = Stopwatch.createStarted();
			waitForDuplication();
			System.out.println(String.format("AIM-%d. waitToStart time = %dms",
					aim.appInstId(), s.elapsed(TimeUnit.MILLISECONDS)));
		}
	}
}
