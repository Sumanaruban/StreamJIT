package edu.mit.streamjit.impl.distributed.controller;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

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
import edu.mit.streamjit.impl.distributed.controller.BufferSizeCalc.GraphSchedule;
import edu.mit.streamjit.tuner.EventTimeLogger;

/**
 * 
 * @author sumanan
 * @since 12 Nov, 2015
 */
public class HeadChannelSeamless implements BoundaryOutputChannel {

	public static final int duplicationFiring = 500;

	private Connection connection;

	final Buffer readBuffer;

	final int dataLength = 10000;
	final Object[] data = new Object[dataLength];

	private volatile int stopCalled;
	private final EventTimeLogger eLogger;

	private volatile CountDownLatch latch;
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

	public HeadChannelSeamless(Buffer buffer, ConnectionProvider conProvider,
			ConnectionInfo conInfo, String bufferTokenName,
			EventTimeLogger eLogger, Counter tailCounter, AppInstanceManager aim) {
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
	}

	public Runnable getRunnable() {
		return new Runnable() {
			@Override
			public void run() {
				eLogger.bEvent("initialization");
				makeConnection();
				canWrite = true;
				waitForDuplication();
				graphSchedule = aim.graphSchedule;
				sendData();
				if (stopCalled == 1)
					duplicateSend(duplicationCount, next);
				else if (stopCalled == 2)
					reqStateDuplicateAndStop();
				if (stopCalled == 1 || stopCalled == 2)
					new DrainerThread().start();
				closeConnection();
			}
		};
	}

	public void duplicationEnabled() {
		latch = new CountDownLatch(1);
	}

	public void sendData() {
		int read = 1;
		while (stopCalled == 0) {
			read = readBuffer.read(data, 0, data.length);
			send(data, read);
			flowControl();
		}
		sendRemining();
	}

	private void flowControl() {
		int expectedFiring = expectedFiring();
		int currentFiring = 0;
		while ((expectedFiring - (currentFiring = currentFiring()) > 5000)
				&& stopCalled == 0) {
			try {
				// TODO: Need to tune this sleep time.
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			if (debugLevel > 0)
				System.out
						.println(String
								.format("flowControl : expectedFiring - %d, currentFiring = %d",
										expectedFiring, currentFiring));
		}

		if (debugLevel > 0) {
			System.out.println(String.format(
					"flowControl : expectedFiring - %d, currentFiring = %d",
					expectedFiring, currentFiring));
			System.out.println("flowControl Over................");
		}
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

	private void sendRemining() {
		int reminder = (count - graphSchedule.totalInDuringInit)
				% graphSchedule.steadyIn;
		int residue = graphSchedule.steadyIn - reminder;
		send(residue);
	}

	private void waitForDuplication() {
		if (latch == null)
			return;
		try {
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
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

	private void duplicateSend(int duplicationCount, HeadChannelSeamless next) {
		duplicateOutputIndex = expectedOutput();
		int itemsToRead;
		int itemsDuplicated = 0;
		while (itemsDuplicated < duplicationCount) {
			itemsToRead = Math.min(data.length, duplicationCount
					- itemsDuplicated);
			int read = readBuffer.read(data, 0, itemsToRead);
			send(data, read);
			next.waitToWrite();
			next.send(data, read);
			itemsDuplicated += read;
		}
		next.latch.countDown();
	}

	public void duplicateAndStop(int duplicationCount, HeadChannelSeamless next) {
		this.duplicationCount = duplicationCount;
		this.next = next;
		this.stopCalled = 1;
	}

	public void reqStateDuplicateAndStop(HeadChannelSeamless next) {
		this.next = next;
		this.stopCalled = 2;
	}

	private void reqStateDuplicateAndStop() {
		int reqStateAt = requestState();
		int items = reqStateAt * graphSchedule.steadyIn
				+ graphSchedule.totalInDuringInit - count;
		send(items);
		duplicateSend(duplicationFiring * graphSchedule.steadyIn, next);
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

	protected void fillUnprocessedData() {
		throw new Error("Method not implemented");
	}
	public void stop() {
		this.stopCalled = 3;
	}

	private int requestState() {
		int i = expectedFiring();
		int reqStateAt = i + 500;
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
			super(String.format("DrainerThread - %d", aim.appInst.id));
		}

		public void run() {
			aim.appManager.intermediateDraining(aim);
		}
	}
}
