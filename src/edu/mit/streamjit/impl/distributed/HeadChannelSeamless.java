package edu.mit.streamjit.impl.distributed;

import java.util.concurrent.CountDownLatch;

import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.common.Counter;
import edu.mit.streamjit.impl.distributed.BufferSizeCalc.GraphSchedule;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionProvider;
import edu.mit.streamjit.impl.distributed.node.AsyncOutputChannel;
import edu.mit.streamjit.tuner.EventTimeLogger;

/**
 * 
 * @author sumanan
 * @since 12 Nov, 2015
 */
public class HeadChannelSeamless extends AsyncOutputChannel {

	final Buffer readBuffer;
	Buffer writeBuffer;

	final int dataLength = 10000;
	final Object[] data = new Object[dataLength];

	private volatile boolean stopCalled;
	private volatile boolean isFinal;
	private final EventTimeLogger eLogger;

	private final CountDownLatch latch;
	volatile boolean canWrite;

	int count;

	GraphSchedule graphSchedule;

	final Counter tailCounter;

	final AppInstanceManager aim;

	private final int debugLevel = 0;

	public HeadChannelSeamless(Buffer buffer, ConnectionProvider conProvider,
			ConnectionInfo conInfo, String bufferTokenName, int debugLevel,
			EventTimeLogger eLogger, boolean waitForDuplication,
			Counter tailCounter, AppInstanceManager aim) {
		super(conProvider, conInfo, bufferTokenName, debugLevel);
		readBuffer = buffer;
		stopCalled = false;
		this.eLogger = eLogger;
		count = 0;
		canWrite = false;
		latch = latch(waitForDuplication);
		this.tailCounter = tailCounter;
		this.aim = aim;
	}

	@Override
	public Runnable getRunnable() {
		final Runnable supperRunnable = super.getRunnable();
		return new Runnable() {
			@Override
			public void run() {
				eLogger.bEvent("initialization");
				supperRunnable.run();
				writeBuffer = getBuffer();
				canWrite = true;
				waitForDuplication();
				graphSchedule = aim.graphSchedule;
				sendData();
				sendRemining();
				stopSuper(isFinal);
			}
		};
	}

	private CountDownLatch latch(boolean waitForDuplication) {
		if (waitForDuplication)
			return new CountDownLatch(1);
		return null;
	}

	public void sendData() {
		int read = 1;
		while (!stopCalled) {
			read = readBuffer.read(data, 0, data.length);
			send(data, read);
			flowControl();
		}
	}

	private void flowControl() {
		int expectedFiring = expectedFiring();
		int currentFiring = 0;
		while ((expectedFiring - (currentFiring = currentFiring()) > 1000)
				&& !stopCalled) {
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

	private int currentFiring() {
		int firing = (tailCounter.count() - graphSchedule.totalOutDuringInit)
				/ graphSchedule.steadyOut;
		return firing;
	}

	private void sendRemining() {
		int reminder = (count - graphSchedule.totalInDuringInit)
				% graphSchedule.steadyIn;
		int residue = graphSchedule.steadyIn - reminder;
		int itemsToRead;
		int itemsSent = 0;
		while (itemsSent < residue) {
			itemsToRead = Math.min(data.length, residue - itemsSent);
			int read = readBuffer.read(data, 0, itemsToRead);
			send(data, read);
			itemsSent += read;
		}
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

	public void duplicateSend(int duplicationCount, HeadChannelSeamless next) {
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
		int written = 0;
		while (written < items) {
			written += writeBuffer.write(data, written, items - written);
			if (written == 0) {
				try {
					// TODO: Verify this sleep time.
					Thread.sleep(500);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		count += written;
	}

	protected void fillUnprocessedData() {
		throw new Error("Method not implemented");
	}

	@Override
	public void stop(boolean isFinal) {
		this.isFinal = isFinal;
		this.stopCalled = true;
	}

	private void stopSuper(boolean isFinal) {
		super.stop(isFinal);
	}
}
