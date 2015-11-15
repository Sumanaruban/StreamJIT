package edu.mit.streamjit.impl.distributed.node;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import edu.mit.streamjit.impl.distributed.common.CompilationInfo.InitScheduleCompleted;
import edu.mit.streamjit.impl.distributed.common.SNMessageElement;
import edu.mit.streamjit.impl.distributed.node.BlobExecuter.BlobThread2;

/**
 * In order to achieve seamless reconfiguration we need to start stateless and
 * stateful graphs differently. If the graph is stateless, we can run the next
 * app instance's initSchedule while the current app instance is running and
 * then join the outputs. If the graph is stateful, we need to stop the current
 * app instance before running the next app instance.
 * 
 * <p>
 * Alternatively, We can get rid of this interface, move the methods of
 * interface to {@link BlobExecuter} as abstract methods, and have two different
 * flavors of {@link BlobExecuter} implementations, one for stateful graph and
 * another for stateless graph.
 * 
 * @author sumanan
 * @since 8 Oct, 2015
 */
interface Starter {
	void start();
	void runInitSchedule(int steadyRunCount);
	void initScheduleRun(BlobThread2 bt) throws InterruptedException,
			IOException;
	void startChannels();
}

/**
 * This {@link Starter} first runs init schedule and waits for command from
 * controller to start steady state execution. This is suitable if the stream
 * graph has neither initial state nor initial drain data (e.g, stateless
 * graphs).
 * 
 * <p>
 * Warning: This class refers {@link BlobExecuter}'s fields frequently as all
 * methods of this class used to be the methods of {@link BlobExecuter} before
 * this refactoring. Alternatively, This class is could be made as an inner
 * class of {@link BlobExecuter}.
 * 
 * @author sumanan
 * @since 8 Oct, 2015
 */
final class Starter2 implements Starter {

	volatile int steadyRunCount;
	private final CountDownLatch latch = new CountDownLatch(1);
	private boolean isChannelsStarted = false;
	final BlobExecuter be;

	Starter2(BlobExecuter be) {
		this.be = be;
	}

	@Override
	public void start() {
		latch.countDown();
	}

	@Override
	public void runInitSchedule(int steadyRunCount) {
		be.outChnlManager.waitToStart();
		be.inChnlManager.waitToStart();

		be.bufferMap = be.buildBufferMap();
		be.blob.installBuffers(be.bufferMap);

		for (BlobThread2 t : be.blobThreads) {
			this.steadyRunCount = steadyRunCount;
			t.start();
		}
		// System.out.println(blobID + " started");
	}

	@Override
	public void initScheduleRun(BlobThread2 bt) throws InterruptedException,
			IOException {
		if (bt.logTime)
			be.bEvent("initScheduleRun");
		for (int i = 0; i < steadyRunCount + 1; i++) {
			if (bt.stopping)
				break;
			bt.coreCode.run();
		}
		if (bt.logTime) {
			long time = be.eEvent("initScheduleRun");
			SNMessageElement me = new InitScheduleCompleted(be.blobID, time);
			be.blobsManagerImpl.sendToController(me);
		}
		latch.await();
	}

	@Override
	public void startChannels() {
		// [2015-11-15] TODO: This logic may not be needed as I have separated
		// communication establishment process from starting an appInst. See
		// Command.START_CHANNELS and related changes.
		if (!isChannelsStarted) {
			be.startChannels();
			isChannelsStarted = true;
		}
	}
}

/**
 * This {@link Starter} straightly starts blobs, which is suitable if the stream
 * graph has either initial state or initial drain data. This does not
 * differentiate init schedule and steady state schedule.
 * 
 * <p>
 * Warning: This class refers {@link BlobExecuter}'s fields frequently as all
 * methods of this class used to be the methods of {@link BlobExecuter} before
 * this refactoring. Alternatively, This class is could be made as an inner
 * class of {@link BlobExecuter}.
 * 
 * @author sumanan
 * @since 8 Oct, 2015
 */
final class Starter1 implements Starter {

	final BlobExecuter be;
	Starter1(BlobExecuter be) {
		this.be = be;
	}

	@Override
	public void start() {
		be.outChnlManager.waitToStart();
		be.inChnlManager.waitToStart();

		be.bufferMap = be.buildBufferMap();
		be.blob.installBuffers(be.bufferMap);

		for (Thread t : be.blobThreads)
			t.start();

		// System.out.println(blobID + " started");
	}

	@Override
	public void runInitSchedule(int steadyRunCount) {
		throw new IllegalStateException(
				"Can not run InitSchedule in advance for stateful graphs.");
	}

	@Override
	public void initScheduleRun(BlobThread2 bt) throws InterruptedException,
			IOException {
	}

	@Override
	public void startChannels() {
		be.startChannels();
	}
}