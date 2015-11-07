package edu.mit.streamjit.impl.distributed.node;

import java.io.IOException;

import edu.mit.streamjit.impl.distributed.common.CompilationInfo.InitScheduleCompleted;
import edu.mit.streamjit.impl.distributed.common.SNMessageElement;
import edu.mit.streamjit.impl.distributed.common.SNMessageElement.SNMessageElementHolder;
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
 * {@link Starter} for stateless graphs.
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
final class StatelessStarter implements Starter {

	volatile int steadyRunCount;
	private final Object initScheduleRunMonitor = new Object();
	private boolean isChannelsStarted = false;
	final BlobExecuter be;

	StatelessStarter(BlobExecuter be) {
		this.be = be;
	}

	@Override
	public void start() {
		synchronized (initScheduleRunMonitor) {
			initScheduleRunMonitor.notifyAll();
		}
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
			be.blobsManagerImpl.streamNode.controllerConnection
					.writeObject(new SNMessageElementHolder(me,
							be.blobsManagerImpl.appInstId));
		}

		synchronized (initScheduleRunMonitor) {
			initScheduleRunMonitor.wait();
		}
	}

	@Override
	public void startChannels() {
		if (!isChannelsStarted) {
			be.startChannels();
			isChannelsStarted = true;
		}
	}
}

/**
 * {@link Starter} for stateful graphs.
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
final class StatefullStarter implements Starter {

	final BlobExecuter be;
	StatefullStarter(BlobExecuter be) {
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