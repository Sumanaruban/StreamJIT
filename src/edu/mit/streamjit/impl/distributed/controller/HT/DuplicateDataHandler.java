package edu.mit.streamjit.impl.distributed.controller.HT;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import edu.mit.streamjit.impl.blob.Buffer;

/**
 * TODO: Need to make the container reading and the usage cyclic.
 * 
 * @author sumanan
 * @since 10 Jan, 2016
 */
public class DuplicateDataHandler {

	static final int dataLength = 10000;

	static final int totalContainers = 5;

	final DuplicateArrayContainer[] containers = new DuplicateArrayContainer[totalContainers];

	final Buffer readBuffer;

	private static DuplicateDataHandler myinstance;

	final AtomicInteger dupCompleted = new AtomicInteger(0);

	public static DuplicateDataHandler createInstance(Buffer buffer) {
		if (myinstance == null) {
			synchronized (DuplicateDataHandler.class) {
				if (myinstance == null)
					myinstance = new DuplicateDataHandler(buffer);
			}
		}
		return myinstance;
	}

	private DuplicateDataHandler(Buffer buffer) {
		this.readBuffer = buffer;
		initializeContainers();
		reset();
	}

	private void initializeContainers() {
		for (int i = 0; i < containers.length; i++) {
			containers[i] = new DuplicateArrayContainer();
		}
	}

	DuplicateArrayContainer container(int index) {
		return containers[index];
	}

	void duplicationCompleted() {
		int val = dupCompleted.incrementAndGet();
		if (val == 2) {
			reset();
		} else if (val > 2) {
			throw new IllegalStateException(String.format("val(%d) > 2", val));
		}
	}

	private void reset() {
		for (DuplicateArrayContainer c : containers) {
			c.reset();
		}
		dupCompleted.set(0);
	}

	class DuplicateArrayContainer {

		final Object[] data = new Object[dataLength];

		int filledDataSize = 0;

		final AtomicBoolean hasData = new AtomicBoolean(false);

		final AtomicInteger readCompleted = new AtomicInteger(0);

		void readCompleted() {
			int val = readCompleted.incrementAndGet();
			if (val == 2) {
				reset();
			} else if (val > 2) {
				throw new IllegalStateException(String.format("val(%d) > 2",
						val));
			}
		}

		void reset() {
			hasData.set(false);
			readCompleted.set(0);
		}

		synchronized boolean fillContainer() {
			if (hasData.get())
				return false;
			filledDataSize = readBuffer.read(data, 0, data.length);
			hasData.set(true);
			return true;
		}
	}
}
