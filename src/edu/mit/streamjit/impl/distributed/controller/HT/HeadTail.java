package edu.mit.streamjit.impl.distributed.controller.HT;

import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.common.Counter;

/**
 * @author sumanan
 * @since 17 Dec, 2015
 */
public class HeadTail {

	public final int appInstId;

	private final Buffer headBuffer;

	private final Counter headCounter;

	final Buffer tailBuffer;

	public final Counter tailCounter;

	private HeadTail(int appInstId, Buffer headBuffer, Buffer tailBuffer,
			Counter headCounter, Counter tailCounter) {
		this.appInstId = appInstId;
		this.headBuffer = headBuffer;
		this.headCounter = headCounter;
		this.tailBuffer = tailBuffer;
		this.tailCounter = tailCounter;
	}

	/**
	 * Creates a new, empty builder.
	 * 
	 * @return a new, empty builder
	 */
	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {
		private int appInstId = -1;

		private Buffer headBuffer = null;

		private Counter headCounter = null;

		private Buffer tailBuffer = null;

		private Counter tailCounter = null;

		public void appInstId(int appInstId) {
			this.appInstId = appInstId;
		}

		/*public void headBuffer(Buffer headBuffer) {
			this.headBuffer = headBuffer;
		}

		public void headCounter(Counter headCounter) {
			this.headCounter = headCounter;
		}*/

		public void tailBuffer(Buffer tailBuffer) {
			this.tailBuffer = tailBuffer;
		}

		public void tailCounter(Counter tailCounter) {
			this.tailCounter = tailCounter;
		}

		public HeadTail build() {
			return new HeadTail(appInstId, headBuffer, tailBuffer, headCounter,
					tailCounter);
		}
	}
}
