package edu.mit.streamjit.impl.distributed.node;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.ConcurrentArrayBuffer;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.common.BlobThread;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryInputChannel;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryOutputChannel;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.CTRLRDrainProcessor;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.DoDrain;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.DrainDataRequest;
import edu.mit.streamjit.impl.distributed.common.Command.CommandProcessor;
import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement;
import edu.mit.streamjit.impl.distributed.common.SNMessageElement;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionProvider;
import edu.mit.streamjit.impl.distributed.common.Utils;

/**
 * {@link BlobsManagerImpl} responsible to run all {@link Blob}s those are
 * assigned to the {@link StreamNode}.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
public class BlobsManagerImpl implements BlobsManager {

	private Set<BlobExecuter> blobExecuters;
	private final StreamNode streamNode;
	private final TCPConnectionProvider conProvider;
	private Map<Token, TCPConnectionInfo> conInfoMap;

	private final CTRLRDrainProcessor drainProcessor;

	private final CommandProcessor cmdProcessor;

	private final ImmutableMap<Token, Buffer> bufferMap;

	public BlobsManagerImpl(ImmutableSet<Blob> blobSet,
			Map<Token, TCPConnectionInfo> conInfoMap, StreamNode streamNode,
			TCPConnectionProvider conProvider) {
		this.conInfoMap = conInfoMap;
		this.streamNode = streamNode;
		this.conProvider = conProvider;

		this.cmdProcessor = new CommandProcessorImpl(streamNode);
		this.drainProcessor = new CTRLRDrainProcessorImpl(streamNode);

		bufferMap = createBufferMap(blobSet);

		for (Blob b : blobSet) {
			b.installBuffers(bufferMap);
		}

		Set<Token> locaTokens = getLocalTokens(blobSet);
		blobExecuters = new HashSet<>();
		for (Blob b : blobSet) {
			ImmutableMap<Token, BoundaryInputChannel> inputChannels = createInputChannels(
					Sets.difference(b.getInputs(), locaTokens), bufferMap);
			ImmutableMap<Token, BoundaryOutputChannel> outputChannels = createOutputChannels(
					Sets.difference(b.getOutputs(), locaTokens), bufferMap);
			blobExecuters
					.add(new BlobExecuter(b, inputChannels, outputChannels));
		}
	}

	@Override
	public void start() {
		for (BlobExecuter be : blobExecuters)
			be.start();
	}

	@Override
	public void stop() {
		for (BlobExecuter be : blobExecuters)
			be.stop();
	}

	// TODO: Buffer sizes, including head and tail buffers, must be optimized.
	// consider adding some tuning factor
	private ImmutableMap<Token, Buffer> createBufferMap(Set<Blob> blobSet) {
		ImmutableMap.Builder<Token, Buffer> bufferMapBuilder = ImmutableMap
				.<Token, Buffer> builder();

		Map<Token, Integer> minInputBufCapaciy = new HashMap<>();
		Map<Token, Integer> minOutputBufCapaciy = new HashMap<>();

		for (Blob b : blobSet) {
			Set<Blob.Token> inputs = b.getInputs();
			for (Token t : inputs) {
				minInputBufCapaciy.put(t, b.getMinimumBufferCapacity(t));
			}

			Set<Blob.Token> outputs = b.getOutputs();
			for (Token t : outputs) {
				minOutputBufCapaciy.put(t, b.getMinimumBufferCapacity(t));
			}
		}

		Set<Token> localTokens = Sets.intersection(minInputBufCapaciy.keySet(),
				minOutputBufCapaciy.keySet());
		Set<Token> globalInputTokens = Sets.difference(
				minInputBufCapaciy.keySet(), localTokens);
		Set<Token> globalOutputTokens = Sets.difference(
				minOutputBufCapaciy.keySet(), localTokens);

		for (Token t : localTokens) {
			int bufSize;
			bufSize = lcm(minInputBufCapaciy.get(t), minOutputBufCapaciy.get(t));
			// TODO: Just to increase the performance. Change it later
			bufSize = Math.max(1000, bufSize);
			Buffer buf = new ConcurrentArrayBuffer(bufSize);
			bufferMapBuilder.put(t, buf);
		}

		for (Token t : Sets.union(globalInputTokens, globalOutputTokens)) {
			bufferMapBuilder.put(t, new ConcurrentArrayBuffer(1000));
		}

		return bufferMapBuilder.build();
	}

	private int gcd(int a, int b) {
		while (true) {
			if (a == 0)
				return b;
			b %= a;
			if (b == 0)
				return a;
			a %= b;
		}
	}

	private int lcm(int a, int b) {
		int val = gcd(a, b);
		return val != 0 ? ((a * b) / val) : 0;
	}

	private Set<Token> getLocalTokens(Set<Blob> blobSet) {
		Set<Token> inputTokens = new HashSet<>();
		Set<Token> outputTokens = new HashSet<>();

		for (Blob b : blobSet) {
			Set<Token> inputs = b.getInputs();
			for (Token t : inputs) {
				inputTokens.add(t);
			}

			Set<Token> outputs = b.getOutputs();
			for (Token t : outputs) {
				outputTokens.add(t);
			}
		}
		return Sets.intersection(inputTokens, outputTokens);
	}

	private ImmutableMap<Token, BoundaryInputChannel> createInputChannels(
			Set<Token> inputTokens, ImmutableMap<Token, Buffer> bufferMap) {
		ImmutableMap.Builder<Token, BoundaryInputChannel> inputChannelMap = new ImmutableMap.Builder<>();
		for (Token t : inputTokens) {
			TCPConnectionInfo conInfo = conInfoMap.get(t);
			inputChannelMap.put(t, new TCPInputChannel(bufferMap.get(t),
					conProvider, conInfo, t.toString(), 0));
		}
		return inputChannelMap.build();
	}

	private ImmutableMap<Token, BoundaryOutputChannel> createOutputChannels(
			Set<Token> outputTokens, ImmutableMap<Token, Buffer> bufferMap) {
		ImmutableMap.Builder<Token, BoundaryOutputChannel> outputChannelMap = new ImmutableMap.Builder<>();
		for (Token t : outputTokens) {
			TCPConnectionInfo conInfo = conInfoMap.get(t);
			outputChannelMap.put(t, new TCPOutputChannel(bufferMap.get(t),
					conProvider, conInfo, t.toString(), 0));
		}
		return outputChannelMap.build();
	}

	private class BlobExecuter {

		private volatile int drainState;
		private final Token blobID;

		private final Blob blob;
		private Set<BlobThread> blobThreads;

		private final ImmutableMap<Token, BoundaryInputChannel> inputChannels;
		private final ImmutableMap<Token, BoundaryOutputChannel> outputChannels;

		Set<Thread> inputChannelThreads;
		Set<Thread> outputChannelThreads;

		private boolean reqDrainData;

		private BlobExecuter(Blob blob,
				ImmutableMap<Token, BoundaryInputChannel> inputChannels,
				ImmutableMap<Token, BoundaryOutputChannel> outputChannels) {
			this.blob = blob;
			this.blobThreads = new HashSet<>();
			assert blob.getInputs().containsAll(inputChannels.keySet());
			assert blob.getOutputs().containsAll(outputChannels.keySet());
			this.inputChannels = inputChannels;
			this.outputChannels = outputChannels;
			inputChannelThreads = new HashSet<>(inputChannels.values().size());
			outputChannelThreads = new HashSet<>(outputChannels.values().size());

			for (int i = 0; i < blob.getCoreCount(); i++) {
				blobThreads.add(new BlobThread(blob.getCoreCode(i)));
			}

			drainState = 0;
			this.blobID = Utils.getBlobID(blob);
		}

		private void start() {
			for (BoundaryInputChannel bc : inputChannels.values()) {
				Thread t = new Thread(bc.getRunnable(), bc.name());
				t.start();
				inputChannelThreads.add(t);
			}

			for (BoundaryOutputChannel bc : outputChannels.values()) {
				Thread t = new Thread(bc.getRunnable(), bc.name());
				t.start();
				outputChannelThreads.add(t);
			}

			for (Thread t : blobThreads)
				t.start();
		}

		private void stop() {

			for (BoundaryInputChannel bc : inputChannels.values()) {
				bc.stop(true);
			}

			for (BoundaryOutputChannel bc : outputChannels.values()) {
				bc.stop(true);
			}

			for (Thread t : blobThreads)
				try {
					t.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
		}

		private void doDrain(boolean reqDrainData) {
			this.reqDrainData = reqDrainData;
			drainState = 1;

			for (BoundaryInputChannel bc : inputChannels.values()) {
				bc.stop(!this.reqDrainData);
			}

			for (Thread t : inputChannelThreads) {
				try {
					t.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			DrainCallback dcb = new DrainCallback(this);
			drainState = 2;
			this.blob.drain(dcb);
		}

		private void drained() {
			drainState = 3;
			for (BlobThread bt : blobThreads) {
				bt.requestStop();
			}

			for (BoundaryOutputChannel bc : outputChannels.values()) {
				bc.stop(!this.reqDrainData);
			}

			for (Thread t : outputChannelThreads) {
				try {
					t.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			drainState = 4;
			SNMessageElement drained = new SNDrainElement.Drained(blobID);
			try {
				streamNode.controllerConnection.writeObject(drained);
			} catch (IOException e) {
				e.printStackTrace();
			}
			// System.out.println("Blob " + blobID + "is drained");

			if (this.reqDrainData) {
				// System.out.println("**********************************");
				DrainData dd = blob.getDrainData();
				drainState = 5;

				for (Token t : dd.getData().keySet()) {
					System.out.println("From Blob: " + t.toString() + " - "
							+ dd.getData().get(t).size());
				}

				ImmutableMap.Builder<Token, ImmutableList<Object>> inputDataBuilder = new ImmutableMap.Builder<>();
				ImmutableMap.Builder<Token, ImmutableList<Object>> outputDataBuilder = new ImmutableMap.Builder<>();

				for (Token t : blob.getInputs()) {
					if (inputChannels.containsKey(t)) {
						BoundaryChannel chanl = inputChannels.get(t);
						ImmutableList<Object> draindata = chanl
								.getUnprocessedData();
						System.out.println(String.format(
								"No of unprocessed data of %s is %d",
								chanl.name(), draindata.size()));
						inputDataBuilder.put(t, draindata);
					}

					// TODO: Unnecessary data copy. Optimise this.
					else {
						Buffer buf = bufferMap.get(t);
						Object[] bufArray = new Object[buf.size()];
						buf.readAll(bufArray);
						assert buf.size() == 0 : String.format(
								"buffer size is %d. But 0 is expected",
								buf.size());
						inputDataBuilder.put(t, ImmutableList.copyOf(bufArray));
					}
				}

				for (Token t : blob.getOutputs()) {
					if (outputChannels.containsKey(t)) {
						BoundaryChannel chanl = outputChannels.get(t);
						ImmutableList<Object> draindata = chanl
								.getUnprocessedData();
						System.out.println(String.format(
								"No of unprocessed data of %s is %d",
								chanl.name(), draindata.size()));
						outputDataBuilder.put(t, draindata);
					}
				}

				SNMessageElement me = new SNDrainElement.DrainedData(blobID,
						dd, inputDataBuilder.build(), outputDataBuilder.build());
				try {
					streamNode.controllerConnection.writeObject(me);
					// System.out.println(blobID + " DrainData has been sent");
					drainState = 6;

				} catch (IOException e) {
					e.printStackTrace();
				}

				// System.out.println("**********************************");
			}
			// printDrainedStatus();
		}

		public Token getBlobID() {
			return Utils.getBlobID(blob);
		}
	}

	private static class DrainCallback implements Runnable {

		private final BlobExecuter blobExec;

		DrainCallback(BlobExecuter be) {
			this.blobExec = be;
		}

		@Override
		public void run() {
			blobExec.drained();
		}
	}

	@Override
	public void drain(Token blobID, boolean reqDrainData) {
		for (BlobExecuter be : blobExecuters) {
			if (be.getBlobID().equals(blobID)) {
				be.doDrain(reqDrainData);
				return;
			}
		}
		throw new IllegalArgumentException(String.format(
				"No blob with blobID %s", blobID));
	}

	/**
	 * Just to added for debugging purpose.
	 */
	private synchronized void printDrainedStatus() {
		System.out.println("****************************************");
		for (BlobExecuter be : blobExecuters) {
			switch (be.drainState) {
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

	@Override
	public void reqDrainedData(Set<Token> blobSet) {
		// ImmutableMap.Builder<Token, DrainData> builder = new
		// ImmutableMap.Builder<>();
		// for (BlobExecuter be : blobExecuters) {
		// if (be.isDrained) {
		// builder.put(be.blobID, be.blob.getDrainData());
		// }
		// }
		//
		// try {
		// streamNode.controllerConnection
		// .writeObject(new SNDrainElement.DrainedData(builder.build()));
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
	}

	public CTRLRDrainProcessor getDrainProcessor() {
		return drainProcessor;
	}

	public CommandProcessor getCommandProcessor() {
		return cmdProcessor;
	}

	/**
	 * Implementation of {@link DrainProcessor} at {@link StreamNode} side. All
	 * appropriate response logic to successfully perform the draining is
	 * implemented here.
	 * 
	 * @author Sumanan sumanan@mit.edu
	 * @since Jul 30, 2013
	 */
	public class CTRLRDrainProcessorImpl implements CTRLRDrainProcessor {

		StreamNode streamNode;

		public CTRLRDrainProcessorImpl(StreamNode streamNode) {
			this.streamNode = streamNode;
		}

		@Override
		public void process(DrainDataRequest drnDataReq) {
			streamNode.getBlobsManager().reqDrainedData(drnDataReq.blobsSet);
		}

		@Override
		public void process(DoDrain drain) {
			streamNode.getBlobsManager()
					.drain(drain.blobID, drain.reqDrainData);
		}
	}

	/**
	 * {@link CommandProcessor} at {@link StreamNode} side.
	 * 
	 * @author Sumanan sumanan@mit.edu
	 * @since May 27, 2013
	 */
	public class CommandProcessorImpl implements CommandProcessor {
		StreamNode streamNode;

		public CommandProcessorImpl(StreamNode streamNode) {
			this.streamNode = streamNode;
		}

		@Override
		public void processSTART() {
			BlobsManager bm = streamNode.getBlobsManager();
			if (bm != null) {
				bm.start();
				long heapMaxSize = Runtime.getRuntime().maxMemory();
				long heapSize = Runtime.getRuntime().totalMemory();
				long heapFreeSize = Runtime.getRuntime().freeMemory();

				System.out
						.println("##############################################");

				System.out.println("heapMaxSize = " + heapMaxSize / 1e6);
				System.out.println("heapSize = " + heapSize / 1e6);
				System.out.println("heapFreeSize = " + heapFreeSize / 1e6);
				System.out.println("StraemJit app is running...");
				System.out
						.println("##############################################");

			} else {
				// TODO: Need to handle this case. Need to send the error
				// message to
				// the controller.
				System.out
						.println("Couldn't start the blobs...BlobsManager is null.");
			}
		}

		@Override
		public void processSTOP() {
			BlobsManager bm = streamNode.getBlobsManager();
			if (bm != null) {
				bm.stop();
				System.out.println("StraemJit app stopped...");
				try {
					streamNode.controllerConnection
							.writeObject(AppStatus.STOPPED);
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else {
				// TODO: Need to handle this case. Need to send the error
				// message to
				// the controller.
				System.out
						.println("Couldn't stop the blobs...BlobsManager is null.");
			}
		}

		@Override
		public void processEXIT() {
			System.out.println("StreamNode is Exiting...");
			streamNode.exit();
		}
	}

}
