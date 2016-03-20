/*
 * Copyright (c) 2013-2014 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package edu.mit.streamjit.impl.distributed.node;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.impl.distributed.common.CTRLCompilationInfo;
import edu.mit.streamjit.impl.distributed.common.CTRLCompilationInfo.CTRLCompilationInfoProcessor;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.CTRLRDrainProcessor;
import edu.mit.streamjit.impl.distributed.common.CTRLRMessageVisitor;
import edu.mit.streamjit.impl.distributed.common.CTRLRSimulateDynamism;
import edu.mit.streamjit.impl.distributed.common.CTRLRSimulateDynamism.BlockCores;
import edu.mit.streamjit.impl.distributed.common.CTRLRSimulateDynamism.CTRLRDynamismProcessor;
import edu.mit.streamjit.impl.distributed.common.CTRLRSimulateDynamism.UnblockCores;
import edu.mit.streamjit.impl.distributed.common.Command;
import edu.mit.streamjit.impl.distributed.common.Command.CommandProcessor;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString.ConfigurationProcessor;
import edu.mit.streamjit.impl.distributed.common.MiscCtrlElements;
import edu.mit.streamjit.impl.distributed.common.MiscCtrlElements.MiscCtrlElementProcessor;
import edu.mit.streamjit.impl.distributed.common.MiscCtrlElements.NewConInfo;
import edu.mit.streamjit.impl.distributed.common.NodeInfo;
import edu.mit.streamjit.impl.distributed.common.Request;
import edu.mit.streamjit.impl.distributed.common.Request.RequestProcessor;
import edu.mit.streamjit.impl.distributed.profiler.Profiler;
import edu.mit.streamjit.impl.distributed.profiler.ProfilerCommand;
import edu.mit.streamjit.impl.distributed.profiler.ProfilerCommand.ProfilerCommandProcessor;
import edu.mit.streamjit.impl.distributed.profiler.StreamNodeProfiler;
import edu.mit.streamjit.util.affinity.Affinity;

/**
 * @author Sumanan sumanan@mit.edu
 * @since May 20, 2013
 */
public class CTRLRMessageVisitorImpl implements CTRLRMessageVisitor {

	private final StreamNode streamNode;
	private final RequestProcessor rp;
	private final ConfigurationProcessor jp;
	private final MiscCtrlElementProcessor miscProcessor;
	private final ProfilerCommandProcessorImpl pm;
	private final CTRLRDynamismProcessorImpl dynP;

	public CTRLRMessageVisitorImpl(StreamNode streamNode) {
		this.streamNode = streamNode;
		this.rp = new RequestProcessorImpl();
		this.jp = new ConfigurationProcessorImpl(streamNode);
		this.miscProcessor = new MiscCtrlElementProcessorImpl();
		this.pm = new ProfilerCommandProcessorImpl();
		this.dynP = new CTRLRDynamismProcessorImpl();
	}

	@Override
	public void visit(Command streamJitCommand) {
		BlobsManager manager = streamNode.getBlobsManager();
		if (manager == null) {
			System.err.println("No AppStatusProcessor processor.");
			return;
		}
		CommandProcessor cp = manager.getCommandProcessor();
		streamJitCommand.process(cp);
	}

	@Override
	public void visit(Request request) {
		request.process(rp);
	}

	@Override
	public void visit(ConfigurationString json) {
		json.process(jp);
	}

	@Override
	public void visit(CTRLRDrainElement ctrlrDrainElement) {

		BlobsManager manager = streamNode.getBlobsManager();
		if (manager == null) {
			System.err.println("No AppStatusProcessor processor.");
			return;
		}
		CTRLRDrainProcessor dp = manager.getDrainProcessor();
		ctrlrDrainElement.process(dp);
	}

	@Override
	public void visit(MiscCtrlElements miscCtrlElements) {
		miscCtrlElements.process(miscProcessor);
	}

	@Override
	public void visit(CTRLCompilationInfo ctrlCompilationInfo) {
		BlobsManager manager = streamNode.getBlobsManager();
		if (manager == null) {
			System.err.println("No AppStatusProcessor processor.");
			return;
		}
		CTRLCompilationInfoProcessor cip = manager
				.getCompilationInfoProcessor();
		ctrlCompilationInfo.process(cip);
	}

	@Override
	public void visit(ProfilerCommand command) {
		command.process(pm);
	}

	@Override
	public void visit(CTRLRSimulateDynamism simulateDynamism) {
		simulateDynamism.process(dynP);
	}

	public class MiscCtrlElementProcessorImpl implements
			MiscCtrlElementProcessor {

		@Override
		public void process(NewConInfo newConInfo) {
			// TODO
			System.err.println("Need to process this soon");
		}
	}

	/**
	 * {@link RequestProcessor} at {@link StreamNode} side.
	 * 
	 * @author Sumanan sumanan@mit.edu
	 * @since May 27, 2013
	 */
	public class RequestProcessorImpl implements RequestProcessor {

		@Override
		public void processAPPStatus() {
			System.out.println("APPStatus requested");
		}

		@Override
		public void processSysInfo() {
			System.out.println("SysInfo requested");
		}

		@Override
		public void processMachineID() {
			try {
				Integer id = streamNode.controllerConnection.readObject();
				streamNode.setNodeID(id);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void processNodeInfo() {
			NodeInfo myInfo = NodeInfo.getMyinfo();
			try {
				streamNode.controllerConnection.writeObject(myInfo);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void processEXIT() {
			System.out.println("StreamNode is Exiting...");
			dynP.StopAll();
			streamNode.exit();
		}
	}

	public class ProfilerCommandProcessorImpl implements
			ProfilerCommandProcessor {

		ProfilerCommandProcessorImpl() {

		}

		@Override
		public void processSTART() {
			createProfiler();
			if (streamNode.profiler.getState() == Thread.State.NEW)
				streamNode.profiler.start();
		}

		/**
		 * Creates a new profiler only if
		 * <ol>
		 * <li>no any profiler has already been created OR
		 * <li>the previously created profiler has been terminated.
		 * </ol>
		 */
		private void createProfiler() {
			if (streamNode.profiler == null) {
				streamNode.profiler = new Profiler(
						new HashSet<StreamNodeProfiler>(),
						streamNode.controllerConnection);
			} else if (streamNode.profiler.getState() == Thread.State.TERMINATED) {
				System.err
						.println("A profiler has already been created and terminated. Creating another new profiler.");
				streamNode.profiler = new Profiler(
						new HashSet<StreamNodeProfiler>(),
						streamNode.controllerConnection);

			} else
				System.err.println("A profiler has already been started.");
		}

		@Override
		public void processSTOP() {
			streamNode.profiler.stopProfiling();
		}

		@Override
		public void processPAUSE() {
			streamNode.profiler.pauseProfiling();
		}

		@Override
		public void processRESUME() {
			streamNode.profiler.resumeProfiling();
		}
	}

	public static class CTRLRDynamismProcessorImpl implements
			CTRLRDynamismProcessor {
		Map<Integer, BlockCore> blockThreads = new HashMap<>();

		@Override
		public void process(BlockCores blockCores) {

			for (Integer core : blockCores.coreSet) {
				if (blockThreads.containsKey(core))
					throw new IllegalArgumentException(name(core) + " exists");
				BlockCore bt = new BlockCore(name(core), core);
				bt.start();
				blockThreads.put(core, bt);
			}
		}

		@Override
		public void process(UnblockCores unblockCores) {
			for (Integer core : unblockCores.coreSet) {
				BlockCore bt = blockThreads.remove(core);
				if (bt == null)
					new IllegalArgumentException(name(core)
							+ " is unblocked already").printStackTrace();
				bt.requestStop();
			}
		}

		String name(Integer core) {
			return String.format("BlockThread-%d", core);
		}

		void StopAll() {
			for (BlockCore bt : blockThreads.values())
				bt.requestStop();
		}
	}

	static final class BlockCore extends Thread {

		private final Integer core;

		private volatile boolean stopping = false;

		BlockCore(String name, Integer core) {
			super(name);
			this.core = core;
		}

		public void requestStop() {
			stopping = true;
		}

		@Override
		public void run() {
			if (core != null)
				Affinity.setThreadAffinity(ImmutableSet.of(core));

			System.out.println(Thread.currentThread().getName()
					+ " starting...");
			boolean b = true;
			while (!stopping && b) {
				b = "Foo".matches("F.*");
			}
			System.out.println(Thread.currentThread().getName()
					+ " stopping...");
		}
	}

	public static void main(String[] args) throws InterruptedException {
		BlockCores bc = new BlockCores(ImmutableSet.of(0, 2));
		CTRLRDynamismProcessorImpl dp = new CTRLRDynamismProcessorImpl();
		dp.process(bc);

		Thread.sleep(60000);
		dp.StopAll();
	}
}
