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
package edu.mit.streamjit.impl.distributed.runtimer;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.CTRLRMessageElement.CTRLRMessageElementHolder;
import edu.mit.streamjit.impl.distributed.common.CompilationInfo;
import edu.mit.streamjit.impl.distributed.common.Error;
import edu.mit.streamjit.impl.distributed.common.NodeInfo;
import edu.mit.streamjit.impl.distributed.common.NodeInfo.NodeInfoProcessor;
import edu.mit.streamjit.impl.distributed.common.Request;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement;
import edu.mit.streamjit.impl.distributed.common.SNException;
import edu.mit.streamjit.impl.distributed.common.SNMessageVisitor;
import edu.mit.streamjit.impl.distributed.common.SNTimeInfo;
import edu.mit.streamjit.impl.distributed.common.SystemInfo;
import edu.mit.streamjit.impl.distributed.common.SystemInfo.SystemInfoProcessor;
import edu.mit.streamjit.impl.distributed.controller.AppInstanceManager;
import edu.mit.streamjit.impl.distributed.controller.StreamJitAppManager;
import edu.mit.streamjit.impl.distributed.node.StreamNode;
import edu.mit.streamjit.impl.distributed.profiler.SNProfileElement;

/**
 * StreamNodeAgent represents a {@link StreamNode} at {@link Controller} side.
 * Controller will be having a StreamNodeAgent for each connected StreamNode.
 * <p>
 * IO connection is not part of the StreamNodeAgent and StreamNodeAgent is
 * expected to be a passive object. This decision is made because single thread
 * or a dispatcher thread pool is expected to be running for asynchronous IO or
 * non blocking IO implementation of the {@link CommunicationManager}. It is
 * {@link CommunicationManager}'s responsibility to run the IO connections of
 * each {@link StreamNode} on appropriate thread. If it is blocking IO then each
 * communication can be run on separate thread. Otherwise, single thread or a
 * thread pool may handle all IO communications. IO threads will read the
 * incoming messages and act accordingly. Controller thread also can send
 * messages to {@link StreamNode}s in parallel.
 * </p>
 */
public abstract class StreamNodeAgent {

	/**
	 * {@link MessageVisitor} for this streamnode.
	 */
	private final SNMessageVisitor mv;

	// TODO: How to avoid the volatileness here. Because we set only once and
	// read forever later. So if it is a volatile, every read will need to
	// access the memory. Is there any way to avoid this?
	// Will removing volatile modifier be OK in this context? consider
	// using piggybacking sync or atomicreferenc with compareandset. This is
	// actually effectively immutable/safe publication case. But how to
	// implement it.
	private volatile StreamJitAppManager manager;

	/**
	 * Assigned nodeID of the corresponding {@link StreamNode}.
	 */
	private final int nodeID;

	/**
	 * Recent {@link AppStatus} from the corresponding {@link StreamNode}.
	 */
	private volatile AppStatus appStatus;

	/**
	 * {@link NodeInfo} of the {@link StreamNode} that is mapped to this
	 * {@link StreamNodeAgent}.
	 */
	private volatile NodeInfo nodeInfo;

	/**
	 * Recent {@link SystemInfo} from the corresponding {@link StreamNode}.
	 */
	private volatile SystemInfo systemInfo;

	/**
	 * Stop the communication with corresponding {@link StreamNode}.
	 * {@link Controller} is expected to set this flag and the IO thread
	 * response for that.
	 */
	private AtomicBoolean stopFlag;

	public StreamNodeAgent(int nodeID) {
		this.nodeID = nodeID;
		stopFlag = new AtomicBoolean(false);
		mv = new SNMessageVisitorImpl();
	}

	/**
	 * @return the nodeID
	 */
	public int getNodeID() {
		return nodeID;
	}

	/**
	 * @return the appStatus
	 */
	public AppStatus getAppStatus() {
		return appStatus;
	}

	/**
	 * @param appStatus
	 *            the appStatus to set
	 */
	public void setAppStatus(AppStatus appStatus) {
		this.appStatus = appStatus;
	}

	/**
	 * @return the nodeInfo
	 */
	public NodeInfo getNodeInfo() {
		if (nodeInfo == null) {
			try {
				writeObject(new CTRLRMessageElementHolder(Request.NodeInfo, -1));
			} catch (IOException e) {
				e.printStackTrace();
			}
			// TODO: By any chance, if the IO thread (SNAgentRunner) calls this
			// function then that thread will get blocked at this loop forever.
			// Because that thread is responsible to read the nodeInfo from the
			// StreamNode and set the nodeInfo variable of this class. Need to
			// handle this issue.
			while (nodeInfo == null) {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		return nodeInfo;
	}

	/**
	 * @return the systemInfo
	 */
	public SystemInfo getSystemInfo() {
		return systemInfo;
	}

	/**
	 * Stop the communication with corresponding {@link StreamNode}.
	 */
	public void stopRequest() {
		this.stopFlag.set(true);
	}

	public boolean isStopRequested() {
		return this.stopFlag.get();
	}

	/**
	 * @return Human readable name of the streamNode.
	 */
	public String streamNodeName() {
		if (nodeInfo == null) {
			getNodeInfo();
		}
		return nodeInfo.getHostName();
	}

	/**
	 * Send a object to corresponding {@link StreamNode}. While IO threads
	 * reading the incoming messages from the stream node, controller may send
	 * any messages through this function.
	 * 
	 * @param obj
	 * @throws IOException
	 */
	public abstract void writeObject(Object obj) throws IOException;

	/**
	 * Is still the connection with corresponding {@link StreamNode} is alive?
	 * 
	 * @return
	 */
	public abstract boolean isConnected();

	/**
	 * Returns {@link InetAddress} of the corresponding {@link StreamNode}.
	 * <p>
	 * This method is introduced later ( not at the initial design phase).
	 * Initially, {@link #getNodeInfo()} is used by {@link Controller} to get
	 * the {@link InetAddress} of the {@link StreamNode}s. But, as
	 * InetAddress.getLocalHost().getHostAddress() returns random addresses
	 * based on how OS orders the all available network interfaces in the
	 * system, this method was needed to correctly keep the IP address of the
	 * streamnode.
	 * 
	 * @return {@link InetAddress} of the {@link StreamNode}.
	 */
	public abstract InetAddress getAddress();

	/**
	 * @return appropriate message visitor based on appInstId.
	 */
	public SNMessageVisitor getMv(int appInstId) {
		if (appInstId < 0)
			return mv;
		else if (manager == null)
			throw new IllegalStateException(
					"StreamJitAppManager has not been set");
		AppInstanceManager appInstManager = manager
				.getAppInstManager(appInstId);
		if (appInstManager == null)
			return mv;
		else
			return appInstManager.mv;
	}

	public void registerManager(StreamJitAppManager manager) {
		assert this.manager == null : "StreamJitAppManager has already been set";
		this.manager = manager;
	}

	private class SystemInfoProcessorImpl implements SystemInfoProcessor {

		@Override
		public void process(SystemInfo systemInfo) {
			StreamNodeAgent.this.systemInfo = systemInfo;
		}
	}

	/**
	 * {@link NodeInfoProcessor} at {@link StreamNode} side.
	 * 
	 * @author Sumanan sumanan@mit.edu
	 * @since Aug 11, 2013
	 */
	private class NodeInfoProcessorImpl implements NodeInfoProcessor {

		@Override
		public void process(NodeInfo nodeInf) {
			nodeInfo = nodeInf;
		}
	}

	/**
	 * Use this {@link SNMessageVisitor} to transfer system information. (-1)
	 * will be used for SNMessageElementHolder.appInstId when transferring
	 * system information.
	 * 
	 * @author Sumanan sumanan@mit.edu
	 * @since May 20, 2013
	 */
	private class SNMessageVisitorImpl implements SNMessageVisitor {

		private final NodeInfoProcessor np;

		private final SystemInfoProcessor sp;

		private SNMessageVisitorImpl() {
			np = new NodeInfoProcessorImpl();
			sp = new SystemInfoProcessorImpl();
		}

		@Override
		public void visit(SystemInfo systemInfo) {
			sp.process(systemInfo);
		}

		@Override
		public void visit(NodeInfo nodeInfo) {
			np.process(nodeInfo);
		}

		@Override
		public void visit(Error error) {
			throw new UnsupportedOperationException(
					"StreamNodeAgent's SNMessageVisitor does not process Error."
							+ " AppInstanceManager's SNMessageVisitor must be called.");
		}

		@Override
		public void visit(AppStatus appStatus) {
			throw new UnsupportedOperationException(
					"StreamNodeAgent's SNMessageVisitor does not process AppStatus."
							+ " AppInstanceManager's SNMessageVisitor must be called.");
		}

		@Override
		public void visit(SNDrainElement snDrainElement) {
			throw new UnsupportedOperationException(
					"StreamNodeAgent's SNMessageVisitor does not process SNDrainElement."
							+ " AppInstanceManager's SNMessageVisitor must be called.");
		}

		@Override
		public void visit(SNException snException) {
			throw new UnsupportedOperationException(
					"StreamNodeAgent's SNMessageVisitor does not process SNException."
							+ " AppInstanceManager's SNMessageVisitor must be called.");
		}

		@Override
		public void visit(SNTimeInfo timeInfo) {
			throw new UnsupportedOperationException(
					"StreamNodeAgent's SNMessageVisitor does not process SNTimeInfo."
							+ " AppInstanceManager's SNMessageVisitor must be called.");
		}

		@Override
		public void visit(CompilationInfo compilationInfo) {
			throw new UnsupportedOperationException(
					"StreamNodeAgent's SNMessageVisitor does not process CompilationInfo."
							+ " AppInstanceManager's SNMessageVisitor must be called.");
		}

		@Override
		public void visit(SNProfileElement snProfileElement) {
			throw new UnsupportedOperationException(
					"StreamNodeAgent's SNMessageVisitor does not process SNProfileElement."
							+ " AppInstanceManager's SNMessageVisitor must be called.");
		}
	}
}