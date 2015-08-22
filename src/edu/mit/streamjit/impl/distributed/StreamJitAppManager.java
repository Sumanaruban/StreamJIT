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
package edu.mit.streamjit.impl.distributed;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.TimeLogger;
import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.AsyncTCPConnection.AsyncTCPConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryInputChannel;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryOutputChannel;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.DrainType;
import edu.mit.streamjit.impl.distributed.common.CTRLRMessageElement;
import edu.mit.streamjit.impl.distributed.common.CTRLRMessageElement.CTRLRMessageElementHolder;
import edu.mit.streamjit.impl.distributed.common.Command;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString.ConfigurationProcessor.ConfigType;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionProvider;
import edu.mit.streamjit.impl.distributed.common.Error.ErrorProcessor;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.MiscCtrlElements.NewConInfo;
import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.impl.distributed.common.SNException;
import edu.mit.streamjit.impl.distributed.common.SNException.AddressBindException;
import edu.mit.streamjit.impl.distributed.common.SNException.SNExceptionProcessor;
import edu.mit.streamjit.impl.distributed.common.SNTimeInfo.SNTimeInfoProcessor;
import edu.mit.streamjit.impl.distributed.common.SNTimeInfoProcessorImpl;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.Utils;
import edu.mit.streamjit.impl.distributed.profiler.MasterProfiler;
import edu.mit.streamjit.impl.distributed.profiler.ProfilerCommand;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;
import edu.mit.streamjit.util.ConfigurationUtils;
import edu.mit.streamjit.util.DrainDataUtils;

/**
 * @author Sumanan sumanan@mit.edu
 * @since Oct 30, 2013
 */
public class StreamJitAppManager {

	private final StreamJitApp<?, ?> app;

	private Map<Token, ConnectionInfo> conInfoMap;

	private final ConnectionManager conManager;

	final Controller controller;

	private final ErrorProcessor ep;

	private final SNExceptionProcessorImpl exP;

	private final SNTimeInfoProcessor timeInfoProcessor;

	private final MasterProfiler profiler;

	public final AppDrainer appDrainer;

	/**
	 * A {@link BoundaryOutputChannel} for the head of the stream graph. If the
	 * first {@link Worker} happened to fall outside the {@link Controller}, we
	 * need to push the {@link CompiledStream}.offer() data to the first
	 * {@link Worker} of the streamgraph.
	 */
	private BoundaryOutputChannel headChannel;

	private Thread headThread;

	private final Token headToken;

	/**
	 * A {@link BoundaryInputChannel} for the tail of the whole stream graph. If
	 * the sink {@link Worker} happened to fall outside the {@link Controller},
	 * we need to pull the sink's output in to the {@link Controller} in order
	 * to make {@link CompiledStream} .pull() to work.
	 */
	TailChannel tailChannel;

	private Thread tailThread;

	private final Token tailToken;

	private boolean isRunning;

	private volatile AppStatus status;

	/**
	 * [2014-03-15] Just to measure the draining time
	 */
	AtomicReference<Stopwatch> stopwatchRef = new AtomicReference<>();

	private final TimeLogger logger;

	AppInstanceManager appInstManager;

	final int noOfnodes;

	public StreamJitAppManager(Controller controller, StreamJitApp<?, ?> app,
			ConnectionManager conManager, TimeLogger logger) {
		noOfnodes = controller.getAllNodeIDs().size();
		this.controller = controller;
		this.app = app;
		this.conManager = conManager;
		this.logger = logger;
		this.timeInfoProcessor = new SNTimeInfoProcessorImpl(logger);
		this.status = AppStatus.NOT_STARTED;
		this.exP = new SNExceptionProcessorImpl();
		this.ep = new ErrorProcessorImpl();
		controller.registerManager(this);
		controller.newApp(app.getStaticConfiguration()); // TODO: Find a
															// good calling
															// place.
		isRunning = false;
		appDrainer = new AppDrainer();
		headToken = Token.createOverallInputToken(app.source);
		tailToken = Token.createOverallOutputToken(app.sink);
		profiler = setupProfiler();
	}

	public ErrorProcessor errorProcessor() {
		return ep;
	}

	public SNExceptionProcessor exceptionProcessor() {
		return exP;
	}

	public SNTimeInfoProcessor timeInfoProcessor() {
		return timeInfoProcessor;
	}

	public AppInstanceManager getAppInstManager(int appInstId) {
		return appInstManager;
	}

	public MasterProfiler getProfiler() {
		return profiler;
	}

	public AppStatus getStatus() {
		return status;
	}

	public boolean isRunning() {
		return isRunning;
	}

	public boolean reconfigure(int multiplier, AppInstance appinst) {
		appInstManager = new AppInstanceManager(appinst, logger, this);
		reset();
		preCompilation(appinst);
		setupHeadTail(conInfoMap, app.bufferMap, multiplier, appinst);
		boolean isCompiled = postCompilation();

		if (isCompiled) {
			start(appinst.id);
			isRunning = true;
		} else {
			drainingFinished(false);
			isRunning = false;
		}

		if (profiler != null) {
			String cfgPrefix = ConfigurationUtils.getConfigPrefix(appinst
					.getConfiguration());
			profiler.logger().newConfiguration(cfgPrefix);
		}

		System.out.println("StraemJit app is running...");
		Utils.printMemoryStatus();
		return isRunning;
	}

	// TODO:seamless.
	/*
	 * public void setDrainer(AbstractDrainer drainer) { assert dp == null :
	 * "SNDrainProcessor has already been set"; this.dp = new
	 * SNDrainProcessorImpl(drainer); }
	 */

	public void stop() {
		this.status = AppStatus.STOPPED;
		tailChannel.reset();
		controller.closeAll();
		// dp.drainer.stop();
		appDrainer.stop();
	}

	public long getFixedOutputTime(long timeout) throws InterruptedException {
		long time = tailChannel.getFixedOutputTime(timeout);
		if (appInstManager.apStsPro.error) {
			return -1l;
		}
		return time;
	}

	/**
	 * Performs intermediate draining.
	 * 
	 * @return <code>true</code> iff the draining is success or the application
	 *         is not running currently.
	 * @throws InterruptedException
	 */
	public boolean intermediateDraining() throws InterruptedException {
		if (isRunning()) {
			boolean ret = appInstManager.drainer.drainIntermediate();
			if (Options.useDrainData && Options.dumpDrainData) {
				String cfgPrefix = ConfigurationUtils
						.getConfigPrefix(appInstManager.appInst.configuration);
				DrainData dd = appInstManager.appInst.drainData;
				DrainDataUtils.dumpDrainData(dd, app.name, cfgPrefix);
			}
			return ret;
		} else
			return true;
	}

	public void drainingFinished(boolean isFinal) {
		System.out.println("App Manager : Draining Finished...");

		if (headChannel != null) {
			try {
				headThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		if (tailChannel != null) {
			if (Options.useDrainData)
				if (isFinal)
					tailChannel.stop(DrainType.FINAL);
				else
					tailChannel.stop(DrainType.INTERMEDIATE);
			else
				tailChannel.stop(DrainType.DISCARD);

			try {
				tailThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		if (isFinal)
			stop();

		isRunning = false;

		Stopwatch sw = stopwatchRef.get();
		if (sw != null && sw.isRunning()) {
			sw.stop();
			long time = sw.elapsed(TimeUnit.MILLISECONDS);
			System.out.println("Draining time is " + time + " milli seconds");
		}
	}

	public void drainingStarted(boolean isFinal) {
		stopwatchRef.set(Stopwatch.createStarted());
		if (headChannel != null) {
			headChannel.stop(isFinal);
			// [2014-03-16] Moved to drainingFinished. In any case if headThread
			// blocked at tcp write, draining will also blocked.
			// try {
			// headThread.join();
			// } catch (InterruptedException e) {
			// e.printStackTrace();
			// }
		}
	}

	private void reset() {
		exP.exConInfos = new HashSet<>();
		// No need to do the following resets as we create new appInstManager at
		// every reconfiguration.
		// appInstManager.apStsPro.reset();
		// appInstManager.ciP.reset();
	}

	private MasterProfiler setupProfiler() {
		MasterProfiler p = null;
		if (Options.needProfiler) {
			p = new MasterProfiler(app.name);
			controller.sendToAll(new CTRLRMessageElementHolder(
					ProfilerCommand.START, -1));
		}
		return p;
	}

	/**
	 * Setup the headchannel and tailchannel.
	 * 
	 * @param cfg
	 * @param bufferMap
	 */
	private void setupHeadTail(Map<Token, ConnectionInfo> conInfoMap,
			ImmutableMap<Token, Buffer> bufferMap, int multiplier,
			AppInstance appinst) {

		ConnectionInfo headconInfo = conInfoMap.get(headToken);
		assert headconInfo != null : "No head connection info exists in conInfoMap";
		assert headconInfo.getSrcID() == controller.controllerNodeID
				|| headconInfo.getDstID() == controller.controllerNodeID : "Head channel should start from the controller. "
				+ headconInfo;

		if (!bufferMap.containsKey(headToken))
			throw new IllegalArgumentException(
					"No head buffer in the passed bufferMap.");

		if (headconInfo instanceof TCPConnectionInfo)
			headChannel = new HeadChannel.TCPHeadChannel(
					bufferMap.get(headToken), controller.getConProvider(),
					headconInfo, "headChannel - " + headToken.toString(), 0,
					app.eLogger);
		else if (headconInfo instanceof AsyncTCPConnectionInfo)
			headChannel = new HeadChannel.AsyncHeadChannel(
					bufferMap.get(headToken), controller.getConProvider(),
					headconInfo, "headChannel - " + headToken.toString(), 0,
					app.eLogger);
		else
			throw new IllegalStateException("Head ConnectionInfo doesn't match");

		ConnectionInfo tailconInfo = conInfoMap.get(tailToken);
		assert tailconInfo != null : "No tail connection info exists in conInfoMap";
		assert tailconInfo.getSrcID() == controller.controllerNodeID
				|| tailconInfo.getDstID() == controller.controllerNodeID : "Tail channel should ends at the controller. "
				+ tailconInfo;

		if (!bufferMap.containsKey(tailToken))
			throw new IllegalArgumentException(
					"No tail buffer in the passed bufferMap.");

		int skipCount = Math.max(Options.outputCount, multiplier * 5);
		tailChannel = tailChannel(bufferMap.get(tailToken), tailconInfo,
				skipCount, appinst);
	}

	private TailChannel tailChannel(Buffer buffer, ConnectionInfo conInfo,
			int skipCount, AppInstance appinst) {
		String appName = app.name;
		int steadyCount = Options.outputCount;
		int debugLevel = 0;
		String bufferTokenName = "tailChannel - " + tailToken.toString();
		ConnectionProvider conProvider = controller.getConProvider();
		String cfgPrefix = ConfigurationUtils.getConfigPrefix(appinst
				.getConfiguration());
		switch (Options.tailChannel) {
			case 1 :
				return new TailChannels.BlockingTailChannel1(buffer,
						conProvider, conInfo, bufferTokenName, debugLevel,
						skipCount, steadyCount, appName, cfgPrefix, app.eLogger);
			case 3 :
				return new TailChannels.BlockingTailChannel3(buffer,
						conProvider, conInfo, bufferTokenName, debugLevel,
						skipCount, steadyCount, appName, cfgPrefix, app.eLogger);
			default :
				return new TailChannels.BlockingTailChannel2(buffer,
						conProvider, conInfo, bufferTokenName, debugLevel,
						skipCount, steadyCount, appName, cfgPrefix, app.eLogger);
		}
	}

	/**
	 * Start the execution of the StreamJit application.
	 */
	private void start(AppInstanceManager aim) {
		if (headChannel != null) {
			headThread = new Thread(headChannel.getRunnable(),
					headChannel.name());
			headThread.start();
		}

		aim.start();

		if (tailChannel != null) {
			tailThread = new Thread(tailChannel.getRunnable(),
					tailChannel.name());
			tailThread.start();
		}
	}

	/**
	 * Performs the steps that need to be done in order to create new blobs at
	 * stream nodes side. Specifically, sends new configuration along with drain
	 * data to stream nodes for compilation.
	 * 
	 * @param appinst
	 */
	private void preCompilation(AppInstance appinst) {
		Configuration.Builder builder = Configuration.builder(appinst
				.getDynamicConfiguration());
		conInfoMap = conManager.conInfoMap(appinst.getConfiguration(),
				appinst.partitionsMachineMap, app.source, app.sink);
		builder.putExtraData(GlobalConstants.CONINFOMAP, conInfoMap);
		builder.putExtraData("appInstId", appinst.id);
		Configuration cfg = builder.build();
		String jsonStirng = cfg.toJson();
		ImmutableMap<Integer, DrainData> drainDataMap = appinst.getDrainData();
		logger.compilationStarted();
		app.eLogger.bEvent("compilation");
		for (int nodeID : controller.getAllNodeIDs()) {
			ConfigurationString json = new ConfigurationString(jsonStirng,
					ConfigType.DYNAMIC, drainDataMap.get(nodeID));
			controller.send(nodeID, new CTRLRMessageElementHolder(json,
					appinst.id));
		}
	}

	/**
	 * Performs the steps that need to be done after the blobs are created.
	 * Specifically, sends deadlock free buffer sizes.
	 * 
	 * @return <code>true</code> iff the compilation process is success.
	 */
	private boolean postCompilation() {
		appInstManager.sendDeadlockfreeBufSizes();
		boolean isCompiled;
		if (appInstManager.apStsPro.compilationError)
			isCompiled = false;
		else
			isCompiled = appInstManager.apStsPro.waitForCompilation();
		app.eLogger.eEvent("compilation");
		logger.compilationFinished(isCompiled, "");
		return isCompiled;
	}

	/**
	 * {@link ErrorProcessor} at {@link Controller} side.
	 * 
	 * @author Sumanan sumanan@mit.edu
	 * @since Aug 11, 2013
	 */
	private class ErrorProcessorImpl implements ErrorProcessor {

		@Override
		public void processFILE_NOT_FOUND() {
			System.err
					.println("No application jar file in streamNode. Terminating...");
			stop();
		}

		@Override
		public void processWORKER_NOT_FOUND() {
			System.err
					.println("No top level class in the jar file. Terminating...");
			stop();
		}
	}

	private class SNExceptionProcessorImpl implements SNExceptionProcessor {

		private final Object abExLock = new Object();

		private Set<ConnectionInfo> exConInfos;

		private SNExceptionProcessorImpl() {
			exConInfos = new HashSet<>();
		}

		@Override
		public void process(AddressBindException abEx) {
			synchronized (abExLock) {
				if (exConInfos.contains(abEx.conInfo)) {
					System.out
							.println("AddressBindException : Already handled...");
					return;
				}

				Token t = null;
				for (Map.Entry<Token, ConnectionInfo> entry : conInfoMap
						.entrySet()) {
					if (abEx.conInfo.equals(entry.getValue())) {
						t = entry.getKey();
						break;
					}
				}

				if (t == null) {
					throw new IllegalArgumentException(
							"Illegal TCP connection - " + abEx.conInfo);
				}

				ConnectionInfo coninfo = conManager
						.replaceConInfo(abEx.conInfo);

				exConInfos.add(abEx.conInfo);

				CTRLRMessageElement msg = new NewConInfo(coninfo, t);

				// TODO: seamless. correct appInstId must be passed.
				controller.send(coninfo.getSrcID(),
						new CTRLRMessageElementHolder(msg, 1));
				controller.send(coninfo.getDstID(),
						new CTRLRMessageElementHolder(msg, 1));
			}
		}

		@Override
		public void process(SNException ex) {
		}
	}

	public class AppDrainer {

		/**
		 * Latch to block the external thread that calls
		 * {@link CompiledStream#awaitDrained()}.
		 */
		private final CountDownLatch finalLatch;

		private AppDrainer() {
			finalLatch = new CountDownLatch(1);
		}

		/**
		 * @return true iff draining of the stream application is finished. See
		 *         {@link CompiledStream#isDrained()} for more details.
		 */
		public final boolean isDrained() {
			return finalLatch.getCount() == 0;
		}

		/**
		 * See {@link CompiledStream#awaitDrained()} for more details.
		 */
		public final void awaitDrained() throws InterruptedException {
			finalLatch.await();
		}

		/**
		 * See {@link CompiledStream#awaitDrained(long, TimeUnit)} for more
		 * details.
		 */
		public final void awaitDrained(long timeout, TimeUnit unit)
				throws InterruptedException, TimeoutException {
			finalLatch.await(timeout, unit);
		}

		/**
		 * In any case, if the application could not be executed (may be due to
		 * {@link Error}), {@link StreamCompiler} or appropriate class can call
		 * this method to release the main thread.
		 */
		public void stop() {
			// TODO: seamless
			// assert state != DrainerState.INTERMEDIATE :
			// "DrainerState.NODRAINING or DrainerState.FINAL is expected.";
			this.finalLatch.countDown();
		}

		public boolean drainFinal(Boolean isSemeFinal) {
			return appInstManager.drainer.drainFinal(isSemeFinal);
		}
	}
}
