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

import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.common.TimeLogger;
import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.CTRLRMessageElement;
import edu.mit.streamjit.impl.distributed.common.CTRLRMessageElement.CTRLRMessageElementHolder;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString.ConfigurationProcessor.ConfigType;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.Error.ErrorProcessor;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.MiscCtrlElements.NewConInfo;
import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.impl.distributed.common.SNException;
import edu.mit.streamjit.impl.distributed.common.SNException.AddressBindException;
import edu.mit.streamjit.impl.distributed.common.SNException.SNExceptionProcessor;
import edu.mit.streamjit.impl.distributed.common.SNTimeInfo.SNTimeInfoProcessor;
import edu.mit.streamjit.impl.distributed.common.SNTimeInfoProcessorImpl;
import edu.mit.streamjit.impl.distributed.common.Utils;
import edu.mit.streamjit.impl.distributed.profiler.MasterProfiler;
import edu.mit.streamjit.impl.distributed.profiler.ProfilerCommand;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;
import edu.mit.streamjit.tuner.EventTimeLogger;
import edu.mit.streamjit.util.ConfigurationUtils;
import edu.mit.streamjit.util.DrainDataUtils;

/**
 * @author Sumanan sumanan@mit.edu
 * @since Oct 30, 2013
 */
public class StreamJitAppManager {

	final StreamJitApp<?, ?> app;

	private Map<Token, ConnectionInfo> conInfoMap;

	private final ConnectionManager conManager;

	final Controller controller;

	private final ErrorProcessor ep;

	private final SNExceptionProcessorImpl exP;

	private final SNTimeInfoProcessor timeInfoProcessor;

	private final MasterProfiler profiler;

	public final AppDrainer appDrainer;

	private volatile AppStatus status;

	private final TimeLogger logger;

	private volatile AppInstanceManager prevAIM = null;
	private volatile AppInstanceManager curAIM = null;

	final int noOfnodes;

	final HeadTailHandler headTailHandler;

	final EventTimeLogger mLogger;

	public final Reconfigurer reconfigurer;

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

		appDrainer = new AppDrainer();
		headTailHandler = new HeadTailHandler(controller, app);
		this.mLogger = app.eLogger;
		this.reconfigurer = new PauseResumeReconfigurer();
		setNewApp(); // TODO: Makes IO communication. Find a good calling place.
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

	private void setNewApp() {
		controller.registerManager(this);
		Configuration.Builder builder = app.getStaticConfiguration();
		builder.addParameter(new IntParameter(GlobalConstants.StarterType, 1,
				2, reconfigurer.starterType()));
		controller.newApp(builder);
	}

	public AppInstanceManager getAppInstManager(int appInstId) {
		if (curAIM.appInstId() == appInstId)
			return curAIM;
		else if (prevAIM != null && prevAIM.appInstId() == appInstId)
			return prevAIM;
		else
			throw new IllegalStateException(String.format(
					"No AppInstanceManager with ID=%d exists", appInstId));
	}

	public MasterProfiler getProfiler() {
		return profiler;
	}

	public AppStatus getStatus() {
		return status;
	}

	public boolean isRunning() {
		return curAIM.isRunning;
	}

	private AppInstanceManager createNewAIM(AppInstance appinst) {
		if (prevAIM != null && prevAIM.isRunning)
			throw new IllegalStateException(
					"Couldn't create a new AIM as already two AppInstances are running. Drain the current AppInstance first.");

		prevAIM = curAIM;
		curAIM = new AppInstanceManager(appinst, logger, this);
		return curAIM;
	}

	// TODO:seamless.
	/*
	 * public void setDrainer(AbstractDrainer drainer) { assert dp == null :
	 * "SNDrainProcessor has already been set"; this.dp = new
	 * SNDrainProcessorImpl(drainer); }
	 */

	public void stop() {
		this.status = AppStatus.STOPPED;
		headTailHandler.tailChannel.reset();
		controller.closeAll();
		// dp.drainer.stop();
		appDrainer.stop();
	}

	public long getFixedOutputTime(long timeout) throws InterruptedException {
		long time = headTailHandler.tailChannel.getFixedOutputTime(timeout);
		if (curAIM.apStsPro.error) {
			return -1l;
		}
		return time;
	}

	/**
	 * Performs intermediate draining.
	 * 
	 * @return <code>true</code> iff the draining is success or the application
	 *         is not running currently.
	 */
	public boolean intermediateDraining(AppInstanceManager aim) {
		if (aim == null)
			return true;

		if (aim.isRunning) {
			boolean ret = aim.drainer.drainIntermediate();
			if (Options.useDrainData && Options.dumpDrainData) {
				String cfgPrefix = ConfigurationUtils
						.getConfigPrefix(aim.appInst.configuration);
				DrainData dd = aim.appInst.drainData;
				DrainDataUtils.dumpDrainData(dd, app.name, cfgPrefix);
			}
			return ret;
		} else
			return true;
	}

	public void drainingFinished(boolean isFinal) {
		headTailHandler.waitToStopHead();
		headTailHandler.stopTail(isFinal);
		headTailHandler.waitToStopTail();
		if (isFinal)
			stop();

		long time = app.eLogger.eEvent("draining");
		System.out.println("Draining time is " + time + " milli seconds");
	}

	public void drainingStarted(boolean isFinal) {
		app.eLogger.bEvent("draining");
		headTailHandler.stopHead(isFinal);
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
	 * Performs the steps that need to be done in order to create new blobs at
	 * stream nodes side. Specifically, sends new configuration along with drain
	 * data to stream nodes for compilation.
	 * 
	 * @param appinst
	 */
	private void preCompilation(AppInstanceManager aim) {
		Configuration.Builder builder = aim.appInst.getDynamicConfiguration();
		conInfoMap = conManager.conInfoMap(aim.appInst.getConfiguration(),
				aim.appInst.partitionsMachineMap, app.source, app.sink);
		builder.putExtraData(GlobalConstants.CONINFOMAP, conInfoMap);
		Configuration cfg = builder.build();
		String jsonStirng = cfg.toJson();
		ImmutableMap<Integer, DrainData> drainDataMap = aim.appInst
				.getDrainData();
		logger.compilationStarted();
		app.eLogger.bEvent("compilation");
		for (int nodeID : controller.getAllNodeIDs()) {
			ConfigurationString json = new ConfigurationString1(jsonStirng,
					ConfigType.DYNAMIC, drainDataMap.get(nodeID));
			controller.send(nodeID, new CTRLRMessageElementHolder(json,
					aim.appInst.id));
		}
	}

	/**
	 * Performs the steps that need to be done after the blobs are created.
	 * Specifically, sends deadlock free buffer sizes.
	 * 
	 * @return <code>true</code> iff the compilation process is success.
	 */
	private boolean postCompilation(AppInstanceManager appInstManager) {
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
			// TODO: seamless
			// Need to drain newAIM also. drainer.drainFinal() method is
			// blocking. Need to to make this unblocking.
			return curAIM.drainer.drainFinal(isSemeFinal);
		}
	}

	public interface Reconfigurer {
		/**
		 * @param multiplier
		 * @param appinst
		 * @return <ol>
		 *         <li>-0: Reconfiguration is successful.
		 *         <li>-1: Intermediate draining has failed.
		 *         <li>-2: Compilation has failed.
		 */
		public int reconfigure(int multiplier, AppInstance appinst);

		/**
		 * Type of starter required at StreamNode side.
		 * 
		 * @return
		 */
		public int starterType();
	}

	private class PauseResumeReconfigurer implements Reconfigurer {

		public int reconfigure(int multiplier, AppInstance appinst) {
			mLogger.bEvent("intermediateDraining");
			boolean intermediateDraining = intermediateDraining(curAIM);
			mLogger.eEvent("intermediateDraining");
			if (!intermediateDraining)
				return 1;

			AppInstanceManager aim = createNewAIM(appinst);
			reset();
			preCompilation(aim);
			headTailHandler.setupHeadTail(conInfoMap, app.bufferMap,
					multiplier, appinst);
			boolean isCompiled = postCompilation(aim);

			if (isCompiled) {
				start(aim);
			} else {
				aim.drainingFinished(false);
			}

			if (profiler != null) {
				String cfgPrefix = ConfigurationUtils.getConfigPrefix(appinst
						.getConfiguration());
				profiler.logger().newConfiguration(cfgPrefix);
			}
			Utils.printMemoryStatus();
			if (aim.isRunning)
				return 0;
			else
				return 2;
		}

		/**
		 * Start the execution of the StreamJit application.
		 */
		private void start(AppInstanceManager aim) {
			headTailHandler.startHead();
			headTailHandler.startTail();
			aim.start();
		}

		@Override
		public int starterType() {
			return 1;
		}
	}
}
