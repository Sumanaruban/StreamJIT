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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.common.TimeLogger;
import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.CTRLCompilationInfo;
import edu.mit.streamjit.impl.distributed.common.CTRLRMessageElement.CTRLRMessageElementHolder;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString.ConfigurationProcessor.ConfigType;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString.ConfigurationString1;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.Error.ErrorProcessor;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.Options;
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

	final ConnectionManager conManager;

	final Controller controller;

	private final ErrorProcessor ep;

	private final SNTimeInfoProcessor timeInfoProcessor;

	final MasterProfiler profiler;

	public final AppDrainer appDrainer;

	private volatile AppStatus status;

	private final TimeLogger logger;

	volatile AppInstanceManager prevAIM = null;
	volatile AppInstanceManager curAIM = null;

	final int noOfnodes;

	final EventTimeLogger mLogger;

	public final Reconfigurer reconfigurer;

	public StreamJitAppManager(Controller controller, StreamJitApp<?, ?> app,
			ConnectionManager conManager, TimeLogger logger, Buffer tailBuffer) {
		noOfnodes = controller.getAllNodeIDs().size();
		this.controller = controller;
		this.app = app;
		this.conManager = conManager;
		this.logger = logger;
		this.timeInfoProcessor = new SNTimeInfoProcessorImpl(logger);
		this.status = AppStatus.NOT_STARTED;
		this.ep = new ErrorProcessorImpl();

		appDrainer = new AppDrainer();
		this.mLogger = app.eLogger;
		this.reconfigurer = reconfigurer(tailBuffer);
		setNewApp(); // TODO: Makes IO communication. Find a good calling place.
		profiler = setupProfiler();
	}

	public ErrorProcessor errorProcessor() {
		return ep;
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

	Reconfigurer reconfigurer(Buffer tailBuffer) {
		if (Options.seamlessReconfig) {
			if (app.stateful)
				return new SeamlessStatefulReconfigurer();
			else
				return new SeamlessStatelessReconfigurer(tailBuffer);
		}
		return new PauseResumeReconfigurer(this);
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

	AppInstanceManager createNewAIM(AppInstance appinst) {
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
		curAIM.headTailHandler.tailChannel.reset();
		controller.closeAll();
		// dp.drainer.stop();
		appDrainer.stop();
		reconfigurer.stop();
		System.out.println(String.format("%s: Stopped.", app.name));
	}

	public long getFixedOutputTime(long timeout) throws InterruptedException {
		long time = curAIM.headTailHandler.tailChannel
				.getFixedOutputTime(timeout);
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

	public void drainingFinished(boolean isFinal, AppInstanceManager aim) {
		reconfigurer.drainingFinished(isFinal, aim);
		if (isFinal)
			stop();
	}

	void reset() {
		// 2015-10-26.
		// As exP is moved to AppInstManager, we don't need to reset it.
		// exP.exConInfos = new HashSet<>();
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
	void preCompilation(AppInstanceManager currentAim,
			AppInstanceManager previousAim) {
		String jsonStirng = currentAim.dynamicCfg(connectionsInUse());
		ImmutableMap<Integer, DrainData> drainDataMap;
		if (previousAim == null)
			drainDataMap = ImmutableMap.of();
		else
			drainDataMap = previousAim.appInst.getDrainData();
		System.out.println("drainDataMap.size() = " + drainDataMap.size());
		logger.compilationStarted();
		app.eLogger.bEvent("compilation");
		for (int nodeID : controller.getAllNodeIDs()) {
			ConfigurationString json = new ConfigurationString1(jsonStirng,
					ConfigType.DYNAMIC, drainDataMap.get(nodeID));
			controller.send(nodeID, new CTRLRMessageElementHolder(json,
					currentAim.appInst.id));
		}
	}

	private void preCompilation(AppInstanceManager currentAim,
			ImmutableMap<Token, Integer> drainDataSize) {
		String jsonStirng = currentAim.dynamicCfg(connectionsInUse());
		logger.compilationStarted();
		app.eLogger.bEvent("compilation");
		for (int nodeID : controller.getAllNodeIDs()) {
			ConfigurationString json = new ConfigurationString.ConfigurationString2(
					jsonStirng, ConfigType.DYNAMIC, drainDataSize);
			controller.send(nodeID, new CTRLRMessageElementHolder(json,
					currentAim.appInst.id));
		}
	}

	private Collection<ConnectionInfo> connectionsInUse() {
		Collection<ConnectionInfo> connectionsInUse = null;
		if (prevAIM != null && prevAIM.conInfoMap != null)
			connectionsInUse = prevAIM.conInfoMap.values();
		return connectionsInUse;
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

		public boolean drainFinal(Boolean isFinal) {
			// TODO: seamless
			// Need to drain newAIM also. drainer.drainFinal() method is
			// blocking. Need to to make this unblocking.
			return curAIM.drainer.drainFinal(isFinal);
		}
	}

	public interface Reconfigurer {
		/**
		 * @param appinst
		 * @return <ol>
		 *         <li>-0: Reconfiguration is successful.
		 *         <li>-1: Intermediate draining has failed.
		 *         <li>-2: Compilation has failed.
		 */
		public int reconfigure(AppInstance appinst);

		/**
		 * Type of starter required at StreamNode side.
		 * 
		 * @return
		 */
		public int starterType();

		public void drainingFinished(boolean isFinal, AppInstanceManager aim);

		public void stop();
	}

	private class SeamlessStatelessReconfigurer implements Reconfigurer {

		private final TailBufferMerger tailMerger;

		private final Thread tailMergerThread;

		SeamlessStatelessReconfigurer(Buffer tailBuffer) {
			tailMerger = new TailBufferMergerStateless(tailBuffer);
			tailMergerThread = createAndStartTailMergerThread();
		}

		public int reconfigure(AppInstance appinst) {
			System.out.println("SeamlessStatelessReconfigurer...");
			AppInstanceManager aim = createNewAIM(appinst);
			reset();
			preCompilation(aim, prevAIM);
			aim.headTailHandler.setupHeadTail(bufferMap(aim.appInstId()), aim);
			boolean isCompiled = aim.postCompilation();

			if (isCompiled) {
				startInit(aim);
				aim.start();
				mLogger.bEvent("intermediateDraining");
				boolean intermediateDraining = intermediateDraining(prevAIM);
				mLogger.eEvent("intermediateDraining");
				if (!intermediateDraining)
					new IllegalStateException(
							"IntermediateDraining of prevAIM failed.")
							.printStackTrace();
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

		ImmutableMap<Token, Buffer> bufferMap(int appInstId) {
			ImmutableMap.Builder<Token, Buffer> builder = ImmutableMap
					.builder();
			builder.put(app.headToken, app.bufferMap.get(app.headToken));
			// TODO: skipCount = 0.
			builder.put(app.tailToken, tailMerger.registerAppInst(appInstId, 0));
			return builder.build();
		}

		/**
		 * Start the execution of the StreamJit application.
		 */
		private void startInit(AppInstanceManager aim) {
			aim.headTailHandler.startHead();
			aim.headTailHandler.startTail();
			aim.runInitSchedule();
		}

		@Override
		public int starterType() {
			return 2;
		}

		@Override
		public void drainingFinished(boolean isFinal, AppInstanceManager aim) {
			if (!isFinal) {
				tailMerger.switchBuf();
				tailMerger.unregisterAppInst(aim.appInstId());
			}
		}

		@Override
		public void stop() {
			tailMerger.stop();
			try {
				tailMergerThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		private Thread createAndStartTailMergerThread() {
			Thread t = new Thread(tailMerger.getRunnable(),
					"TailBufferMergerStateless");
			t.start();
			return t;
		}
	}

	private class SeamlessStatefulReconfigurer implements Reconfigurer {
		public int reconfigure(AppInstance appinst) {
			System.out.println("SeamlessStatefulReconfigurer...");
			AppInstanceManager aim = createNewAIM(appinst);
			reset();
			preCompilation(aim, drainDataSize1());
			aim.headTailHandler.setupHeadTail(app.bufferMap, aim);
			boolean isCompiled = aim.postCompilation();

			if (isCompiled) {
				if (prevAIM != null) {
					mLogger.bEvent("intermediateDraining");
					boolean intermediateDraining = intermediateDraining(prevAIM);
					mLogger.eEvent("intermediateDraining");
					if (!intermediateDraining)
						return 1;
					// TODO : Should send node specific DrainData. Don't send
					// the full drain data to all node.
					CTRLCompilationInfo initialState = new CTRLCompilationInfo.InitialState(
							prevAIM.appInst.drainData);
					controller.sendToAll(new CTRLRMessageElementHolder(
							initialState, aim.appInst.id));
				}
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
			if (aim.isRunning) {
				aim.requestDDsizes();
				return 0;
			} else
				return 2;
		}
		/**
		 * Start the execution of the StreamJit application.
		 */
		private void start(AppInstanceManager aim) {
			aim.headTailHandler.startHead();
			aim.headTailHandler.startTail();
			aim.start();
		}

		@Override
		public int starterType() {
			return 1;
		}

		@Override
		public void drainingFinished(boolean isFinal, AppInstanceManager aim) {
		}

		@Override
		public void stop() {
		}

		ImmutableMap<Token, Integer> drainDataSize1() {
			if (prevAIM == null)
				return null;
			return prevAIM.getDDsizes();
		}

		ImmutableMap<Token, Integer> drainDataSize() {
			if (prevAIM == null)
				return null;
			if (prevAIM.appInst.drainData == null)
				return null;
			ImmutableMap.Builder<Token, Integer> sizeBuilder = ImmutableMap
					.builder();
			for (Map.Entry<Token, ImmutableList<Object>> en : prevAIM.appInst.drainData
					.getData().entrySet()) {
				// System.out.println(en.getKey() + "=" + en.getValue().size());
				sizeBuilder.put(en.getKey(), en.getValue().size());
			}
			return sizeBuilder.build();
		}
	}
}
