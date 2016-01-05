package edu.mit.streamjit.impl.distributed.controller;

import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.distributed.common.CTRLCompilationInfo;
import edu.mit.streamjit.impl.distributed.common.CTRLRMessageElement.CTRLRMessageElementHolder;
import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.impl.distributed.common.Utils;
import edu.mit.streamjit.impl.distributed.controller.StreamJitAppManager.Reconfigurer;
import edu.mit.streamjit.impl.distributed.controller.HT.HeadChannelSeamless;
import edu.mit.streamjit.impl.distributed.controller.HT.TailBufferMerger;
import edu.mit.streamjit.impl.distributed.controller.HT.TailBufferMerger.BufferProvider;
import edu.mit.streamjit.impl.distributed.controller.HT.TailBufferMergerPauseResume;
import edu.mit.streamjit.impl.distributed.controller.HT.TailBufferTwoWayMerger;
import edu.mit.streamjit.tuner.EventTimeLogger;
import edu.mit.streamjit.util.ConfigurationUtils;

/**
 * TODO: This class could be improved in object oriented design aspect.
 * Currently, it distinguishes runs different types of {@link TailBufferMerger}
 * based on a boolean flag.
 * 
 * @author sumanan
 * @since 15 Nov, 2015
 */
public abstract class SeamlessReconfigurer implements Reconfigurer {

	protected final StreamJitAppManager appManager;

	protected final TailBufferMerger tailMerger;

	private final Thread tailMergerThread;

	private final EventTimeLogger reconfigEvntLogger;

	BufferProvider bufProvider;

	protected final String name;

	SeamlessReconfigurer(StreamJitAppManager streamJitAppManager,
			Buffer tailBuffer, boolean needSeamlessTailMerger, String name) {
		this.name = name;
		this.appManager = streamJitAppManager;
		this.reconfigEvntLogger = new EventTimeLogger.FileEventTimeLogger(
				appManager.app.name, "Reconfigurer", false,
				Options.throughputMeasurementPeriod >= 1000);
		tailMerger = tailMerger(tailBuffer, needSeamlessTailMerger);
		bufProvider = tailMerger.bufferProvider();
		tailMergerThread = createAndStartTailMergerThread(needSeamlessTailMerger);
	}

	public int reconfigure(AppInstance appinst) {
		System.out.println(name + "...");
		event("Cfg" + appinst.id);
		AppInstanceManager aim = appManager.createNewAIM(appinst);
		appManager.reset();
		preCompilation(aim);
		aim.headTailHandler.setupHeadTail(bufferMap(aim.appInstId()), aim,
				true, tailMerger);
		tailMerger.newAppInst(aim.headTailHandler.headTail(), skipCount());
		boolean isCompiled = aim.postCompilation();
		if (appManager.prevAIM != null) {
			HeadChannelSeamless prevHeadChnl = appManager.prevAIM.headTailHandler
					.headChannelSeamless();
			HeadChannelSeamless curHeadChnl = appManager.curAIM.headTailHandler
					.headChannelSeamless();
			curHeadChnl.duplicationEnabled();
			connectWithPrevHeadChnl(prevHeadChnl, curHeadChnl);
		}

		if (isCompiled) {
			compiled(aim);
			event("S-" + aim.appInstId());
		} else {
			// TODO : [2015-11-18]
			// This calling causes java.lang.NullPointerException at
			// edu.mit.streamjit.impl.distributed.HeadTailHandler.waitToStopHead(HeadTailHandler.java:167)
			aim.drainingFinished(false);
		}

		if (appManager.profiler != null) {
			String cfgPrefix = ConfigurationUtils.getConfigPrefix(appinst
					.getConfiguration());
			appManager.profiler.logger().newConfiguration(cfgPrefix);
		}
		Utils.printMemoryStatus();
		if (aim.isRunning) {
			aimRunning(aim);
			return 0;
		} else
			return 2;
	}

	protected abstract void aimRunning(AppInstanceManager aim);

	protected abstract void compiled(AppInstanceManager aim);

	protected abstract void connectWithPrevHeadChnl(
			HeadChannelSeamless prevHeadChnl, HeadChannelSeamless curHeadChnl);

	protected abstract void preCompilation(AppInstanceManager aim);

	@Override
	public void drainingFinished(boolean isFinal, AppInstanceManager aim) {
		if (!isFinal) {
			tailMerger.startMerge();
			tailMerger.appInstStopped(aim.appInstId());
		}
		event("F-" + aim.appInstId());
	}

	private TailBufferMerger tailMerger(Buffer tailBuffer,
			boolean needSeamlessTailMerger) {
		if (needSeamlessTailMerger)
			return new TailBufferTwoWayMerger(tailBuffer, reconfigEvntLogger);
		return new TailBufferMergerPauseResume(tailBuffer);
	}

	@Override
	public void stop() {
		tailMerger.stop();
		if (tailMergerThread != null)
			try {
				tailMergerThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	}

	private Thread createAndStartTailMergerThread(boolean needSeamlessTailMerger) {
		if (needSeamlessTailMerger) {
			Thread t = new Thread(tailMerger.getRunnable(),
					"TailBufferMergerStateless");
			t.start();
			return t;
		}
		return null;
	}

	ImmutableMap<Token, Buffer> bufferMap(int appInstId) {
		ImmutableMap.Builder<Token, Buffer> builder = ImmutableMap.builder();
		builder.put(appManager.app.headToken,
				appManager.app.bufferMap.get(appManager.app.headToken));
		builder.put(appManager.app.tailToken, bufProvider.newBuffer());
		return builder.build();
	}

	int skipCount() {
		if (appManager.prevAIM == null)
			return 0;
		return HeadChannelSeamless.duplicationFiring()
				* appManager.prevAIM.graphSchedule().steadyOut;
	}

	protected void event(String eventName) {
		reconfigEvntLogger.logEvent(eventName, 0);
	}

	static class SeamlessStatefulReconfigurer extends SeamlessReconfigurer {

		SeamlessStatefulReconfigurer(StreamJitAppManager streamJitAppManager,
				Buffer tailBuffer) {
			super(streamJitAppManager, tailBuffer, true,
					"SeamlessStatefulReconfigurer");
		}

		protected void preCompilation(AppInstanceManager aim) {
			appManager.preCompilation(aim, drainDataSize1());
		}

		protected void connectWithPrevHeadChnl(
				HeadChannelSeamless prevHeadChnl,
				HeadChannelSeamless curHeadChnl) {
			prevHeadChnl.reqStateDuplicateAndStop(
					HeadChannelSeamless.duplicationFiring()
							* appManager.prevAIM.graphSchedule().steadyIn,
					curHeadChnl);
		}

		protected void compiled(AppInstanceManager aim) {
			aim.startChannels();
			if (appManager.prevAIM != null) {
				// TODO : Should send node specific DrainData. Don't send
				// the full drain data to all node.
				CTRLCompilationInfo initialState = new CTRLCompilationInfo.InitialState(
						appManager.prevAIM.getState());
				appManager.controller.sendToAll(new CTRLRMessageElementHolder(
						initialState, aim.appInst.id));
			}
			start(aim);
		}

		protected void aimRunning(AppInstanceManager aim) {
			aim.requestDDsizes();
		}

		/**
		 * Start the execution of the StreamJit application.
		 */
		private void start(AppInstanceManager aim) {
			aim.start();
		}

		@Override
		public int starterType() {
			return 1;
		}

		ImmutableMap<Token, Integer> drainDataSize1() {
			if (appManager.prevAIM == null)
				return null;
			return appManager.prevAIM.getDDsizes();
		}

		ImmutableMap<Token, Integer> drainDataSize() {
			if (appManager.prevAIM == null)
				return null;
			if (appManager.prevAIM.appInst.drainData == null)
				return null;
			ImmutableMap.Builder<Token, Integer> sizeBuilder = ImmutableMap
					.builder();
			for (Map.Entry<Token, ImmutableList<Object>> en : appManager.prevAIM.appInst.drainData
					.getData().entrySet()) {
				// System.out.println(en.getKey() + "=" + en.getValue().size());
				sizeBuilder.put(en.getKey(), en.getValue().size());
			}
			return sizeBuilder.build();
		}
	}

	static class SeamlessStatelessReconfigurer extends SeamlessReconfigurer {

		SeamlessStatelessReconfigurer(StreamJitAppManager streamJitAppManager,
				Buffer tailBuffer) {
			super(streamJitAppManager, tailBuffer, true,
					"SeamlessStatelessReconfigurer");
		}

		protected void preCompilation(AppInstanceManager aim) {
			appManager.preCompilation(aim, appManager.prevAIM);
		}

		protected void compiled(AppInstanceManager aim) {
			// startInit(aim);
			aim.startChannels();
			aim.start();
		}

		protected void connectWithPrevHeadChnl(
				HeadChannelSeamless prevHeadChnl,
				HeadChannelSeamless curHeadChnl) {
			prevHeadChnl.duplicateAndStop(
					HeadChannelSeamless.duplicationFiring()
							* appManager.prevAIM.graphSchedule().steadyIn,
					curHeadChnl);
		}

		protected void aimRunning(AppInstanceManager aim) {
		}

		/**
		 * Start the execution of the StreamJit application.
		 */
		private void startInit(AppInstanceManager aim) {
			aim.startChannels();
			aim.runInitSchedule();
		}

		@Override
		public int starterType() {
			return 1;
		}
	}
}
