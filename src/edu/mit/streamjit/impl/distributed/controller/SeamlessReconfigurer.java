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
import edu.mit.streamjit.impl.distributed.controller.HT.DynamicTailBufferMerger;
import edu.mit.streamjit.impl.distributed.controller.HT.HeadChannelSeamless;
import edu.mit.streamjit.impl.distributed.controller.HT.StaticTailBufferMerger;
import edu.mit.streamjit.impl.distributed.controller.HT.TailBufferMerger;
import edu.mit.streamjit.impl.distributed.controller.HT.TailBufferMerger.BufferProvider;
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

	protected final boolean adaptiveReconfig;

	SeamlessReconfigurer(StreamJitAppManager streamJitAppManager,
			Buffer tailBuffer, boolean adaptiveReconfig, String name) {
		if (adaptiveReconfig)
			this.name = "Adaptive" + name;
		else
			this.name = name;
		this.appManager = streamJitAppManager;
		this.adaptiveReconfig = adaptiveReconfig;
		this.reconfigEvntLogger = new EventTimeLogger.FileEventTimeLogger(
				appManager.app.name, "Reconfigurer", false,
				Options.throughputMeasurementPeriod >= 1000);
		tailMerger = tailMerger(tailBuffer, adaptiveReconfig);
		bufProvider = tailMerger.bufferProvider();
		tailMergerThread = createAndStartTailMergerThread();
	}

	public int reconfigure(AppInstance appinst) {
		System.out.println(name + "...");
		event("Cfg" + appinst.id);
		AppInstanceManager aim = appManager.createNewAIM(appinst);
		appManager.reset();
		HeadChannelSeamless prevHeadChnl = null;
		if (appManager.prevAIM != null) {
			prevHeadChnl = appManager.prevAIM.headTailHandler
					.headChannelSeamless();
			prevHeadChnl.limitSend(1);
		}
		preCompilation(aim);
		aim.headTailHandler.setupHeadTail(appinst.app.headBuffer,
				bufProvider.newBuffer(), aim, true, tailMerger);
		tailMerger.newAppInst(aim.headTailHandler.headTail(), skipCount());
		boolean isCompiled = aim.postCompilation();

		if (isCompiled) {
			HeadChannelSeamless curHeadChnl = appManager.curAIM.headTailHandler
					.headChannelSeamless();
			if (appManager.prevAIM != null) {
				prevHeadChnl = appManager.prevAIM.headTailHandler
						.headChannelSeamless();
				curHeadChnl.duplicationEnabled();
				connectWithPrevHeadChnl(prevHeadChnl, curHeadChnl);
			}
			aim.startChannels();
			curHeadChnl.waitToStart();
			compiled(aim);
			appinst.app.tp.tpStatistics.cfgStarted(aim.appInstId());
			event("S-" + aim.appInstId());
		} else {
			if (prevHeadChnl != null)
				prevHeadChnl.limitSend(HeadChannelSeamless.fcTimeGap);
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
		aim.appInst.app.tp.tpStatistics.cfgEnded(aim.appInstId());
		event("F-" + aim.appInstId());
	}

	private TailBufferMerger tailMerger(Buffer tailBuffer,
			boolean needDynamicTailMerger) {
		if (needDynamicTailMerger)
			return new DynamicTailBufferMerger(tailBuffer, reconfigEvntLogger,
					true);
		return new StaticTailBufferMerger(tailBuffer, reconfigEvntLogger);
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

	private Thread createAndStartTailMergerThread() {
		Thread t = new Thread(tailMerger.getRunnable(),
				"TailBufferMergerStateless");
		t.start();
		return t;
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
				Buffer tailBuffer, boolean adaptiveReconfig) {
			super(streamJitAppManager, tailBuffer, adaptiveReconfig,
					"SeamlessStatefulReconfigurer");
		}

		protected void preCompilation(AppInstanceManager aim) {
			appManager.preCompilation(aim, drainDataSize1());
		}

		protected void connectWithPrevHeadChnl(
				HeadChannelSeamless prevHeadChnl,
				HeadChannelSeamless curHeadChnl) {
			if (adaptiveReconfig)
				prevHeadChnl.reqStateAndDuplicate(curHeadChnl);
			else {
				prevHeadChnl.reqStateDuplicateAndStop(
						HeadChannelSeamless.duplicationFiring()
								* appManager.prevAIM.graphSchedule().steadyIn,
						curHeadChnl);
			}
		}

		protected void compiled(AppInstanceManager aim) {
			if (appManager.prevAIM != null) {
				// TODO : Should send node specific DrainData. Don't send
				// the full drain data to all node.
				CTRLCompilationInfo initialState = new CTRLCompilationInfo.InitialState(
						appManager.prevAIM.getState());
				appManager.controller.sendToAll(new CTRLRMessageElementHolder(
						initialState, aim.appInst.id));
			}
			aim.start();
		}

		protected void aimRunning(AppInstanceManager aim) {
			aim.requestDDsizes();
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
				Buffer tailBuffer, boolean adaptiveReconfig) {
			super(streamJitAppManager, tailBuffer, adaptiveReconfig,
					"SeamlessStatelessReconfigurer");
		}

		protected void preCompilation(AppInstanceManager aim) {
			appManager.preCompilation(aim, appManager.prevAIM);
		}

		protected void compiled(AppInstanceManager aim) {
			// aim.runInitSchedule();
			aim.start();
		}

		protected void connectWithPrevHeadChnl(
				HeadChannelSeamless prevHeadChnl,
				HeadChannelSeamless curHeadChnl) {
			if (adaptiveReconfig) {
				prevHeadChnl.duplicate(curHeadChnl);
			} else {
				prevHeadChnl.duplicateAndStop(
						HeadChannelSeamless.duplicationFiring()
								* appManager.prevAIM.graphSchedule().steadyIn,
						curHeadChnl);
			}
		}

		protected void aimRunning(AppInstanceManager aim) {
		}

		@Override
		public int starterType() {
			return 1;
		}
	}
}
