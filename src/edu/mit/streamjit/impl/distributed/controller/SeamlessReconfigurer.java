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
import edu.mit.streamjit.impl.distributed.controller.HT.TailBufferMergerPauseResume;
import edu.mit.streamjit.impl.distributed.controller.HT.TailBufferMergerStateless;
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

	private final TailBufferMerger tailMerger;

	private final Thread tailMergerThread;

	private final EventTimeLogger reconfigEvntLogger;

	SeamlessReconfigurer(StreamJitAppManager streamJitAppManager,
			Buffer tailBuffer, boolean needSeamlessTailMerger) {
		this.appManager = streamJitAppManager;
		this.reconfigEvntLogger = new EventTimeLogger.FileEventTimeLogger(
				appManager.app.name, "Reconfigurer", false,
				Options.throughputMeasurementPeriod >= 1000);
		tailMerger = tailMerger(tailBuffer, needSeamlessTailMerger);
		tailMergerThread = createAndStartTailMergerThread(needSeamlessTailMerger);
	}

	@Override
	public void drainingFinished(boolean isFinal, AppInstanceManager aim) {
		if (!isFinal) {
			tailMerger.startMerge();
			tailMerger.unregisterAppInst(aim.appInstId());
		}
		event("F-" + aim.appInstId());
	}

	private TailBufferMerger tailMerger(Buffer tailBuffer,
			boolean needSeamlessTailMerger) {
		if (needSeamlessTailMerger)
			return new TailBufferMergerStateless(tailBuffer);
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

	ImmutableMap<Token, Buffer> bufferMap(int appInstId, int skipCount) {
		ImmutableMap.Builder<Token, Buffer> builder = ImmutableMap.builder();
		builder.put(appManager.app.headToken,
				appManager.app.bufferMap.get(appManager.app.headToken));
		builder.put(appManager.app.tailToken,
				tailMerger.registerAppInst(appInstId, skipCount));
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
			super(streamJitAppManager, tailBuffer, true);
		}

		public int reconfigure(AppInstance appinst) {
			System.out.println("SeamlessStatefulReconfigurer...");
			event("Cfg" + appinst.id);
			AppInstanceManager aim = appManager.createNewAIM(appinst);
			appManager.reset();
			appManager.preCompilation(aim, drainDataSize1());
			aim.headTailHandler.setupHeadTail(
					bufferMap(aim.appInstId(), skipCount()), aim, true);
			boolean isCompiled = aim.postCompilation();
			if (appManager.prevAIM != null) {
				HeadChannelSeamless prevHeadChnl = appManager.prevAIM.headTailHandler
						.headChannelSeamless();
				HeadChannelSeamless curHeadChnl = appManager.curAIM.headTailHandler
						.headChannelSeamless();
				curHeadChnl.duplicationEnabled();
				prevHeadChnl.reqStateDuplicateAndStop(curHeadChnl);
			}

			if (isCompiled) {
				aim.startChannels();
				if (appManager.prevAIM != null) {
					// TODO : Should send node specific DrainData. Don't send
					// the full drain data to all node.
					CTRLCompilationInfo initialState = new CTRLCompilationInfo.InitialState(
							appManager.prevAIM.getState());
					appManager.controller
							.sendToAll(new CTRLRMessageElementHolder(
									initialState, aim.appInst.id));
				}
				start(aim);
				event("S-" + appinst.id);
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
				aim.requestDDsizes();
				return 0;
			} else
				return 2;
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
			super(streamJitAppManager, tailBuffer, true);
		}

		public int reconfigure(AppInstance appinst) {
			System.out.println("SeamlessStatelessReconfigurer...");
			event("Cfg" + appinst.id);
			AppInstanceManager aim = appManager.createNewAIM(appinst);
			appManager.reset();
			appManager.preCompilation(aim, appManager.prevAIM);
			aim.headTailHandler.setupHeadTail(
					bufferMap(aim.appInstId(), skipCount()), aim, true);
			boolean isCompiled = aim.postCompilation();
			if (appManager.prevAIM != null) {
				HeadChannelSeamless prevHeadChnl = appManager.prevAIM.headTailHandler
						.headChannelSeamless();
				HeadChannelSeamless curHeadChnl = appManager.curAIM.headTailHandler
						.headChannelSeamless();
				curHeadChnl.duplicationEnabled();
				prevHeadChnl.duplicateAndStop(
						HeadChannelSeamless.duplicationFiring()
								* appManager.prevAIM.graphSchedule().steadyIn,
						curHeadChnl);
			}

			if (isCompiled) {
				startInit(aim);
				aim.start();
				event("S-" + appinst.id);
			} else {
				aim.drainingFinished(false);
			}

			if (appManager.profiler != null) {
				String cfgPrefix = ConfigurationUtils.getConfigPrefix(appinst
						.getConfiguration());
				appManager.profiler.logger().newConfiguration(cfgPrefix);
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
		private void startInit(AppInstanceManager aim) {
			aim.startChannels();
			aim.runInitSchedule();
		}

		@Override
		public int starterType() {
			return 2;
		}
	}

}
