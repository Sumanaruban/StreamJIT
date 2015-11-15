package edu.mit.streamjit.impl.distributed;

import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.distributed.StreamJitAppManager.Reconfigurer;
import edu.mit.streamjit.impl.distributed.common.CTRLCompilationInfo;
import edu.mit.streamjit.impl.distributed.common.CTRLRMessageElement.CTRLRMessageElementHolder;
import edu.mit.streamjit.impl.distributed.common.Utils;
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

	protected final EventTimeLogger mLogger;

	private final TailBufferMerger tailMerger;

	private final Thread tailMergerThread;

	SeamlessReconfigurer(StreamJitAppManager streamJitAppManager,
			Buffer tailBuffer, boolean needSeamlessTailMerger) {
		this.appManager = streamJitAppManager;
		this.mLogger = appManager.mLogger;
		tailMerger = tailMerger(tailBuffer, needSeamlessTailMerger);
		tailMergerThread = createAndStartTailMergerThread(needSeamlessTailMerger);
	}

	@Override
	public void drainingFinished(boolean isFinal, AppInstanceManager aim) {
		if (!isFinal) {
			tailMerger.switchBuf();
			tailMerger.unregisterAppInst(aim.appInstId());
		}
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

	ImmutableMap<Token, Buffer> bufferMap(int appInstId) {
		ImmutableMap.Builder<Token, Buffer> builder = ImmutableMap.builder();
		builder.put(appManager.app.headToken,
				appManager.app.bufferMap.get(appManager.app.headToken));
		// TODO: skipCount = 0.
		builder.put(appManager.app.tailToken,
				tailMerger.registerAppInst(appInstId, 0));
		return builder.build();
	}

	static class SeamlessStatefulReconfigurer extends SeamlessReconfigurer {

		SeamlessStatefulReconfigurer(StreamJitAppManager streamJitAppManager,
				Buffer tailBuffer) {
			super(streamJitAppManager, tailBuffer, false);
		}

		public int reconfigure(AppInstance appinst) {
			System.out.println("SeamlessStatefulReconfigurer...");
			AppInstanceManager aim = appManager.createNewAIM(appinst);
			appManager.reset();
			appManager.preCompilation(aim, drainDataSize1());
			aim.headTailHandler.setupHeadTail(bufferMap(aim.appInstId()), aim);
			boolean isCompiled = aim.postCompilation();

			if (isCompiled) {
				aim.startChannels();
				if (appManager.prevAIM != null) {
					mLogger.bEvent("intermediateDraining");
					boolean intermediateDraining = appManager
							.intermediateDraining(appManager.prevAIM);
					mLogger.eEvent("intermediateDraining");
					if (!intermediateDraining)
						return 1;
					// TODO : Should send node specific DrainData. Don't send
					// the full drain data to all node.
					CTRLCompilationInfo initialState = new CTRLCompilationInfo.InitialState(
							appManager.prevAIM.appInst.drainData);
					appManager.controller
							.sendToAll(new CTRLRMessageElementHolder(
									initialState, aim.appInst.id));
				}
				start(aim);
			} else {
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
			AppInstanceManager aim = appManager.createNewAIM(appinst);
			appManager.reset();
			appManager.preCompilation(aim, appManager.prevAIM);
			aim.headTailHandler.setupHeadTail(bufferMap(aim.appInstId()), aim);
			boolean isCompiled = aim.postCompilation();

			if (isCompiled) {
				startInit(aim);
				aim.start();
				mLogger.bEvent("intermediateDraining");
				boolean intermediateDraining = appManager
						.intermediateDraining(appManager.prevAIM);
				mLogger.eEvent("intermediateDraining");
				if (!intermediateDraining)
					new IllegalStateException(
							"IntermediateDraining of prevAIM failed.")
							.printStackTrace();
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
