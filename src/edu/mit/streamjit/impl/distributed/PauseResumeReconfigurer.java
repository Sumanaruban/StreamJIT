package edu.mit.streamjit.impl.distributed;

import edu.mit.streamjit.impl.distributed.StreamJitAppManager.Reconfigurer;
import edu.mit.streamjit.impl.distributed.common.Utils;
import edu.mit.streamjit.tuner.EventTimeLogger;
import edu.mit.streamjit.util.ConfigurationUtils;

/**
 * Performs Pause and Resume reconfiguration.
 * <p>
 * This class was an inner class of StreamJitAppManager, and has been moved to
 * new file (here) in order to keep StreamJitAppManager concise. At anypoint we
 * can move this class back to StreamJitAppManager as an inner class.
 * 
 * @author sumanan
 * @since 15 Nov, 2015
 */
class PauseResumeReconfigurer implements Reconfigurer {

	/**
	 * 
	 */
	private final StreamJitAppManager appManager;

	private final EventTimeLogger mLogger;

	/**
	 * @param streamJitAppManager
	 */
	PauseResumeReconfigurer(StreamJitAppManager streamJitAppManager) {
		this.appManager = streamJitAppManager;
		this.mLogger = appManager.mLogger;
	}

	public int reconfigure(AppInstance appinst) {
		System.out.println("PauseResumeReconfigurer...");
		mLogger.bEvent("intermediateDraining");
		boolean intermediateDraining = appManager
				.intermediateDraining(appManager.curAIM);
		mLogger.eEvent("intermediateDraining");
		if (!intermediateDraining)
			return 1;

		AppInstanceManager aim = appManager.createNewAIM(appinst);
		appManager.reset();
		appManager.preCompilation(aim, appManager.prevAIM);
		aim.headTailHandler.setupHeadTail(appManager.app.bufferMap, aim);
		boolean isCompiled = aim.postCompilation();

		if (isCompiled) {
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
		if (aim.isRunning)
			return 0;
		else
			return 2;
	}

	/**
	 * Start the execution of the StreamJit application.
	 */
	private void start(AppInstanceManager aim) {
		aim.startChannels();
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
}