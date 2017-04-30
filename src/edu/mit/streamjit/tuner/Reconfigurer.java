package edu.mit.streamjit.tuner;

import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.impl.distributed.controller.AppInstance;
import edu.mit.streamjit.impl.distributed.controller.ConfigurationManager;
import edu.mit.streamjit.impl.distributed.controller.ConfigurationManager.NewConfiguration;
import edu.mit.streamjit.impl.distributed.controller.StreamJitApp;
import edu.mit.streamjit.impl.distributed.controller.StreamJitAppManager;
import edu.mit.streamjit.impl.distributed.node.StreamNode;
import edu.mit.streamjit.util.EventTimeLogger;
import edu.mit.streamjit.util.Pair;

/**
 * Re-factored the {@link OnlineTuner} and moved all streamjit app
 * reconfiguration related methods to this new class.
 * 
 * @author sumanan
 * @since 10 Mar, 2015
 */
public class Reconfigurer {

	final StreamJitAppManager manager;
	final StreamJitApp<?, ?> app;
	final ConfigurationManager cfgManager;
	final ConfigurationPrognosticator prognosticator;
	final EventTimeLogger mLogger;

	public Reconfigurer(StreamJitAppManager manager, StreamJitApp<?, ?> app,
			ConfigurationManager cfgManager) {
		this.manager = manager;
		this.app = app;
		this.cfgManager = cfgManager;
		this.prognosticator = prognosticator(app);
		this.mLogger = app.eLogger;
	}

	private ConfigurationPrognosticator prognosticator(StreamJitApp<?, ?> app) {
		if (Options.prognosticate)
			return new GraphPropertyPrognosticator(app);
		else
			return new ConfigurationPrognosticator.NoPrognostication();
	}

	/**
	 * Reconfigures and returns a {@link Pair}. If ret.first == false, then no
	 * more tuning. If ret.second > 0, the reconfiguration is successful; caller
	 * can call getFixedOutputTime() to measure the running time. If ret.second
	 * < 0, the reconfiguration is unsuccessful. Meanings of the negative values
	 * are follows
	 * <ol>
	 * <li>-1: is reserved for timeout.
	 * <li>-2: App has stopped. No more tuning.
	 * <li>-3: Invalid configuration.
	 * <li>-4: {@link ConfigurationPrognosticator} has rejected the
	 * configuration.
	 * <li>-5: Draining failed. Another draining is in progress.
	 * <li>-6: Reconfiguration has failed at {@link StreamNode} side. E.g.,
	 * Compilation error.
	 * <li>-7: Misc problems.
	 * 
	 * @param cfgJson
	 * @return
	 */
	public Pair<Boolean, Integer> reconfigure(NewConfiguration newConfig) {
		int reason = 1;

		if (manager.getStatus() == AppStatus.STOPPED)
			return new Pair<Boolean, Integer>(false, -2);

		if (!newConfig.verificationPassed)
			return new Pair<Boolean, Integer>(true, -3);

		if (!newConfig.isPrognosticationPassed())
			return new Pair<Boolean, Integer>(true, -4);

		mLogger.eEvent("serialcfg");
		try {
			AppInstance appinst = app.newConfiguration(newConfig);
			int reconfigure = manager.reconfigurer.reconfigure(appinst);
			if (reconfigure == 1)
				return new Pair<Boolean, Integer>(false, -5);
			else if (reconfigure == 2)
				reason = -6;
		} catch (Exception ex) {
			ex.printStackTrace();
			System.err
					.println("Couldn't compile the stream graph with this configuration");
			reason = -7;
		}
		return new Pair<Boolean, Integer>(true, reason);
	}

	public NewConfiguration newConfiguration(Configuration config) {
		mLogger.bEvent("CfgManagerNewcfg");
		NewConfiguration newConfiguration = cfgManager.newConfiguration(config);
		mLogger.eEvent("CfgManagerNewcfg");

		boolean prog = false;
		if (newConfiguration.verificationPassed) {
			mLogger.bEvent("Prognosticate");
			prog = prognosticator.prognosticate(newConfiguration);
			mLogger.eEvent("Prognosticate");
		}
		newConfiguration.setPrognosticationPassed(prog);
		return newConfiguration;
	}

	public void terminate() {
		if (manager.isRunning()) {
			// drainer.startDraining(1);
			manager.appDrainer.drainFinal(false);
		} else {
			manager.stop();
		}
	}

	public long getFixedOutputTime(long timeout) {
		// TODO: need to check the manager's status before passing the
		// time. Exceptions, final drain, etc may causes app to stop
		// executing.
		mLogger.bEvent("getFixedOutputTime");
		long time = -1;
		try {
			time = manager.getFixedOutputTime(timeout);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		mLogger.eEvent("getFixedOutputTime");
		return time;
	}
}