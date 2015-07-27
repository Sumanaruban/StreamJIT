package edu.mit.streamjit.tuner;

import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.common.TimeLogger;
import edu.mit.streamjit.impl.common.drainer.AbstractDrainer;
import edu.mit.streamjit.impl.distributed.ConfigurationManager;
import edu.mit.streamjit.impl.distributed.StreamJitApp;
import edu.mit.streamjit.impl.distributed.StreamJitAppManager;
import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.impl.distributed.node.StreamNode;
import edu.mit.streamjit.tuner.EventTimeLogger.FileEventTimeLogger;
import edu.mit.streamjit.util.Pair;

/**
 * Re-factored the {@link OnlineTuner} and moved all streamjit app
 * reconfiguration related methods to this new class.
 * 
 * @author sumanan
 * @since 10 Mar, 2015
 */
public class Reconfigurer {

	final AbstractDrainer drainer;
	final StreamJitAppManager manager;
	final StreamJitApp<?, ?> app;
	final ConfigurationManager cfgManager;
	final TimeLogger logger;
	final ConfigurationPrognosticator prognosticator;
	final EventTimeLogger mLogger;

	public Reconfigurer(AbstractDrainer drainer, StreamJitAppManager manager,
			StreamJitApp<?, ?> app, ConfigurationManager cfgManager,
			TimeLogger logger) {
		this.drainer = drainer;
		this.manager = manager;
		this.app = app;
		this.cfgManager = cfgManager;
		this.logger = logger;
		this.prognosticator = prognosticator(app);
		this.mLogger = eventTimeLogger();
	}

	private ConfigurationPrognosticator prognosticator(StreamJitApp<?, ?> app) {
		if (Options.prognosticate)
			return new GraphPropertyPrognosticator(app);
		else
			return new ConfigurationPrognosticator.NoPrognostication();
	}

	private EventTimeLogger eventTimeLogger() {
		if (Options.logEventTime)
			return new FileEventTimeLogger(app.name, "controller");
		else
			return new EventTimeLogger.NoEventTimeLogger();
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
	public Pair<Boolean, Integer> reconfigure(Configuration config) {
		int reason = 1;

		if (manager.getStatus() == AppStatus.STOPPED)
			return new Pair<Boolean, Integer>(false, -2);

		mLogger.bEvent("cfgManagerNewcfg");
		boolean validCfg = cfgManager.newConfiguration(config);
		mLogger.eEvent("cfgManagerNewcfg");
		if (!validCfg)
			return new Pair<Boolean, Integer>(true, -3);

		mLogger.bEvent("prognosticate");
		boolean prog = prognosticator.prognosticate(config);
		mLogger.eEvent("prognosticate");
		if (!prog)
			return new Pair<Boolean, Integer>(true, -4);

		mLogger.eEvent("serialcfg");
		try {
			mLogger.bEvent("intermediateDraining");
			boolean intermediateDraining = intermediateDraining();
			mLogger.eEvent("intermediateDraining");
			if (!intermediateDraining)
				return new Pair<Boolean, Integer>(false, -5);

			drainer.setBlobGraph(app.blobGraph);
			int multiplier = getMultiplier(config);
			mLogger.bEvent("managerReconfigure");
			boolean reconfigure = manager.reconfigure(multiplier);
			mLogger.eEvent("managerReconfigure");
			if (!reconfigure)
				reason = -6;
		} catch (Exception ex) {
			ex.printStackTrace();
			System.err
					.println("Couldn't compile the stream graph with this configuration");
			reason = -7;
		}
		return new Pair<Boolean, Integer>(true, reason);
	}

	/**
	 * Performs intermediate draining.
	 * 
	 * @return <code>true</code> iff the draining is success or the application
	 *         is not running currently.
	 * @throws InterruptedException
	 */
	private boolean intermediateDraining() throws InterruptedException {
		if (manager.isRunning()) {
			return drainer.drainIntermediate();
		} else
			return true;
	}

	private int getMultiplier(Configuration config) {
		int multiplier = 50;
		IntParameter mulParam = config.getParameter("multiplier",
				IntParameter.class);
		if (mulParam != null)
			multiplier = mulParam.getValue();
		System.err.println("Reconfiguring...multiplier = " + multiplier);
		return multiplier;
	}

	public void terminate() {
		if (manager.isRunning()) {
			// drainer.startDraining(1);
			drainer.drainFinal(true);
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