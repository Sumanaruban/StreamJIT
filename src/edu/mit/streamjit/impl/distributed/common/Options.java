package edu.mit.streamjit.impl.distributed.common;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import sun.misc.PerformanceLogger;
import edu.mit.streamjit.impl.common.drainer.AbstractDrainer;
import edu.mit.streamjit.impl.common.drainer.AbstractDrainer.DrainDataAction;
import edu.mit.streamjit.impl.distributed.controller.ConnectionManager.AllConnectionParams;
import edu.mit.streamjit.impl.distributed.controller.ConnectionManager.AsyncTCPNoParams;
import edu.mit.streamjit.impl.distributed.controller.ConnectionManager.BlockingTCPNoParams;
import edu.mit.streamjit.impl.distributed.controller.DistributedStreamCompiler;
import edu.mit.streamjit.impl.distributed.controller.StreamJitAppManager.Reconfigurer;
import edu.mit.streamjit.impl.distributed.controller.HT.TailChannels;
import edu.mit.streamjit.impl.distributed.controller.HT.TailChannels.BlockingTailChannel1;
import edu.mit.streamjit.impl.distributed.controller.HT.TailChannels.BlockingTailChannel2;
import edu.mit.streamjit.impl.distributed.node.AffinityManager;
import edu.mit.streamjit.impl.distributed.node.AffinityManagers.AllParallelAffinityManager;
import edu.mit.streamjit.impl.distributed.node.AffinityManagers.CoreCodeAffinityManager;
import edu.mit.streamjit.impl.distributed.node.AffinityManagers.EmptyAffinityManager;
import edu.mit.streamjit.impl.distributed.node.AffinityManagers.EqualAffinityManager;
import edu.mit.streamjit.impl.distributed.node.AffinityManagers.FileAffinityManager;
import edu.mit.streamjit.impl.distributed.node.AffinityManagers.OneCoreAffinityManager;
import edu.mit.streamjit.impl.distributed.node.StreamNode;
import edu.mit.streamjit.tuner.OnlineTuner;
import edu.mit.streamjit.tuner.TCPTuner;

/**
 * Program options. Loads the values from "options.properties".
 * 
 * @author sumanan
 * @since 1 Mar, 2015
 */
public final class Options {

	/**
	 * We can set this value at class loading time also as follows.
	 * 
	 * <code>maxThreadCount = Math.max(Runtime.getntime().availableProcessors() / 2,
	 * 1);</code>
	 * 
	 * Lets hard code this for the moment.
	 */
	public static final int maxNumCores;

	/**
	 * To turn on or off the dead lock handler. see {@link AbstractDrainer} for
	 * it's usage.
	 */
	public static final boolean needDrainDeadlockHandler;

	/**
	 * Turn On/Off the profiling.
	 */
	public static final boolean needProfiler;

	/**
	 * Output count for tuning. Tuner measures the running time for this number
	 * of outputs.
	 */
	public static final int outputCount;

	/**
	 * Period to measure and print the throughput. This throughput measurement
	 * feature get turned off if this value is less than 1. Time unit is ms. See
	 * {@link TailChannels}.
	 */
	public static final int throughputMeasurementPeriod;

	/**
	 * Save all configurations tired by open tuner in to
	 * "configurations//app.name" directory.
	 */
	public static final boolean saveAllConfigurations;

	/**
	 * Enables {@link DistributedStreamCompiler} to run on a single node. When
	 * this is enabled, noOfNodes passed as compiler argument has no effect.
	 */
	public static final boolean singleNodeOnline;

	/**
	 * Type of run. Currently 3 types of runs are in use.
	 * <ol>
	 * 0 - No tuning; fixed configuration run. The fixed configuration should be
	 * stored in the the "configurations" directory with the name of
	 * "fixed_[appName].cfg". In this case of run, {@link PerformanceLogger}
	 * will measure the time taken to generate fixed number of outputs and log
	 * into FixedOutPut.txt. See {@link TailChannels} for the file logging
	 * details.
	 * <ol>
	 * 1 - Tuning. OpenTuner will be started in this case.
	 * <ol>
	 * 2 - Evaluate configuration files. ( compares final cfg with hand tuned
	 * cfg. Both file should be presented in the running directory.
	 */
	public static final int run;

	/**
	 * Reduce Search Space (rss). Currently, search space becomes huge. Nearly
	 * 10^12000 possible configurations, which is insanely huge and OpenTuner
	 * cannot handle it effectively.
	 */
	public static final boolean rss = true;

	/**
	 * In this mode, StreamJIT will printout debug messages.
	 */
	public static final boolean debug = true;

	/**
	 * Decides how to start the opentuner. In first 2 cases, controller starts
	 * opentuner and establishes connection with it on a random port no range
	 * from 5000-65536. User can provide port no in 3 case.
	 * 
	 * <ol>
	 * <li>0 - Controller starts the tuner automatically on a terminal. User can
	 * see Opentuner related outputs in the new terminal.
	 * <li>1 - Controller starts the tuner automatically as a Python process. No
	 * explicit window will be opened. Suitable for remote running through SSH
	 * terminal.
	 * <li>2 - User has to manually start the tuner with correct portNo as
	 * argument. Port no 12563 is used in this case. But it can be changed at
	 * {@link TCPTuner#startTuner(String)}. We need this option to run the
	 * tuning on remote machines.
	 * </ol>
	 */
	public static final int tunerStartMode;

	/**
	 * if true uses Compiler2, interpreter otherwise.
	 */
	public static final boolean useCompilerBlob;

	/**
	 * To turn on or turn off the drain data. If this is false, drain data will
	 * be ignored and every new reconfiguration will run with fresh inputs.
	 */
	public static final boolean useDrainData;

	// Following are miscellaneous options to avoid rebuilding jar files every
	// time to change some class selections. You may decide to remove these
	// variables in a stable release.
	// TODO: Fix all design pattern related issues.

	/**
	 * <ol>
	 * <li>0 - {@link AllConnectionParams}
	 * <li>1 - {@link BlockingTCPNoParams}
	 * <li>2 - {@link AsyncTCPNoParams}
	 * <li>default: {@link AsyncTCPNoParams}
	 * </ol>
	 */
	public static final int connectionManager;

	/**
	 * <ol>
	 * <li>1 - {@link BlockingTailChannel1}
	 * <li>2 - {@link BlockingTailChannel2}
	 * <li>default: {@link BlockingTailChannel2}
	 * </ol>
	 */
	public static final int tailChannel;

	/**
	 * When {@link TailChannels} wait for fixed number of outputs, this flag
	 * tells them whether they should wait infinitely to receive the fixed
	 * number of outputs or not.
	 */
	public static final boolean timeOut;

	/**
	 * {@link OnlineTuner}'s verifier verifies the configurations if
	 * {@link #run}==2. evaluationCount determines the number of re runs for a
	 * configuration. Default value is 2.
	 */
	public static final int evaluationCount;

	/**
	 * {@link OnlineTuner}'s verifier verifies the configurations if
	 * {@link #run}==2. verificationCount determines the number of re runs for a
	 * set of configurations in the verify.txt. Default value is 1.
	 */
	public static final int verificationCount;

	/**
	 * Maximum numbers of rounds to tune.
	 */
	public static final int tuningRounds;

	/**
	 * Type of {@link AffinityManager} to use.
	 * <ol>
	 * <li>0 - {@link EmptyAffinityManager}.
	 * <li>1 - {@link OneCoreAffinityManager}.
	 * <li>2 - {@link EqualAffinityManager}.
	 * <li>3 - {@link CoreCodeAffinityManager}.
	 * <li>4 - {@link AllParallelAffinityManager}.
	 * <li>5 - {@link FileAffinityManager}.
	 * <li>Other integers - {@link CoreCodeAffinityManager}.
	 * </ol>
	 * 
	 */
	public static final int AffinityManager;

	/**
	 * Large multiplier -> Large compilation time and Large waiting time.
	 */
	public static final int multiplierMaxValue;

	public static final boolean prognosticate;

	public static final int bigToSmallBlobRatio;

	public static final int loadRatio;

	public static final int blobToNodeRatio;

	public static final int boundaryChannelRatio;

	// StreamJit benchmark app's constructor arguments.
	public static final int appArg1;
	public static final int appArg2;

	// BlockingTailChannel3's skip time and steady time in mills.
	public static final int skipMills;
	public static final int steadyMills;
	public static final int noOutputTimeLimit;

	public static final boolean logEventTime;

	// Draining and DrainData management options.
	/**
	 * If the {@link StreamNode} thread calls BlobExecuer.doDrain(), sometimes
	 * it causes deadlock. So it is always better to call BlobExecuer.doDrain()
	 * in a new thread. So make this flag on always. This flag is added for
	 * experimentation purpose.
	 */
	public static final boolean doDraininNewThread;

	/**
	 * Stores draindata in to the app directory. Read the draindata from disk
	 * and reconfigure the app with the drain data when running in verifying
	 * mode.
	 */
	public static final boolean dumpDrainData;

	/**
	 * Always use {@link DrainDataAction#FINISH} when reconfiguring. See
	 * {@link DrainDataAction#FINISH} for its functionality.
	 */
	public static final boolean DDActionFINISH;

	// Reconfiguration related options.
	/**
	 * Type of {@link Reconfigurer} to use.
	 * <ol>
	 * <li>0 - {@link PauseResumeReconfigurer}.
	 * <li>1 - {@link EquationBasedReconfigurer}.
	 * <li>2 - {@link AdaptiveReconfigurer}.
	 * </ol>
	 */
	public static final int Reconfigurer;

	static {
		Properties prop = loadProperties();
		throughputMeasurementPeriod = Integer.parseInt(prop
				.getProperty("throughputMeasurementPeriod"));;
		maxNumCores = Integer.parseInt(prop.getProperty("maxNumCores"));
		useCompilerBlob = Boolean.parseBoolean(prop
				.getProperty("useCompilerBlob"));
		needDrainDeadlockHandler = Boolean.parseBoolean(prop
				.getProperty("needDrainDeadlockHandler"));
		needProfiler = Boolean.parseBoolean(prop.getProperty("needProfiler"));
		outputCount = Integer.parseInt(prop.getProperty("outputCount"));
		run = Integer.parseInt(prop.getProperty("run"));
		tunerStartMode = Integer.parseInt(prop.getProperty("tunerStartMode"));
		saveAllConfigurations = Boolean.parseBoolean(prop
				.getProperty("saveAllConfigurations"));
		singleNodeOnline = Boolean.parseBoolean(prop
				.getProperty("singleNodeOnline"));
		useDrainData = Boolean.parseBoolean(prop.getProperty("useDrainData"));
		connectionManager = Integer.parseInt(prop
				.getProperty("connectionManager"));
		tailChannel = Integer.parseInt(prop.getProperty("tailChannel"));
		evaluationCount = Integer.parseInt(prop.getProperty("evaluationCount"));
		verificationCount = Integer.parseInt(prop
				.getProperty("verificationCount"));
		multiplierMaxValue = Integer.parseInt(prop
				.getProperty("multiplierMaxValue"));
		prognosticate = Boolean.parseBoolean(prop.getProperty("prognosticate"));
		bigToSmallBlobRatio = Integer.parseInt(prop
				.getProperty("bigToSmallBlobRatio"));
		loadRatio = Integer.parseInt(prop.getProperty("loadRatio"));
		blobToNodeRatio = Integer.parseInt(prop.getProperty("blobToNodeRatio"));
		boundaryChannelRatio = Integer.parseInt(prop
				.getProperty("boundaryChannelRatio"));
		timeOut = Boolean.parseBoolean(prop.getProperty("timeOut"));
		tuningRounds = Integer.parseInt(prop.getProperty("tuningRounds"));
		appArg1 = Integer.parseInt(prop.getProperty("appArg1"));
		appArg2 = Integer.parseInt(prop.getProperty("appArg2"));
		skipMills = Integer.parseInt(prop.getProperty("skipMills"));
		steadyMills = Integer.parseInt(prop.getProperty("steadyMills"));
		noOutputTimeLimit = Integer.parseInt(prop
				.getProperty("noOutputTimeLimit"));
		logEventTime = Boolean.parseBoolean(prop.getProperty("logEventTime"));
		doDraininNewThread = Boolean.parseBoolean(prop
				.getProperty("doDraininNewThread"));
		dumpDrainData = Boolean.parseBoolean(prop.getProperty("dumpDrainData"));
		Reconfigurer = Integer.parseInt(prop.getProperty("Reconfigurer"));
		DDActionFINISH = Boolean.parseBoolean(prop
				.getProperty("DDActionFINISH"));
		AffinityManager = Integer.parseInt(prop.getProperty("AffinityManager"));
	}

	public static Properties getProperties() {
		Properties prop = new Properties();
		setProperty(prop, "tunerStartMode", tunerStartMode);
		setProperty(prop, "useDrainData", useDrainData);
		setProperty(prop, "needDrainDeadlockHandler", needDrainDeadlockHandler);
		setProperty(prop, "run", run);
		setProperty(prop, "saveAllConfigurations", saveAllConfigurations);
		setProperty(prop, "outputCount", outputCount);
		setProperty(prop, "useCompilerBlob", useCompilerBlob);
		setProperty(prop, "throughputMeasurementPeriod",
				throughputMeasurementPeriod);
		setProperty(prop, "singleNodeOnline", singleNodeOnline);
		setProperty(prop, "maxNumCores", maxNumCores);
		setProperty(prop, "needProfiler", needProfiler);
		setProperty(prop, "connectionManager", connectionManager);
		setProperty(prop, "tailChannel", tailChannel);
		setProperty(prop, "evaluationCount", evaluationCount);
		setProperty(prop, "verificationCount", verificationCount);
		setProperty(prop, "multiplierMaxValue", multiplierMaxValue);
		setProperty(prop, "prognosticate", prognosticate);
		setProperty(prop, "bigToSmallBlobRatio", bigToSmallBlobRatio);
		setProperty(prop, "loadRatio", loadRatio);
		setProperty(prop, "blobToNodeRatio", blobToNodeRatio);
		setProperty(prop, "boundaryChannelRatio", boundaryChannelRatio);
		setProperty(prop, "timeOut", timeOut);
		setProperty(prop, "tuningRounds", tuningRounds);
		setProperty(prop, "appArg1", appArg1);
		setProperty(prop, "appArg2", appArg2);
		setProperty(prop, "skipMills", skipMills);
		setProperty(prop, "steadyMills", steadyMills);
		setProperty(prop, "noOutputTimeLimit", noOutputTimeLimit);
		setProperty(prop, "logEventTime", logEventTime);
		setProperty(prop, "doDraininNewThread", doDraininNewThread);
		setProperty(prop, "dumpDrainData", dumpDrainData);
		setProperty(prop, "Reconfigurer", Reconfigurer);
		setProperty(prop, "DDActionFINISH", DDActionFINISH);
		setProperty(prop, "AffinityManager", AffinityManager);
		return prop;
	}

	private static Properties loadProperties() {
		Properties prop = new Properties();
		InputStream input = null;
		try {
			input = new FileInputStream("options.properties");
			prop.load(input);
		} catch (IOException ex) {
			System.err.println("Failed to load options.properties");
		}
		return prop;
	}

	private static void setProperty(Properties prop, String name, Boolean val) {
		prop.setProperty(name, val.toString());
	}

	private static void setProperty(Properties prop, String name, Integer val) {
		prop.setProperty(name, val.toString());
	}

	public static void storeProperties() {
		OutputStream output = null;
		try {
			output = new FileOutputStream("options.properties");
			Properties prop = getProperties();
			prop.store(output, null);
		} catch (IOException io) {
			io.printStackTrace();
		} finally {
			if (output != null) {
				try {
					output.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
