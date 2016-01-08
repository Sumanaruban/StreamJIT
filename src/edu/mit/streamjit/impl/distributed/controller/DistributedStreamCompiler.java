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
package edu.mit.streamjit.impl.distributed.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Input.ManualInput;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.common.BufferWriteCounter;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.InputBufferFactory;
import edu.mit.streamjit.impl.common.OutputBufferFactory;
import edu.mit.streamjit.impl.common.TimeLogger;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.impl.distributed.controller.ConfigurationManager.NewConfiguration;
import edu.mit.streamjit.impl.distributed.controller.StreamJitAppManager.AppDrainer;
import edu.mit.streamjit.impl.distributed.controller.HT.HeadChannels.HeadBuffer;
import edu.mit.streamjit.impl.distributed.controller.HT.ThroughputGraphGenerator;
import edu.mit.streamjit.impl.distributed.controller.HT.ThroughputPrinter;
import edu.mit.streamjit.impl.distributed.node.StreamNode;
import edu.mit.streamjit.impl.distributed.runtimer.CommunicationManager.CommunicationType;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;
import edu.mit.streamjit.partitioner.HorizontalPartitioner;
import edu.mit.streamjit.partitioner.Partitioner;
import edu.mit.streamjit.tuner.OnlineTuner;
import edu.mit.streamjit.tuner.Reconfigurer;
import edu.mit.streamjit.tuner.Verifier;
import edu.mit.streamjit.util.ConfigurationUtils;
import edu.mit.streamjit.util.Pair;

/**
 * 
 * The OneToOneElement that is asked to compile by this {@link Compiler} must be
 * unique. Compilation will fail if default subtypes of the
 * {@link OneToOneElement}s such as {@link Pipeline}, {@link Splitjoin} and etc
 * are be passed.
 * <p>
 * TODO: {@link DistributedStreamCompiler} must work with 1 {@link StreamNode}
 * as well. In that case, it should behave like a
 * {@link ConcurrentStreamCompiler}.
 * </p>
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 6, 2013
 */
public class DistributedStreamCompiler implements StreamCompiler {

	/**
	 * Configuration from Opentuner.
	 */
	private Configuration cfg;

	/**
	 * Total number of nodes including controller node.
	 */
	private int noOfnodes;

	private final boolean measureThroughput = true;

	/**
	 * Run the whole application on the controller node. No distributions. See
	 * {@link #DistributedStreamCompiler(int, Configuration)}
	 */
	public DistributedStreamCompiler() {
		this(1, null);
	}

	/**
	 * See {@link #DistributedStreamCompiler(int, Configuration)}. As no
	 * configuration is passed, tuner will activated to tune for better
	 * configuration.
	 */
	public DistributedStreamCompiler(int noOfnodes) {
		this(noOfnodes, null);
	}

	/**
	 * Run the application with the passed configuration. Pass null if the
	 * intention is to tune the application.
	 * 
	 * @param noOfnodes
	 *            : Total number of nodes the stream application intended to run
	 *            including the controller node. If it is 1 then it means the
	 *            whole stream application is supposed to run on controller.
	 * @param cfg
	 *            Run the application with the passed {@link Configuration}. If
	 *            it is null, tuner will be activated to tune for better
	 *            configuration.
	 */
	public DistributedStreamCompiler(int noOfnodes, Configuration cfg) {
		if (noOfnodes < 1)
			throw new IllegalArgumentException("noOfnodes must be 1 or greater");
		if (Options.singleNodeOnline) {
			System.out
					.println("Flag GlobalConstants.singleNodeOnline is enabled."
							+ " noOfNodes passed as compiler argument has no effect");
			this.noOfnodes = 1;
		} else
			this.noOfnodes = noOfnodes;

		this.cfg = cfg;
	}

	public <I, O> CompiledStream compile(OneToOneElement<I, O> stream,
			Input<I> input, Output<O> output) {
		StreamJitApp<I, O> app = new StreamJitApp<>(stream);
		Controller controller = establishController();

		PartitionManager partitionManager = new HotSpotTuning(app);
		ConfigurationManager cfgManager = new ConfigurationManager(app,
				partitionManager);
		ConnectionManager conManager = connectionManager(controller.controllerNodeID);

		AppInstance appinst = setConfiguration(controller, app,
				partitionManager, conManager, cfgManager);

		Pair<Buffer, ThroughputPrinter> p = tailBuffer(output, app);
		Buffer tail = p.first;
		ThroughputPrinter tp = p.second;

		TimeLogger logger = new TimeLoggers.FileTimeLogger(app.name);
		StreamJitAppManager manager = new StreamJitAppManager(controller, app,
				conManager, logger, tail);
		// final AbstractDrainer drainer = new DistributedDrainer(app, logger,
		// manager);
		// drainer.setAppInstance(appinst);

		boolean needTermination = setBufferMap(input, tail, manager.appDrainer,
				app);

		manager.reconfigurer.reconfigure(appinst);
		CompiledStream cs = new DistributedCompiledStream(manager.appDrainer,
				tp);

		if (Options.tune > 0 && this.cfg != null) {
			Reconfigurer configurer = new Reconfigurer(manager, app,
					cfgManager, logger);
			tuneOrVerify(configurer, needTermination);
		} else {
			runFixedCfg(Options.boundaryChannelRatio, manager.appDrainer,
					app.name);
		}
		return cs;
	}

	private ConnectionManager connectionManager(int controllerNodeID) {
		switch (Options.connectionManager) {
			case 0 :
				return new ConnectionManager.AllConnectionParams(
						controllerNodeID);
			case 1 :
				return new ConnectionManager.BlockingTCPNoParams(
						controllerNodeID);
			default :
				return new ConnectionManager.AsyncTCPNoParams(controllerNodeID);
		}
	}

	private <I, O> Configuration fixedCfg(StreamJitApp<I, O> app,
			Controller controller, Configuration defaultCfg) {
		Configuration cfg1 = ConfigurationUtils.readConfiguration(app.name,
				"fixed");
		if (cfg1 == null) {
			controller.closeAll();
			throw new IllegalConfigurationException();
		} else if (!verifyCfg(defaultCfg, cfg1)) {
			System.err
					.println("Reading the configuration from configuration file");
			System.err
					.println("No matching between parameters in the read "
							+ "configuration and parameters in the default configuration");
			controller.closeAll();
			throw new IllegalConfigurationException();
		}
		return cfg1;
	}

	private Controller establishController() {
		Map<CommunicationType, Integer> conTypeCount = new HashMap<>();

		if (this.noOfnodes == 1)
			conTypeCount.put(CommunicationType.LOCAL, 1);
		else
			conTypeCount.put(CommunicationType.TCP, this.noOfnodes - 1);
		Controller controller = new Controller();
		controller.connect(conTypeCount);
		return controller;
	}

	private <I, O> Map<Integer, List<Set<Worker<?, ?>>>> getMachineWorkerMap(
			Integer[] machineIds, OneToOneElement<I, O> stream,
			Worker<I, ?> source, Worker<?, O> sink) {
		int totalCores = machineIds.length;

		Partitioner<I, O> horzPartitioner = new HorizontalPartitioner<>();
		List<Set<Worker<?, ?>>> partitionList = horzPartitioner
				.partitionEqually(stream, source, sink, totalCores);

		Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap = new HashMap<Integer, List<Set<Worker<?, ?>>>>();
		for (Integer machineID : machineIds) {
			partitionsMachineMap.put(machineID,
					new ArrayList<Set<Worker<?, ?>>>());
		}

		int index = 0;
		while (index < partitionList.size()) {
			for (Integer machineID : partitionsMachineMap.keySet()) {
				if (!(index < partitionList.size()))
					break;
				partitionsMachineMap.get(machineID).add(
						partitionList.get(index++));
			}
		}
		return partitionsMachineMap;
	}

	private <I, O> AppInstance manualPartition(StreamJitApp<I, O> app) {
		Integer[] machineIds = new Integer[this.noOfnodes - 1];
		for (int i = 0; i < machineIds.length; i++) {
			machineIds[i] = i + 1;
		}
		Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap = getMachineWorkerMap(
				machineIds, app.streamGraph, app.source, app.sink);
		AppInstance appinst = app.newPartitionMap(partitionsMachineMap);
		return appinst;
	}

	/**
	 * Sets head and tail buffers.
	 */
	private <I, O> boolean setBufferMap(Input<I> input, Buffer tail,
			final AppDrainer drainer, StreamJitApp<I, O> app) {
		// TODO: derive a algorithm to find good buffer size and use here.
		Buffer head = InputBufferFactory.unwrap(input).createReadableBuffer(
				10000);

		boolean needTermination;

		if (input instanceof ManualInput) {
			needTermination = false;
			InputBufferFactory
					.setManualInputDelegate(
							(ManualInput<I>) input,
							new InputBufferFactory.AbstractManualInputDelegate<I>(
									head) {
								@Override
								public void drain() {
									// drainer.startDraining(2);
									drainer.drainFinal(true);
								}
							});
		} else {
			needTermination = true;
			head = new HeadBuffer(head, drainer);
		}

		ImmutableMap.Builder<Token, Buffer> bufferMapBuilder = ImmutableMap
				.<Token, Buffer> builder();

		bufferMapBuilder.put(app.headToken, head);
		bufferMapBuilder.put(app.tailToken, tail);

		app.bufferMap = bufferMapBuilder.build();
		return needTermination;
	}

	private <O> Pair<Buffer, ThroughputPrinter> tailBuffer(Output<O> output,
			StreamJitApp<?, ?> app) {
		Buffer tail = OutputBufferFactory.unwrap(output).createWritableBuffer(
				10000);
		return measureThroughput(tail, app);
	}

	private Pair<Buffer, ThroughputPrinter> measureThroughput(
			Buffer tailBuffer, StreamJitApp<?, ?> app) {
		ThroughputPrinter tp;
		Buffer b;
		if (measureThroughput) {
			BufferWriteCounter bc = new BufferWriteCounter(tailBuffer);
			tp = new ThroughputPrinter(bc, app.name, null,
					"DistributedStreamCompiler", "tailBuffer.txt");
			b = bc;
		} else {
			b = tailBuffer;
			tp = null;
		}
		return new Pair<Buffer, ThroughputPrinter>(b, tp);
	}

	private <I, O> AppInstance setConfiguration(Controller controller,
			StreamJitApp<I, O> app, PartitionManager partitionManager,
			ConnectionManager conManager, ConfigurationManager cfgManager) {
		BlobFactory bf = new DistributedBlobFactory(partitionManager,
				conManager, Math.max(noOfnodes - 1, 1));
		Configuration defaultCfg = bf.getDefaultConfiguration(Workers
				.getAllWorkersInGraph(app.source));

		if (this.cfg != null) {
			if (!verifyCfg(defaultCfg, this.cfg)) {
				System.err
						.println("No matching between parameters in the passed "
								+ "configuration and parameters in the default configuration");
				controller.closeAll();
				throw new IllegalConfigurationException();
			}
		} else if (Options.tune == 0) {
			this.cfg = fixedCfg(app, controller, defaultCfg);
		} else
			this.cfg = defaultCfg;

		NewConfiguration newConfig = cfgManager.newConfiguration(this.cfg);
		return app.newConfiguration(newConfig);
	}

	private <I, O> boolean verifyCfg(Configuration defaultCfg, Configuration cfg) {
		if (defaultCfg.getParametersMap().keySet()
				.equals(cfg.getParametersMap().keySet()))
			return true;
		return false;
	}

	private void tuneOrVerify(Reconfigurer configurer, boolean needTermination) {
		Runnable r;
		if (Options.tune == 1) {
			r = new OnlineTuner(configurer, needTermination, this.cfg);
			new Thread(r, "OnlineTuner").start();
		} else if (Options.tune == 2) {
			r = new Verifier(configurer);
			new Thread(r, "Verifier").start();
		} else {
			throw new IllegalStateException(
					"Neither OnlineTuner nor Verifer has been started.");
		}
	}

	private void runFixedCfg(int seconds, final AppDrainer drainer,
			String appName) {
		System.err
				.println(String
						.format("No tuning or no verification run. Going to run for %d seconds with fixed cfg.",
								seconds));
		try {
			Thread.sleep(seconds * 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		drainer.drainFinal(true);
		System.out.println("Draining Called.");

		try {
			ThroughputGraphGenerator.summarize(appName);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static class DistributedCompiledStream implements CompiledStream {

		private final AppDrainer drainer;

		private final ThroughputPrinter throughputPrinter;

		public DistributedCompiledStream(AppDrainer drainer,
				ThroughputPrinter throughputPrinter) {
			this.drainer = drainer;
			this.throughputPrinter = throughputPrinter;
		}

		@Override
		public void awaitDrained() throws InterruptedException {
			drainer.awaitDrained();
			stop();
		}

		@Override
		public void awaitDrained(long timeout, TimeUnit unit)
				throws InterruptedException, TimeoutException {
			drainer.awaitDrained(timeout, unit);
			stop();
		}

		@Override
		public boolean isDrained() {
			boolean ret = drainer.isDrained();
			if (ret)
				stop();
			return ret;
		}

		private void stop() {
			if (throughputPrinter != null)
				throughputPrinter.stop();
		}
	}

	private class IllegalConfigurationException extends RuntimeException {

		private static final long serialVersionUID = 1L;

		private static final String tag = "IllegalConfigurationException";

		private IllegalConfigurationException() {
			super(tag);
		}

		private IllegalConfigurationException(String msg) {
			super(String.format("%s : %s", tag, msg));
		}
	}
}