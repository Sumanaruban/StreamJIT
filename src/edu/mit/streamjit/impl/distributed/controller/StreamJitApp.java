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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Portal;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.StatefulFilter;
import edu.mit.streamjit.api.StreamCompilationFailedException;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.AbstractReadOnlyBuffer;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.common.BufferWriteCounter;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.ConnectWorkersVisitor;
import edu.mit.streamjit.impl.common.MessageConstraint;
import edu.mit.streamjit.impl.common.OutputBufferFactory;
import edu.mit.streamjit.impl.common.Portals;
import edu.mit.streamjit.impl.common.TimeLogger;
import edu.mit.streamjit.impl.common.VerifyStreamGraph;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.common.drainer.BlobGraph;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.impl.distributed.common.Utils;
import edu.mit.streamjit.impl.distributed.controller.ConfigurationManager.NewConfiguration;
import edu.mit.streamjit.impl.distributed.controller.HT.ThroughputPrinter;
import edu.mit.streamjit.impl.distributed.node.StreamNode;
import edu.mit.streamjit.impl.interp.Interpreter;
import edu.mit.streamjit.tuner.EventTimeLogger;
import edu.mit.streamjit.tuner.EventTimeLogger.FileEventTimeLogger;
import edu.mit.streamjit.util.Pair;

/**
 * This class contains all static information about the current StreamJit
 * application. {@link AppInstance} contains all dynamic information of the
 * current streamJit application.
 * <p>
 * This class is immutable.
 * </p>
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Oct 8, 2013
 */
public class StreamJitApp<I, O> {

	private int appInstUniqueIDGen = 0;

	/**
	 * Since this is final, lets make public
	 */
	public final String topLevelClass;

	public final Worker<I, ?> source;

	public final Worker<?, O> sink;

	public final String jarFilePath;

	public final String name;

	final OneToOneElement<I, O> streamGraph;

	public final List<MessageConstraint> constraints;

	public final Visualizer visualizer;

	public final EventTimeLogger eLogger;

	public final TimeLogger logger;

	/**
	 * is app stateful?
	 */
	public final boolean stateful;

	public final Token headToken;

	public final Token tailToken;

	public Buffer headBuffer;

	public final Buffer tailBuffer;

	private final boolean measureThroughput = true;

	public final ThroughputPrinter tp;

	// TODO: Make these variables final. Or add getter setter methods.
	// Stream graph's total input and output rates when multiplier == 1.
	public int steadyIn = -1;
	public int steadyOut = -1;

	public StreamJitApp(OneToOneElement<I, O> streamGraph, Output<O> output) {
		this.streamGraph = streamGraph;
		Pair<Worker<I, ?>, Worker<?, O>> srcSink = visit(streamGraph);
		this.name = streamGraph.getClass().getSimpleName();
		this.topLevelClass = streamGraph.getClass().getName();
		this.source = srcSink.first;
		this.sink = srcSink.second;
		this.jarFilePath = streamGraph.getClass().getProtectionDomain()
				.getCodeSource().getLocation().getPath();
		this.constraints = getConstrains();
		Utils.newApp(name);
		visualizer = new Visualizer.DotVisualizer(streamGraph);
		eLogger = eventTimeLogger();
		stateful = isGraphStateful();
		headToken = Token.createOverallInputToken(source);
		tailToken = Token.createOverallOutputToken(sink);
		logger = new TimeLoggers.FileTimeLogger(name);

		Pair<Buffer, ThroughputPrinter> p = tailBuffer(output);
		tailBuffer = p.first;
		tp = p.second;
	}

	private EventTimeLogger eventTimeLogger() {
		if (Options.logEventTime)
			return new FileEventTimeLogger(name, "controller", false, false);
		else
			return new EventTimeLogger.NoEventTimeLogger();
	}

	/**
	 * Uses an {@link Interpreter} blob to clear or minimize a {@link DrainData}
	 * . This method can be called after a final draining to clear the data in
	 * the intermediate buffers.
	 * 
	 * @param drainData
	 *            : {@link DrainData} that is received after a draining.
	 * @return : A {@link DrainData} that remains after running an
	 *         {@link Interpreter} blob.
	 */
	public DrainData minimizeDrainData(DrainData drainData) {
		Interpreter.InterpreterBlobFactory interpFactory = new Interpreter.InterpreterBlobFactory();
		Blob interp = interpFactory.makeBlob(Workers
				.getAllWorkersInGraph(source), interpFactory
				.getDefaultConfiguration(Workers.getAllWorkersInGraph(source)),
				1, drainData);
		interp.installBuffers(bufferMapWithEmptyHead());
		Runnable interpCode = interp.getCoreCode(0);
		final AtomicBoolean interpFinished = new AtomicBoolean();
		interp.drain(new Runnable() {
			@Override
			public void run() {
				interpFinished.set(true);
			}
		});
		while (!interpFinished.get())
			interpCode.run();
		return interp.getDrainData();
	}

	/**
	 * Remove the original headbuffer and replace it with a new empty buffer.
	 */
	private ImmutableMap<Token, Buffer> bufferMapWithEmptyHead() {
		ImmutableMap.Builder<Token, Buffer> bufMapBuilder = ImmutableMap
				.builder();
		Buffer head = new AbstractReadOnlyBuffer() {
			@Override
			public int size() {
				return 0;
			}

			@Override
			public Object read() {
				return null;
			}
		};

		bufMapBuilder.put(headToken, head);
		bufMapBuilder.put(tailToken, tailBuffer);

		return bufMapBuilder.build();
	}

	private Pair<Worker<I, ?>, Worker<?, O>> visit(OneToOneElement<I, O> stream) {
		checkforDefaultOneToOneElement(stream);
		ConnectWorkersVisitor primitiveConnector = new ConnectWorkersVisitor();
		stream.visit(primitiveConnector);
		Worker<I, ?> source = (Worker<I, ?>) primitiveConnector.getSource();
		Worker<?, O> sink = (Worker<?, O>) primitiveConnector.getSink();

		VerifyStreamGraph verifier = new VerifyStreamGraph();
		stream.visit(verifier);
		return new Pair<Worker<I, ?>, Worker<?, O>>(source, sink);
	}

	/**
	 * TODO: Need to check for other default subtypes of {@link OneToOneElement}
	 * s. Now only checks for first generation children.
	 * 
	 * @param stream
	 * @throws StreamCompilationFailedException
	 *             if stream is default subtype of OneToOneElement
	 */
	private void checkforDefaultOneToOneElement(OneToOneElement<I, O> stream) {
		if (stream.getClass() == Pipeline.class
				|| stream.getClass() == Splitjoin.class
				|| stream.getClass() == Filter.class) {
			throw new StreamCompilationFailedException(
					"Default subtypes of OneToOneElement are not accepted for"
							+ " compilation by this compiler. OneToOneElement"
							+ " that passed should be unique");
		}
	}

	private List<MessageConstraint> getConstrains() {
		// TODO: Copied form DebugStreamCompiler. Need to be verified for this
		// context.
		List<MessageConstraint> constraints = MessageConstraint
				.findConstraints(source);
		Set<Portal<?>> portals = new HashSet<>();
		for (MessageConstraint mc : constraints)
			portals.add(mc.getPortal());
		for (Portal<?> portal : portals)
			Portals.setConstraints(portal, constraints);
		return constraints;
	}

	/**
	 * Uses {@link StreamPathBuilder} to generate all paths in the streamGraph
	 * of this {@link StreamJitApp}. Check {@link StreamPathBuilder} for more
	 * information.
	 * 
	 * @return Set of all paths in the streamGraph of this {@link StreamJitApp}.
	 */
	public Set<List<Integer>> paths() {
		return StreamPathBuilder.paths(streamGraph);
	}

	/**
	 * Static information of the {@link StreamJitApp} that is essential for
	 * {@link StreamNode}s to set up. This configuration will be sent to
	 * {@link StreamNode}s when setting up a new app (Only once).
	 * 
	 * @return static information of the app that is needed by steramnodes.
	 */
	public Configuration.Builder getStaticConfiguration() {
		Configuration.Builder builder = Configuration.builder();
		builder.putExtraData(GlobalConstants.JARFILE_PATH, jarFilePath);
		builder.putExtraData(GlobalConstants.TOPLEVEL_WORKER_NAME,
				topLevelClass);
		builder.putExtraData(GlobalConstants.APP_NAME, name);
		return builder;
	}
	public AppInstance newConfiguration(NewConfiguration newConfiguration) {
		if (!newConfiguration.verificationPassed)
			throw new IllegalStateException(
					"Invalid newConfiguration. newConfiguration.verificationPassed=false.");
		return new AppInstance(this, appInstUniqueIDGen++,
				newConfiguration.partitionsMachineMap,
				newConfiguration.configuration, newConfiguration.blobGraph);
	}
	/**
	 * Builds {@link BlobGraph} from the partitionsMachineMap, and verifies for
	 * any cycles among blobs. If it is a valid partitionsMachineMap, (i.e., no
	 * cycles among the blobs), then creates and returns an AppInstance; returns
	 * null otherwise.
	 * 
	 * @return a new {@link AppInstance} if the partition is acyclic.
	 *         <code>null</code> otherwise.
	 */
	public AppInstance newPartitionMap(
			Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap) {
		BlobGraph bg;
		try {
			bg = AppInstance.verifyConfiguration(partitionsMachineMap);
		} catch (StreamCompilationFailedException ex) {
			return null;
		}
		return new AppInstance(this, appInstUniqueIDGen++,
				partitionsMachineMap, null, bg);
	}

	private boolean isGraphStateful() {
		for (Worker<?, ?> w : Workers.getAllWorkersInGraph(source)) {
			if (w instanceof StatefulFilter)
				return true;
		}
		return false;
	}

	private Pair<Buffer, ThroughputPrinter> tailBuffer(Output<O> output) {
		if (output == null)
			return new Pair<Buffer, ThroughputPrinter>(null, null);

		Buffer tail = OutputBufferFactory.unwrap(output).createWritableBuffer(
				10000);
		return measureThroughput(tail);
	}

	private Pair<Buffer, ThroughputPrinter> measureThroughput(Buffer tailBuffer) {
		ThroughputPrinter tp;
		Buffer b;
		if (measureThroughput) {
			BufferWriteCounter bc = new BufferWriteCounter(tailBuffer);
			tp = new ThroughputPrinter(bc, name, null, "StreamJitApp",
					"tailBuffer.txt");
			b = bc;
		} else {
			b = tailBuffer;
			tp = null;
		}
		return new Pair<Buffer, ThroughputPrinter>(b, tp);
	}
}
