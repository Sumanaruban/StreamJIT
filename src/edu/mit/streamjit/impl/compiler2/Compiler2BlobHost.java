/*
 * Copyright (c) 2013-2015 Massachusetts Institute of Technology
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
package edu.mit.streamjit.impl.compiler2;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import edu.mit.streamjit.api.StatefulFilter;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.blob.Blob.ExecutionStatistics.ExecutionStatisticsBuilder;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.compiler2.Compiler2.InitDataReadInstruction;
import edu.mit.streamjit.impl.compiler2.Compiler2.SplitJoinRemovalReplayer;
import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.impl.distributed.node.BlobsManagerImpl.StateCallback;
import edu.mit.streamjit.impl.interp.Interpreter;
import edu.mit.streamjit.util.CollectionUtils;
import edu.mit.streamjit.util.Pair;
import edu.mit.streamjit.util.bytecode.methodhandles.Combinators;
import static edu.mit.streamjit.util.bytecode.methodhandles.LookupUtils.findConstructor;
import static edu.mit.streamjit.util.bytecode.methodhandles.LookupUtils.findVirtual;
import edu.mit.streamjit.util.NothrowCallable;
import edu.mit.streamjit.util.bytecode.Module;
import edu.mit.streamjit.util.bytecode.ModuleClassLoader;
import edu.mit.streamjit.util.bytecode.methodhandles.ProxyFactory;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.SwitchPoint;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The actual blob produced by a Compiler2.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 11/1/2013
 */
public class Compiler2BlobHost implements Blob {
	private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();
	private static final MethodHandle MAIN_LOOP = findVirtual(LOOKUP, "mainLoop");
	private static final MethodHandle DO_INIT = findVirtual(LOOKUP, "doInit");
	private static final MethodHandle DO_ADJUST = findVirtual(LOOKUP, "doAdjust");
	private static final MethodHandle THROW_NEW_ASSERTION_ERROR = MethodHandles.filterReturnValue(
			findConstructor(LOOKUP, AssertionError.class, MethodType.methodType(void.class, Object.class)),
			MethodHandles.throwException(void.class, AssertionError.class));
	private static final MethodHandle NOP = Combinators.nop();
	private static final MethodHandle MAIN_LOOP_NOP = MethodHandles.insertArguments(MAIN_LOOP, 1, NOP);

	/* provided by Compiler2 */
	private final ImmutableSet<Worker<?, ?>> workers;
	private final Configuration config;
	private final ImmutableSortedSet<Token> inputTokens, outputTokens;
	private final MethodHandle initCode;
	private final ImmutableList<MethodHandle> steadyStateCode;
	private final ImmutableList<MethodHandle> storageAdjusts;
	/**
	 * Instructions to load items for the init schedule.  unload() will
	 * unload all items, as the init schedule only runs if all reads can
	 * be satisfied.
	 */
	private ImmutableList<ReadInstruction> initReadInstructions;
	/**
	 * Instructions to write output from the init schedule.
	 */
	private ImmutableList<WriteInstruction> initWriteInstructions;
	/**
	 * Instructions to move items from init storage to steady-state storage.
	 */
	private ImmutableList<Runnable> migrationInstructions;
	/**
	 * Instructions to load items for the steady-state schedule.  unload()
	 * will only unload items loaded by load(); the drain instructions will
	 * retrieve any unconsumed items in the storage.
	 */
	private final ImmutableList<ReadInstruction> readInstructions;
	/**
	 * Instructions to write output from the steady-state schedule.
	 */
	private final ImmutableList<WriteInstruction> writeInstructions;
	/**
	 * Instructions to extract items from steady-state storage for transfer
	 * to a DrainData object.  For input storage, this only extracts
	 * unconsumed items (items at live indices except the last throughput
	 * indices, which are covered by the read instructions' unload()).
	 */
	private final ImmutableList<DrainInstruction> drainInstructions;
	private final ImmutableMap<Token, Buffer> precreatedBuffers;
	/* provided by the host */
	private final boolean collectTimings;
	private final ImmutableMap<Token, Integer> minimumBufferCapacity;
	private final ImmutableMap<Token, Integer> minimumSteadyBufferCapacity;
	private final ImmutableMap<Token, Integer> minimumInitBufferCapacity;
	private ImmutableMap<Token, Buffer> buffers;
	private final ImmutableList<Runnable> coreCode;
	private final SwitchPoint sp1 = new SwitchPoint(), sp2 = new SwitchPoint();
	private final Phaser barrier;
	private volatile Runnable drainCallback;
	private volatile DrainData drainData;
	private final ExecutionStatisticsBuilder esBuilder = new ExecutionStatisticsBuilder();
	private final boolean logTimings;

	// The next 4 variables are to compile with drain data sizes and then insert
	// the actual drain data later.
	private volatile boolean needDrainData = false;
	private List<SplitJoinRemovalReplayer> SplitJoinRemovalList = null;
	private Map<Token, Storage> drainDataStorages = null;
	private ImmutableMap<Storage, ConcreteStorage> initStorage = null;

	public Compiler2BlobHost(ImmutableSet<Worker<?, ?>> workers,
			Configuration configuration,
			ImmutableSortedSet<Token> inputTokens,
			ImmutableSortedSet<Token> outputTokens,
			MethodHandle initCode,
			ImmutableList<MethodHandle> steadyStateCode,
			ImmutableList<MethodHandle> storageAdjusts,
			List<ReadInstruction> initReadInstructions,
			List<WriteInstruction> initWriteInstructions,
			List<Runnable> migrationInstructions,
			List<ReadInstruction> readInstructions,
			List<WriteInstruction> writeInstructions,
			List<DrainInstruction> drainInstructions,
			ImmutableMap<Token, Buffer> precreatedBuffers) {
		this.workers = workers;
		this.config = configuration;
		this.inputTokens = inputTokens;
		this.outputTokens = outputTokens;
		this.initCode = initCode;
		this.steadyStateCode = steadyStateCode;
		this.storageAdjusts = storageAdjusts;
		this.initReadInstructions = ImmutableList.copyOf(initReadInstructions);
		this.initWriteInstructions = ImmutableList.copyOf(initWriteInstructions);
		this.migrationInstructions = ImmutableList.copyOf(migrationInstructions);
		this.readInstructions = ImmutableList.copyOf(readInstructions);
		this.writeInstructions = ImmutableList.copyOf(writeInstructions);
		this.drainInstructions = ImmutableList.copyOf(drainInstructions);
		this.precreatedBuffers = precreatedBuffers;

		this.collectTimings = config.getExtraData("timings") != null ? (Boolean)config.getExtraData("timings") : false;
		this.logTimings = this.collectTimings || Options.logEventTime;

		this.minimumBufferCapacity = getMinCapacity(Iterables.concat(this.initReadInstructions, this.readInstructions), Iterables.concat(this.initWriteInstructions, this.writeInstructions));
		this.minimumSteadyBufferCapacity = getMinCapacity(this.readInstructions, this.writeInstructions);
		this.minimumInitBufferCapacity = getMinCapacity(this.initReadInstructions, this.initWriteInstructions);
		MethodHandle mainLoop = MAIN_LOOP.bindTo(this),
				doInit = DO_INIT.bindTo(this),
				doAdjust = DO_ADJUST.bindTo(this),
				mainLoopNop = MAIN_LOOP_NOP.bindTo(this);
		ProxyFactory pf = new ProxyFactory(new ModuleClassLoader(new Module()));
		ImmutableList.Builder<Runnable> coreCodeRunnables = ImmutableList.builder();
		for (int i = 0; i < this.steadyStateCode.size(); ++i) {
			MethodHandle ssc = this.steadyStateCode.get(i);
			MethodHandle code = sp1.guardWithTest(mainLoopNop, sp2.guardWithTest(mainLoop.bindTo(ssc), NOP));
			coreCodeRunnables.add(pf.createProxy("Proxy"+i, ImmutableMap.of("run", code), Runnable.class));
		}
		this.coreCode = coreCodeRunnables.build();
		MethodHandle throwAE = THROW_NEW_ASSERTION_ERROR.bindTo("Can't happen! Barrier action reached after draining?");
		MethodHandle barrierAction = sp1.guardWithTest(doInit, sp2.guardWithTest(doAdjust, throwAE));
		final Runnable onAdvanceRunnable = pf.createProxy("BarrierAction", ImmutableMap.of("run", barrierAction), Runnable.class);
		this.barrier = new Phaser(coreCode.size()) {
			@Override
			protected boolean onAdvance(int phase, int registeredParties) {
				onAdvanceRunnable.run();
				return super.onAdvance(phase, registeredParties);
			}
		};
	}

	private ImmutableMap<Token, Integer> getMinCapacity(
			Iterable<ReadInstruction> ri,
			Iterable<WriteInstruction> wi) {
		List<Map<Token, Integer>> capacityRequirements = new ArrayList<>();
		for (ReadInstruction i : ri)
			capacityRequirements.add(i.getMinimumBufferCapacity());
		for (WriteInstruction i : wi)
			capacityRequirements.add(i.getMinimumBufferCapacity());
		return CollectionUtils.union((key, value) -> Collections.max(value), capacityRequirements);
	}

	@Override
	public Set<Worker<?, ?>> getWorkers() {
		return workers;
	}

	@Override
	public Set<Token> getInputs() {
		return inputTokens;
	}

	@Override
	public Set<Token> getOutputs() {
		return outputTokens;
	}

	@Override
	public int getMinimumBufferCapacity(Token token) {
		if (!inputTokens.contains(token) && !outputTokens.contains(token))
			throw new IllegalArgumentException(token.toString()+" not an input or output of this blob");
		return minimumBufferCapacity.get(token);
	}

	@Override
	public int getMinimumSteadyBufferCapacity(Token token) {
		if (!inputTokens.contains(token) && !outputTokens.contains(token))
			throw new IllegalArgumentException(token.toString()+" not an input or output of this blob");
		return minimumSteadyBufferCapacity.get(token);
	}

	@Override
	public int getMinimumInitBufferCapacity(Token token) {
		if (!inputTokens.contains(token) && !outputTokens.contains(token))
			throw new IllegalArgumentException(token.toString()+" not an input or output of this blob");
		return minimumInitBufferCapacity.get(token);
	}

	@Override
	public void installBuffers(Map<Token, Buffer> buffers) {
		if (this.buffers != null)
			throw new IllegalStateException("installBuffers called more than once");
		buffers = CollectionUtils.union(buffers, precreatedBuffers);
		ImmutableMap.Builder<Token, Buffer> builder = ImmutableMap.builder();
		for (Token t : Sets.union(inputTokens, outputTokens)) {
			Buffer b = buffers.get(t);
			if (b == null)
				throw new IllegalArgumentException("no buffer for token "+t);
			if (b.capacity() < getMinimumBufferCapacity(t))
				throw new IllegalArgumentException(String.format(
						"buffer for %s has capacity %d, but minimum is %d",
						t, b.capacity(), getMinimumBufferCapacity(t)));
			builder.put(t, b);
		}
		this.buffers = builder.build();

		for (ReadInstruction i : Iterables.concat(this.initReadInstructions, this.readInstructions))
			i.init(this.buffers);
		for (WriteInstruction i : Iterables.concat(this.initWriteInstructions, this.writeInstructions))
			i.init(this.buffers);
	}

	@Override
	public int getCoreCount() {
		return coreCode.size();
	}

	@Override
	public Runnable getCoreCode(int core) {
		return coreCode.get(core);
	}

	@Override
	public void drain(Runnable callback) {
		drainCallback = callback;
	}

	@Override
	public DrainData getDrainData() {
		return drainData;
	}

	private void mainLoop(MethodHandle coreCode) throws Throwable {
		try {
			coreCode.invokeExact();
			barrier.arriveAndAwaitAdvance();
		} catch (Throwable ex) {
			barrier.forceTermination();
			SwitchPoint.invalidateAll(new SwitchPoint[]{sp1, sp2});
			ex.printStackTrace();
			throw ex;
		}
	}

	private void doInit() throws Throwable {
		Stopwatch initTime = null;
		if (logTimings)
			initTime = Stopwatch.createStarted();

		if(needDrainData)
			throw new IllegalStateException("Expecting drain data. insertDrainData first.");

		for (int i = 0; i < initReadInstructions.size(); ++i) {
			ReadInstruction inst = initReadInstructions.get(i);
			while (!inst.load())
				if (isDraining()) {
					doDrain(initReadInstructions.subList(0, i), ImmutableList.<DrainInstruction>of());
					return;
				}
		}

		initCode.invoke();

		doWrites(initWriteInstructions);

		for (Runnable r : migrationInstructions)
			r.run();

		//Show the GC we won't use these anymore.
		initReadInstructions = null;
		initWriteInstructions = null;
		migrationInstructions = null;

		readOrDrain();

		SwitchPoint.invalidateAll(new SwitchPoint[]{sp1});

		if (logTimings){
			initTime.stop();
			esBuilder.initTime(initTime.elapsed(TimeUnit.MILLISECONDS));
		}
	}

	private final Stopwatch adjustTime = Stopwatch.createUnstarted();
	private int adjustCount = 0;
	private void doAdjust() throws Throwable {
		if (logTimings)
			adjustTime.start();

		++adjustCount;
		doWrites(writeInstructions);

		for (MethodHandle h : storageAdjusts)
			h.invokeExact();

		if(requireState)
			stateSend();

		readOrDrain();

		if (logTimings)
			adjustTime.stop();
	}

	private void stateSend() {
		if (stateAdjustCount == adjustCount)
			sendState(getState(drainInstructions));
		else if (stateAdjustCount < adjustCount) {
			throw new IllegalStateException(
					String.format(
							"adjustCount(=%d) has already passed the stateAdjustCount(=%d).",
							adjustCount, stateAdjustCount));
		}
	}

	/**
	 * Handle short writes round-robin so other Blobs can make progress (thus
	 * freeing up buffer space).
	 * @param writes the write instructions to execute
	 */
	private static void doWrites(List<? extends NothrowCallable<Boolean>> writeInstructions) {
		ArrayList<NothrowCallable<Boolean>> writes = new ArrayList<>(writeInstructions);
		while (!writes.isEmpty())
			for (Iterator<NothrowCallable<Boolean>> it = writes.iterator(); it.hasNext();) {
				NothrowCallable<Boolean> write = it.next();
				if (write.call())
					it.remove();
			}
	}

	private void readOrDrain() {
		for (int i = 0; i < readInstructions.size(); ++i) {
			ReadInstruction inst = readInstructions.get(i);
			while (!inst.load())
				if (isDraining()) {
					doDrain(readInstructions.subList(0, i), drainInstructions);
					return;
				}
		}
	}

	/**
	 * Extracts elements from storage and puts them in a DrainData for an
	 * interpreter blob.
	 * @param reads read instructions whose load() completed (thus requiring
	 * unload())
	 * @param drains drain instructions, if we're in the steady-state, or an
	 * empty list if we didn't complete init
	 */
	private void doDrain(List<ReadInstruction> reads, List<DrainInstruction> drains) {
		Stopwatch drainTime = null;
		if (logTimings)
			drainTime = Stopwatch.createStarted();

		List<Map<Token, Object[]>> data = new ArrayList<>(reads.size() + drains.size());
		for (ReadInstruction i : reads)
			data.add(i.unload());
		for (DrainInstruction i : drains)
			data.add(i.call());
		ImmutableMap<Token, List<Object>> mergedData = CollectionUtils.union((key, value) -> {
			int size = 0;
			for (Object[] v : value)
				size += v.length;
			List<Object> data1 = new ArrayList<>(size);
			for (Object[] v : value)
				data1.addAll(Arrays.asList(v));
			return data1;
		}, data);
		//Try once to write data on output edges, then let the interpreter handle it.
		Predicate<Token> isOutput = Predicates.in(getOutputs());
		for (Map.Entry<Token, List<Object>> e : Maps.filterKeys(mergedData, isOutput).entrySet()) {
			final Buffer b = buffers.get(e.getKey());
			final Object[] d = e.getValue().toArray();
			int written = b.write(d, 0, d.length);
			//Remove the data we wrote.
			e.getValue().subList(0, written).clear();
		}
		DrainData forInterp = new DrainData(mergedData,
				//We put state back in the workers via StateHolders, which are
				//DrainInstructions, so no state in the DrainData.  (It will be
				//in the DrainData produced by the interpreter blob, so
				//distributed will still see it.)
				ImmutableTable.<Integer, String, Object>of());

		Interpreter.InterpreterBlobFactory interpFactory = new Interpreter.InterpreterBlobFactory();
		Blob interp = interpFactory.makeBlob(workers, interpFactory.getDefaultConfiguration(workers), 1, forInterp);
		interp.installBuffers(buffers);
		Runnable interpCode = interp.getCoreCode(0);
		final AtomicBoolean interpFinished = new AtomicBoolean();
		interp.drain(() -> interpFinished.set(true));
		while (!interpFinished.get())
			interpCode.run();
		this.drainData = getState(drains);

		SwitchPoint.invalidateAll(new SwitchPoint[]{sp1, sp2});

		if (logTimings) {
			drainTime.stop();
			esBuilder.drainTime(drainTime.elapsed(TimeUnit.MILLISECONDS));
			esBuilder.adjustTime(adjustTime.elapsed(TimeUnit.MILLISECONDS));
			esBuilder.adjustCount(adjustCount);
		}
		drainCallback.run();
		if (collectTimings)
			esBuilder.build().print();
	}

	private boolean isDraining() {
		return drainCallback != null;
	}

	public static interface ReadInstruction {
		public void init(Map<Token, Buffer> buffers);
		public Map<Token, Integer> getMinimumBufferCapacity();
		/**
		 * Loads data items from a Buffer into ConcreteStorage.  Returns true
		 * if the load was successful.  This operation is atomic; either all the
		 * data items are loaded (and load() returns true), or none are and it
		 * returns false.
		 * @return true iff the load succeeded.
		 */
		public boolean load();
		/**
		 * Retrieves data items from a ConcreteStorage.  To be called only after
		 * load() returns true, before executing a steady-state iteration.  This
		 * method only retrieves items loaded by load(); a drain instruction
		 * will retrieve other data.
		 * @return
		 */
		public Map<Token, Object[]> unload();
	}

	public static interface WriteInstruction extends NothrowCallable<Boolean> {
		public void init(Map<Token, Buffer> buffers);
		public Map<Token, Integer> getMinimumBufferCapacity();
		/**
		 * Writes data items to the output Buffer.  Returns true if all data
		 * items were written, or false if more writing is necessary.
		 * @return true iff all data was written
		 */
		@Override
		public Boolean call();
	}

	public static interface DrainInstruction extends NothrowCallable<Map<Token, Object[]>> {
		@Override
		public Map<Token, Object[]> call();
	}

	@Override
	public ExecutionStatistics getExecutionStatistics() {
		return esBuilder.build();
	}

	/**
	 * The methods below are to compile with sizes.
	 * @author Sumanan
	 * 25 Aug, 2015
	 */

	void setDrainDataVariables(boolean needDrainData,
			List<SplitJoinRemovalReplayer> SplitJoinRemovalList,
			Map<Token, Storage> drainDataStorages,
			ImmutableMap<Storage, ConcreteStorage> initStorage) {
		this.needDrainData = needDrainData;
		this.SplitJoinRemovalList = SplitJoinRemovalList;
		this.drainDataStorages = drainDataStorages;
		this.initStorage = initStorage;
	}

	@Override
	public void insertDrainData(DrainData initialState)
			throws IllegalStateException {
		if (initialState == null && !needDrainData)
			return;
		if (!needDrainData)
			throw new IllegalStateException("Can not insert drain data.");
		ImmutableMap<Token, ImmutableList<Object>> initialStateDataMap = replaceDummyData(initialState);
		for (SplitJoinRemovalReplayer replayer : SplitJoinRemovalList)
			replayer.replay();
		ReadInstruction ri = new InitDataReadInstruction(initStorage,
				initialStateDataMap);
		updateInitReadInstructions(ri);
		releaseDrainDataVariables();
	}

	private ImmutableMap<Token, ImmutableList<Object>> replaceDummyData(
			DrainData initialState) {
		// TODO: [29-8-2015] - Send drain data that belongs to this blob and
		// then enable the following check.
		// if (initialState.getData().size() != drainDataStorages.size())
		// throw new IllegalStateException(
		// String.format(
		// "Miss match between initialDrainDataBufferSizes(%d) and the actual drain data(%d)",
		// initialState.getData().size(),
		// drainDataStorages.size()));
		ImmutableMap.Builder<Token, ImmutableList<Object>> initialStateDataMapBuilder = ImmutableMap
				.builder();
		for (Map.Entry<Token, Storage> en : drainDataStorages.entrySet()) {
			Storage s = en.getValue();
			Token t = en.getKey();
			ImmutableList<Object> data = initialState.getData(t);
			int storageDataSize = s.initialData().get(0).first.size();
			if (data == null)
				throw new IllegalStateException(String.format(
						"Actual drain data of %s is null", t));
			if (storageDataSize != data.size())
				throw new IllegalStateException(
						String.format(
								"%s:Initial drain data size = %d, Actual drain data size = %d",
								t, storageDataSize, data.size()));
			Pair<ImmutableList<Object>, IndexFunction> newPair = new Pair<>(
					data, s.initialData().get(0).second);
			s.initialData().set(0, newPair);
			initialStateDataMapBuilder.put(t, data);
		}
		return initialStateDataMapBuilder.build();
	}

	private void updateInitReadInstructions(ReadInstruction firstRI) {
		List<ReadInstruction> initRIs = new ArrayList<>();
		initRIs.add(firstRI);
		for (int i = 1; i < initReadInstructions.size(); i++) {
			initRIs.add(initReadInstructions.get(i));
		}
		this.initReadInstructions = ImmutableList.copyOf(initRIs);
	}


	/**
	 * Set to null so that GC will clean these variable.
	 */
	private void releaseDrainDataVariables() {
		this.needDrainData = false;
		this.SplitJoinRemovalList = null;
		this.drainDataStorages = null;
		this.initStorage = null;
	}

	ImmutableMap<Token, Integer> ddSizes = null;

	public ImmutableMap<Token, Integer> getDDSizes() {
		return ddSizes;
	}

	void setDDSizes(ImmutableMap<Token, Integer> ddSizes) {
		this.ddSizes = ddSizes;
	}

	int stateAdjustCount;
	StateCallback stateCallback;
	volatile boolean requireState = false;

	public void requestState(int adjustCount, StateCallback callback) {
		stateAdjustCount = adjustCount;
		stateCallback = callback;
		requireState = true;
	}

	private void sendState(DrainData state) {
		stateCallback.sendState(state);
		requireState = false;
		stateCallback = null;
		stateAdjustCount = 0;
	}

	private DrainData getState(List<DrainInstruction> drains) {
		List<Map<Token, Object[]>> data = new ArrayList<>(drains.size());
		for (DrainInstruction i : drains)
			if (!(i instanceof StateHolder))
				data.add(i.call());

		ImmutableMap<Token, List<Object>> mergedData = CollectionUtils.union((
				key, value) -> {
			int size = 0;
			for (Object[] v : value)
				size += v.length;
			List<Object> data1 = new ArrayList<>(size);
			for (Object[] v : value)
				data1.addAll(Arrays.asList(v));
			return data1;
		}, data);
		return new DrainData(mergedData, state());
	}

	/**
	 * Copied form {@link Interpreter#getDrainData}
	 * 
	 * @return
	 */
	private ImmutableTable<Integer, String, Object> state() {
		ImmutableTable.Builder<Integer, String, Object> stateBuilder = ImmutableTable
				.builder();
		for (Worker<?, ?> worker : workers) {
			if (!(worker instanceof StatefulFilter))
				continue;
			int id = Workers.getIdentifier(worker);
			for (Class<?> klass = worker.getClass(); !klass
					.equals(StatefulFilter.class); klass = klass
					.getSuperclass()) {
				for (Field f : klass.getDeclaredFields()) {
					if ((f.getModifiers() & (Modifier.STATIC | Modifier.FINAL)) != 0)
						continue;
					f.setAccessible(true);
					try {
						stateBuilder.put(id, f.getName(), f.get(worker));
					} catch (IllegalArgumentException | IllegalAccessException ex) {
						throw new AssertionError(ex);
					}
				}
			}
		}
		return stateBuilder.build();
	}
}
