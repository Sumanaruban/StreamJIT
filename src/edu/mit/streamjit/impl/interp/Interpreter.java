package edu.mit.streamjit.impl.interp;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Iterables;
import edu.mit.streamjit.api.IllegalStreamGraphException;
import edu.mit.streamjit.api.Rate;
import edu.mit.streamjit.api.StatefulFilter;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.SwitchParameter;
import edu.mit.streamjit.impl.common.IOInfo;
import edu.mit.streamjit.impl.common.MessageConstraint;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.util.EmptyRunnable;
import edu.mit.streamjit.util.Pair;
import edu.mit.streamjit.util.ReflectionUtils;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An Interpreter interprets a section of a stream graph.  An Interpreter's
 * interpret() method will run a pull schedule on the "bottom-most" filters in
 * the section (filters that are not predecessor of other filters in the blob),
 * firing them as many times as possible.
 *
 * An Interpreter has input and output channels, identified by opaque Token
 * objects.  An input channel is any channel from a worker not in the
 * Interpreter's stream graph section to one inside it, and an output channel,
 * vice versa.  The Interpreter expects these channels to already be installed
 * on the workers.
 *
 * To communicate between two Interpreter instances on the same machine, use
 * a synchronized channel implementation to connect outputs of one interpreter
 * to the inputs of the other.
 *
 * To communicate between interpreter instances on different machines, have a
 * thread on one machine poll() on output channels (if you can afford one thread
 * per output, use a blocking channel implementation) and send that data to the
 * other machine, then use threads on the other machine to read the data and
 * offer() it to input channels. It's tempting to put the send/receive in the
 * channel implementations themselves, but this may block the interpreter on
 * I/O, and makes implementing peek() on the receiving side tricky.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/22/2013
 */
public class Interpreter implements Blob {
	private final ImmutableSet<Worker<?, ?>> workers, sinks;
	private final Configuration config;
	private final ImmutableSet<Token> inputs, outputs;
	private final ImmutableMap<Token, Integer> minimumBufferSizes;
	/**
	 * Maps workers to all constraints of which they are recipients.
	 */
	private final Map<Worker<?, ?>, List<MessageConstraint>> constraintsForRecipient = new IdentityHashMap<>();
	/**
	 * When running normally, null.  After drain() has been called, contains the
	 * callback we should execute.  After the callback is executed, becomes an
	 * empty Runnable that we execute in place of interpret().
	 */
	private final AtomicReference<Runnable> callback = new AtomicReference<>();
	private final ImmutableSet<IOInfo> ioinfo;
	/**
	 * Maps Channels to the buffers they correspond to.  Output channels are
	 * flushed to output buffers; input channels are checked for data when we
	 * can't fire a source.
	 */
	private ImmutableMap<Channel<?>, Buffer> inputBuffers, outputBuffers;
	public Interpreter(Iterable<Worker<?, ?>> workersIter, Iterable<MessageConstraint> constraintsIter, Configuration config) {
		this(workersIter, constraintsIter, config, null);
	}
	public Interpreter(Iterable<Worker<?, ?>> workersIter, Iterable<MessageConstraint> constraintsIter, Configuration config, DrainData initialState) {
		this.workers = ImmutableSet.copyOf(workersIter);
		this.sinks = Workers.getBottommostWorkers(workers);
		this.config = config;

		//Validate constraints.
		for (MessageConstraint mc : constraintsIter)
			if (this.workers.contains(mc.getSender()) != this.workers.contains(mc.getRecipient()))
				throw new IllegalArgumentException("Constraint crosses interpreter boundary: "+mc);
		for (MessageConstraint constraint : constraintsIter) {
			Worker<?, ?> recipient = constraint.getRecipient();
			List<MessageConstraint> constraintList = constraintsForRecipient.get(recipient);
			if (constraintList == null) {
				constraintList = new ArrayList<>();
				constraintsForRecipient.put(recipient, constraintList);
			}
			constraintList.add(constraint);
		}
		//Create channels.
		SwitchParameter<ChannelFactory> parameter = this.config.getParameter("channelFactory", SwitchParameter.class, ChannelFactory.class);
		ChannelFactory factory = parameter.getValue();
		for (IOInfo info : IOInfo.internalEdges(workers)) {
			Channel channel = factory.makeChannel((Worker)info.upstream(), (Worker)info.downstream());
			Workers.getOutputChannels(info.upstream()).set(info.getUpstreamChannelIndex(), channel);
			Workers.getInputChannels(info.downstream()).set(info.getDownstreamChannelIndex(), channel);
			if (initialState != null) {
				for (Object o : initialState.getData(info.token()))
					channel.push(o);
			}
		}
		ImmutableSet.Builder<Token> inputTokens = ImmutableSet.builder(), outputTokens = ImmutableSet.builder();
		ImmutableMap.Builder<Token, Integer> minimumBufferSize = ImmutableMap.builder();
		for (IOInfo info : IOInfo.externalEdges(workers)) {
			Channel channel = factory.makeChannel((Worker)info.upstream(), (Worker)info.downstream());
			List channelList;
			int index;
			if (info.isInput()) {
				channelList = Workers.getInputChannels(info.downstream());
				index = info.getDownstreamChannelIndex();
			} else {
				channelList = Workers.getOutputChannels(info.upstream());
				index = info.getUpstreamChannelIndex();
			}
			if (channelList.isEmpty())
				channelList.add(channel);
			else
				channelList.set(index, channel);

			(info.isInput() ? inputTokens : outputTokens).add(info.token());
			if (info.isInput()) {
				Worker<?, ?> w = info.downstream();
				int chanIdx = info.getDownstreamChannelIndex();
				int rate = Math.max(w.getPeekRates().get(chanIdx).max(), w.getPopRates().get(chanIdx).max());
				minimumBufferSize.put(info.token(), rate);

				//Only fill input channels -- any existing output will be used
				//as input in the downstream blob.
				if (initialState != null)
					for (Object o : initialState.getData(info.token()))
						channel.push(o);
			}
		}

		if (initialState != null) {
			for (Worker<?, ?> w : workers) {
				int id = Workers.getIdentifier(w);
				ImmutableSet<Field> fields = ReflectionUtils.getAllFields(w.getClass());
				for (Map.Entry<String, Object> e : initialState.getWorkerState(id).entrySet())
					for (Field f : fields)
						if (!Modifier.isFinal(f.getModifiers()) && !Modifier.isStatic(f.getModifiers())
								&& f.getName().equals(e.getKey())) {
							f.setAccessible(true);
							try {
								f.set(w, e.getValue());
							} catch (IllegalAccessException ex) {
								throw new AssertionError(ex);
							}
						}
			}
		}

		this.inputs = inputTokens.build();
		this.outputs = outputTokens.build();
		this.minimumBufferSizes = minimumBufferSize.build();
		this.ioinfo = IOInfo.externalEdges(workers);
	}

	@Override
	public Set<Token> getInputs() {
		return inputs;
	}

	@Override
	public Set<Token> getOutputs() {
		return outputs;
	}

	@Override
	public int getMinimumBufferCapacity(Token token) {
		Integer i = minimumBufferSizes.get(token);
		return (i != null) ? i : 1;
	}

	@Override
	public void installBuffers(Map<Token, Buffer> buffers) {
		ImmutableMap.Builder<Channel<?>, Buffer> inputBufferBuilder = ImmutableMap.builder(), outputBufferBuilder = ImmutableMap.builder();
		for (IOInfo info : ioinfo) {
			Buffer buffer = buffers.get(info.token());
			if (buffer != null)
				(info.isInput() ? inputBufferBuilder : outputBufferBuilder).put(info.channel(), buffer);
		}
		this.inputBuffers = inputBufferBuilder.build();
		this.outputBuffers = outputBufferBuilder.build();
	}

	@Override
	public ImmutableSet<Worker<?, ?>> getWorkers() {
		return workers;
	}

	@Override
	public int getCoreCount() {
		return 1;
	}

	@Override
	public Runnable getCoreCode(int core) {
		checkElementIndex(core, getCoreCount());
		return new Runnable() {
			@Override
			public void run() {
				Runnable callback = Interpreter.this.callback.get();
				if (callback == null)
					interpret();
				else {
					//Do any remaining work.
					interpret();
					//Run the callback (which may be empty).
					callback.run();
					//Set the callback to empty so we only run it once.
					Interpreter.this.callback.set(new EmptyRunnable());
				}
			}
		};
	}

	@Override
	public void drain(Runnable callback) {
		//Set the callback; the core code will run it after its next interpret().
		if (!this.callback.compareAndSet(null, checkNotNull(callback)))
			throw new IllegalStateException("drain() called multiple times");
	}

	@Override
	public DrainData getDrainData() {
		ImmutableMap.Builder<Token, ImmutableList<Object>> dataBuilder = ImmutableMap.builder();
		for (IOInfo info : IOInfo.allEdges(workers))
			dataBuilder.put(info.token(), ImmutableList.copyOf(info.channel()));

		ImmutableTable.Builder<Integer, String, Object> stateBuilder = ImmutableTable.builder();
		for (Worker<?, ?> worker : workers) {
			if (!(worker instanceof StatefulFilter))
				continue;
			int id = Workers.getIdentifier(worker);
			for (Class<?> klass = worker.getClass(); !klass.equals(StatefulFilter.class); klass = klass.getSuperclass()) {
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

		return new DrainData(dataBuilder.build(), stateBuilder.build());
	}

	public static class InterpreterBlobFactory implements BlobFactory {
		public InterpreterBlobFactory() {}
		@Override
		public Blob makeBlob(Set<Worker<?, ?>> workers, Configuration config, int maxNumCores, DrainData initialState) {
			//TODO: get the constraints!
			return new Interpreter(workers, Collections.<MessageConstraint>emptyList(), config, initialState);
		}
		@Override
		public Configuration getDefaultConfiguration(Set<Worker<?, ?>> workers) {
			//TODO: more choices
			List<ChannelFactory> channelFactories = Arrays.<ChannelFactory>asList(new ChannelFactory() {
				@Override
				public <E> Channel<E> makeChannel(Worker<?, E> upstream, Worker<E, ?> downstream) {
					return new ArrayChannel<>();
				}
				@Override
				public boolean equals(Object other) {
					return getClass() == other.getClass();
				}
				@Override
				public int hashCode() {
					return 0;
				}
			});
			Configuration.SwitchParameter<ChannelFactory> facParam
					= new Configuration.SwitchParameter<>("channelFactory",
					ChannelFactory.class, channelFactories.get(0), channelFactories);
			return Configuration.builder().addParameter(facParam).build();
		}
		@Override
		public boolean equals(Object o) {
			//All InterpreterBlobFactory instances are equal.
			return o != null && getClass() == o.getClass();
		}
		@Override
		public int hashCode() {
			return 9001;
		}
	}

	/**
	 * Interprets the stream graph section by running a pull schedule on the
	 * "bottom-most" workers in the section (firing predecessors as required if
	 * possible) until no more progress can be made.  Returns true if any
	 * "bottom-most" workers were fired.  Note that returning false does not
	 * mean no workers were fired -- some predecessors might have been fired,
	 * but others prevented the "bottom-most" workers from firing.
	 * @return true iff progress was made
	 */
	public boolean interpret() {
		//Fire each sink once if possible, then repeat until we can't fire any
		//sinks.
		boolean fired, everFired = false;
		do {
			fired = false;
			for (Worker<?, ?> sink : sinks)
				everFired |= fired |= pull(sink);

			//Because we're using unbounded Channels, if we keep getting input,
			//we'll keep firing our workers.  We need to flush output to buffers
			//to prevent memory exhaustion and starvation of the next Blob.
			pushOutputs();
		} while (fired);

		return everFired;
	}

	private void pushOutputs() {
		//Flush in a round-robin manner to avoid deadlocks where our consumer is
		//blocked on another one of our channels.
		List<Channel<?>> check = new ArrayList<>(outputBuffers.keySet());
		while (!check.isEmpty()) {
			for (int i = check.size()-1; i >= 0; --i) {
				Channel<?> channel = check.get(i);
				if (channel.isEmpty()) {
					check.remove(i);
					continue;
				}

				Buffer buffer = outputBuffers.get(channel);
				int room = Math.min(buffer.capacity() - buffer.size(), channel.size());
				if (room == 0)
					continue;

				Object[] data = new Object[room];
				for (int j = 0; j < data.length; ++j)
					data[j] = channel.pop();
				int written = 0;
				int tries = 0;
				while (written < data.length) {
					written += buffer.write(data, written, data.length - written);
					++tries;
				}
				assert tries == 1 : "We checked we have space, but still needed "+tries+" tries";
			}
		}
	}

	/**
	 * Fires upstream filters just enough to allow worker to fire, or returns
	 * false if this is impossible.
	 *
	 * This is an implementation of Figure 3-12 from Bill's thesis.
	 *
	 * @param worker the worker to fire
	 * @return true if the worker fired, false if it didn't
	 */
	private boolean pull(Worker<?, ?> worker) {
		//This stack holds all the unsatisfied workers we've encountered
		//while trying to fire the argument.
		Deque<Worker<?, ?>> stack = new ArrayDeque<>();
		stack.push(worker);
		recurse:
		while (!stack.isEmpty()) {
			Worker<?, ?> current = stack.element();
			assert workers.contains(current) : "Executing outside stream graph section";
			//If we're already trying to fire current, current depends on
			//itself, so throw.  TODO: explain which constraints are bad?
			//We have to pop then push so contains can't just find the top
			//of the stack every time.  (no indexOf(), annoying)
			stack.pop();
			if (stack.contains(current))
				throw new IllegalStreamGraphException("Unsatisfiable message constraints", current);
			stack.push(current);

			//Execute predecessors based on data dependencies.
			int channel = indexOfUnsatisfiedChannel(current);
			if (channel != -1) {
				if (!workers.contains(Iterables.get(Workers.getPredecessors(current), channel, null))) {
					//Try to get an item for this channel.
					Channel unsatChannel = Workers.getInputChannels(current).get(channel);
					Buffer buffer = inputBuffers.get(unsatChannel);
					//TODO: compute how much and use readAll()
					Object item = buffer.read();
					if (item != null) {
						unsatChannel.push(item);
						continue recurse; //try again
					} else
						return false; //Couldn't fire.
				}

				//Otherwise, recursively fire the worker blocking us.
				stack.push(Workers.getPredecessors(current).get(channel));
				continue recurse;
			}

			List<MessageConstraint> constraints = constraintsForRecipient.get(current);
			if (constraints != null)
				//Execute predecessors based on message dependencies; that is,
				//execute any filter that might send a message to the current
				//worker for delivery just prior to its next firing, to ensure
				//that delivery cannot be missed.
				for (MessageConstraint constraint : constraintsForRecipient.get(current)) {
					Worker<?, ?> sender = constraint.getSender();
					long deliveryTime = constraint.getDeliveryTime(Workers.getExecutions(sender));
					//If deliveryTime == current.getExecutions() + 1, it's for
					//our next execution.  (If it's <= current.getExecutions(),
					//we already missed it!)
					if (deliveryTime <= (Workers.getExecutions(current) + 1)) {
						//We checked in our constructor that message constraints
						//do not cross the interpreter boundary.  Assert that.
						assert workers.contains(sender);
						stack.push(sender);
						continue recurse;
					}
				}

			Workers.doWork(current);
			afterFire(current);
			stack.pop(); //return from the recursion
		}

		//Stack's empty: we fired the argument.
		return true;
	}

	/**
	 * Searches the given worker's input channels for one that requires more
	 * elements before the worker can fire, returning the index of the found
	 * channel or -1 if the worker can fire.
	 */
	private <I, O> int indexOfUnsatisfiedChannel(Worker<I, O> worker) {
		List<Channel<? extends I>> channels = Workers.getInputChannels(worker);
		List<Rate> peekRates = worker.getPeekRates();
		List<Rate> popRates = worker.getPopRates();
		for (int i = 0; i < channels.size(); ++i) {
			Rate peek = peekRates.get(i), pop = popRates.get(i);
			if (peek.max() == Rate.DYNAMIC || pop.max() == Rate.DYNAMIC)
				throw new UnsupportedOperationException("Unbounded input rates not yet supported");
			int required = Math.max(peek.max(), pop.max());
			if (channels.get(i).size() < required)
				return i;
		}
		return -1;
	}

	/**
	 * Called after the given worker is fired.  Provided for the debug
	 * interpreter to check rate declarations.
	 * @param worker the worker that just fired
	 */
	protected void afterFire(Worker<?, ?> worker) {}
}
