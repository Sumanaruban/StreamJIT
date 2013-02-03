package org.mit.jstreamit;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

/**
 * A PrimitiveWorker encapsulates a primitive worker in the stream graph (a
 * Filter, Splitter or Joiner) and manages its connections to other workers in
 * support of the interpreter.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/19/2012
 */
/* package private */ abstract class PrimitiveWorker<I, O> implements StreamElement<I, O> {
	private List<PrimitiveWorker<?, ? extends I>> predecessors = new ArrayList<>(1);
	private List<PrimitiveWorker<? super O, ?>> successors = new ArrayList<>(1);
	private List<Channel<? extends I>> inputChannels = new ArrayList<>(1);
	private List<Channel<? super O>> outputChannels = new ArrayList<>(1);
	private final List<Portal.Message> messages = new ArrayList<>();
	private long executions;

	void addPredecessor(PrimitiveWorker<?, ? extends I> predecessor, Channel<? extends I> channel) {
		if (predecessor == null || channel == null)
			throw new NullPointerException();
		if (predecessor == this)
			throw new IllegalArgumentException();
		predecessors.add(predecessor);
		inputChannels.add(channel);
	}
	void addSuccessor(PrimitiveWorker<? super O, ?> successor, Channel<? super O> channel) {
		if (successor == null || channel == null)
			throw new NullPointerException();
		if (successor == this)
			throw new IllegalArgumentException();
		successors.add(successor);
		outputChannels.add(channel);
	}

	List<PrimitiveWorker<?, ? extends I>> getPredecessors() {
		return predecessors;
	}

	List<PrimitiveWorker<? super O, ?>> getSuccessors() {
		return successors;
	}

	List<Channel<? extends I>> getInputChannels() {
		return inputChannels;
	}

	List<Channel<? super O>> getOutputChannels() {
		return outputChannels;
	}

	enum StreamPosition {
		UPSTREAM, DOWNSTREAM, EQUAL, INCOMPARABLE;
		public StreamPosition opposite() {
			switch (this) {
				case UPSTREAM:
					return DOWNSTREAM;
				case DOWNSTREAM:
					return UPSTREAM;
				case EQUAL:
				case INCOMPARABLE:
					return this;
				default:
					throw new AssertionError();
			}
		}
	}

	/**
	 * Compare the position of this worker to the given other worker. Returns
	 * UPSTREAM if this worker is upstream of the other worker, DOWNSTREAM if
	 * this worker is downstream of the other worker, EQUAL if this worker is
	 * the other worker (reference equality), or INCOMPARABLE if this worker is
	 * neither upstream nor downstream of the other worker (e.g., this and other
	 * are in parallel branches of a splitjoin).
	 *
	 * Obviously, this method requires the workers in a stream graph be
	 * connected.  See ConnectPrimitiveWorkersVisitor.
	 * @param other the other worker to compare against
	 * @return a StreamPosition
	 */
	StreamPosition compareStreamPosition(PrimitiveWorker<?, ?> other) {
		if (other == null)
			throw new NullPointerException();
		if (this == other)
			return StreamPosition.EQUAL;

		Queue<PrimitiveWorker<?, ?>> frontier = new ArrayDeque<>();
		//BFS downstream.
		frontier.add(this);
		while (!frontier.isEmpty()) {
			PrimitiveWorker<?, ?> cur = frontier.remove();
			if (cur == other)
				return StreamPosition.UPSTREAM;
			frontier.addAll(cur.getSuccessors());
		}

		//BFS upstream.
		frontier.add(this);
		while (!frontier.isEmpty()) {
			PrimitiveWorker<?, ?> cur = frontier.remove();
			if (cur == other)
				return StreamPosition.DOWNSTREAM;
			frontier.addAll(cur.getPredecessors());
		}

		return StreamPosition.INCOMPARABLE;
	}

	/**
	 * Returns a set of all predecessors of this worker.
	 * @return a set of all predecessors of this worker
	 */
	Set<PrimitiveWorker<?, ?>> getAllPredecessors() {
		Set<PrimitiveWorker<?, ?>> closed = new HashSet<>();
		Queue<PrimitiveWorker<?, ?>> frontier = new ArrayDeque<>();
		frontier.addAll(this.getPredecessors());
		while (!frontier.isEmpty()) {
			PrimitiveWorker<?, ?> cur = frontier.remove();
			closed.add(cur);
			frontier.addAll(cur.getPredecessors());
		}
		return Collections.unmodifiableSet(closed);
	}

	/**
	 * Returns a set of all successors of this worker.
	 * @return a set of all successors of this worker
	 */
	Set<PrimitiveWorker<?, ?>> getAllSuccessors() {
		Set<PrimitiveWorker<?, ?>> closed = new HashSet<>();
		Queue<PrimitiveWorker<?, ?>> frontier = new ArrayDeque<>();
		frontier.addAll(this.getSuccessors());
		while (!frontier.isEmpty()) {
			PrimitiveWorker<?, ?> cur = frontier.remove();
			closed.add(cur);
			frontier.addAll(cur.getSuccessors());
		}
		return Collections.unmodifiableSet(closed);
	}

	public abstract void work();

	/**
	 * Called by interpreters to do work and other related tasks, such as
	 * processing messages.
	 */
	void doWork() {
		while (!messages.isEmpty() && messages.get(0).timeToReceive == executions+1) {
			Portal.Message m = messages.remove(0);
			try {
				m.method.invoke(this, m.args);
			} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
				throw new IllegalStreamGraphException("Bad stuff happened while processing message", ex);
			}
		}

		//TODO: implement prework here
		work();

		++executions;
	}

	/**
	 * Returns the number of completed executions of this worker.
	 * @return the number of completed executions of this worker.
	 */
	long getExecutions() {
		return executions;
	}

	void sendMessage(Portal.Message message) {
		if (message.timeToReceive <= executions)
			throw new AssertionError("Message delivery missed: "+executions+", "+message);
		//Insert in order sorted by time-to-receive.
		int insertionPoint = Collections.binarySearch(messages, message);
		if (insertionPoint < 0)
			insertionPoint = -(insertionPoint + 1);
		messages.add(insertionPoint, message);
	}

	/*
	 * Ideally, subclasses would provide their rates to our constructor.  But
	 * that would require splitters and joiners to commit to their rates before
	 * knowing how many outputs/inputs they have.  If the user wants to build
	 * a splitjoin with a variable number of elements, he or she would have to
	 * figure out how many before constructing the splitjoin, rather than being
	 * able to call Splitjoin.add() in a loop.  Even if the user wants a fixed
	 * number of elements in the splitjoin, he or she shouldn't have to repeat
	 * him or herself by specifying a splitter size, joiner size and then adding
	 * that many elements (the StreamIt language doesn't require this, for
	 * example).
	 */

	/**
	 * Returns a list of peek rates, such that the rate for the i-th channel is
	 * returned at index i.  Workers with only one input will return a singleton
	 * list.
	 * @return a list of peek rates
	 */
	abstract List<Rate> getPeekRates();

	/**
	 * Returns a list of pop rates, such that the rate for the i-th channel is
	 * returned at index i.  Workers with only one input will return a singleton
	 * list.
	 * @return a list of pop rates
	 */
	abstract List<Rate> getPopRates();

	/**
	 * Returns a list of push rates, such that the rate for the i-th channel is
	 * returned at index i. Workers with only one output will return a singleton
	 * list.
	 * @return a list of push rates
	 */
	abstract List<Rate> getPushRates();
}
