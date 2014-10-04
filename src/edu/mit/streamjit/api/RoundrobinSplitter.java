package edu.mit.streamjit.api;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;

/**
 * A RoundrobinSplitter splits its input by passing data items to each child in
 * turn.  RoundrobinSplitter supports any number of children, passing the same
 * number of data items to each during each execution.  To specify different
 * weights, use WeightedRoundrobinSplitter.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/7/2013
 */
public final class RoundrobinSplitter<T> extends Splitter<T, T> {
	private final int itemsPerExecution;
	/**
	 * Creates a new RoundrobinSplitter that distributes one item to each child
	 * per execution.
	 */
	public RoundrobinSplitter() {
		this(1);
	}
	/**
	 * Creates a new RoundrobinSplitter that distributes itemsPerExecutions
	 * items to each child per execution.
	 */
	public RoundrobinSplitter(int itemsPerExecution) {
		this.itemsPerExecution = itemsPerExecution;
	}

	@Override
	public int supportedOutputs() {
		return Splitter.UNLIMITED;
	}

	@Override
	public void work() {
		for (int i = 0; i < outputs(); ++i)
			for (int j = 0; j < itemsPerExecution; ++j)
				push(i, pop());
	}

	@Override
	public ImmutableList<Rate> getPeekRates() {
		//We don't peek.
		return ImmutableList.of(Rate.create(0));
	}

	@Override
	public ImmutableList<Rate> getPopRates() {
		return ImmutableList.of(Rate.create(itemsPerExecution*outputs()));
	}

	@Override
	public List<Rate> getPushRates() {
		return Collections.nCopies(outputs(), Rate.create(itemsPerExecution));
	}

	@Override
	public String toString() {
		return String.format("RoundrobinSplitter(%d)@%d", itemsPerExecution, getIdentifier());
	}
}
