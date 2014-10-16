package edu.mit.streamjit.impl.compiler2;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import java.util.Set;

/**
 * A strategy that doesn't use parameters and always does whatever it does.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 1/15/2014
 */
public final class AlwaysStrategy implements RemovalStrategy, FusionStrategy, UnboxingStrategy {
	@Override
	public void makeParameters(Set<Worker<?, ?>> workers, Configuration.Builder builder) {
		//no parameters necessary
	}

	@Override
	public boolean remove(WorkerActor a, Configuration config) {
		return true;
	}

	@Override
	public boolean fuseUpward(ActorGroup group, Configuration config) {
		return true;
	}

	@Override
	public boolean unboxStorage(Storage storage, Configuration config) {
		return true;
	}

	@Override
	public boolean unboxInput(WorkerActor actor, Configuration config) {
		return true;
	}

	@Override
	public boolean unboxOutput(WorkerActor actor, Configuration config) {
		return true;
	}
}
