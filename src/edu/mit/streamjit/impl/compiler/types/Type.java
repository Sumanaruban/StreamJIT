package edu.mit.streamjit.impl.compiler.types;

import edu.mit.streamjit.impl.compiler.Module;

/**
 * The types of Values.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/6/2013
 */
public abstract class Type {
	Type() {}

	protected abstract Module getModule();
	protected abstract TypeFactory getTypeFactory();
}
