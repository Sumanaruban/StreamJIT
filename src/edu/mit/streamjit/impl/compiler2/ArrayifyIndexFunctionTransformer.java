package edu.mit.streamjit.impl.compiler2;

import com.google.common.base.Supplier;
import edu.mit.streamjit.util.Combinators;
import java.io.Serializable;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.NavigableSet;

/**
 * An IndexFunctionTransformer that evaluates the given function over its
 * domain, then returns a function that indexes into an array of the outputs.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 12/8/2013
 */
public class ArrayifyIndexFunctionTransformer implements IndexFunctionTransformer, Serializable {
	private static final long serialVersionUID = 1L;
	private final boolean compact;
	public ArrayifyIndexFunctionTransformer() {
		this(false);
	}
	public ArrayifyIndexFunctionTransformer(boolean compact) {
		this.compact = compact;
	}
	@Override
	public MethodHandle transform(MethodHandle fxn, Supplier<? extends NavigableSet<Integer>> domainSupplier) {
		NavigableSet<Integer> domain = domainSupplier.get();
		int[] array = new int[domain.last() - domain.first() + 1];
		int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;
		for (int i : domain) {
			int x;
			try {
				x = (int)fxn.invokeExact(i);
			} catch (Throwable ex) {
				throw new AssertionError("Index functions should not throw", ex);
			}
			array[i - domain.first()] = x;
			min = Math.min(min, x);
			max = Math.max(max, x);
		}

		Object arrayObj = array;
		if (compact) {
			if (min >= Byte.MIN_VALUE && max <= Byte.MAX_VALUE) {
				byte[] a = new byte[array.length];
				for (int i = 0; i < array.length; ++i)
					a[i] = (byte)array[i];
				arrayObj = a;
			} else if (min >= Short.MIN_VALUE && max <= Short.MAX_VALUE) {
				short[] a = new short[array.length];
				for (int i = 0; i < array.length; ++i)
					a[i] = (short)array[i];
				arrayObj = a;
			} else if (min >= Character.MIN_VALUE && max <= Character.MAX_VALUE) {
				char[] a = new char[array.length];
				for (int i = 0; i < array.length; ++i)
					a[i] = (char)array[i];
				arrayObj = a;
			}
			//TODO: we could subtract the minimum from all values and add it
			//back after indexing, which might let us fit in a smaller array.
		}

		return MethodHandles.filterArguments(MethodHandles.arrayElementGetter(arrayObj.getClass()).bindTo(arrayObj), 0,
				Combinators.sub(MethodHandles.identity(int.class), domain.first()))
				.asType(MethodType.methodType(int.class, int.class));
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		final ArrayifyIndexFunctionTransformer other = (ArrayifyIndexFunctionTransformer)obj;
		if (this.compact != other.compact)
			return false;
		return true;
	}
	@Override
	public int hashCode() {
		int hash = 7;
		hash = 83 * hash + (this.compact ? 1 : 0);
		return hash;
	}
	@Override
	public String toString() {
		return getClass().getSimpleName() + "(compact="+compact+")";
	}
}
