package edu.mit.streamjit.test.sanity;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmark.Dataset;
import edu.mit.streamjit.test.BenchmarkProvider;
import edu.mit.streamjit.test.Benchmarker;
import edu.mit.streamjit.test.Datasets;
import edu.mit.streamjit.test.SuppliedBenchmark;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tests filters that call helper functions from work().
 *
 * TODO: enable these tests when the compiler passes them or gracefully handles
 * its errors (i.e., without taking down the test harness)
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 8/29/2013
 */
//@ServiceProvider(BenchmarkProvider.class)
public class HelperFunctionSanity implements BenchmarkProvider {
	@Override
	@SuppressWarnings({"unchecked", "unchecked"})
	public Iterator<Benchmark> iterator() {
		Dataset ds = Datasets.allIntsInRange(0, 100000);
		ds = ds.withOutput(Datasets.transformOne(new Function<Integer, Integer>() {
			@Override
			public Integer apply(Integer input) {
				return input + 9001;
			}
		}, (Input)ds.input()));

		Benchmark[] benchmarks = {
			new SuppliedBenchmark("HelperAdder", HelperAdder.class, ImmutableList.of(9001), ds),
			new SuppliedBenchmark("PrivateStaticHelperAdder", PrivateStaticHelperAdder.class, ds),
			new SuppliedBenchmark("PublicStaticHelperAdder", PublicStaticHelperAdder.class, ds),
		};
		return Arrays.asList(benchmarks).iterator();
	}

	public static final class HelperAdder extends Filter<Integer, Integer> {
		private final int addend;
		public HelperAdder(int addend) {
			super(1, 1);
			this.addend = addend;
		}
		@Override
		public void work() {
			push(compute(pop()));
		}
		private int compute(int input) {
			return input + addend;
		}
	}

	public static final class PrivateStaticHelperAdder extends Filter<Integer, Integer> {
		public PrivateStaticHelperAdder() {
			super(1, 1);
		}
		@Override
		public void work() {
			push(compute(pop()));
		}
		private static int compute(int input) {
			return input + 9001;
		}
	}

	public static final class PublicStaticHelperAdder extends Filter<Integer, Integer> {
		public PublicStaticHelperAdder() {
			super(1, 1);
		}
		@Override
		public void work() {
			push(compute(pop()));
		}
		public static int compute(int input) {
			return input + 9001;
		}
	}

	public static void main(String[] args) {
		Benchmarker.runBenchmarks(new HelperFunctionSanity(), new DebugStreamCompiler());
	}
}
