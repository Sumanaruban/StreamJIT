/**
 * @author Sumanan sumanan@mit.edu
 * @since Apr 16, 2013
 */
package edu.mit.streamjit.test.sanity.nestedsplitjoinexample2;

import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

import com.google.common.collect.ImmutableList;
import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.RoundrobinSplitter;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.distributed.DistributedStreamCompiler;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.BenchmarkProvider;
import edu.mit.streamjit.test.Datasets;
import edu.mit.streamjit.test.SuppliedBenchmark;
import edu.mit.streamjit.test.Benchmark.Dataset;

/**
 * This is just a nested, inner split join stream structure that is to verify
 * the correctness of the StreamJit system.
 */
public class NestedSplitJoin2 {

	/**
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws InterruptedException {
		GlobalConstants.tunerMode = 1;
		// OneToOneElement<Float, Float> core = new splitjoin1();
		// // StreamCompiler sc = new DebugStreamCompiler();
		// // StreamCompiler sc = new ConcurrentStreamCompiler(4);
		// StreamCompiler sc = new DistributedStreamCompiler(2);
		// // StreamCompiler sc = new Compiler2StreamCompiler();
		//
		// ManualInput<Float> in = Input.<Float> createManualInput();
		// CompiledStream stream = sc.compile(core, in,
		// Output.<Float> toPrintStream(System.out));
		//
		// for (int i = 0; i < 40000; i++) {
		// Thread.sleep(10);
		// while (!in.offer(new Float(i))) {
		// Thread.sleep(1);
		// }
		// }
		// in.drain();
		// stream.awaitDrained();

		// Benchmarker.runBenchmarks(new NestedSplitJoinBenchmarkProvider(),
		// new CompilerStreamCompiler());
		// Benchmarker.runBenchmarks(new NestedSplitJoinBenchmarkProvider(),
		// new DistributedStreamCompiler(2));

		Benchmark benchmark = new NestedSplitJoinBenchmarkProvider().iterator()
				.next();

		// StreamCompiler compiler = new CompilerStreamCompiler();
		StreamCompiler compiler = new DistributedStreamCompiler(2); //
		// StreamCompiler compiler = new DebugStreamCompiler(); //
		// StreamCompiler compiler = new ConcurrentStreamCompiler(2);

		Dataset input = benchmark.inputs().get(0);
		CompiledStream stream = compiler.compile(benchmark.instantiate(),
				input.input(), Output.blackHole());
		stream.awaitDrained();

	}

	@ServiceProvider(BenchmarkProvider.class)
	public static class NestedSplitJoinBenchmarkProvider implements
			BenchmarkProvider {
		@Override
		public Iterator<Benchmark> iterator() {
			Path path = Paths.get("data/minimal100m.in");
			Input<Float> input = Input.fromBinaryFile(path, Float.class,
					ByteOrder.LITTLE_ENDIAN);
			Input<Float> repeated = Datasets.nCopies(500000000, input);
			Dataset dataset = new Dataset(path.getFileName().toString(),
					repeated);

			ImmutableList.Builder<Benchmark> builder = ImmutableList.builder();
			builder.add(new SuppliedBenchmark("NestedSplitJoin",
					NestedSplitJoinCore.class, ImmutableList.of(), dataset));
			return builder.build().iterator();
		}
	}

	public static class NestedSplitJoinCore extends Splitjoin<Float, Float> {

		public NestedSplitJoinCore() {
			super(new RoundrobinSplitter<Float>(),
					new RoundrobinJoiner<Float>());
			add(new splitjoin1(1));
			add(new Pipeline<Float, Float>(new multiplier(1, 2),
					new splitjoin1(2), new multiplier(1, 2)));
			add(new multiplier(1, 2));
			add(new splitjoin2(1));
		}
	}

	private static class multiplier extends Filter<Float, Float> {
		final int fact;
		final int assignedNode;

		multiplier(int fact, int assignedNode) {
			super(1, 1);
			this.fact = fact;
			this.assignedNode = assignedNode;
		}

		@Override
		public void work() {
			float i = pop();
			// System.out.println(Workers.getIdentifier(this) + " - " + i);
			if (getNodeID() != assignedNode) {
				try {
					// System.out.println("Sleeping...");
					Thread.sleep(1);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			push(fact * i);
		}
	}

	public static class splitjoin1 extends Splitjoin<Float, Float> {

		public splitjoin1(int assignedNode) {
			super(new RoundrobinSplitter<Float>(),
					new<Float> RoundrobinJoiner());
			add(new splitjoin2(assignedNode));
			add(new multiplier(1, assignedNode));
			add(new splitjoin2(assignedNode));

		}
	}

	public static class splitjoin2 extends Splitjoin<Float, Float> {
		public splitjoin2(int assignedNode) {
			super(new RoundrobinSplitter<Float>(),
					new RoundrobinJoiner<Float>());
			add(new multiplier(1, assignedNode));
			add(new multiplier(1, assignedNode));
			add(new multiplier(1, assignedNode));
		}
	}
}
