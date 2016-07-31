package edu.mit.streamjit.test.apps;

import java.io.IOException;
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
import edu.mit.streamjit.api.Output.BinaryFileOutput;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.compiler2.Compiler2StreamCompiler;
import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmark.Dataset;
import edu.mit.streamjit.test.BenchmarkProvider;
import edu.mit.streamjit.test.Datasets;
import edu.mit.streamjit.test.SuppliedBenchmark;

/**
 * Increasing workload over time.
 * 
 * @author sumanan
 * @since 25 Jan, 2016
 */
public class StateSizeVsReconfig {

	public static void main(String[] args) throws InterruptedException,
			IOException {
		Path path = Paths.get("data/minimal100.in");
		Input<Double> input = Input.fromBinaryFile(path, Double.class,
				ByteOrder.LITTLE_ENDIAN);
		input = Datasets.cycle(input);
		BinaryFileOutput<Double> output = Output.toBinaryFile(
				"StateSizeVsReconfig.out", Double.class);

		StreamCompiler sc = new Compiler2StreamCompiler();
		CompiledStream st = sc.compile(new StateSizeVsReconfigCore(), input,
				output);
		st.awaitDrained();
		output.close();
	}

	@ServiceProvider(BenchmarkProvider.class)
	public static class StateSizeVsReconfigBenchmarkProvider implements
			BenchmarkProvider {
		@Override
		public Iterator<Benchmark> iterator() {
			Path path = Paths.get("data/minimal100.in");
			Input<Double> input = Input.fromBinaryFile(path, Double.class,
					ByteOrder.LITTLE_ENDIAN);
			Dataset dataset = new Dataset(path.getFileName().toString(), input);
			ImmutableList.Builder<Benchmark> builder = ImmutableList.builder();
			builder.add(new SuppliedBenchmark("StateSizeVsReconfigCore",
					StateSizeVsReconfigCore.class, ImmutableList.of(), dataset));
			return builder.build().iterator();
		}
	}

	public static class StateSizeVsReconfigCore
			extends
				Pipeline<Double, Double> {

		public StateSizeVsReconfigCore() {
			add(new MovingAvg(Options.appArg1));
			add(new MovingAvg(Options.appArg1));
			add(new MovingAvg(Options.appArg1));
			add(new MovingAvg(Options.appArg1));
			add(new MovingAvg(Options.appArg1));
			add(new MovingAvg(Options.appArg1));
			add(new MovingAvg(Options.appArg1));
			add(new MovingAvg(Options.appArg1));
			// add(new Sink());
		}
	}

	private static class MovingAvg extends Filter<Double, Double> {
		private final int peek;

		public MovingAvg(int peek) {
			super(1, 1, peek);
			this.peek = peek;
		}

		@Override
		public void work() {
			double tot = 0;
			for (int i = 0; i < peek; i++)
				tot = tot + peek(i);
			push(tot / peek);
			pop();
		}
	}

	private static class Sink extends Filter<Double, Double> {
		public Sink() {
			super(1, 1);
		}

		@Override
		public void work() {
			double i = pop();
			System.out.println(i);
			push(i);
		}
	}
}
