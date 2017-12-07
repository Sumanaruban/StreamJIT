package edu.mit.streamjit.test.apps;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.Output.BinaryFileOutput;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.StatefulFilter;
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
public class StateSizeVsReconfig2 {

	public static void main(String[] args) throws InterruptedException,
			IOException {
		Path path = Paths.get("data/minimal100.in");
		Input<Double> input = Input.fromBinaryFile(path, Double.class,
				ByteOrder.LITTLE_ENDIAN);
		input = Datasets.cycle(input);
		BinaryFileOutput<Double> output = Output.toBinaryFile(
				"StateSizeVsReconfig2.out", Double.class);

		StreamCompiler sc = new Compiler2StreamCompiler();
		CompiledStream st = sc.compile(new StateSizeVsReconfigCore2(), input,
				output);
		st.awaitDrained();
		output.close();
	}

	@ServiceProvider(BenchmarkProvider.class)
	public static class StateSizeVsReconfig2BenchmarkProvider implements
			BenchmarkProvider {
		@Override
		public Iterator<Benchmark> iterator() {
			Path path = Paths.get("data/minimal100.in");
			Input<Double> input = Input.fromBinaryFile(path, Double.class,
					ByteOrder.LITTLE_ENDIAN);
			Dataset dataset = new Dataset(path.getFileName().toString(), input);
			ImmutableList.Builder<Benchmark> builder = ImmutableList.builder();
			builder.add(new SuppliedBenchmark("StateSizeVsReconfigCore2",
					StateSizeVsReconfigCore2.class, ImmutableList.of(), dataset));
			return builder.build().iterator();
		}
	}

	public static class StateSizeVsReconfigCore2
			extends
				Pipeline<Double, Double> {

		public StateSizeVsReconfigCore2() {
			add(new StateFilter(Options.appArg1));
			add(new StateFilter(Options.appArg1));
			add(new StateFilter(Options.appArg1));
			add(new StateFilter(Options.appArg1));
			add(new StateFilter(Options.appArg1));
			add(new StateFilter(Options.appArg1));
			add(new StateFilter(Options.appArg1));
			add(new StateFilter(Options.appArg1));
			// add(new Sink());
		}
	}

	private static class StateFilter extends StatefulFilter<Double, Double> {
		List<Double> data;

		public StateFilter(int length) {
			super(1, 1);
			initList(length);
		}

		private void initList(int length) {
			data = new ArrayList<>(length);
			for (int i = 0; i < length; i++) {
				data.add(56.56);
			}
		}

		@Override
		public void work() {
			double tot = 0;
			for (double d : data)
				tot = tot + d;
			push(tot / data.size());
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
