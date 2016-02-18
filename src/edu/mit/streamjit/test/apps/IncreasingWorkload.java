package edu.mit.streamjit.test.apps;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
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
import edu.mit.streamjit.test.SuppliedBenchmark;

/**
 * Increasing workload over time.
 * 
 * @author sumanan
 * @since 25 Jan, 2016
 */
public class IncreasingWorkload {

	public static void main(String[] args) throws InterruptedException,
			IOException {
		Path path = Paths.get("data/minimal100.in");
		Input<Float> input = Input.fromBinaryFile(path, Float.class,
				ByteOrder.LITTLE_ENDIAN);

		BinaryFileOutput<Float> output = Output.toBinaryFile("incWorkload.out",
				Float.class);

		StreamCompiler sc = new Compiler2StreamCompiler();
		CompiledStream st = sc.compile(new IncreasingWorkloadCore(), input,
				output);
		st.awaitDrained();
		output.close();
	}

	@ServiceProvider(BenchmarkProvider.class)
	public static class IncreasingWorkloadBenchmarkProvider implements
			BenchmarkProvider {
		@Override
		public Iterator<Benchmark> iterator() {
			Path path = Paths.get("data/minimal100.in");
			Input<Float> input = Input.fromBinaryFile(path, Float.class,
					ByteOrder.LITTLE_ENDIAN);
			Dataset dataset = new Dataset(path.getFileName().toString(), input);
			ImmutableList.Builder<Benchmark> builder = ImmutableList.builder();
			builder.add(new SuppliedBenchmark("IncreasingWorkload",
					IncreasingWorkloadCore.class, ImmutableList.of(), dataset));
			return builder.build().iterator();
		}
	}

	public static class IncreasingWorkloadCore extends Pipeline<Float, Float> {

		public IncreasingWorkloadCore() {
			add(new multiplier());
			add(new multiplier());
			add(new multiplier());
			add(new multiplier());
			add(new multiplier());
			add(new multiplier());
		}
	}

	private static class multiplier2 extends Filter<Float, Float> {

		public multiplier2() {
			super(1, 1);
		}

		@Override
		public void work() {
			float i = pop();
			push((float) (2.00 * i));
		}
	}

	private static class multiplier extends Filter<Float, Float> {

		private RuntimeMXBean rb = ManagementFactory.getRuntimeMXBean();
		final int dec;
		final int step = 30;

		public multiplier() {
			super(1, 1);
			if (Options.loadRatio > 1)
				dec = Options.loadRatio;
			else
				dec = 1;
		}

		@Override
		public void work() {
			long upTime = rb.getUptime();
			upTime = upTime / 1000;
			upTime = (upTime / step) * step;
			boolean b = true;
			while (upTime > 0) {
				b = "Foo".matches("F.*");
				b = "Foo".matches("F.*");
				upTime = upTime - dec;
			}
			float i = pop();
			if (b)
				push((float) (3.00 * i));
			else
				push((float) (2.00 * i));
		}
	}
}
