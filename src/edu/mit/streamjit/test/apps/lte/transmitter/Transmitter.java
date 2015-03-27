package edu.mit.streamjit.test.apps.lte.transmitter;

import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmarker;
import edu.mit.streamjit.test.Datasets;
import edu.mit.streamjit.test.SuppliedBenchmark;
import edu.mit.streamjit.test.Benchmark.Dataset;



public class Transmitter {
	
	public static void main(String[] args) throws InterruptedException {
		StreamCompiler sc = new DebugStreamCompiler();
		Benchmarker.runBenchmark(new TransmitterBenchmark(), sc).get(0).print(System.out);
//		OneToOneElement<Byte, Byte> streamgraph = new Pipeline<>(new TurboEncoder(),new Modulator(),new AntennaArray());
//		StreamCompiler compiler = new DebugStreamCompiler();
//		Path path = Paths.get("src/edu/mit/streamjit/transmitter/data.in");
//		Input<Byte> input = Input.fromBinaryFile(path, Byte.class,
//				ByteOrder.LITTLE_ENDIAN);
//		Input<Byte> repeated = Datasets.nCopies(1, input);
//		Output<Byte> out = Output.blackHole();
//		CompiledStream stream = compiler.compile(streamgraph, repeated, out);
//		stream.awaitDrained();
	}
	
	@ServiceProvider(Benchmark.class)
	public static final class TransmitterBenchmark extends SuppliedBenchmark {
		public TransmitterBenchmark() {
			super("Transmitter", TransmitterKernel.class, new Dataset("src/edu/mit/streamjit/transmitter/data.in",
					(Input)Input.fromBinaryFile(Paths.get("src/edu/mit/streamjit/transmitter/data.in"), Byte.class, ByteOrder.LITTLE_ENDIAN)
//					, (Supplier)Suppliers.ofInstance((Input)Input.fromBinaryFile(Paths.get("/home/jbosboom/streamit/streams/apps/benchmarks/asplos06/fft/streamit/FFT5.out"), Float.class, ByteOrder.LITTLE_ENDIAN))
			));
		}
	}
	
	public static final class TransmitterKernel extends Pipeline<Byte, Float> {
		
		public TransmitterKernel() {
			this.add(new TurboEncoder(),new Modulator(),new AntennaArray());
		}
		
	}

	private static class Add extends edu.mit.streamjit.api.Filter<Byte, Byte> {

		public Add() {
			super(2, 1);
		}

		@Override
		public void work() {
			Byte a = pop();
			Byte b = pop();
			System.out.println(a+" "+b+" ");
			Byte c =(byte)(a+b);
			push(c);
		}
	}

	private static class IntPrinter extends
			edu.mit.streamjit.api.Filter<Byte, Byte> {

		public IntPrinter() {
			super(1, 1);
		}

		@Override
		public void work() {
			byte a = pop();
			System.out.println(a);
			push(a);

		}
	}
}
