package edu.mit.streamjit.impl.distributed;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.BenchmarkProvider;
import edu.mit.streamjit.test.Benchmark.Dataset;
import edu.mit.streamjit.test.apps.channelvocoder7.ChannelVocoder7;
import edu.mit.streamjit.test.apps.fmradio.FMRadio.FMRadioBenchmarkProvider;

public class DistributedAppRunner {

	/**
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws InterruptedException {
		if (args.length == 0) {
			System.out.println("Usage format");
			System.out
					.println("DistributedAppRunner <BenchmarkProvider> [noOfNodes],"
							+ "[tunerMode]");

			System.out
					.println("tunerMode \n\t 0 - Start the Open tuner on a xterm. "
							+ "\n\t 1 - Open tuner will not be started automatically. "
							+ "\n\t\tUser has to start the Opentuer with the listening portNo "
							+ "argument of 12563.");
			return;
		}

		int noOfNodes = 2;
		int tunerMode = 0;

		// TODO: Need to create the BenchmarkProvider from this argument.
		String benchmarkProviderName = args[0];

		BenchmarkProvider bp = new ChannelVocoder7();

		if (args.length > 1) {
			try {
				noOfNodes = Integer.parseInt(args[1]);
			} catch (NumberFormatException ex) {
				System.err.println("Invalid noOfNodes...");
				System.err.println("Please verify the second argument.");
				System.out.println(String.format(
						"System is using default value, %d, for noofNodes.",
						noOfNodes));
			}
		}

		if (args.length > 2) {
			try {
				tunerMode = Integer.parseInt(args[2]);
			} catch (NumberFormatException ex) {
				System.err.println("Invalid tunerType...");
				System.err.println("Please verify the third argument.");
				System.out.println(String.format(
						"System is using default value, %d, for tunerType.",
						tunerMode));
			}
		}

		Benchmark benchmark = bp.iterator().next();
		// StreamCompiler compiler = new Compiler2StreamCompiler();
		StreamCompiler compiler = new DistributedStreamCompiler(noOfNodes);
		// StreamCompiler compiler = new DebugStreamCompiler();
		// StreamCompiler compiler = new ConcurrentStreamCompiler(2);

		Dataset input = benchmark.inputs().get(0);
		CompiledStream stream = compiler.compile(benchmark.instantiate(),
				input.input(), Output.toPrintStream(System.out));
		stream.awaitDrained();
	}
}
