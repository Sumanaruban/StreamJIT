package edu.mit.streamjit.tutorial;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Input.ManualInput;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.distributed.controller.DistributedStreamCompiler;

/**
 * This example demonstrate how to use manual input in StreamJIT.
 * 
 * @author Sumanaruban Rajadurai (Suman)
 * @since 21 Feb 2017
 */
public class UseManualInput {

	public static void main(String[] args) throws InterruptedException {
		StreamCompiler sc = new DistributedStreamCompiler();
		ManualInput<Float> in = Input.<Float> createManualInput();
		OneToOneElement<Float, Float> core = new SampleFilter();
		CompiledStream stream = sc.compile(core, in,
				Output.<Float> toPrintStream(System.out));

		for (int i = 0; i < 40000; i++) {
			// Thread.sleep(10);
			while (!in.offer(new Float(i))) {
				Thread.sleep(1);
			}
		}
		in.drain();
		stream.awaitDrained();
	}

	public static class SampleFilter extends Filter<Float, Float> {

		public SampleFilter() {
			super(1, 1);
		}

		@Override
		public void work() {
			push(2 * pop());
		}
	}
}
