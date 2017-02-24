package edu.mit.streamjit.tutorial;

import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.compiler2.Compiler2StreamCompiler;

/**
 * Having a try-catch statement in the work method throws compile time assertion
 * error. Yuehan found this issue.
 * 
 * The solution is putting try-catch statement into a new static method and call
 * that method from the work method.
 * 
 * @author Sumanaruban Rajadurai (Suman)
 * @since 24 Feb 2017
 */
public class TryCatchIssue {

	public static void main(String[] args) throws InterruptedException {
		StreamCompiler sc = new Compiler2StreamCompiler();
		Path path = Paths.get("data/fmradio.in");
		Input<Float> in = Input.fromBinaryFile(path, Float.class,
				ByteOrder.LITTLE_ENDIAN);
		OneToOneElement<Float, Float> core = new SampleFilter();
		CompiledStream stream = sc.compile(core, in,
				Output.<Float> toPrintStream(System.out));
		stream.awaitDrained();
	}

	public static class SampleFilter extends Filter<Float, Float> {

		public SampleFilter() {
			super(1, 1);
		}

		@Override
		public void work() {
			float num = pop();
			Integer.parseInt("123");

			// Program will run correctly if we remove this try-catch statement.
			try {
				Integer.parseInt("123");
			} catch (Exception ex) {
				ex.printStackTrace();
			}

			push((float) tryParseInt("123"));
		}

		public static int tryParseInt(String s) {
			try {
				return Integer.parseInt(s);
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			return -1;
		}
	}
}