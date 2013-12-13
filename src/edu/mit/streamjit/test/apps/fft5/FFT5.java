package edu.mit.streamjit.test.apps.fft5;

import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.compiler2.Compiler2StreamCompiler;
import edu.mit.streamjit.test.SuppliedBenchmark;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Datasets;
import edu.mit.streamjit.test.Benchmarker;

/**
 * Rewritten StreamIt's asplos06 benchmarks. Refer
 * STREAMIT_HOME/apps/benchmarks/asplos06/fft/streamit/FFT5.str for original
 * implementations. Each StreamIt's language constructs (i.e., pipeline, filter
 * and splitjoin) are rewritten as classes in StreamJit.
 *
 * @author Sumanan sumanan@mit.edu
 * @since Mar 14, 2013
 */
public class FFT5 {
	public static void main(String[] args) throws InterruptedException {
		StreamCompiler sc = new Compiler2StreamCompiler();
		Benchmarker.runBenchmark(new FFT5Benchmark(), sc).get(0).print(System.out);
	}

	@ServiceProvider(Benchmark.class)
	public static final class FFT5Benchmark extends SuppliedBenchmark {
		public FFT5Benchmark() {
			super("FFT5", FFT5Kernel.class, Datasets.nCopies(100000, 10.0f));
		}
	}

	/**
	 * This represents "void->void pipeline FFT5()".
	 */
	public static class FFT5Kernel extends Pipeline<Float, Float> {
		public FFT5Kernel() {
			this(256);
		}
		public FFT5Kernel(int ways) {
			add(new FFTReorder(ways));
			for (int j = 2; j <= ways; j *= 2) {
				add(new CombineDFT(j));
			}
		}
	}

	private static class CombineDFT extends Filter<Float, Float> {
		private final float wn_r, wn_i;
		private final int n;
		private CombineDFT(int n) {
			super(2 * n, 2 * n, 2 * n);
			this.n = n;
			this.wn_r = (float) Math.cos(2 * 3.141592654 / n);
			this.wn_i = (float) Math.sin(2 * 3.141592654 / n);
		}
		public void work() {
			int i;
			float w_r = 1;
			float w_i = 0;
			float[] results = new float[2 * n];

			for (i = 0; i < n; i += 2) {
				// this is a temporary work-around since there seems to be
				// a bug in field prop that does not propagate nWay into the
				// array references. --BFT 9/10/02

				// int tempN = nWay;
				// Fixed --jasperln

				// removed nWay, just using n --sitij 9/26/03

				float y0_r = peek(i);
				float y0_i = peek(i + 1);

				float y1_r = peek(n + i);
				float y1_i = peek(n + i + 1);

				float y1w_r = y1_r * w_r - y1_i * w_i;
				float y1w_i = y1_r * w_i + y1_i * w_r;

				results[i] = y0_r + y1w_r;
				results[i + 1] = y0_i + y1w_i;

				results[n + i] = y0_r - y1w_r;
				results[n + i + 1] = y0_i - y1w_i;

				float w_r_next = w_r * wn_r - w_i * wn_i;
				float w_i_next = w_r * wn_i + w_i * wn_r;
				w_r = w_r_next;
				w_i = w_i_next;
			}

			for (i = 0; i < 2 * n; i++) {
				pop();
				push(results[i]);
			}
		}

	}

	private static class FFTReorderSimple extends Filter<Float, Float> {
		private final int totalData;
		private final int n;
		private FFTReorderSimple(int n) {
			super(2 * n, 2 * n, 2 * n);
			this.n = n;
			this.totalData = 2*n;
		}
		public void work() {
			int i;

			for (i = 0; i < totalData; i += 4) {
				push(peek(i));
				push(peek(i + 1));
			}

			for (i = 2; i < totalData; i += 4) {
				push(peek(i));
				push(peek(i + 1));
			}

			for (i = 0; i < n; i++) {
				pop();
				pop();
			}
		}
	}

	private static class FFTReorder extends Pipeline<Float, Float> {
		private FFTReorder(int n) {
			for (int i = 1; i < (n / 2); i *= 2)
				add(new FFTReorderSimple(n / i));
		}
	}
}
