package edu.mit.streamjit.test.apps.sar;

import static edu.mit.streamjit.test.apps.sar.Statics.L;
import static edu.mit.streamjit.test.apps.sar.Statics.Rmax;
import static edu.mit.streamjit.test.apps.sar.Statics.Rmin;
import static edu.mit.streamjit.test.apps.sar.Statics.Tp;
import static edu.mit.streamjit.test.apps.sar.Statics.X0;
import static edu.mit.streamjit.test.apps.sar.Statics.Xc;
import static edu.mit.streamjit.test.apps.sar.Statics.Y0;
import static edu.mit.streamjit.test.apps.sar.Statics.c;
import static edu.mit.streamjit.test.apps.sar.Statics.f0;
import static edu.mit.streamjit.test.apps.sar.Statics.fc;
import static edu.mit.streamjit.test.apps.sar.Statics.lambda_min;

import java.io.IOException;
import java.util.Collections;

import com.google.common.base.Stopwatch;
import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.DuplicateSplitter;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.WeightedRoundrobinJoiner;
import edu.mit.streamjit.impl.compiler2.Compiler2StreamCompiler;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.SuppliedBenchmark;
import edu.mit.streamjit.test.apps.sar.GenRawSARStr.GenRawSAR;
import edu.mit.streamjit.test.apps.sar.Utils.Complex;

/**
 * Ported from StreamIt. Refer
 * STREAMIT_HOME/apps/benchmarks/sar/streamit/SAR.str for the original
 * implementation.
 * 
 * @author sumanan
 * @since 30 Mar, 2015
 */
public class SAR {

	public static void main(String[] args) throws InterruptedException,
			IOException {
		int noOfNodes;
		Stopwatch sw = Stopwatch.createStarted();
		try {
			noOfNodes = Integer.parseInt(args[0]);
		} catch (Exception ex) {
			noOfNodes = 3;
		}

		int ITEMS = 10_000_000;

		Input in = (Input) Input.fromIterable(Collections.nCopies(ITEMS,
				(byte) 0));

		// startSNs(noOfNodes);
		StreamCompiler compiler = new Compiler2StreamCompiler();

		OneToOneElement<Void, Double> streamGraph = new SARKernel();
		CompiledStream stream = compiler.compile(streamGraph, in,
				Output.blackHole());
		stream.awaitDrained();

	}

	private static void startSNs(int noOfNodes) throws IOException {
		for (int i = 1; i < noOfNodes; i++)
			new ProcessBuilder("xterm", "-e", "java", "-jar", "StreamNode.jar")
					.start();
		// new ProcessBuilder("java", "-jar", "StreamNode.jar").start();
	}

	@ServiceProvider(Benchmark.class)
	public static final class SARBenchmark extends SuppliedBenchmark {
		// how many dummy timing elements to provide
		private static final int ITEMS = 10_000_000;
		public SARBenchmark() {
			super("SARKernel", SARKernel.class, new Dataset("" + ITEMS,
					(Input) Input.fromIterable(Collections.nCopies(ITEMS,
							(byte) 0))));
		}
	}

	// Function formSARimage() - part of Kernel 1 - Given the raw SAR complex
	// data, forms the SAR image.
	//
	// Modified companion code to Mehrdad Soumekh's text book
	// "Synthetic Aperture Radar Signal Processing with Matlab Algorithms",
	// Wiley, New York, NY, 1999.
	//
	// This function digitally reconstructs the SAR image using spatial
	// frequency interpolation (see noted text, Section 4.5).
	public static final class SARKernel extends Pipeline<Void, Double> {
		public SARKernel() {
			// genRawSAR.m
			// %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
			// %% u domain parameters and arrays for compressed SAR signal %%
			// %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

			double duc = ((Xc * lambda_min) / (4 * Y0)) / 1.2;

			/* number of samples on aperture */
			int mc = 2 * (int) Math.ceil(L / duc);

			double dku = Math.PI * 2 / ((double) mc * duc);

			/* synthetic aperture array */
			double[] uc = new double[mc];
			double[] kuc = new double[mc];

			for (int i = 0; i < mc; i++) {
				uc[i] = duc * (((double) i) - ((double) mc) / 2.0);
				kuc[i] = dku * (((double) i) - ((double) mc) / 2.0);
			}

			// %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
			// %% u domain parameters and arrays for SAR signal %%
			// %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

			double theta_min = Math.atan((0 - Y0 - L) / (Xc - X0));
			double theta_max = Math.atan((Y0 + L) / (Xc - X0));

			double du = lambda_min
					/ (1.4 * 2 * (Math.sin(theta_max) - Math.sin(theta_min)));

			/* number of samples on aperture */
			int m = 2 * (int) Math.ceil(Math.PI / (du * dku));
			du = Math.PI * 2 / (m * dku);

			/* synthetic aperture array */
			double[] u = new double[m];
			double[] ku = new double[m];

			for (int i = 0; i < m; i++) {
				u[i] = du * (((float) i) - ((float) m) / 2.0);
				ku[i] = dku * (((float) i) - ((float) m) / 2.0);
			}

			// %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
			// %% Fast-time domain parmeters and arrays %%
			// %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

			double Ts = (2 / c) * Rmin;
			double Tf = (2 / c) * Rmax + Tp;
			double T = Tf - Ts;
			Ts = Ts - 0.1 * T;
			Tf = Tf + 0.1 * T;
			T = Tf - Ts;
			double Tmin = Math.max(T, (4 * X0) / (c * Math.cos(theta_max)));
			double dt = 1 / (4 * f0);

			/* number of time samples */
			int n = 2 * (int) Math.ceil((0.5 * Tmin) / dt);

			/* Wavenumber array */
			double[] t = new double[n];
			double[] k = new double[n];

			for (int i = 0; i < n; i++) {
				t[i] = Ts + i * dt;
				k[i] = (Math.PI * 2 / c)
						* (fc + 4 * f0 * (((float) i) - ((float) n) / 2.0) / n);
			}

			// NOTE: mock filter to ger around compiler limitation w.r.t. null
			// splitters
			Filter<Void, Integer> filter1 = new Filter<Void, Integer>(0, 1) {
				@Override
				public void work() {
					push(1);
				}
			};
			add(filter1);

			Splitjoin<Integer, Complex> splitJoin1 = new Splitjoin<>(
					new DuplicateSplitter<Integer>(),
					new WeightedRoundrobinJoiner<Complex>(1, mc));
			splitJoin1.add(new GenRawSARStr.FastTimeFilter(n, t));
			splitJoin1.add(new Pipeline<>(new GenRawSAR(Tf, n, m, mc, t, k, ku,
					uc), new FFT.FTX2D(n, mc)));

			add(splitJoin1);

			Filter<Complex, Complex> filter2 = new Filter<Utils.Complex, Utils.Complex>(
					n + n * mc, n * mc) {
				@Override
				public void work() {
					for (int i = 0; i < n; i++) {
						Complex ftf = pop();
						for (int j = 0; j < mc; j++) {
							Complex s = pop();
							Complex out = new Complex();

							out.real = s.real * ftf.real - s.imag * ftf.imag;
							out.imag = s.imag * ftf.real + s.real * ftf.imag;

							push(out);
						}
					}
				}
			};
			add(filter2);
			// Digital Spotlighting and Bandwidth Expansion in ku Domain
			// via Slow-time Compression and Decompression

			// // Compression
			add(new Compression(n, mc, k, uc));

			// // Narrow-bandwidth Polar Format Processed reconstruction
			add(new FFT.FTY2D(n, mc));

			// // Zero-padding in ku domain for slow-time upsampling
			add(new ZeroPadding(n, m, mc));

			// // Transform to (omega,u) domain
			add(new FFT.iFTY2D(n, m));

			// // Decompression
			add(new Decompression(n, m, k, u));

			// // Digitally-spotlighted SAR signal spectrum
			add(new FFT.FTY2D(n, m));

			// SAR RECONSTRUCTION (multiple stages)
			// - 2D Fourier Matched Filtering and Interpolation
			// - Inverse 2D FFT for spatial domain image
			add(new Reconstruction(n, m, k, ku));

			// NOTE: to compate to MATLAB output, transpose again
			// add floatTranspose(266, m);
			// add(new FloatPrinter());

		}
	}

	public static final class FloatPrinter extends Filter<Double, Void> {

		public FloatPrinter() {
			super(1, 0);
		}

		@Override
		public void work() {
			System.out.println(pop());
		}
	}

	// Zero-padding
	public static final class ZeroPadding extends Filter<Complex, Complex> {

		private final int n;
		private final int m;
		private final int mc;

		private final int mz;
		private final double q;
		private final Complex zero = new Complex(0, 0);

		public ZeroPadding(int n, int m, int mc) {
			super(n * mc, n * m);
			this.n = n;
			this.m = m;
			this.mc = mc;
			mz = m - mc;
			q = (double) m / mc;
		}

		@Override
		public void work() {
			for (int i = 0; i < n; i++) {
				for (int j = 0; j < m; j++) {
					if ((j < mz / 2) || (j >= mc + mz / 2)) {
						push(zero);
					} else {
						Complex in = pop();
						Complex out = new Complex(0, 0);
						out.real = q * in.real;
						out.imag = q * in.imag;
						push(out);
					}
				}
			}
		}
	}

	public static final class Compression extends Filter<Complex, Complex> {
		private final int n;
		private final int mc;
		private final double[] k;
		private final double[] uc;

		private final double[][] cos_value;
		private final double[][] sin_value;

		public Compression(int n, int mc, double[] k, double[] uc) {
			super(n * mc, n * mc);
			if (uc.length != mc)
				throw new IllegalArgumentException(
						String.format(
								"Arrat uc's length must be equal to mc. uc.length=%d, mc=%d\n",
								uc.length, mc));
			if (k.length != n)
				throw new IllegalArgumentException(
						String.format(
								"Arrat k's length must be equal to n. k.length=%d, n=%d\n",
								k.length, n));
			this.n = n;
			this.mc = mc;
			this.k = k;
			this.uc = uc;
			this.cos_value = new double[n][mc];
			this.sin_value = new double[n][mc];

			for (int i = 0; i < n; i++) {
				for (int j = 0; j < mc; j++) {
					double value = 2 * (k[i] * (Math.sqrt(Math.pow(Statics.Xc,
							2) + Math.pow(uc[j], 2)) - Statics.Xc));
					cos_value[i][j] = Math.cos(value);
					sin_value[i][j] = Math.sin(value);
				}
			}
		}

		@Override
		public void work() {
			for (int i = 0; i < n; i++) {
				for (int j = 0; j < mc; j++) {
					Complex in = pop();
					Complex out = new Complex();

					out.real = (in.real * cos_value[i][j])
							- (in.imag * sin_value[i][j]);

					out.imag = (in.imag * cos_value[i][j])
							+ (in.real * sin_value[i][j]);

					push(out);
				}
			}

		}
	}

	public static final class Decompression extends Filter<Complex, Complex> {
		private final int n;
		private final int m;
		private final double[] k;
		private final double[] u;

		private final double[][] cos_value;
		private final double[][] sin_value;

		public Decompression(int n, int m, double[] k, double[] u) {
			super(n * m, n * m);
			if (u.length != m)
				throw new IllegalArgumentException(
						String.format(
								"Arrat uc's length must be equal to mc. uc.length=%d, mc=%d\n",
								u.length, m));
			if (k.length != n)
				throw new IllegalArgumentException(
						String.format(
								"Arrat k's length must be equal to n. k.length=%d, n=%d\n",
								k.length, n));
			this.n = n;
			this.m = m;
			this.k = k;
			this.u = u;
			this.cos_value = new double[n][m];
			this.sin_value = new double[n][m];

			for (int i = 0; i < n; i++) {
				for (int j = 0; j < m; j++) {
					double value = 2 * (k[i] * (Math.sqrt(Math.pow(Statics.Xc,
							2) + Math.pow(u[j], 2)) - Statics.Xc));
					cos_value[i][j] = Math.cos(value);
					sin_value[i][j] = Math.sin(value);
				}
			}
		}

		@Override
		public void work() {
			for (int i = 0; i < n; i++) {
				for (int j = 0; j < m; j++) {
					Complex in = pop();
					Complex out = new Complex();

					out.real = (in.real * cos_value[i][j])
							+ (in.imag * sin_value[i][j]);

					out.imag = (in.imag * cos_value[i][j])
							- (in.real * sin_value[i][j]);

					push(out);
				}
			}
		}
	}

	public static final class Reconstruction extends Pipeline<Complex, Double> {

		public Reconstruction(int n, int m, double[] k, double[] ku) {
			add(new MatchedFiltering(n, m, k, ku));
			int nx = 266;
			add(new ConvolutionInterpolation(n, nx, m, k, ku));

			// / Inverse 2D FFT for spatial domain image f(x,y) [p203 fig4.6]
			add(new FFT.iFTY2D(nx, m));
			add(new FFT.iFTX2D(nx, m));

			add(new Utils.ComplexAbsoluate());
			add(new Utils.FloatTranspose(nx, m));

		}
	}

	public static final class MatchedFiltering extends Filter<Complex, Complex> {
		private final int n;
		private final int m;
		private final double[] k;
		private final double[] ku;

		// NOTE: <kx> is redefined because of a lack of proper support
		// for global arrays; the array itself is too big to be fully
		// unrolled since it leads to 1.4M lines of code and chokes gcc
		private final double[][] kx;
		private final Complex[][] fs0;

		public MatchedFiltering(int n, int m, double[] k, double[] ku) {
			super(n * m, n * m);
			if (ku.length != m)
				throw new IllegalArgumentException(
						String.format(
								"Arrat uc's length must be equal to mc. uc.length=%d, mc=%d\n",
								ku.length, m));
			if (k.length != n)
				throw new IllegalArgumentException(
						String.format(
								"Arrat k's length must be equal to n. k.length=%d, n=%d\n",
								k.length, n));
			this.n = n;
			this.m = m;
			this.k = k;
			this.ku = ku;
			this.kx = new double[n][m];
			this.fs0 = Utils.initializedComplexArray(n, m);

			for (int i = 0; i < n; i++) {
				for (int j = 0; j < m; j++) {
					double val = (4 * (k[i] * k[i])) - (ku[j] * ku[j]);

					if (val > 0) {
						kx[i][j] = Math.sqrt(val);
					} else {
						kx[i][j] = 0;
					}
				}
			}

			for (int i = 0; i < n; i++) {
				for (int j = 0; j < m; j++) {
					if (kx[i][j] > 0) {
						double value = kx[i][j] * Statics.Xc + ku[j] + 0.25
								* Math.PI - 2 * k[i] * Statics.Xc;
						fs0[i][j] = new Complex();
						fs0[i][j].real = Math.cos(value);
						fs0[i][j].imag = Math.sin(value);
					}
				}
			}
		}

		@Override
		public void work() {
			for (int i = 0; i < n; i++) {
				for (int j = 0; j < m; j++) {
					Complex fs = pop();
					Complex out = new Complex();

					out.real = fs.real * fs0[i][j].real - fs.imag
							* fs0[i][j].imag;
					out.imag = fs.real * fs0[i][j].imag + fs.imag
							* fs0[i][j].real;
					push(out);
				}
			}
		}
	}

	public static final class ConvolutionInterpolation
			extends
				Filter<Complex, Complex> {
		private final int n;
		private final int nx;
		private final int m;
		private final double[] k;
		private final double[] ku;

		// NOTE: <kx> is redefined because of a lack of proper support
		// for global arrays; the array itself is too big to be fully
		// unrolled since it leads to 1.4M lines of code and chokes gcc
		private final double[][] kx;
		double kxmin = 1e25;
		double kxmax = 0;

		double dkx = Math.PI / Statics.X0;
		int is = 8;
		int I = 2 * is + 1;

		double kxs = (double) is * dkx;
		private final double[] KX;

		public ConvolutionInterpolation(int n, int nx, int m, double[] k,
				double[] ku) {
			super(n * m, nx * m);
			if (ku.length != m)
				throw new IllegalArgumentException(
						String.format(
								"Arrat uc's length must be equal to mc. uc.length=%d, mc=%d\n",
								ku.length, m));
			if (k.length != n)
				throw new IllegalArgumentException(
						String.format(
								"Arrat k's length must be equal to n. k.length=%d, n=%d\n",
								k.length, n));
			this.n = n;
			this.nx = nx;
			this.m = m;
			this.k = k;
			this.ku = ku;
			this.kx = new double[n][m];
			this.KX = new double[nx];

			for (int i = 0; i < n; i++) {
				for (int j = 0; j < m; j++) {
					double val = (4 * (k[i] * k[i])) - (ku[j] * ku[j]);

					if (val > 0) {
						kx[i][j] = Math.sqrt(val);
					} else {
						kx[i][j] = 0;
					}

					if (kxmax < kx[i][j]) {
						kxmax = kx[i][j];
					}
					if (kxmin > kx[i][j]) {
						kxmin = kx[i][j];
					}
				}
			}

			for (int i = 0; i < nx; i++) {
				KX[i] = kxmin + ((0 - is) - 2 + i) * dkx;
			}
		}

		@Override
		public void work() {
			Complex[][] F = Utils.initializedComplexArray(nx, m);
			int[][] ikx = new int[I][m];
			double[][] SINC = new double[I][m];
			double[][] HAM = new double[I][m];

			for (int i = 0; i < n; i++) {
				for (int l = 0; l < I; l++) {
					for (int j = 0; j < m; j++) {
						int icKX_j = (int) Math.round((kx[i][j] - KX[0]) / dkx) + 1;

						ikx[l][j] = icKX_j + (l - is);
						ikx[l][j] = ikx[l][j] + (nx * j);

						double nKX_l_j = KX[(ikx[l][j] - 1) % nx];

						double x = (nKX_l_j - kx[i][j]) / dkx;
						if (x == 0) {
							SINC[l][j] = 1;
						} else {
							SINC[l][j] = Math.sin(Math.PI * x) / (Math.PI * x);
						}

						HAM[l][j] = 0.54 + (0.46 * Math.cos((Math.PI / kxs)
								* (nKX_l_j - kx[i][j])));
					}
				}

				for (int j = 0; j < I; j++) {
					for (int l = 0; l < m; l++) {
						Complex t = peek((i * m) + l);

						int ri = (ikx[j][l] - 1) % nx;
						int ci = (ikx[j][l] - 1) / nx;

						F[ri][ci].real += t.real * SINC[j][l] * HAM[j][l];
						F[ri][ci].imag += t.imag * SINC[j][l] * HAM[j][l];
					}
				}
			}

			for (int j = 0; j < nx; j++) {
				for (int l = 0; l < m; l++) {
					push(F[j][l]);
				}
			}
			for (int j = 0; j < n; j++) {
				for (int l = 0; l < m; l++) {
					pop();
				}
			}
		}
	}
}