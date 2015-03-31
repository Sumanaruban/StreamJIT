package edu.mit.streamjit.test.apps.sar;

import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.test.apps.sar.Utils.Complex;

public class GenRawSARStr {

	public static final class GenRawSAR extends Filter<Integer, Complex> {

		private final double Tf;
		private final int n;
		private final int m;
		private final int mc;
		private final double[] t;
		private final double[] k;
		private final double[] ku;
		private final double[] uc;

		// %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
		// %% Pre-determine the pixel size of the eventual image in the %%
		// %% X-dimm, to control placement of SAR returns relative to %%
		// %% pixel-based templates placement. (Code from formSARimage()) %%
		// %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
		private double kxmin = 1e25;
		private double kxmax = 0;
		private final double dkx = Math.PI / Statics.X0;
		private final int is = 8;
		private int nx = 0;
		private final double[][] kx;

		// %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
		// %% Parameters of Targets %%
		// %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
		//
		// % SAR reflectors within digital spotlight filter
		// % [targets in an equally spaced mesh, all with unity
		// % reflection coefficient.]

		/* letter image size */
		private final int N_pix = 24;

		private double npixx = 0;
		private double npixy = 0;
		private int xni = 0;
		private int yni = 0;
		private int ntarget = 0;
		// NOTE: to statically resolve the value of ntarget requires aggressive
		// constant propagation of kx[][] which is 70K elements in size; so
		// hardcode ntarget = 48 for now
		private final double[] xn;
		private final double[] yn;
		private final double[] fn;

		public GenRawSAR(double Tf, int n, int m, int mc, double[] t,
				double[] k, double[] ku, double[] uc) {
			super(1, n * mc);
			if (t.length != n)
				throw new IllegalArgumentException(
						String.format(
								"Arrat t's length must be equal to n. t.length=%d, n=%d\n",
								t.length, n));
			if (k.length != n)
				throw new IllegalArgumentException(
						String.format(
								"Arrat k's length must be equal to n. k.length=%d, n=%d\n",
								k.length, n));
			if (ku.length != m)
				throw new IllegalArgumentException(
						String.format(
								"Arrat ku's length must be equal to m. ku.length=%d, m=%d\n",
								ku.length, m));
			if (uc.length != mc)
				throw new IllegalArgumentException(
						String.format(
								"Arrat uc's length must be equal to mc. uc.length=%d, mc=%d\n",
								uc.length, mc));
			this.Tf = Tf;
			this.n = n;
			this.m = m;
			this.mc = mc;
			this.t = t;
			this.k = k;
			this.ku = ku;
			this.uc = uc;
			this.kx = new double[n][m];
			this.xn = new double[48];
			this.yn = new double[48];
			this.fn = new double[48];
			init();
		}

		private void init() {

			// %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
			// %% Pre-determine the pixel size of the eventual image in the %%
			// %% X-dimm, to control placement of SAR returns relative to %%
			// %% pixel-based templates placement. (Code from formSARimage()) %%
			// %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
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

			nx = 2 * (int) Math.ceil((0.5 * (kxmax - kxmin)) / dkx);
			nx = nx + 2 * is + 4;

			// %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
			// %% Parameters of Targets %%
			// %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
			npixx = (N_pix / (float) nx) * 2 * Statics.X0;
			npixy = (N_pix / (float) m) * 2 * Statics.Y0;

			xni = (int) ((((2 * Statics.X0 - Math.ceil(npixx / 2)) - Math
					.floor(npixx / 2)) / (2 * npixx)) + 1);

			yni = (int) ((((2 * Statics.Y0 - Math.ceil(npixy / 2)) - Math
					.floor(npixy / 2)) / (2 * npixy)) + 1);

			/* number of targets */
			ntarget = xni * yni;

			for (int i = 0; i < xni; i++) {
				for (int j = 0; j < yni; j++) {
					xn[i * yni + j] = (Math.floor(npixx / 2) + 2 * npixx * i)
							- Statics.X0;
					yn[i * yni + j] = (Math.floor(npixy / 2) + 2 * npixy * j)
							- Statics.Y0;
					fn[i * yni + j] = 1;
				}
			}

		}
		@Override
		public void work() {
			double[][] td = new double[n][mc];
			Complex[][] S = Utils.initializedComplexArray(n, mc);
			pop();

			for (int h = 0; h < ntarget; h++) {
				for (int i = 0; i < n; i++) {
					for (int j = 0; j < mc; j++) {
						td[i][j] = t[i]
								- 2
								* Math.sqrt(Math.pow(Statics.Xc + xn[h], 2)
										+ Math.pow(yn[h] - uc[j], 2))
								/ Statics.c;

						if ((td[i][j] >= 0.0) && (td[i][j] <= Statics.Tp)
								&& (Math.abs(uc[j]) <= Statics.L)
								&& (t[i] < Tf)) {
							double value = Math.PI
									* 2
									* ((Statics.fc - Statics.f0) * td[i][j] + (Statics.f0 / Statics.Tp)
											* Math.pow(td[i][j], 2));
							S[i][j].real += fn[h] * Math.cos(value);
							S[i][j].imag += fn[h] * Math.sin(value);
						}
					}
				}
			}

			for (int i = 0; i < n; i++) {
				double cos_value = Math.cos(Math.PI * 2 * Statics.fc * t[i]);
				double sin_value = Math.sin(Math.PI * 2 * Statics.fc * t[i]);

				for (int j = 0; j < mc; j++) {
					Complex out = new Complex(0, 0);
					out.real = S[i][j].real * cos_value + S[i][j].imag
							* sin_value;
					out.imag = S[i][j].imag * cos_value - S[i][j].real
							* sin_value;
					push(out);
				}
			}
		}
	}

	public static final class FastTimeFilter extends Pipeline<Integer, Complex> {

		public FastTimeFilter(int n, double[] t) {
			if (t.length != n)
				throw new IllegalArgumentException(
						String.format(
								"Array t's length must be equal to n. t.length=%d, n=%d\n",
								t.length, n));
			add(new Filter1(n, t));
			add(new FFT.FTX1D(n));
			add(new Utils.Conjugate(n));
		}

		static class Filter1 extends Filter<Integer, Complex> {
			private final int n;
			private final double[] t;
			public Filter1(int n, double[] t) {
				super(1, n);
				this.n = n;
				this.t = t;
			}

			@Override
			public void work() {
				double[] td0 = new double[n];
				Complex[] s0 = Utils.initializedComplexArray(n);
				Complex[] ftf = Utils.initializedComplexArray(n);
				pop();
				for (int i = 0; i < n; i++) {
					td0[i] = t[i] - 2 * Statics.Xc / Statics.c;
					if ((td0[i] >= 0.0) && (td0[i] <= Statics.Tp)) {
						double value = Math.PI
								* 2
								* ((Statics.fc - Statics.f0) * td0[i] + (Statics.f0 / Statics.Tp)
										* Math.pow(td0[i], 2));
						s0[i].real = Math.cos(value);
						s0[i].imag = Math.sin(value);
					}

					double value = Math.PI * 2 * Statics.fc * t[i];
					double cos_value = Math.cos(value);
					double sin_value = Math.sin(value);

					ftf[i].real = s0[i].real * cos_value + s0[i].imag
							* sin_value;
					ftf[i].imag = s0[i].imag * cos_value - s0[i].real
							* sin_value;

					push(ftf[i]);
				}

			}
		}
	}
}
