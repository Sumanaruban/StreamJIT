package edu.mit.streamjit.test.apps.sar;

import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.test.apps.sar.Utils.Complex;

/**
 * Ported from StreamIt. Refer
 * STREAMIT_HOME/apps/benchmarks/sar/streamit/FFT.str for the original
 * implementation.
 * 
 * @author sumanan
 * @since 30 Mar, 2015
 */
public class FFT {

	public static final class FTX1D extends Pipeline<Complex, Complex> {
		public FTX1D(int rows) {
			add(new FFT1Dshift(rows));
			add(new FFT1D(rows));
			add(new FFT1Dshift(rows));
		}
	}

	public static final class FTX2D extends Pipeline<Complex, Complex> {
		public FTX2D(int rows, int cols) {
			add(new FFT2Dshift(rows, cols));
			add(new FFT2D(rows, cols));
			add(new FFT2Dshift(rows, cols));
		}
	}

	public static final class FTY2D extends Pipeline<Complex, Complex> {
		public FTY2D(int rows, int cols) {
			add(new Transpose(rows, cols));
			add(new FFT2Dshift(cols, rows));
			add(new FFT2D(cols, rows));
			add(new FFT2Dshift(cols, rows));
			add(new Transpose(cols, rows));
		}
	}

	public static final class iFTX2D extends Pipeline<Complex, Complex> {
		public iFTX2D(int rows, int cols) {
			add(new FFT2Dshift(rows, cols));
			add(new iFFT2D(rows, cols));
			add(new FFT2Dshift(rows, cols));
		}
	}

	public static final class iFTY2D extends Pipeline<Complex, Complex> {
		public iFTY2D(int rows, int cols) {
			add(new Transpose(rows, cols));
			add(new FFT2Dshift(cols, rows));
			add(new iFFT2D(cols, rows));
			add(new FFT2Dshift(cols, rows));
			add(new Transpose(cols, rows));
		}
	}

	// transpose a 2D stream
	public static final class Transpose extends Filter<Complex, Complex> {
		private final int rows;
		private final int cols;

		public Transpose(int rows, int cols) {
			super(rows * cols, rows * cols);
			this.rows = rows;
			this.cols = cols;
		}

		@Override
		public void work() {
			for (int j = 0; j < cols; j++)
				for (int i = 0; i < rows; i++)
					push(peek(i * cols + j));
			for (int i = 0; i < rows * cols; i++)
				pop();
		}
	}

	// 1D FFT shift: swap the two halves of a vector
	public static final class FFT1Dshift extends Filter<Complex, Complex> {
		private final int rows;
		private final int rows_mid;

		public FFT1Dshift(int rows) {
			super(rows, rows);
			this.rows = rows;

			if (rows % 2 == 1) {
				rows_mid = (rows - 1) / 2;
			} else {
				rows_mid = rows / 2;
			}
		}

		@Override
		public void work() {
			int r = rows_mid;

			for (int i = 0; i < rows; i++) {
				Complex t = peek(r);
				push(t);

				if (++r == rows) {
					r = 0;
				}
			}

			for (int i = 0; i < rows; i++)
				pop();
		}
	}

	// 2D FFT shift: criss-cross swap of the four matrix quadrants
	// NOTE: if there were a reverse roundrobin joiner, it would be
	// trivial to do this data reorganization
	public static final class FFT2Dshift extends Filter<Complex, Complex> {
		private final int rows;
		private final int cols;
		private final int rows_mid;
		private final int cols_mid;

		public FFT2Dshift(int rows, int cols) {
			super(rows * cols, rows * cols);
			this.rows = rows;
			this.cols = cols;

			if (rows % 2 == 1) {
				rows_mid = (rows - 1) / 2;
			} else {
				rows_mid = rows / 2;
			}

			if (cols % 2 == 1) {
				cols_mid = (cols - 1) / 2;
			} else {
				cols_mid = cols / 2;
			}
		}

		@Override
		public void work() {

			Complex[][] temp = Utils.initializedComplexArray(rows, cols);
			int r = rows_mid;
			int c = cols_mid;

			for (int i = 0; i < rows; i++) {
				for (int j = 0; j < cols; j++) {
					temp[r][c] = pop();

					if (++c == cols) {
						c = 0;
					}
				}

				if (++r == rows) {
					r = 0;
				}
			}

			for (int i = 0; i < rows; i++) {
				for (int j = 0; j < cols; j++) {
					push(temp[i][j]);
				}
			}
		}
	}

	// 1D FFT
	public static final class FFT1D extends Filter<Complex, Complex> {
		private final double[] cos_value;
		private final double[] sin_value;
		private final int rows;

		public FFT1D(int rows) {
			super(rows, rows, rows);
			cos_value = new double[rows];
			sin_value = new double[rows];
			this.rows = rows;

			for (int i = 0; i < rows; i++) {
				cos_value[i] = Math.cos(2 * Math.PI * (double) i
						/ (double) rows);
				sin_value[i] = Math.sin(2 * Math.PI * (double) i
						/ (double) rows);
			}
		}

		@Override
		public void work() {
			Complex[] temp = Utils.initializedComplexArray(rows);

			for (int i = 0; i < rows; i++) {
				float real = 0;
				float imag = 0;

				for (int j = 0; j < rows; j++) {
					int k = (i * j) % rows;

					Complex t = peek(j);
					real += (t.real * cos_value[k]) + (t.imag * sin_value[k]);
					imag += (t.imag * cos_value[k]) - (t.real * sin_value[k]);
				}
				temp[i].real = real;
				temp[i].imag = imag;
			}

			for (int i = 0; i < rows; i++) {
				pop();
				push(temp[i]);
			}
		}
	}

	// 2D FFT
	public static final class FFT2D extends Filter<Complex, Complex> {
		private final double[] cos_value;
		private final double[] sin_value;
		private final int rows;
		private final int cols;

		public FFT2D(int rows, int cols) {
			super(rows * cols, rows * cols);
			cos_value = new double[rows];
			sin_value = new double[rows];
			this.rows = rows;
			this.cols = cols;

			for (int i = 0; i < rows; i++) {
				cos_value[i] = Math.cos(2 * Math.PI * (double) i
						/ (double) rows);
				sin_value[i] = Math.sin(2 * Math.PI * (double) i
						/ (double) rows);
			}
		}

		@Override
		public void work() {

			Complex[][] temp = Utils.initializedComplexArray(rows, cols);

			for (int i = 0; i < cols; i++) {
				for (int j = 0; j < rows; j++) {
					float real = 0;
					float imag = 0;

					for (int k = 0; k < rows; k++) {
						int l = (j * k) % rows;

						Complex t = peek(k * cols + i);

						real += (t.real * cos_value[l])
								+ (t.imag * sin_value[l]);
						imag += (t.imag * cos_value[l])
								- (t.real * sin_value[l]);
					}
					temp[j][i].real = real;
					temp[j][i].imag = imag;
				}
			}

			for (int i = 0; i < rows; i++) {
				for (int j = 0; j < cols; j++) {
					pop();
					push(temp[i][j]);
				}
			}
		}
	}

	// 2D iFFT
	public static final class iFFT2D extends Filter<Complex, Complex> {
		private final double[] cos_value;
		private final double[] sin_value;
		private final int rows;
		private final int cols;

		public iFFT2D(int rows, int cols) {
			super(rows * cols, rows * cols);
			cos_value = new double[rows];
			sin_value = new double[rows];
			this.rows = rows;
			this.cols = cols;

			for (int i = 0; i < rows; i++) {
				cos_value[i] = Math.cos(2 * Math.PI * (double) i
						/ (double) rows);
				sin_value[i] = Math.sin(2 * Math.PI * (double) i
						/ (double) rows);
			}
		}

		@Override
		public void work() {

			Complex[][] temp = Utils.initializedComplexArray(rows, cols);

			for (int i = 0; i < cols; i++) {
				for (int j = 0; j < rows; j++) {
					float real = 0;
					float imag = 0;

					for (int k = 0; k < rows; k++) {
						int l = (j * k) % rows;

						Complex t = peek(k * cols + i);

						real += (t.real * cos_value[l])
								- (t.imag * sin_value[l]);
						imag += (t.imag * cos_value[l])
								+ (t.real * sin_value[l]);
					}
					temp[j][i].real = real / (float) rows;
					temp[j][i].imag = imag / (float) rows;
				}
			}

			for (int i = 0; i < rows; i++) {
				for (int j = 0; j < cols; j++) {
					pop();
					push(temp[i][j]);
				}
			}
		}
	}
}
