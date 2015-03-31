package edu.mit.streamjit.test.apps.sar;

import java.io.Serializable;

import edu.mit.streamjit.api.Filter;

/**
 * 
 * Ported from StreamIt. Refer
 * STREAMIT_HOME/apps/benchmarks/sar/streamit/Utils.str for the original
 * implementation.
 * 
 * @author sumanan
 * @since 30 Mar, 2015
 */
public class Utils {

	// convert a stream of complex to its complex conjugate
	public static final class Conjugate extends Filter<Complex, Complex> {
		private final int n;

		public Conjugate(int n) {
			super(n, n);
			this.n = n;
		}

		@Override
		public void work() {
			for (int i = 0; i < n; i++) {
				Complex out = pop();
				out.imag = 0 - out.imag;
				push(out);
			}
		}
	}

	// output is absoluate value of complex input
	public static final class ComplexAbsoluate extends Filter<Complex, Double> {

		public ComplexAbsoluate() {
			super(1, 1);
		}

		@Override
		public void work() {
			Complex in = pop();
			double out = Math.sqrt(Math.pow(in.real, 2) + Math.pow(in.imag, 2));
			push(out);
		}
	}

	// transpose matrix of floating point value
	public static final class FloatTranspose extends Filter<Double, Double> {
		private final int rows;
		private final int cols;

		public FloatTranspose(int rows, int cols) {
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

	public static final class PrintComplex extends Filter<Complex, Void> {
		private final int n;
		private final int m;
		private final boolean transpose;
		public PrintComplex(int n, int m, boolean transpose) {
			super(m * n, 0);
			this.n = n;
			this.m = m;
			this.transpose = transpose;
		}

		@Override
		public void work() {
			Complex[][] t = initializedComplexArray(n, m);
			for (int i = 0; i < n; i++) {
				for (int j = 0; j < m; j++) {
					t[i][j] = pop();
				}
			}

			if (!transpose) {
				for (int i = 0; i < n; i++)
					for (int j = 0; j < m; j++)
						System.out.println((t[i][j].real));
				for (int i = 0; i < n; i++)
					for (int j = 0; j < m; j++)
						System.out.println((t[i][j].imag));
			} else {
				for (int j = 0; j < m; j++)
					for (int i = 0; i < n; i++)
						System.out.println((t[i][j].real));
				for (int j = 0; j < m; j++)
					for (int i = 0; i < n; i++)
						System.out.println((t[i][j].imag));
			}
		}
	}

	public static class Complex implements Serializable {
		private static final long serialVersionUID = 1L;
		public double real;
		public double imag;
		public Complex(double real, double imag) {
			this.real = real;
			this.imag = imag;
		}

		public Complex() {
			this(0, 0);
		}
	}

	public static Complex[][] initializedComplexArray(int rows, int cols) {
		Complex[][] C = new Complex[rows][cols];
		for (int i = 0; i < rows; i++)
			for (int j = 0; j < cols; j++)
				C[i][j] = new Complex();
		return C;
	}

	public static Complex[] initializedComplexArray(int rows) {
		Complex[] C = new Complex[rows];
		for (int i = 0; i < rows; i++)
			C[i] = new Complex();
		return C;
	}
}
