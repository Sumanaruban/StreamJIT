package edu.mit.streamjit.impl.distributed;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.distributed.common.CTRLCompilationInfo;
import edu.mit.streamjit.impl.distributed.common.CTRLRMessageElement;
import edu.mit.streamjit.impl.distributed.common.CompilationInfo.BufferSizes;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;
import edu.mit.streamjit.util.ilpsolve.ILPSolver;
import edu.mit.streamjit.util.ilpsolve.ILPSolver.LinearExpr;
import edu.mit.streamjit.util.ilpsolve.ILPSolver.Variable;

/**
 * Calculates {@link Buffer} sizes to ensure deadlock free steady state
 * execution of blobs. Most of the code in this file was extracted
 * {@link StreamJitAppManager} with some re factorization.
 * 
 * @author sumanan
 * @since 6 Feb, 2015
 */
public class BufferSizeCalc {

	private static final boolean printFinalBufSizes = false;

	/**
	 * Sometimes buffer sizes cause performance problems. Method
	 * finalInputBufSizes() ensures the input buffer size is at least
	 * factor*steadyInput. By changing this parameter, we can scale up or down
	 * the buffer sizes.
	 */
	public static final int factor = 3;

	private static class bufInfo {
		int steadyInput;
		int steadyOutput;
		int initInput;
		int initOutput;
		Variable outVar;
		Variable inVar;

		void addconstrain(ILPSolver solver) {
			LinearExpr exp = outVar.asLinearExpr(steadyOutput).minus(
					steadyInput, inVar);
			solver.constrainAtLeast(exp, (initInput + steadyInput - initOutput));
		}
	}

	/**
	 * Calculates the input buffer sizes to avoid deadlocks. Added on
	 * [2014-03-07]. Finds out the buffer sizes through ILP solving.
	 * {@link #sendNewbufSizes()} doesn't guarantee deadlock freeness.
	 */
	public static GraphSchedule finalInputBufSizes(
			Map<Integer, BufferSizes> bufSizes, StreamJitApp<?, ?> app) {

		Map<Token, Integer> minInitInputBufCapacity = new HashMap<>();
		Map<Token, Integer> minInitOutputBufCapacity = new HashMap<>();
		Map<Token, Integer> minSteadyInputBufCapacity = new HashMap<>();
		Map<Token, Integer> minSteadyOutputBufCapacity = new HashMap<>();
		ImmutableMap.Builder<Token, Integer> finalInputBufCapacity = new ImmutableMap.Builder<>();

		for (BufferSizes b : bufSizes.values()) {
			minInitInputBufCapacity.putAll(b.minInitInputBufCapacity);
			minInitOutputBufCapacity.putAll(b.minInitOutputBufCapacity);
			minSteadyInputBufCapacity.putAll(b.minSteadyInputBufCapacity);
			minSteadyOutputBufCapacity.putAll(b.minSteadyOutputBufCapacity);
		}

		ILPSolver solver = new ILPSolver();
		Map<Token, bufInfo> bufInfos = new HashMap<>();
		Map<Token, Variable> variables = new HashMap<>();
		for (Token blob : app.blobGraph.getBlobIds()) {
			Variable v = solver.newVariable(blob.toString());
			variables.put(blob, v);
			LinearExpr expr = v.asLinearExpr(1);
			solver.constrainAtLeast(expr, 1);
			Set<Token> outputs = app.blobGraph.getOutputs(blob);
			for (Token out : outputs) {
				if (out.isOverallOutput())
					continue;
				bufInfo b = new bufInfo();
				b.initOutput = minInitOutputBufCapacity.get(out);
				b.steadyOutput = minSteadyOutputBufCapacity.get(out);
				b.outVar = v;
				bufInfos.put(out, b);
			}
		}

		for (Token blob : app.blobGraph.getBlobIds()) {
			Set<Token> inputs = app.blobGraph.getInputs(blob);
			Variable v = variables.get(blob);
			if (v == null)
				throw new IllegalStateException("No variable");
			for (Token in : inputs) {
				if (in.isOverallInput())
					continue;
				bufInfo b = bufInfos.get(in);
				if (b == null)
					throw new IllegalStateException("No buffer info");
				b.initInput = minInitInputBufCapacity.get(in);
				b.steadyInput = minSteadyInputBufCapacity.get(in);
				b.inVar = v;
				b.addconstrain(solver);
			}
		}

		LinearExpr lf = null;
		for (Variable v : variables.values()) {
			if (lf == null)
				lf = v.asLinearExpr(1);
			else
				lf = lf.plus(1, v);
		}
		solver.minimize(lf);
		solver.solve();

		ImmutableMap.Builder<Token, Integer> steadyRunCount = new ImmutableMap.Builder<>();

		for (Token blob : app.blobGraph.getBlobIds()) {
			int mul = variables.get(blob).value();
			steadyRunCount.put(blob, mul);
			Set<Token> outputs = app.blobGraph.getOutputs(blob);
			// System.out.println("Multiplication factor of blob "
			// + blob.toString() + " is " + mul);
			for (Token out : outputs) {
				if (out.isOverallOutput())
					continue;
				int initOut = minInitOutputBufCapacity.get(out);
				int steadyOut = minSteadyOutputBufCapacity.get(out);
				int scaledInSize = scaledSize(out, minInitInputBufCapacity,
						minSteadyInputBufCapacity);
				int newInSize = Math.max(initOut + steadyOut * mul,
						scaledInSize);
				finalInputBufCapacity.put(out, newInSize);
			}
		}

		ImmutableMap<Token, Integer> finalInputBuf = finalInputBufCapacity
				.build();

		if (printFinalBufSizes)
			printFinalSizes(minInitInputBufCapacity, minInitOutputBufCapacity,
					minSteadyInputBufCapacity, minSteadyOutputBufCapacity,
					finalInputBuf);

		return new GraphSchedule(finalInputBuf, steadyRunCount.build());
	}

	private static void printFinalSizes(
			Map<Token, Integer> minInitInputBufCapacity,
			Map<Token, Integer> minInitOutputBufCapacity,
			Map<Token, Integer> minSteadyInputBufCapacity,
			Map<Token, Integer> minSteadyOutputBufCapacity,
			Map<Token, Integer> finalInputBuf) {
		System.out
				.println("InputBufCapacity \t\t - Init \t\t - Steady \t\t - final");
		for (Map.Entry<Token, Integer> en : minInitInputBufCapacity.entrySet()) {
			System.out.println(en.getKey() + "\t\t - " + en.getValue()
					+ "\t\t - " + minSteadyInputBufCapacity.get(en.getKey())
					+ "\t\t - " + finalInputBuf.get(en.getKey()));
		}
		System.out.println("minOutputBufCapacity requirement");
		for (Map.Entry<Token, Integer> en : minInitOutputBufCapacity.entrySet()) {
			System.out.println(en.getKey() + "\t\t - " + en.getValue()
					+ "\t\t - " + minSteadyOutputBufCapacity.get(en.getKey())
					+ "\t\t - " + finalInputBuf.get(en.getKey()));
		}
	}

	public static int scaledSize(Token t,
			Map<Token, Integer> minInitBufCapacity,
			Map<Token, Integer> minSteadyBufCapacity) {
		return Math.max(factor * minSteadyBufCapacity.get(t),
				minInitBufCapacity.get(t));
	}

	/**
	 * [6 Feb, 2015] Moved from {@link StreamJitAppManager}. This method is not
	 * in use. Lets keep this for a while.
	 * <p>
	 * Calculates the input buffer sizes to avoid deadlocks. Added on
	 * [2014-03-01]
	 */
	private void sendNewbufSizes(Map<Integer, BufferSizes> bufSizes,
			StreamJitApp<?, ?> app, Controller controller) {
		Map<Token, Integer> minInputBufCapacity = new HashMap<>();
		Map<Token, Integer> minOutputBufCapacity = new HashMap<>();
		ImmutableMap.Builder<Token, Integer> finalInputBufCapacity = new ImmutableMap.Builder<>();
		Map<Token, Integer> IORatio = new HashMap<>();

		for (BufferSizes b : bufSizes.values()) {
			minInputBufCapacity.putAll(b.minInitInputBufCapacity);
			minOutputBufCapacity.putAll(b.minInitOutputBufCapacity);
		}
		System.out.println("minInputBufCapacity requirement");
		for (Map.Entry<Token, Integer> en : minInputBufCapacity.entrySet()) {
			System.out.println(en.getKey() + " - " + en.getValue());
		}
		System.out.println("minOutputBufCapacity requirement");
		for (Map.Entry<Token, Integer> en : minOutputBufCapacity.entrySet()) {
			System.out.println(en.getKey() + " - " + en.getValue());
		}

		for (Token t : minInputBufCapacity.keySet()) {
			if (t.isOverallInput())
				continue;
			int outSize = minOutputBufCapacity.get(t);
			int inSize = minInputBufCapacity.get(t);
			IORatio.put(t, (int) Math.ceil(((double) inSize) / outSize));
		}

		for (Token blob : app.blobGraph.getBlobIds()) {
			int mul = 1;
			Set<Token> outputs = app.blobGraph.getOutputs(blob);
			for (Token out : outputs) {
				if (out.isOverallOutput())
					continue;
				mul = Math.max(mul, IORatio.get(out));
			}
			System.out.println("Multiplication factor of blob "
					+ blob.toString() + " is " + mul);
			for (Token out : outputs) {
				if (out.isOverallOutput())
					continue;
				int outSize = minOutputBufCapacity.get(out);
				int inSize = minInputBufCapacity.get(out);
				int newInSize = Math.max(outSize * mul, inSize);
				finalInputBufCapacity.put(out, newInSize);
			}
		}

		ImmutableMap<Token, Integer> finalInputBuf = finalInputBufCapacity
				.build();

		System.out.println("finalInputBufCapacity");
		for (Map.Entry<Token, Integer> en : finalInputBuf.entrySet()) {
			System.out.println(en.getKey() + " - " + en.getValue());
		}

		CTRLRMessageElement me = new CTRLCompilationInfo.FinalBufferSizes(
				finalInputBuf);
		controller.sendToAll(me);
	}

	/**
	 * Contains the whole graph's init schedule, steady state schedule and
	 * buffer sizes.
	 * 
	 * @author sumanan
	 * @since 23 Sep, 2015
	 */
	public static class GraphSchedule {
		/**
		 * Buffer sizes of the blobs inputs that avoid deadlock in the system.
		 */
		public final ImmutableMap<Token, Integer> bufferSizes;

		/**
		 * During the whole graph initialization, upper blobs may need to run
		 * few steady state runs in order to produce enough data for the down
		 * blobs' init schedule.
		 */
		public final ImmutableMap<Token, Integer> steadyRunCount;

		GraphSchedule(ImmutableMap<Token, Integer> bufferSizes,
				ImmutableMap<Token, Integer> steadyRunCount) {
			this.bufferSizes = bufferSizes;
			this.steadyRunCount = steadyRunCount;
		}
	}
}
