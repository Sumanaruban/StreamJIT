package edu.mit.streamjit.impl.distributed;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.distributed.common.CompilationInfo.BufferSizes;
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

	/**
	 * Calculates the input buffer sizes to avoid deadlocks. Added on
	 * [2014-03-07]. Finds out the buffer sizes through ILP solving.
	 * {@link #sendNewbufSizes()} doesn't guarantee deadlock freeness.
	 */
	public static ImmutableMap<Token, Integer> sendNewbufSizes2(
			Map<Integer, BufferSizes> bufSizes, StreamJitApp<?, ?> app) {
		class bufInfo {
			int steadyInput;
			int steadyOutput;
			int initInput;
			int initOutput;
			Variable outVar;
			Variable inVar;

			void addconstrain(ILPSolver solver) {
				LinearExpr exp = outVar.asLinearExpr(steadyOutput).minus(
						steadyInput, inVar);
				solver.constrainAtLeast(exp,
						(initInput + steadyInput - initOutput));
			}
		}

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

		for (Token blob : app.blobGraph.getBlobIds()) {
			int mul = variables.get(blob).value();
			Set<Token> outputs = app.blobGraph.getOutputs(blob);
			// System.out.println("Multiplication factor of blob "
			// + blob.toString() + " is " + mul);
			for (Token out : outputs) {
				if (out.isOverallOutput())
					continue;
				int initOut = minInitOutputBufCapacity.get(out);
				int steadyOut = minSteadyOutputBufCapacity.get(out);
				int initIn = minInitInputBufCapacity.get(out);
				int steadyIn = minSteadyInputBufCapacity.get(out);
				int inMax = Math.max(initIn, steadyIn);
				int newInSize = Math.max(initOut + steadyOut * mul, inMax);
				finalInputBufCapacity.put(out, newInSize);
			}
		}

		ImmutableMap<Token, Integer> finalInputBuf = finalInputBufCapacity
				.build();

		/*
		 * System.out
		 * .println("InputBufCapacity \t\t - Init \t\t - Steady \t\t - final");
		 * for (Map.Entry<Token, Integer> en :
		 * minInitInputBufCapacity.entrySet()) { System.out.println(en.getKey()
		 * + "\t\t - " + en.getValue() + "\t\t - " +
		 * minSteadyInputBufCapacity.get(en.getKey()) + "\t\t - " +
		 * finalInputBuf.get(en.getKey())); }
		 * System.out.println("minOutputBufCapacity requirement"); for
		 * (Map.Entry<Token, Integer> en : minInitOutputBufCapacity.entrySet())
		 * { System.out.println(en.getKey() + "\t\t - " + en.getValue() +
		 * "\t\t - " + minSteadyOutputBufCapacity.get(en.getKey()) + "\t\t - " +
		 * finalInputBuf.get(en.getKey())); }
		 */

		/*
		 * CTRLRMessageElement me = new CTRLCompilationInfo.FinalBufferSizes(
		 * finalInputBuf); controller.sendToAll(me);
		 */
		return finalInputBuf;
	}
}