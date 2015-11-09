package edu.mit.streamjit.impl.distributed;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.distributed.common.CTRLCompilationInfo;
import edu.mit.streamjit.impl.distributed.common.CTRLRMessageElement;
import edu.mit.streamjit.impl.distributed.common.CTRLRMessageElement.CTRLRMessageElementHolder;
import edu.mit.streamjit.impl.distributed.common.CompilationInfo.BufferSizes;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;
import edu.mit.streamjit.util.Pair;
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

	/**
	 * Calculates the input buffer sizes to avoid deadlocks. Added on
	 * [2014-03-07]. Finds out the buffer sizes through ILP solving.
	 * {@link #sendNewbufSizes()} doesn't guarantee deadlock freeness.
	 */
	public static GraphSchedule finalInputBufSizes(
			Map<Integer, BufferSizes> bufSizes, AppInstance appInst) {
		ImmutableMap.Builder<Token, Integer> finalInputBufCapacity = new ImmutableMap.Builder<>();
		ImmutableMap.Builder<Token, Integer> steadyRunCount = new ImmutableMap.Builder<>();
		boolean inputConsidered = true;
		MinInfo minInfo = new MinInfo(bufSizes);
		int totalGraphOutDuringInit = 0;
		int totalGraphInDuringInit = 0;

		Map<Token, Variable> variables = ilpSolve(appInst, inputConsidered,
				minInfo);

		for (Token blob : appInst.blobGraph.getBlobIds()) {
			int mul = variables.get(blob).value();
			steadyRunCount.put(blob, mul);
			Set<Token> outputs = appInst.blobGraph.getOutputs(blob);
			for (Token out : outputs) {
				int initOut = minInfo.minInitOutputBufCapacity.get(out);
				int steadyOut = minInfo.minSteadyOutputBufCapacity.get(out);
				int totalOut = initOut + steadyOut * mul;
				if (out.isOverallOutput()) {
					totalGraphOutDuringInit = totalOut;
				} else {
					int scaledInSize = scaledSize(out,
							minInfo.minInitInputBufCapacity,
							minInfo.minSteadyInputBufCapacity);
					int newInSize = Math.max(totalOut, scaledInSize);
					finalInputBufCapacity.put(out, newInSize);
				}
			}
		}

		totalGraphInDuringInit = totalGraphInDuringInit(appInst, minInfo,
				variables);

		ImmutableMap<Token, Integer> finalInputBuf = finalInputBufCapacity
				.build();
		if (printFinalBufSizes)
			printFinalSizes(minInfo, finalInputBuf);
		steadyStateRatios(minInfo, appInst);
		return new GraphSchedule(finalInputBuf, steadyRunCount.build(),
				totalGraphInDuringInit, totalGraphOutDuringInit);
	}

	private static int totalGraphInDuringInit(AppInstance appInst,
			MinInfo minInfo, Map<Token, Variable> variables) {
		int totalGraphInDuringInit;
		Token headToken = appInst.app.headToken;
		int mul = variables.get(headToken).value();
		int initIn = minInfo.minInitInputBufCapacity.get(headToken);
		int steadyIn = minInfo.minSteadyInputBufCapacity.get(headToken);
		totalGraphInDuringInit = initIn + steadyIn * mul;
		return totalGraphInDuringInit;
	}

	/**
	 * Calculates blobs' execution ratios during steady state execution.
	 * Calculates the total graph's steady state input and output.
	 */
	private static Pair<Integer, Integer> steadyStateRatios(MinInfo minInfo,
			AppInstance appInst) {
		int steadyIn = -1;
		int steadyOut = -1;
		Pair<Token, Token> p = getGlobalOutTokens(appInst);
		Token globalOutToken = p.first;
		Token globalOutBlob = p.second;
		Token globalInToken = getGlobalInToken(appInst);
		boolean inputConsidered = false;

		Map<Token, Variable> variables = ilpSolve(appInst, inputConsidered,
				minInfo);
		for (Token blob : appInst.blobGraph.getBlobIds()) {
			int steadyRun = variables.get(blob).value();
			// System.out.println("Steady run factor of blob " + blob.toString()
			// + " is " + steadyRun);
			if (blob.equals(globalInToken))
				steadyIn = minInfo.minSteadyInputBufCapacity.get(globalInToken)
						* steadyRun;
			if (blob.equals(globalOutBlob))
				steadyOut = minInfo.minSteadyOutputBufCapacity
						.get(globalOutToken) * steadyRun;
		}
		System.out.println("Total graph's steady in = " + steadyIn);
		System.out.println("Total graph's steady out = " + steadyOut);
		return new Pair<Integer, Integer>(steadyIn, steadyOut);
	}

	private static Map<Token, Variable> ilpSolve(AppInstance appInst,
			boolean inputConsidered, MinInfo minInfo) {
		ILPSolver solver = new ILPSolver();
		Map<Token, bufInfo> bufInfos = new HashMap<>();
		Map<Token, Variable> variables = new HashMap<>();
		setOutputVariables(appInst, inputConsidered, minInfo, solver, bufInfos,
				variables);
		setInputVariables(appInst, minInfo, solver, bufInfos, variables);
		solve(solver, variables);
		return variables;
	}

	private static void solve(ILPSolver solver, Map<Token, Variable> variables) {
		LinearExpr lf = null;
		for (Variable v : variables.values()) {
			if (lf == null)
				lf = v.asLinearExpr(1);
			else
				lf = lf.plus(1, v);
		}
		solver.minimize(lf);
		solver.solve();
	}

	private static void setOutputVariables(AppInstance appInst,
			boolean inputConsidered, MinInfo minInfo, ILPSolver solver,
			Map<Token, bufInfo> bufInfos, Map<Token, Variable> variables) {
		for (Token blob : appInst.blobGraph.getBlobIds()) {
			Variable v = solver.newVariable(blob.toString());
			variables.put(blob, v);
			LinearExpr expr = v.asLinearExpr(1);
			solver.constrainAtLeast(expr, 1);
			Set<Token> outputs = appInst.blobGraph.getOutputs(blob);
			for (Token out : outputs) {
				if (out.isOverallOutput())
					continue;
				bufInfo b = makeBufInfo(inputConsidered);
				b.addOutputs(minInfo.minSteadyOutputBufCapacity.get(out),
						minInfo.minInitOutputBufCapacity.get(out));
				b.outVar = v;
				bufInfos.put(out, b);
			}
		}
	}

	private static void setInputVariables(AppInstance appInst, MinInfo minInfo,
			ILPSolver solver, Map<Token, bufInfo> bufInfos,
			Map<Token, Variable> variables) {
		for (Token blob : appInst.blobGraph.getBlobIds()) {
			Set<Token> inputs = appInst.blobGraph.getInputs(blob);
			Variable v = variables.get(blob);
			if (v == null)
				throw new IllegalStateException("No variable");
			for (Token in : inputs) {
				if (in.isOverallInput())
					continue;
				bufInfo b = bufInfos.get(in);
				if (b == null)
					throw new IllegalStateException("No buffer info");
				b.addInputs(minInfo.minSteadyInputBufCapacity.get(in),
						minInfo.minInitInputBufCapacity.get(in));
				b.inVar = v;
				b.addconstrain(solver);
			}
		}
	}

	/**
	 * @return Very last blob's blobID and global output token.
	 */
	private static Pair<Token, Token> getGlobalOutTokens(AppInstance appInst) {
		for (Token blob : appInst.blobGraph.getBlobIds()) {
			Set<Token> outputs = appInst.blobGraph.getOutputs(blob);
			for (Token out : outputs) {
				if (out.isOverallOutput()) {
					return new Pair<>(out, blob);
				}
			}
		}
		throw new IllegalStateException("Global output token is Null");
	}

	/**
	 * TODO: Remove this method and use {@link StreamJitApp#tailToken}, instead.
	 * 
	 * @param appInst
	 * @return
	 */
	private static Token getGlobalInToken(AppInstance appInst) {
		for (Token blob : appInst.blobGraph.getBlobIds()) {
			Set<Token> inputs = appInst.blobGraph.getInputs(blob);
			for (Token in : inputs) {
				if (in.isOverallInput()) {
					return in;
				}
			}
		}
		throw new IllegalStateException("Global input token is Null");
	}

	private static bufInfo makeBufInfo(boolean inputConsidered) {
		if (inputConsidered)
			return new bufInfo1();
		return new bufInfo2();
	}

	private static void printFinalSizes(MinInfo minInfo,
			Map<Token, Integer> finalInputBuf) {
		System.out
				.println("InputBufCapacity \t\t - Init \t\t - Steady \t\t - final");
		for (Map.Entry<Token, Integer> en : minInfo.minInitInputBufCapacity
				.entrySet()) {
			System.out.println(en.getKey() + "\t\t - " + en.getValue()
					+ "\t\t - "
					+ minInfo.minSteadyInputBufCapacity.get(en.getKey())
					+ "\t\t - " + finalInputBuf.get(en.getKey()));
		}
		System.out.println("minOutputBufCapacity requirement");
		for (Map.Entry<Token, Integer> en : minInfo.minInitOutputBufCapacity
				.entrySet()) {
			System.out.println(en.getKey() + "\t\t - " + en.getValue()
					+ "\t\t - "
					+ minInfo.minSteadyOutputBufCapacity.get(en.getKey())
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
			AppInstance appInst, Controller controller) {
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

		for (Token blob : appInst.blobGraph.getBlobIds()) {
			int mul = 1;
			Set<Token> outputs = appInst.blobGraph.getOutputs(blob);
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
		controller.sendToAll(new CTRLRMessageElementHolder(me, appInst.id));
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

		public final int totalOutDuringInit;
		public final int totalInDuringInit;

		GraphSchedule(ImmutableMap<Token, Integer> bufferSizes,
				ImmutableMap<Token, Integer> steadyRunCount,
				int totalInDuringInit, int totalOutDuringInit) {
			this.bufferSizes = bufferSizes;
			this.steadyRunCount = steadyRunCount;
			this.totalInDuringInit = totalInDuringInit;
			this.totalOutDuringInit = totalOutDuringInit;
		}
	}

	private static class MinInfo {
		Map<Token, Integer> minInitInputBufCapacity = new HashMap<>();
		Map<Token, Integer> minInitOutputBufCapacity = new HashMap<>();
		Map<Token, Integer> minSteadyInputBufCapacity = new HashMap<>();
		Map<Token, Integer> minSteadyOutputBufCapacity = new HashMap<>();

		MinInfo(Map<Integer, BufferSizes> bufSizes) {
			for (BufferSizes b : bufSizes.values()) {
				minInitInputBufCapacity.putAll(b.minInitInputBufCapacity);
				minInitOutputBufCapacity.putAll(b.minInitOutputBufCapacity);
				minSteadyInputBufCapacity.putAll(b.minSteadyInputBufCapacity);
				minSteadyOutputBufCapacity.putAll(b.minSteadyOutputBufCapacity);
			}
		}
	}
}

abstract class bufInfo {
	protected int steadyInput;
	protected int steadyOutput;
	protected int initInput;
	protected int initOutput;
	Variable outVar;
	Variable inVar;

	abstract void addInputs(int steadyInput, int initInput);
	abstract void addOutputs(int steadyOutput, int initOutput);
	abstract void addconstrain(ILPSolver solver);
}

class bufInfo1 extends bufInfo {

	void addconstrain(ILPSolver solver) {
		LinearExpr exp = outVar.asLinearExpr(steadyOutput).minus(steadyInput,
				inVar);
		solver.constrainAtLeast(exp, (initInput + steadyInput - initOutput));
	}

	@Override
	void addInputs(int steadyInput, int initInput) {
		this.steadyInput = steadyInput;
		this.initInput = initInput;
	}

	@Override
	void addOutputs(int steadyOutput, int initOutput) {
		this.steadyOutput = steadyOutput;
		this.initOutput = initOutput;
	}
}

class bufInfo2 extends bufInfo {

	void addconstrain(ILPSolver solver) {
		LinearExpr exp = outVar.asLinearExpr(steadyOutput).minus(steadyInput,
				inVar);
		solver.constrainEquals(exp, 0);
	}

	@Override
	void addInputs(int steadyInput, int initInput) {
		this.steadyInput = steadyInput;
		this.initInput = 0;
	}

	@Override
	void addOutputs(int steadyOutput, int initOutput) {
		this.steadyOutput = steadyOutput;
		this.initOutput = 0;
	}
}