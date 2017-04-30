package edu.mit.streamjit.test.apps;

import java.nio.ByteOrder;
import java.nio.file.Paths;

import com.google.common.collect.ImmutableList;

import edu.mit.streamjit.api.DuplicateSplitter;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.test.SuppliedBenchmark;

public class TDE_PPBankBenchmark extends SuppliedBenchmark {

	public TDE_PPBankBenchmark() {
		super(String.format("TDE_PPBank %d,%d", Options.appArg1,
				Options.appArg2), TDE_PPBank.class, ImmutableList
				.of(Options.appArg1), new Dataset("tde_pp.in",
				(Input) Input.fromBinaryFile(Paths.get("data/tde_pp.in"),
						Float.class, ByteOrder.LITTLE_ENDIAN)));
	}

	public static final class TDE_PPBank extends Splitjoin<Float, Float> {
		public TDE_PPBank(int replicas) {
			super(new DuplicateSplitter<Float>(), new RoundrobinJoiner<Float>());
			for (int i = 0; i < replicas; i++) {
				add(new TDE_PP.TDE_PPKernel());
			}
		}

		public TDE_PPBank() {
			this(Options.appArg1);
		}
	}
}