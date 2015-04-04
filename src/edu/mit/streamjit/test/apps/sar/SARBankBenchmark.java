package edu.mit.streamjit.test.apps.sar;

import java.util.Collections;

import com.google.common.collect.ImmutableList;

import edu.mit.streamjit.api.DuplicateSplitter;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.test.SuppliedBenchmark;

/**
 * @author sumanan
 * @since 4 Apr, 2015
 */
public class SARBankBenchmark extends SuppliedBenchmark {

	public static final int ITEMS = 10_000_000;
	public SARBankBenchmark() {
		super(String.format("SARBank %d", Options.appArg1), SARBank.class,
				ImmutableList.of(Options.appArg1), new Dataset("" + ITEMS,
						(Input) Input.fromIterable(Collections.nCopies(ITEMS,
								(byte) 0))));
	}
	public static final class SARBank extends Splitjoin<Void, Double> {
		public SARBank(int replicas) {
			super(new DuplicateSplitter<Void>(), new RoundrobinJoiner<Double>());
			for (int i = 0; i < replicas; i++) {
				add(new SAR.SARKernel());
			}
		}

		public SARBank() {
			this(Options.appArg1);
		}
	}
}
