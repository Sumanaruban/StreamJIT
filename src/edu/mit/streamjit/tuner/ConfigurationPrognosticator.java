package edu.mit.streamjit.tuner;

import java.io.IOException;
import java.io.OutputStreamWriter;

import com.google.common.collect.ImmutableList;

import edu.mit.streamjit.impl.distributed.common.Utils;
import edu.mit.streamjit.impl.distributed.controller.StreamJitApp;
import edu.mit.streamjit.impl.distributed.controller.ConfigurationManager.NewConfiguration;

/**
 * Prognosticates a {@link NewConfiguration} given by the OpenTuner and tell
 * whether the {@link NewConfiguration} is more likely to give a better search
 * objective improvement or not. Depends on the prognosticated information,
 * {@link OnlineTuner} may reconfigure the application or reject the
 * configuration. Currently, the search objective is performance optimization.
 * In future, some other resource optimization objectives may be added (e.g.,
 * Energy minimization).
 * 
 * @author sumanan
 * @since 6 Jan, 2015
 */
public interface ConfigurationPrognosticator {

	/**
	 * Prognosticate a {@link NewConfiguration} and tell whether the
	 * {@link NewConfiguration} is more likely to give a better search objective
	 * improvement or not.
	 * 
	 * @param config
	 * @return {@code true} iff the config is more likely to give a better
	 *         search objective improvement.
	 */
	public boolean prognosticate(NewConfiguration config);

	/**
	 * An auxiliary method that can be used to update a configuration's running
	 * time. Has been added for data analysis purpose.
	 * 
	 * @param time
	 */
	public void time(double time);

	/**
	 * No Prognostication. The method {@link #prognosticate(NewConfiguration)}
	 * always returns {@code true}
	 */
	public static final class NoPrognostication implements
			ConfigurationPrognosticator {

		@Override
		public boolean prognosticate(NewConfiguration config) {
			return true;
		}

		@Override
		public void time(double time) {
		}
	}

	/**
	 * ManyPrognosticators calls set of {@link ConfigurationPrognosticator}s.
	 */
	public static final class ManyPrognosticators implements
			ConfigurationPrognosticator {

		private final ImmutableList<ConfigurationPrognosticator> configProgs;

		private final OutputStreamWriter writer;

		public ManyPrognosticators(StreamJitApp<?, ?> app) {
			writer = Utils.fileWriter(app.name, "manyProgs.txt");
			ConfigurationPrognosticator cp1 = new GraphPropertyPrognosticator(
					app, writer, false);
			ConfigurationPrognosticator cp2 = new DistanceMatrixPrognosticator(
					writer, false);
			ImmutableList.Builder<ConfigurationPrognosticator> builder = ImmutableList
					.builder();
			builder.add(cp1);
			builder.add(cp2);
			configProgs = builder.build();
			writeTimeHeader(writer);
		}

		public ManyPrognosticators(ConfigurationPrognosticator cp1,
				ConfigurationPrognosticator cp2,
				ConfigurationPrognosticator... cps) {
			ImmutableList.Builder<ConfigurationPrognosticator> builder = ImmutableList
					.builder();
			writer = Utils.fileWriter("manyProgs.txt");
			builder.add(cp1);
			builder.add(cp2);
			builder.add(cps);
			configProgs = builder.build();
			writeTimeHeader(writer);
		}

		@Override
		public boolean prognosticate(NewConfiguration config) {
			boolean ret = true;
			for (ConfigurationPrognosticator cp : configProgs) {
				ret = ret & cp.prognosticate(config);
			}
			return ret;
		}

		@Override
		public void time(double time) {
			for (ConfigurationPrognosticator cp : configProgs) {
				cp.time(time);
			}
			try {
				writer.write(String.format("%.0f\n", time));
				writer.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private static void writeTimeHeader(OutputStreamWriter writer) {
			try {
				writer.write(String.format("%.7s", "time"));
				writer.write("\n");
				writer.flush();
			} catch (IOException e) {

			}
		}
	}
}
