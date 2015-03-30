package edu.mit.streamjit.tuner;

import java.io.FileWriter;

import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.distributed.StreamJitApp;
import edu.mit.streamjit.impl.distributed.common.Utils;

/**
 * Prognosticates the {@link Configuration}s given by the OpenTuner and tell
 * whether a {@link Configuration} is more likely to give a better search
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
	 * Prognosticate a {@link Configuration} and tell whether a
	 * {@link Configuration} is more likely to give a better search objective
	 * improvement or not.
	 * 
	 * @param config
	 * @return {@code true} iff the config is more likely to give a better
	 *         search objective improvement.
	 */
	public boolean prognosticate(Configuration config);

	/**
	 * An auxiliary method that can be used to update a configuration's running
	 * time. Has been added for data analysis purpose.
	 * 
	 * @param time
	 */
	public void time(double time);

	/**
	 * No Prognostication. The method {@link #prognosticate(Configuration)}
	 * always returns {@code true}
	 */
	public static final class NoPrognostication implements
			ConfigurationPrognosticator {

		@Override
		public boolean prognosticate(Configuration config) {
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

		private final ImmutableSet<ConfigurationPrognosticator> configProgs;

		public ManyPrognosticators(StreamJitApp<?, ?> app) {
			FileWriter writer = Utils.fileWriter(app.name, "manyProgs.txt");
			ConfigurationPrognosticator cp1 = new GraphPropertyPrognosticator(
					app, writer, true);
			ConfigurationPrognosticator cp2 = new DistanceMatrixPrognosticator(
					writer);
			ImmutableSet.Builder<ConfigurationPrognosticator> builder = ImmutableSet
					.builder();
			builder.add(cp1);
			builder.add(cp2);
			configProgs = builder.build();
		}

		public ManyPrognosticators(ConfigurationPrognosticator cp1,
				ConfigurationPrognosticator cp2,
				ConfigurationPrognosticator... cps) {
			ImmutableSet.Builder<ConfigurationPrognosticator> builder = ImmutableSet
					.builder();
			builder.add(cp1);
			builder.add(cp2);
			builder.add(cps);
			configProgs = builder.build();
		}

		@Override
		public boolean prognosticate(Configuration config) {
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
		}
	}
}
