package edu.mit.streamjit.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.FloatParameter;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.common.Configuration.Parameter;
import edu.mit.streamjit.impl.common.Configuration.SwitchParameter;

/**
 * {@link ConfigurationUtils} contains common utility methods those deal with
 * {@link Configuration}.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 10, 2013
 *
 */
public class ConfigurationUtils {

	public static final String configDir = "configurations";

	/**
	 * Reads configuration from ./appName/configurations/namePrefix_appName.cfg
	 * and returns it.
	 * 
	 * @param appName
	 *            name of the streamJit app.
	 * 
	 * @param namePrefix
	 *            prefix to add to the cfg file name.
	 * 
	 * @return {@link Configuration} object if valid file exists. Otherwise
	 *         returns null.
	 */
	public static Configuration readConfiguration(String appName,
			String namePrefix) {
		checkNotNull(appName);
		namePrefix = namePrefix == null ? "" : namePrefix;
		String cfgFilePath = String.format("%s%s%s%s%s_%s.cfg", appName,
				File.separator, configDir, File.separator, namePrefix, appName);
		return readConfiguration(cfgFilePath);
	}

	/**
	 * @param cfgFilePath
	 *            path of the configuration file that need to be read.
	 * @return {@link Configuration} object if valid file exists. Otherwise
	 *         returns null.
	 */
	public static Configuration readConfiguration(String cfgFilePath) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(
					cfgFilePath));
			String json = reader.readLine();
			reader.close();
			return Configuration.fromJson(json);
		} catch (IOException ex) {
			System.err.println(String
					.format("File reader error. No %s configuration file.",
							cfgFilePath));
		} catch (Exception ex) {
			System.err.println(String.format(
					"File %s is not a configuration file.", cfgFilePath));
		}
		return null;
	}

	/**
	 * Reads configuration from ./appName/configurations/namePrefix_appName.cfg
	 * and returns it.
	 * 
	 * @param appName
	 *            name of the streamJit app.
	 * 
	 * @param namePrefix
	 *            prefix to add to the cfg file name.
	 * 
	 * @return {@link Configuration} object if valid file exists. Otherwise
	 *         returns null.
	 */
	public static Configuration readConfiguration(String appName, int namePrefix) {
		Integer i = namePrefix;
		return readConfiguration(appName, i.toString());
	}

	/**
	 * Saves the configuration into
	 * ./appName/configurations/namePrefix_appName.cfg. output _.cfg file will
	 * be named as namePrefix_appName.cfg.
	 * 
	 * @param config
	 *            {@link Configuration} that need to be saved.
	 * @param namePrefix
	 *            prefix to add to the out put file name.
	 * @param appName
	 *            name of the streamJit app. output _.cfg file will be named as
	 *            namePrefix_appName.cfg.
	 */
	public static void saveConfg(Configuration config, String namePrefix,
			String appName) {
		String json = config.toJson();
		saveConfg(json, namePrefix, appName);
	}

	/**
	 * Saves the configuration into
	 * ./appName/configurations/namePrefix_appName.cfg. output _.cfg file will
	 * be named as namePrefix_appName.cfg.
	 * 
	 * @param configJson
	 *            Json representation of the {@link Configuration} that need to
	 *            be saved.
	 * @param namePrefix
	 *            prefix to add to the out put file name.
	 * @param appName
	 *            name of the streamJit app. output _.cfg file will be named as
	 *            namePrefix_appName.cfg.
	 */
	public static void saveConfg(String configJson, String namePrefix,
			String appName) {
		try {

			File dir = new File(String.format("%s%s%s", appName,
					File.separator, configDir));
			if (!dir.exists())
				if (!dir.mkdirs()) {
					System.err.println("Make directory failed");
					return;
				}

			File file = new File(dir, String.format("%s_%s.cfg", namePrefix,
					appName));
			FileWriter writer = new FileWriter(file, false);
			writer.write(configJson);
			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Adds @param prefix as an extra data to the @param config. Returned
	 * configuration object will contain an extra data named "configPrefix".
	 * 
	 * @param config
	 *            {@link Configuration} object in which the configuration prefix
	 *            need to be added.
	 * @param prefix
	 *            prefix that need to be added to the configuration.
	 * @return Same @param config with configPrefix as an extra data.
	 */
	public static Configuration addConfigPrefix(Configuration config,
			String prefix) {
		if (config == null)
			return config;
		Configuration.Builder builder = Configuration.builder(config);
		builder.putExtraData("configPrefix", prefix);
		return builder.build();
	}

	/**
	 * Gets configuration's prefix name from the configuration and returns.
	 * 
	 * @param config
	 * @return prefix name of the configuration if exists; empty string
	 *         otherwise.
	 */
	public static String getConfigPrefix(Configuration config) {
		if (config == null)
			return "";
		String prefix = (String) config.getExtraData("configPrefix");
		return prefix == null ? "" : prefix;
	}

	public static void printSearchSpaceSize(Configuration cfg) {
		int inparam = 0;
		int swtparam = 0;
		int floatparam = 0;
		int otherParams = 0;
		double sssize = 0;

		for (Parameter p : cfg.getParametersMap().values()) {
			if (p.getClass() == Configuration.IntParameter.class) {
				inparam++;
				IntParameter i = (IntParameter) p;
				sssize += Math.log10(i.getMax() - i.getMin());
			}

			else if (p.getClass() == Configuration.SwitchParameter.class) {
				swtparam++;
				SwitchParameter<?> j = (SwitchParameter<?>) p;
				sssize += Math.log10(j.getUniverse().size());
			}

			else if (p.getClass() == Configuration.FloatParameter.class) {
				floatparam++;
				FloatParameter i = (FloatParameter) p;
				sssize += Math.log10((i.getMax() - i.getMin()) * 1000);
			} else
				otherParams++;
		}

		System.out.println("No of total parameters = "
				+ cfg.getParametersMap().size());
		System.out.println("No of IntParameters = " + inparam);
		System.out.println("No of SwitchParameter = " + swtparam);
		System.out.println("No of FloatParameter = " + floatparam);
		System.out.println("No of other parameters = " + otherParams);
		System.out.println(String.format("SearchSpace size = 10^(%.3f)",
				sssize));
	}
}
