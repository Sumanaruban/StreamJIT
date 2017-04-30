package edu.mit.streamjit.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.DrainData;

/**
 * @author Sumanaruban Rajadurai (Suman)
 *
 * @since 04 Sep, 2015
 */
public class DrainDataUtils {

	/**
	 * Reads {@link DrainData} from
	 * ./appName/configurations/namePrefix_appName.dd and returns it.
	 * 
	 * @param appName
	 *            name of the streamJit app.
	 * 
	 * @param namePrefix
	 *            prefix to add to the dd file name.
	 * 
	 * @return {@link DrainData} object if valid file exists. Otherwise returns
	 *         null.
	 */
	public static DrainData readDrainData(String appName, String namePrefix) {
		checkNotNull(appName);
		namePrefix = namePrefix == null ? "" : namePrefix;
		String ddFilePath = String.format("%s%s%s%s%s_%s.dd", appName,
				File.separator, ConfigurationUtils.configDir, File.separator,
				namePrefix, appName);
		return readDrainData(ddFilePath);
	}

	/**
	 * @param ddFilePath
	 *            path of the draindata file that need to be read.
	 * @return {@link DrainData} object if valid file exists. Otherwise returns
	 *         null.
	 */
	public static DrainData readDrainData(String ddFilePath) {
		DrainData dd = null;
		try {
			FileInputStream fin = new FileInputStream(ddFilePath);
			ObjectInputStream ois = new ObjectInputStream(fin);
			dd = (DrainData) ois.readObject();
			ois.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return dd;
	}

	/**
	 * Dumps {@link DrainData} into
	 * ./appName/configurations/namePrefix_appName.dd. output _.dd file will be
	 * named as namePrefix_appName.dd.
	 * 
	 * @param drainData
	 * @param namePrefix
	 * @param appName
	 */
	public static void dumpDrainData(DrainData drainData, String appName,
			String namePrefix) {
		if (drainData == null)
			return;
		try {
			File dir = new File(String.format("%s%s%s", appName,
					File.separator, ConfigurationUtils.configDir));
			if (!dir.exists())
				if (!dir.mkdirs()) {
					System.err.println("Make directory failed");
					return;
				}
			File file = new File(dir, String.format("%s_%s.dd", namePrefix,
					appName));
			FileOutputStream fout = new FileOutputStream(file, false);
			ObjectOutputStream oos = new ObjectOutputStream(fout);
			oos.writeObject(drainData);
			oos.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		printDrainDataStats(drainData, appName, namePrefix);
	}

	/**
	 * Prints drain data statistics to ./appName/draindatasize.txt.
	 * 
	 * @param drainData
	 * @param appName
	 * @param namePrefix
	 */
	public static void printDrainDataStats(DrainData drainData, String appName,
			String namePrefix) {
		try {
			String fileName = String.format("%s%sdraindatasize.txt", appName,
					File.separator);
			FileWriter writer = new FileWriter(fileName, true);
			String header = String
					.format("----------------------------%s----------------------------\n",
							namePrefix);
			writer.write(header);
			// System.out.println(header);
			if (drainData == null) {
				String msg = "drain data is null";
				// System.out.print(msg);
				writer.write(msg);
			} else
				for (Token t : drainData.getData().keySet()) {
					int size = drainData.getData().get(t).size();
					if (size != 0) {
						String msg = String.format("%s - %d\n", t.toString(),
								size);
						// System.out.print(msg);
						writer.write(msg);
					}
				}
			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void printDrainDataStats(DrainData dd) {
		System.out.println("**********printDrainDataStats*************");
		if (dd != null) {
			for (Token t : dd.getData().keySet()) {
				int size = dd.getData().get(t).size();
				if (size > 0)
					System.out.println("From Blob: " + t.toString() + " - "
							+ size);
			}
		}
	}
}
