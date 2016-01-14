package edu.mit.streamjit.impl.distributed.controller.HT;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import edu.mit.streamjit.impl.distributed.common.Utils;
import edu.mit.streamjit.util.TimeLogProcessor;

/**
 * @author sumanan
 * @since 7 Dec, 2015
 */
public class ThroughputGraphGenerator {

	private static String tpFile = "../tailBuffer.txt";
	private static final String recfgEventFile = "../eventTime_Reconfigurer.txt";

	public static void main(String[] args) throws IOException {
		summarize("BeamFormer1Kernel");
	}

	public static void summarize(String appName) throws IOException {
		File summaryDir = new File(String.format("%s%ssummary", appName,
				File.separator));
		Utils.createDir(summaryDir.getPath());
		// processTp(appName, summaryDir);
		// newTPPeriod(3, appName, summaryDir);
		File f = createTPPlotFile(summaryDir, appName, tpFile, recfgEventFile);
		TimeLogProcessor.plot(summaryDir, f);
	}

	/**
	 * Creates plot file for {@link #ptotalFile}.
	 */
	private static File createTPPlotFile(File dir, String appName,
			String tpfile, String cfgEventFile) throws IOException {
		String title = TimeLogProcessor.getTitle(appName);
		boolean pdf = true;
		File plotfile = new File(dir, "throughput.plt");
		FileWriter writer = new FileWriter(plotfile, false);
		if (pdf) {
			writer.write("set terminal pdfcairo enhanced color\n");
			writer.write(String.format("set output \"%s_TP.pdf\"\n", title));
		} else {
			writer.write("set terminal postscript eps enhanced color\n");
			writer.write(String.format("set output \"%s_TP.eps\"\n", title));
		}
		writer.write("set ylabel \"Throughput(items/s)\"\n");
		writer.write("set xlabel \"Time(s)\"\n");
		writer.write(String.format("set title \"%s\"\n", title));

		writer.write("set tic scale 0\n");
		writer.write("set grid ytics lc rgb \"#C0C0C0\"\n");
		writer.write("set nokey\n");
		writer.write("unset border\n");

		writer.write("stats \"../tailBuffer.txt\" using 1 prefix \"x\" noout\n");
		writer.write("set xrange [x_min:x_max]\n");

		writer.write(String.format(
				"stats \"%s\" using 3 prefix \"Throughput\" noout\n", tpfile));
		writer.write("ymax=1.5*Throughput_max\n");
		writer.write("set yrange [0:ymax]\n");
		writer.write("endPoint= 3*ymax/4" + "\n");

		writer.write(String
				.format("plot \"%s\" using 2:(endPoint) with impulses,\\",
						cfgEventFile));
		writer.write("\n");
		writer.write("'' using 2:(endPoint):1 with labels rotate by 90 left font \"Times Italic,10\",\\");
		writer.write("\n");
		writer.write(String
				.format("\"%s\" using 1:3 smooth unique title \"Throughput\" lc rgb \"blue\"\n",
						tpfile));
		writer.close();
		return plotfile;
	}

	private static void processTp(String appName, File outDir)
			throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(
				String.format("%s%stailBuffer.txt", appName, File.separator)));
		File outFile = new File(outDir, "tail.txt");
		FileWriter writer = new FileWriter(outFile, false);
		String line;
		while ((line = reader.readLine()) != null) {
			String[] arr = line.split("\t");
			if (arr.length < 3)
				continue;

			String t = arr[0].trim();
			int time = 0;
			try {
				time = (int) Double.parseDouble(t);
			} catch (Exception e) {
				continue;
			}

			String tp = arr[4].trim();
			int val = 0;
			try {
				val = (int) Double.parseDouble(tp);
			} catch (Exception e) {
				continue;
			}

			String data = String.format("%s\t%s\t%d\n", arr[0], arr[2], val);
			writer.write(data);
		}
		writer.flush();
		reader.close();
		writer.close();
		tpFile = "tail.txt";
	}

	/**
	 * @param period
	 *            Period in seconds.
	 * @param appName
	 * @param outDir
	 * @throws IOException
	 */
	private static void newTPPeriod(int period, String appName, File outDir)
			throws IOException {
		double[] vals = new double[period];
		int i = 0;
		BufferedReader reader = new BufferedReader(new FileReader(
				String.format("%s%stailBuffer.txt", appName, File.separator)));
		File outFile = new File(outDir, "tail.txt");
		FileWriter writer = new FileWriter(outFile, false);
		String line;
		while ((line = reader.readLine()) != null) {
			String[] arr = line.split("\t");
			if (arr.length < 3)
				continue;
			String tp = arr[4].trim();
			double val = 0;
			try {
				val = Double.parseDouble(tp);
			} catch (Exception e) {
				continue;
			}

			vals[i++] = val;
			if (i == vals.length) {
				double sum = 0;
				for (double d : vals)
					sum += d;
				int avg = (int) sum / vals.length;
				String data = String
						.format("%s\t%s\t%d\n", arr[0], arr[2], avg);
				writer.write(data);
				i = 0;
			}
		}
		writer.flush();
		reader.close();
		writer.close();
		tpFile = "tail.txt";
	}
}
