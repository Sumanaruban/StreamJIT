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

	private static final String tpFile = "../tailBuffer.txt";
	private static final String recfgEventFile = "../eventTime_Reconfigurer.txt";

	public static void main(String[] args) throws IOException {
		summarize("BeamFormer1Kernel");
	}

	public static void summarize(String appName) throws IOException {
		File summaryDir = new File(String.format("%s%ssummary", appName,
				File.separator));
		Utils.createDir(summaryDir.getPath());
		// processTp(appName, summaryDir);
		File f = createTotalStatsPlotFile(summaryDir, appName);
		TimeLogProcessor.plot(summaryDir, f);
	}

	/**
	 * Creates plot file for {@link #ptotalFile}.
	 */
	private static File createTotalStatsPlotFile(File dir, String appName)
			throws IOException {
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
		writer.write("#set yrange [0:*]\n");

		writer.write(String.format(
				"stats \"%s\" using 3 prefix \"Throughput\" noout\n", tpFile));
		writer.write("endPoint= 3*Throughput_max/4" + "\n");

		writer.write(String.format(
				"plot \"%s\" using 2:(endPoint) with impulses,\\",
				recfgEventFile));
		writer.write("\n");
		writer.write("'' using 2:(endPoint):1 with labels rotate by 90 left font \"Times Italic,10\",\\");
		writer.write("\n");
		writer.write(String
				.format("\"%s\" using 1:3 smooth unique title \"Throughput\" lc rgb \"blue\"\n",
						tpFile));
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
	}
}
