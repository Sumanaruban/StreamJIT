package edu.mit.streamjit.impl.distributed;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

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
				"stats \"%s\" using 3 prefix \"Throughput\" noout\n",
				tpFile));
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
}
