package edu.mit.streamjit.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.mit.streamjit.impl.distributed.common.Utils;

public class HeapSummarizer {

	public static void main(String[] args) throws IOException {
		summarizeHeap("FMRadioCore");
	}

	public static void summarizeHeap(String appName) throws IOException {
		File summaryDir = new File(String.format("%s%ssummary", appName,
				File.separator));
		Utils.createDir(summaryDir.getPath());
		File f1 = writeHeapStat(
				String.format("%s%sslurm-662553.out", appName, File.separator),
				summaryDir);
		File f2 = writeHeapStat(
				String.format("%s%sslurm-662554.out", appName, File.separator),
				summaryDir);
		File f = makeHeapPlotFile(summaryDir, appName, f1.getName(),
				f2.getName());
		TimeLogProcessor.plot(summaryDir, f);
	}

	private static File writeHeapStat(String fileName, File outDir)
			throws IOException {
		List<Integer> heapSize = processSNHeap(fileName, "heapSize");
		List<Integer> heapMaxSize = processSNHeap(fileName, "heapMaxSize");
		List<Integer> heapFreeSize = processSNHeap(fileName, "heapFreeSize");
		File f = new File(fileName);
		String outFileName = String.format("%s_heapStatus.txt", f.getName());
		File outFile = new File(outDir, outFileName);
		FileWriter writer = new FileWriter(outFile, false);
		for (int i = 0; i < heapSize.size(); i++) {
			String msg = String.format("%-6d\t%-6d\t%-6d\t%-6d\n", i + 1,
					heapFreeSize.get(i), heapSize.get(i), heapMaxSize.get(i));
			writer.write(msg);
		}
		writer.close();
		return outFile;
	}

	private static List<Integer> processSNHeap(String fileName, String heapType)
			throws IOException {
		String slurmPrefix = "0: ";
		BufferedReader reader = new BufferedReader(new FileReader(fileName));
		String line;
		int i = 0;
		List<Integer> ret = new ArrayList<Integer>(3000);
		while ((line = reader.readLine()) != null) {
			// Slurm adds prefix to every sysout line.
			if (line.startsWith(slurmPrefix))
				line = line.substring(slurmPrefix.length());
			if (line.startsWith(heapType)) {
				String[] arr = line.split(" ");
				String time = arr[2].trim();
				time = time.substring(0, time.length() - 2);
				int val = Integer.parseInt(time);
				ret.add(val);
			}
		}
		reader.close();
		return ret;
	}

	private static File makeHeapPlotFile(File dir, String name,
			String dataFile1, String dataFile2) throws IOException {
		boolean pdf = true;
		File plotfile = new File(dir, "heapplot.plt");
		FileWriter writer = new FileWriter(plotfile, false);
		if (pdf) {
			writer.write("set terminal pdf enhanced color\n");
			writer.write(String.format("set output \"%sHeap.pdf\"\n", name));
		} else {
			writer.write("set terminal postscript eps enhanced color\n");
			writer.write(String.format("set output \"%sHeap.eps\"\n", name));
		}
		writer.write("set ylabel \"Memory(MB)\"\n");
		writer.write("set xlabel \"Tuning Rounds\"\n");
		writer.write(String.format("set title \"%sHeap\"\n", name));
		writer.write("set grid\n");
		writer.write("#set yrange [0:*]\n");

		writer.write(String
				.format("plot \"%s\" using 1:2 with linespoints title \"Heap Free Size\","
						+ "\"%s\" using 1:3 with linespoints title \"Heap Size\","
						+ "\"%s\" using 1:4 with linespoints title \"Heap Max Size\" \n",
						dataFile1, dataFile1, dataFile1));
		writer.write(String
				.format("plot \"%s\" using 1:2 with linespoints title \"Heap Free Size\","
						+ "\"%s\" using 1:3 with linespoints title \"Heap Size\","
						+ "\"%s\" using 1:4 with linespoints title \"Heap Max Size\" \n",
						dataFile2, dataFile2, dataFile2));

		writer.close();
		return plotfile;
	}

}
