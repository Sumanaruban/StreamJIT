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
		summarizeHeap("FMRadioCore", "", 0);
	}

	public static void summarizeHeap(String appName, String fileName, int nodeID)
			throws IOException {
		File summaryDir = new File(String.format("%s%ssummary", appName,
				File.separator));
		Utils.createDir(summaryDir.getPath());
		File f1 = writeHeapStat(fileName,
				summaryDir, nodeID);
		File f = makeHeapPlotFile(summaryDir, f1.getName(), nodeID);
		TimeLogProcessor.plot(summaryDir, f);
	}

	private static File writeHeapStat(String fileName, File outDir, int nodeID)
			throws IOException {
		List<Integer> heapSize = processSNHeap(fileName, "heapSize");
		List<Integer> heapMaxSize = processSNHeap(fileName, "heapMaxSize");
		List<Integer> heapFreeSize = processSNHeap(fileName, "heapFreeSize");
		String outFileName = String.format("node%d_heapStatus.txt", nodeID);
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

	private static File makeHeapPlotFile(File dir,
			String dataFile1, int nodeID) throws IOException {
		boolean pdf = true;
		String basename = String.format("node%d", nodeID);
		String plotFileName = String.format("%s_heapplot.plt", basename);
		File plotfile = new File(dir, plotFileName);
		FileWriter writer = new FileWriter(plotfile, false);

		if (pdf) {
			writer.write("set terminal pdf enhanced color\n");
			writer.write(String
					.format("set output \"%s_Heap.pdf\"\n", basename));
		} else {
			writer.write("set terminal postscript eps enhanced color\n");
			writer.write(String
					.format("set output \"%s_Heap.eps\"\n", basename));
		}
		writer.write("set ylabel \"Memory(MB)\"\n");
		writer.write("set xlabel \"Tuning Rounds\"\n");
		writer.write(String.format("set title \"%s Heap\"\n", basename));
		writer.write("set grid\n");
		writer.write("#set yrange [0:*]\n");

		writer.write(String
				.format("plot \"%s\" using 1:2 with linespoints title \"Heap Free Size\","
						+ "\"%s\" using 1:3 with linespoints title \"Heap Size\","
						+ "\"%s\" using 1:4 with linespoints title \"Heap Max Size\" \n",
						dataFile1, dataFile1, dataFile1));
		writer.close();
		return plotfile;
	}
}
