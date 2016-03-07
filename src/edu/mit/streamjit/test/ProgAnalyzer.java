package edu.mit.streamjit.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.distributed.common.Utils;
import edu.mit.streamjit.impl.distributed.controller.ConfigurationManager;
import edu.mit.streamjit.impl.distributed.controller.ConfigurationManager.NewConfiguration;
import edu.mit.streamjit.impl.distributed.controller.HotSpotTuning;
import edu.mit.streamjit.impl.distributed.controller.PartitionManager;
import edu.mit.streamjit.impl.distributed.controller.StreamJitApp;
import edu.mit.streamjit.test.apps.channelvocoder7.ChannelVocoder7;
import edu.mit.streamjit.test.apps.filterbank6.FilterBank6;
import edu.mit.streamjit.test.apps.fmradio.FMRadio.FMRadioBenchmarkProvider;
import edu.mit.streamjit.test.sanity.nestedsplitjoinexample.NestedSplitJoin;
import edu.mit.streamjit.tuner.ConfigurationPrognosticator;
import edu.mit.streamjit.tuner.DistanceMatrixPrognosticator;
import edu.mit.streamjit.tuner.GraphPropertyPrognosticator;
import edu.mit.streamjit.util.ConfigurationUtils;
import edu.mit.streamjit.util.TimeLogProcessor;

/**
 * Converts configuration data and running time to CSV format for R analysis.
 * 
 * @author sumanan
 * @since 26 Mar, 2015
 */
public class ProgAnalyzer {
	public static void main(String[] args) throws IOException {
		String appName;
		try {
			appName = args[0];
		} catch (Exception ex) {
			appName = "FMRadioCore";
		}

		int noOfNodes;
		try {
			noOfNodes = Integer.parseInt(args[1]);
		} catch (Exception ex) {
			noOfNodes = 3;
		}

		ProgAnalyzer csv = new ProgAnalyzer(appName, noOfNodes);
		csv.startWrite();
	}

	private final String appName;
	private final StreamJitApp<?, ?> app;
	private final ConfigurationManager cfgManager;

	public ProgAnalyzer(String appName, int nodes) {
		this.appName = appName;
		OneToOneElement<?, ?> streamGraph = streamGraph();
		this.app = new StreamJitApp<>(streamGraph, null);
		PartitionManager partitionManager = new HotSpotTuning(app);
		partitionManager.getDefaultConfiguration(
				Workers.getAllWorkersInGraph(app.source), nodes);
		this.cfgManager = new ConfigurationManager(app, partitionManager);
	}

	public void startWrite() throws IOException {

		java.util.List<CfgProgWriter> cfgProgList = new ArrayList<ProgAnalyzer.CfgProgWriter>();
		cfgProgList.add(new CfgProgWriter(new GraphPropertyPrognosticator(app),
				"graph"));
		cfgProgList.add(new CfgProgWriter(new DistanceMatrixPrognosticator(
				appName), "dist"));

		Map<String, Integer> runningTime = processRunTime(appName);
		for (int i = 1; i <= runningTime.size(); i++) {
			System.out.println("cfg = " + i);
			Stopwatch sw = Stopwatch.createStarted();
			Configuration cfg = ConfigurationUtils
					.readConfiguration(appName, i);
			System.out.println("readConfiguration = "
					+ sw.elapsed(TimeUnit.MILLISECONDS));
			if (cfg == null)
				continue;

			Integer time = runningTime.get(new Integer(i).toString());
			if (time == null)
				time = -1;

			NewConfiguration newConfig = cfgManager.newConfiguration(cfg);

			for (CfgProgWriter w : cfgProgList)
				w.newCfg(newConfig, time, i);
		}
		for (CfgProgWriter w : cfgProgList)
			w.end();
	}

	private void writeHeader(FileWriter writer) throws IOException {
		writer.write("Cfg\t");
		writer.write("Accepted\t");
		writer.write("Rejected\n");
	}

	private static Map<String, Integer> processRunTime(String appName)
			throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(
				tuneRunTimeFile(appName)));
		String line;
		String cfgPrefix = "Init";
		int i = 0;
		Map<String, Integer> ret = new HashMap<>(5000);
		while ((line = reader.readLine()) != null) {
			if (line.startsWith("----------------------------"))
				cfgPrefix = cfgString(line);
			else if (line.startsWith("Execution")) {
				String[] arr = line.split(" ");
				String time = arr[3].trim();
				time = time.substring(0, time.length() - 2);
				int val = Integer.parseInt(time);
				ret.put(cfgPrefix, val);
			}
		}
		reader.close();
		return ret;
	}

	private static String cfgString(String line) {
		String l = line.replace('-', ' ');
		return l.trim();
	}

	private static String tuneRunTimeFile(String appName) {
		String tuneDirPath = String.format("%s%stune", appName, File.separator);
		File tuneDir = new File(tuneDirPath);
		if (tuneDir.exists())
			return String
					.format("%s%srunTime.txt", tuneDirPath, File.separator);

		String origFilePath = String.format("%s%srunTime.txt.orig", appName,
				File.separator);
		File origFile = new File(origFilePath);
		if (origFile.exists())
			return origFilePath;

		return String.format("%s%srunTime.txt", appName, File.separator);
	}

	private OneToOneElement<?, ?> streamGraph() {
		Benchmark benchmark;
		if (appName.equals("FMRadioCore"))
			benchmark = new FMRadioBenchmarkProvider().iterator().next();
		else if (appName.equals("NestedSplitJoinCore"))
			benchmark = new NestedSplitJoin.NestedSplitJoinBenchmarkProvider()
					.iterator().next();
		else if (appName.equals("ChannelVocoder7Kernel"))
			benchmark = new ChannelVocoder7().iterator().next();
		else if (appName.equals("FilterBankPipeline"))
			benchmark = new FilterBank6.FilterBankBenchmark();
		else if (appName.equals("FMRadioCore"))
			benchmark = new FMRadioBenchmarkProvider().iterator().next();
		else
			throw new IllegalArgumentException("No benchmark to instantiate");
		OneToOneElement<?, ?> streamGraph = benchmark.instantiate();
		return streamGraph;
	}

	private static File createPlotFile2(File dir, String appName,
			String dataFile) throws IOException {
		String title = TimeLogProcessor.getTitle(appName);
		boolean pdf = true;
		String extensionRemoved = dataFile.split("\\.")[0];
		File plotfile = new File(dir, String.format("%sProgA.plt",
				extensionRemoved));
		FileWriter writer = new FileWriter(plotfile, false);
		if (pdf) {
			writer.write("set terminal pdf enhanced color\n");
			writer.write(String.format("set output \"%s%s.pdf\"\n", title,
					extensionRemoved));
		} else {
			writer.write("set terminal postscript eps enhanced color\n");
			writer.write(String.format("set output \"%s.eps\"\n", title));
		}
		writer.write(String.format("set title \"%s\"\n", title));
		writer.write("set grid\n");
		writer.write("#set yrange [0:*]\n");
		writer.write("set ylabel \"Time(ms)\"\n");
		writer.write("set xlabel \"CfgIF\"\n");
		writer.write(String.format(
				"plot \"%s\" using 1:2 with points title \"Distance\"\n",
				dataFile));
		writer.write("set xlabel \"weightedDist\"\n");
		writer.write(String.format(
				"plot \"%s\" using 3:16 with points title \"weightedDist\"\n",
				dataFile));
		writer.write("set xlabel \"NormalizedDistance\"\n");
		writer.write(String
				.format("plot \"%s\" using 4:16 with points title \"NormalizedDistance\"\n",
						dataFile));
		writer.write("set xlabel \"weightedNormalizedDistance\"\n");
		writer.write(String
				.format("plot \"%s\" using 5:16 with points title \"weightedNormalizedDistance\"\n",
						dataFile));
		writer.write("set xlabel \"Multiplier\"\n");
		writer.write(String.format(
				"plot \"%s\" using 6:16 with points title \"Multiplier\"\n",
				dataFile));
		writer.write("set xlabel \"Unroll\"\n");
		writer.write(String.format(
				"plot \"%s\" using 7:16 with points title \"Unroll\"\n",
				dataFile));
		writer.write("set xlabel \"AllocationStrategy\"\n");
		writer.write(String
				.format("plot \"%s\" using 8:16 with points title \"AllocationStrategy\"\n",
						dataFile));
		writer.write("set xlabel \"InternalStorage\"\n");
		writer.write(String
				.format("plot \"%s\" using 9:16 with points title \"InternalStorage\"\n",
						dataFile));
		writer.write("set xlabel \"Partitioning\"\n");
		writer.write(String.format(
				"plot \"%s\" using 10:16 with points title \"Partitioning\"\n",
				dataFile));
		writer.write("set xlabel \"Unboxing\"\n");
		writer.write(String.format(
				"plot \"%s\" using 11:16 with points title \"Unboxing\"\n",
				dataFile));
		writer.write("set xlabel \"Removal\"\n");
		writer.write(String.format(
				"plot \"%s\" using 12:16 with points title \"Removal\"\n",
				dataFile));
		writer.write("set xlabel \"Fusion\"\n");
		writer.write(String.format(
				"plot \"%s\" using 13:16 with points title \"Fusion\"\n",
				dataFile));
		writer.close();
		return plotfile;
	}

	private class CfgProgWriter {

		private final ConfigurationPrognosticator prog;
		private final String name;
		private final FileWriter writer;
		int acceptCount = 0;
		int rejectCount = 0;

		CfgProgWriter(ConfigurationPrognosticator prog, String name)
				throws IOException {
			this.name = name;
			this.prog = prog;
			String fileName = String.format("%s%sPA.txt",
					TimeLogProcessor.getTitle(appName), name);
			writer = Utils.fileWriter(appName, fileName);
			writeHeader(writer);
		}

		private void newCfg(NewConfiguration newConfig, Integer time,
				Integer cfgPrefix) throws IOException {
			boolean val = prog.prognosticate(newConfig);
			prog.time(time);

			writer.write(cfgPrefix.toString());
			writer.write('\t');
			if (val) {
				acceptCount++;
				writer.write(time.toString());
				writer.write("\t");
				writer.write("0");
				writer.write("\n");

			} else {
				rejectCount++;
				writer.write("0");
				writer.write("\t");
				writer.write(time.toString());
				writer.write("\n");

			}
		}

		private void end() throws IOException {
			writer.flush();
			writer.close();
			System.out.println(String.format("%s - Accepted=%d, Rejected=%d",
					name, acceptCount, rejectCount));
		}
	}
}
