package edu.mit.streamjit.impl.distributed.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.api.StreamCompilationFailedException;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.Builder;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.common.Configuration.PartitionParameter;
import edu.mit.streamjit.impl.common.Configuration.SwitchParameter;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.common.drainer.BlobGraph;
import edu.mit.streamjit.impl.compiler2.Compiler2BlobFactory;
import edu.mit.streamjit.impl.concurrent.ConcurrentChannelFactory;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.NodeInfo;
import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.impl.distributed.common.SystemInfo;
import edu.mit.streamjit.impl.distributed.common.Utils;
import edu.mit.streamjit.impl.distributed.node.StreamNode;
import edu.mit.streamjit.impl.interp.ChannelFactory;
import edu.mit.streamjit.impl.interp.Interpreter;

/**
 * This class contains a StreamJIT app's specific running instance related
 * information (dynamic information) such as {@link Configuration} and
 * corresponding {@link BlobGraph}, partitionsMachineMap, and etc.
 * {@link StreamJitApp} contains all static information of the current streamJit
 * application.
 * 
 * For each new {@link Configuration}, a new {@link AppInstance} will be
 * created.
 * 
 * @author sumanan
 * @since 29 May, 2015
 */
public class AppInstance {

	/**
	 * Identifier to identify different {@link AppInstance}s.
	 * <ol>
	 * <li>
	 * (-1) is allocated to transfer system information such as {@link NodeInfo}
	 * and {@link SystemInfo}.
	 * <li>
	 * 0 is for the very first AppInstance that comes from the default
	 * configuration.
	 * <li>
	 * 1 and above are for the configurations that are received from OpenTuner.
	 * </ol>
	 */
	public final int id;

	public final StreamJitApp<?, ?> app;

	public final BlobGraph blobGraph;

	public final ImmutableMap<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap;

	/**
	 * {@link BlobFactory#getDefaultConfiguration(java.util.Set) generates the
	 * initial configuration}.
	 */
	public final Configuration configuration;

	public final int multiplier;

	/**
	 * Keeps track of assigned machine Ids of each blob. This information is
	 * need for draining. TODO: If possible use a better solution.
	 */
	public Map<Token, Integer> blobtoMachineMap;

	public DrainData drainData = null;

	/**
	 * Builds {@link BlobGraph} from the partitionsMachineMap, and verifies for
	 * any cycles among blobs. If it is a valid partitionsMachineMap, (i.e., no
	 * cycles among the blobs), then returns the built {@link BlobGraph}.
	 * 
	 * @param partitionsMachineMap
	 * 
	 * @throws StreamCompilationFailedException
	 *             if any cycles found among blobs.
	 */
	public static BlobGraph verifyConfiguration(
			Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap) {

		if (!Options.singleNodeOnline) {
			// printPartition(partitionsMachineMap);
		}

		List<Set<Worker<?, ?>>> partitionList = new ArrayList<>();
		for (List<Set<Worker<?, ?>>> lst : partitionsMachineMap.values()) {
			partitionList.addAll(lst);
		}

		BlobGraph bg = null;
		try {
			bg = new BlobGraph(partitionList);
		} catch (StreamCompilationFailedException ex) {
			System.err.println("Cycles found in the worker->blob assignment");
			printPartition(partitionsMachineMap);
			throw ex;
		}
		return bg;
	}

	private static void printPartition(
			Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap) {
		for (int machine : partitionsMachineMap.keySet()) {
			System.err.print("\nMachine - " + machine);
			for (Set<Worker<?, ?>> blobworkers : partitionsMachineMap
					.get(machine)) {
				System.err.print("\n\tBlob worker set : ");
				for (Worker<?, ?> w : blobworkers) {
					System.err.print(Workers.getIdentifier(w) + " ");
				}
			}
		}
		System.err.println();
	}

	AppInstance(StreamJitApp<?, ?> app, int id,
			Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap,
			Configuration configuration, BlobGraph blobGraph) {
		this.id = id;
		this.app = app;
		this.partitionsMachineMap = ImmutableMap.copyOf(partitionsMachineMap);
		this.blobGraph = blobGraph;
		this.configuration = configuration;
		this.multiplier = getMultiplier(configuration);
		app.visualizer.newConfiguration(configuration);
		app.visualizer.newPartitionMachineMap(partitionsMachineMap);
	}

	private int getMultiplier(Configuration config) {
		int multiplier = 1;
		IntParameter mulParam = config.getParameter("multiplier",
				IntParameter.class);
		if (mulParam != null)
			multiplier = mulParam.getValue();
		System.err.println(String.format("%s - multiplier = %d", toString(),
				multiplier));
		return multiplier;
	}

	/**
	 * From aggregated drain data, get subset of it which is relevant to a
	 * particular machine. Builds and returns machineID to DrainData map.
	 * 
	 * @return Drain data mapped to machines.
	 */
	public ImmutableMap<Integer, DrainData> getDrainData() {
		ImmutableMap.Builder<Integer, DrainData> builder = ImmutableMap
				.builder();
		if (this.drainData != null) {
			for (Integer machineID : partitionsMachineMap.keySet()) {
				List<Set<Worker<?, ?>>> blobList = partitionsMachineMap
						.get(machineID);
				DrainData dd = drainData.subset(getWorkerIds(blobList));
				builder.put(machineID, dd);
			}
		}
		return builder.build();
	}

	/**
	 * @return the configuration
	 */
	public Configuration getConfiguration() {
		return configuration;
	}

	/**
	 * For every reconfiguration, this method may be called by an appropriate
	 * class to get new configuration information that can be sent to all
	 * participating {@link StreamNode}s. Mainly this configuration contains
	 * graph partition information for running this {@link AppInstance} on a
	 * distributed environment.
	 * 
	 * @return {@link Configuration.Builder} that is specific to this
	 *         {@link AppInstance}.
	 */
	Builder getDynamicConfiguration() {
		Configuration.Builder builder = Configuration.builder();
		int maxCores = maxCores();
		PartitionParameter.Builder partParam = addMachineCoreMap(maxCores);
		BlobFactory bf = addBlobFactories(partParam);
		addBlobs(partParam, maxCores, bf);
		builder.addParameter(partParam.build());
		builder.putExtraData("appInstId", id);
		if (Options.useCompilerBlob)
			builder.addSubconfiguration("blobConfigs", getConfiguration());
		else
			builder.addSubconfiguration("blobConfigs", getInterpreterConfg());
		return builder;
	}

	private PartitionParameter.Builder addMachineCoreMap(int maxCores) {
		Map<Integer, Integer> machineCoreMap = new HashMap<>();
		for (Entry<Integer, List<Set<Worker<?, ?>>>> machine : partitionsMachineMap
				.entrySet()) {
			machineCoreMap.put(machine.getKey(), machine.getValue().size()
					* maxCores);
		}
		PartitionParameter.Builder partParam = PartitionParameter.builder(
				GlobalConstants.PARTITION, machineCoreMap);
		return partParam;
	}

	private BlobFactory addBlobFactories(PartitionParameter.Builder partParam) {
		BlobFactory intFactory = new Interpreter.InterpreterBlobFactory();
		BlobFactory comp2Factory = new Compiler2BlobFactory();
		partParam.addBlobFactory(intFactory);
		partParam.addBlobFactory(comp2Factory);
		return Options.useCompilerBlob ? comp2Factory : intFactory;
	}

	private void addBlobs(PartitionParameter.Builder partParam, int maxCores,
			BlobFactory bf) {
		blobtoMachineMap = new HashMap<>();
		for (Integer machineID : partitionsMachineMap.keySet()) {
			List<Set<Worker<?, ?>>> blobList = partitionsMachineMap
					.get(machineID);
			for (Set<Worker<?, ?>> blobWorkers : blobList) {
				partParam.addBlob(machineID, maxCores, bf, blobWorkers);

				// TODO: Temp fix to build.
				Token t = Utils.getblobID(blobWorkers);
				blobtoMachineMap.put(t, machineID);
			}
		}
	}

	private Configuration getInterpreterConfg() {
		Configuration.Builder builder = Configuration.builder();
		List<ChannelFactory> universe = Arrays
				.<ChannelFactory> asList(new ConcurrentChannelFactory());
		SwitchParameter<ChannelFactory> cfParameter = new SwitchParameter<ChannelFactory>(
				"channelFactory", ChannelFactory.class, universe.get(0),
				universe);

		builder.addParameter(cfParameter);
		return builder.build();
	}

	private int maxCores() {
		IntParameter maxCoreParam = configuration.getParameter("maxNumCores",
				IntParameter.class);
		if (maxCoreParam != null)
			return maxCoreParam.getValue();
		return Options.maxNumCores;
	}

	private Set<Integer> getWorkerIds(List<Set<Worker<?, ?>>> blobList) {
		Set<Integer> workerIds = new HashSet<>();
		for (Set<Worker<?, ?>> blobworkers : blobList) {
			for (Worker<?, ?> w : blobworkers) {
				workerIds.add(Workers.getIdentifier(w));
			}
		}
		return workerIds;
	}

	public String toString() {
		return String.format("AppInstance-%d", id);
	}
}
