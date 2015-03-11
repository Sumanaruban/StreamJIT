/*
 * Copyright (c) 2013-2014 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package edu.mit.streamjit.impl.distributed;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.mit.streamjit.api.StreamCompilationFailedException;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.SwitchParameter;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.common.drainer.BlobGraph;
import edu.mit.streamjit.tuner.OnlineTuner;

public class ConfigurationManager {

	public int rejectCount = 0;

	private final StreamJitApp<?, ?> app;

	private final PartitionManager partitionManager;

	private Set<Integer> downNodes = new HashSet<>();

	private final Set<Worker<?, ?>> allWorkers;

	private final int noOfMachines;

	public ConfigurationManager(StreamJitApp<?, ?> app,
			PartitionManager partitionManager, int noOfMachines) {
		this.app = app;
		this.partitionManager = partitionManager;
		allWorkers = Workers.getAllWorkersInGraph(app.source);
		this.noOfMachines = noOfMachines;
	}

	/**
	 * This method may be called to by the {@link OnlineTuner} to interpret a
	 * new configuration and execute the steramjit app with the new
	 * configuration.
	 * <p>
	 * Builds partitionsMachineMap and {@link BlobGraph} from the new
	 * Configuration, and verifies for any cycles among blobs. If it is a valid
	 * configuration, (i.e., no cycles among the blobs), then {@link #app}
	 * object's member variables {@link StreamJitApp#blobConfiguration},
	 * {@link StreamJitApp#blobGraph} and
	 * {@link StreamJitApp#partitionsMachineMap} will be assigned according to
	 * reflect the new configuration, no changes otherwise.
	 * 
	 * @param config
	 *            configuration from {@link OnlineTuner}.
	 * @return true iff valid configuration is passed.
	 */
	public boolean newConfiguration(Configuration config) {
		// for (Parameter p : config.getParametersMap().values()) {
		// if (p instanceof IntParameter) {
		// IntParameter ip = (IntParameter) p;
		// System.out.println(ip.getName() + " - " + ip.getValue());
		// } else if (p instanceof SwitchParameter<?>) {
		// SwitchParameter<?> sp = (SwitchParameter<?>) p;
		// System.out.println(sp.getName() + " - " + sp.getValue());
		// } else
		// System.out.println(p.getName() + " - Unknown type");
		// }

		config = reMapDownNodeWorkers(config);
		Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap = partitionManager
				.partitionMap(config);
		if (downNodeCheck(partitionsMachineMap))
			return false;

		try {
			app.verifyConfiguration(partitionsMachineMap);
		} catch (StreamCompilationFailedException ex) {
			return false;
		}
		app.setConfiguration(config);
		return true;
	}

	public void nodeDown(int nodeID) {
		downNodes.add(nodeID);
	}

	public void nodeUp(int nodeID) {
		downNodes.remove(nodeID);
	}

	/**
	 * @param partitionsMachineMap
	 * @return reject or not.
	 */
	private boolean downNodeCheck(
			Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap) {
		for (Integer downNode : downNodes) {
			List<Set<Worker<?, ?>>> partitons = partitionsMachineMap
					.get(downNode);
			if (partitons == null)
				continue;
			int size = partitons.size();
			System.out.println(String
					.format("No of blobs assigned to DownNode-%d is %d",
							downNode, size));
			if (size > 0) {
				System.err.println("Rejecting");
				rejectCount++;
				return true;
			}
		}
		return false;
	}

	/**
	 * FIXME: This is a temporary fix to simulate the dynamism. This breaks the
	 * OOP abstraction and any changes in {@link #partitionManager} will break
	 * this code. TODO: Move this re mapping task to partition manager.
	 * 
	 * @param config
	 * @return
	 */
	private Configuration reMapDownNodeWorkers(Configuration config) {
		if (downNodes.size() == 0)
			return config;

		Configuration.Builder b = Configuration.builder(config);
		for (Worker<?, ?> w : allWorkers) {
			int id = Workers.getIdentifier(w);
			String paramName = String.format("worker%dtomachine", id);
			SwitchParameter<Integer> sp = (SwitchParameter<Integer>) config
					.getParameter(paramName);
			if (sp == null)
				continue;
			int machine = sp.getValue();
			if (downNodes.contains(machine)) {
				int newVal = nextUpNode(machine);
				SwitchParameter<Integer> spNew = new SwitchParameter<Integer>(
						paramName, Integer.class, newVal, sp.getUniverse());
				b.removeParameter(paramName);
				b.addParameter(spNew);
			}
		}
		return b.build();
	}

	private int nextUpNode(int currentVal) {
		for (int i = 0; i < noOfMachines; i++) {
			int newVal = (currentVal + i) % noOfMachines + 1;
			if (!downNodes.contains(newVal))
				return newVal;
		}
		throw new IllegalArgumentException("No valid up nodes...");
	}
}
