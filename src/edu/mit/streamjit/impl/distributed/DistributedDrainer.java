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

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.common.TimeLogger;
import edu.mit.streamjit.impl.common.drainer.AbstractDrainer;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement;
import edu.mit.streamjit.impl.distributed.common.CTRLRMessageElement.CTRLRMessageElementHolder;

/**
 * @author Sumanan sumanan@mit.edu
 * @since Aug 13, 2013
 */
public class DistributedDrainer extends AbstractDrainer {

	AppInstanceManager appInstManager;

	public DistributedDrainer(AppInstance appInst, TimeLogger logger,
			AppInstanceManager appInstManager) {
		super(appInst, logger);
		this.appInstManager = appInstManager;
	}

	@Override
	protected void drainingDone(boolean isFinal) {
		appInstManager.drainingFinished(isFinal);
	}

	@Override
	protected void drain(Token blobID, DrainDataAction drainDataAction) {
		// System.out.println("Drain requested to blob " + blobID);
		if (!appinst.blobtoMachineMap.containsKey(blobID))
			throw new IllegalArgumentException(blobID
					+ " not found in the blobtoMachineMap");
		int nodeID = appinst.blobtoMachineMap.get(blobID);
		appInstManager.appManager.controller.send(nodeID,
				new CTRLRMessageElementHolder(new CTRLRDrainElement.DoDrain(
						blobID, drainDataAction), appInstManager.appInstId()));
	}

	@Override
	protected void drainingDone(Token blobID, boolean isFinal) {
		// Nothing to clean in Distributed case.
	}

	@Override
	protected void prepareDraining(boolean isFinal) {
		appInstManager.drainingStarted(isFinal);
	}
}
