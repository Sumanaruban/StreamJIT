package edu.mit.streamjit.impl.distributed;

import edu.mit.streamjit.impl.common.TimeLogger;
import edu.mit.streamjit.impl.common.drainer.AbstractDrainer;
import edu.mit.streamjit.impl.distributed.runtimer.DistributedDrainer;

/**
 * This class is responsible to manage an {@link AppInstance} including
 * starting, stopping and draining it.
 * 
 * @author sumanan
 * @since 22 Jul, 2015
 */
public class AppInstanceManager {

	private final AppInstance appInst;
	private final AbstractDrainer drainer;

	AppInstanceManager(AppInstance appInst, TimeLogger logger) {
		this.appInst = appInst;
		// TODO:
		// Read this. Don't let the "this" reference escape during construction
		// http://www.ibm.com/developerworks/java/library/j-jtp0618/
		this.drainer = new DistributedDrainer(appInst, logger, this);

	}
}
