package com.cinchapi.concourse;

import com.cinchapi.concourse.server.Cluster;
import com.cinchapi.concourse.server.RoundRobinCluster;
import com.cinchapi.concourse.services.ConcourseService;

public class ConcourseServer {

	private final Cluster cluster;

	public ConcourseServer() {
		this.cluster = new RoundRobinCluster();
		// TODO add the nodes based on what is in the prefs
	}

	/**
	 * Return a {@link ConcourseService}, which facilitates interaction with the
	 * underlying data store.
	 * 
	 * @return a <code>service</code>.
	 */
	public ConcourseService getService(){
		return cluster.getNode().getService();
	}

}
