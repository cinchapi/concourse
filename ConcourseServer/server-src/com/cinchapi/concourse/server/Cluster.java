package com.cinchapi.concourse.server;

/**
 * Uses an algorithm to provide a {@link Node} from a distributed collection.
 * 
 * @author jnelson
 */
public interface Cluster {

	/**
	 * Returns a {@link Node} based on an algorithm.
	 * 
	 * @return a <code>node</code>.
	 */
	public Node getNode();

}
