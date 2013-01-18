package com.cinchapi.concourse.server;

/**
 * Abstract implementation of the {@link Cluster} interface.
 * 
 * @author jnelson
 */
public abstract class AbstractCluster implements Cluster {

	@Override
	public final Node getNode(){
		return getNodeSpi();
	}

	/**
	 * Defines an algorithm to return a {@link Node} from a collection.
	 * 
	 * @return a <code>node</code>.
	 */
	protected abstract Node getNodeSpi();

}
