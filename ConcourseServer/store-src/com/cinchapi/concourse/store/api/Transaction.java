package com.cinchapi.concourse.store.api;


/**
 * A series of {@link Entity} {@link Mutation}s that can be written to a store by a {@link WriteService}.
 * 
 * @param <M> - the valid {@link Mutation} type
 * 
 * @author jnelson
 *
 */
public interface Transaction<M extends Mutation> {
	
	/**
	 * Record a {@link Mutation} in this <code>transaction</code>.
	 * @param mutation
	 * @return this
	 */
	public Transaction<M> record(M mutation);
	

}
