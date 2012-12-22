package com.cinchapi.concourse.store.api;

/**
 * A service that writes to a store.
 * @author jnelson
 *
 * @param <T> - the {@link StoreTransaction} type
 */
public interface WriteService<T extends StoreTransaction> {
	
	/**
	 * Start a <code>transaction</code>.
	 * @return the {@link StoreTransaction}.
	 */
	public T startTransaction();
	
	/**
	 * Write a <code>transaction</code> to the store.
	 * @param transaction
	 * @return <code>true</code> if the <code>transaction</code> is written.
	 */
	public boolean writeTransaction(T transaction);

}
