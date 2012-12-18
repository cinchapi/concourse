package com.cinchapi.concourse.store;

/**
 * Retrieves {@link StoredItem}s from a store.
 * @author jnelson
 *
 * @param <I> - the type of <code>id</code> used to refer to {@link StoredItem}s
 */
public interface RetrievalService<I> {
	
	/**
	 * Retrieve the {@link StoredItem} identified by the <code>id</code>.
	 * @param id
	 * @return the {@link StoredItem} or null if it cannot be found
	 */
	public StoredItem<I> retrieve(I id);

}
