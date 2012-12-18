package com.cinchapi.concourse.store;

/**
 * A {@link StoredItem} is retrieved from a {@link RetrievalService}.
 * @author jnelson
 *
 * @param <I> - the type of <code>id</code> used to refer to the {@link StorageItem}
 */
public interface StoredItem<I>{
	
	/**
	 * Get the <code>value</code> mapped from the <code>key</code>
	 * @param key
	 * @return the <code>value</code>
	 */
	public Object get(String key);
	
	

}
