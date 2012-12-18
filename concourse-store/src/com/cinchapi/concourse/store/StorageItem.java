package com.cinchapi.concourse.store;

/**
 * A {@link StorageService} accepts a {@link StorageItem} and persists it to a store. A {@link StorageItem} can have
 * one or more <code>attributes</code> (key/value mappings).
 * @author jnelson
 *
 * @param <I> - the type of <code>id</code> used to refer to the {@link StorageItem}
 */
public interface StorageItem<I> {
	
	/**
	 * Get the <code>id</code> that refers to this {@link StorageItem}
	 * @return the <code>id</code>
	 */
	public I getId();
	
	/**
	 * Add an <code>attribute</code> to this {@link StorageItem}.
	 * @param key
	 * @param value
	 * @return this {@link StorageItem}
	 */
	public StorageItem<I> addAttribute(String key, Object value);

}
