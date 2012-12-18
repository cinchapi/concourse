package com.cinchapi.concourse.store.sql;

import org.joda.time.DateTime;

import com.cinchapi.concourse.store.StorageItem;

/**
 * A {@link StorageItem} that is persisted to a {@link SqlStore}.
 * @author jnelson
 * 
 * @param <I> - the type of <code>id</code> used to refer to the {@link SqlStorageItem}
 *
 */
public interface SqlStorageItem<I> extends StorageItem<I>{

	/**
	 * Add an <code>attribute</code> to this {@link StorageItem}.
	 * @param key
	 * @param value
	 * @return this {@link SqlStorageItem}
	 */
	public SqlStorageItem<I> addAttribute(String key, String value);
	
	/**
	 * Add an <code>attribute</code> to this {@link StorageItem}.
	 * @param key
	 * @param value
	 * @return this {@link SqlStorageItem}
	 */
	public SqlStorageItem<I> addAttribute(String key, int value);
	
	/**
	 * Add an <code>attribute</code> to this {@link StorageItem}.
	 * @param key
	 * @param value
	 * @return this {@link SqlStorageItem}
	 */
	public SqlStorageItem<I> addAttribute(String key, long value);
	
	/**
	 * Add an <code>attribute</code> to this {@link StorageItem}.
	 * @param key
	 * @param value
	 * @return this {@link SqlStorageItem}
	 */
	public SqlStorageItem<I> addAttribute(String key, double value);
	
	/**
	 * Add an <code>attribute</code> to this {@link StorageItem}.
	 * @param key
	 * @param value
	 * @return this {@link SqlStorageItem}
	 */
	public SqlStorageItem<I> addAttribute(String key, float value);
	
	/**
	 * Add an <code>attribute</code> to this {@link StorageItem}.
	 * @param key
	 * @param value
	 * @return this {@link SqlStorageItem}
	 */
	public SqlStorageItem<I> addAttribute(String key, boolean value);
	
	/**
	 * Add an <code>attribute</code> to this {@link StorageItem}.
	 * @param key
	 * @param value
	 * @return this {@link SqlStorageItem}
	 */
	public SqlStorageItem<I> addAttribute(String key, DateTime value);
}
