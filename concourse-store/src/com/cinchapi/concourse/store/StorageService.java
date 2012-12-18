package com.cinchapi.concourse.store;

import com.cinchapi.concourse.exceptions.StorageException;

/**
 * A {@link StorageService} persists {@link StorageItem}s to a store.
 * @author jnelson
 *
 * @param <I> - the type of <code>id</code> used to refer to a {@link StorageItem}
 */
public interface StorageService<I> {
	
	/**
	 * Request a new {@link StorageItem}
	 * @return a {@link StorageItem}
	 */
	public StorageItem<I> requestStorageItem();
	
	/**
	 * Save the {@link StorageItem}
	 * @param item
	 * @return <code>true</code> if the {@link StorageItem} is stored
	 * @throws StorageException
	 */
	public boolean insert(StorageItem<I> item) throws StorageException;
	
	/**
	 * Update a {@link StoredItem}.
	 * @param item
	 * @param key
	 * @param value
	 * @return <code>true</code> if the {@link StoredItem} is updated
	 */
	public boolean update(StoredItem<I> item, String key, Object value);

}
