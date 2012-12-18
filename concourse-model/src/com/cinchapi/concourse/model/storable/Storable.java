package com.cinchapi.concourse.model.storable;

import com.cinchapi.concourse.exceptions.StorageException;

/**
 * An object that is storeable.
 * @author jnelson
 *
 */
public interface Storable{

	/**
	 * Store the object.
	 * @return <code>true</code> if the object is stored
	 * @throws StorageException if the store operation fails
	 */
	public boolean store() throws StorageException;
	
}
