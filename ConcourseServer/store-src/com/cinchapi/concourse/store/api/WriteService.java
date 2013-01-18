package com.cinchapi.concourse.store.api;

import com.cinchapi.concourse.model.Revision;

/**
 * A service that writes to a store.
 * 
 * @author jnelson
 */
public interface WriteService {

	/**
	 * Write a <code>modification</code> to the store.
	 * 
	 * @param modification
	 */
	public void write(Revision<?> mod);

}
