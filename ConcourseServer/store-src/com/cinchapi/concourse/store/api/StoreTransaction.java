package com.cinchapi.concourse.store.api;

import com.cinchapi.concourse.model.api.Modification;


/**
 * A {@link Modification} list that can be written to a store by a {@link WriteService}.
 * 
 * @author jnelson
 *
 */
public interface StoreTransaction {
	
	/**
	 * Add a {@link Modification} to this <code>transaction</code>.
	 * @param modification
	 * @return this
	 */
	public StoreTransaction add(Modification<?> modification);
	

}
