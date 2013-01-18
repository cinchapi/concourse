package com.cinchapi.concourse.store.api;

import java.util.List;

import com.cinchapi.concourse.model.Id;

/**
 * A service that can query a store.
 * 
 * @author jnelson
 */
public interface QueryService {

	/**
	 * Query a store and get an {@link Id} list that represents that matching
	 * {@link Entity} objects.
	 * 
	 * @param query
	 * @return a list of matching <code>ids</code>.
	 */
	public List<Id> query(String query);

}
