package com.cinchapi.concourse.store.api;

import java.util.List;

import com.cinchapi.concourse.id.Id;

/**
 * A service that can query a store.
 * @author jnelson
 *
 */
public interface QueryService {
	
	/**
	 * Query a store and get a list of matching {@link Entity} <code>ids</code>.
	 * @param query
	 * @return a list of matching <code>ids</code>.
	 */
	public List<Id> query(String query);

}
