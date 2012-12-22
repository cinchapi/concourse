package com.cinchapi.concourse;

import java.util.Set;

import com.cinchapi.concourse.id.Id;
import com.cinchapi.concourse.model.api.Entity;

/**
 * Provides search indexing and retrieval for Concourse data.
 * @author jnelson
 *
 */
public interface SearchService {
	
	/**
	 * Remove all the <code>indices</code> for the <code>values</code> mapped from <code>key</code> on the {@link Entity}
	 * identified by <code>id</code>.
	 * @param key
	 * @param id
	 * @return <code>true</code> if the indices are removed.
	 */
	public boolean deindex(String key, Id id);
	
	/**
	 * Index the <code>values</code> mapped from <code>key</code> on the {@link Entity} identified by <code>id</code>.
	 * @param key
	 * @param id
	 * @return <code>true</code> if the indices are created.
	 */
	public boolean index(String key, Id id);
	
	/**
	 * Return every {@link Entity} in the <code>classifier</code> that have indexed <code>values</code> mapped from <code>key</code>
	 * and matching the <code>query</code>.
	 * @param classifier
	 * @param key
	 * @param query
	 * @return the <code>id</code> list.
	 */
	public Set<Id> search(String classifier, String key, String query);

}
