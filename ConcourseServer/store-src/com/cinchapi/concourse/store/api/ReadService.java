package com.cinchapi.concourse.store.api;

import com.cinchapi.concourse.model.Entity;
import com.cinchapi.concourse.model.Id;
import com.cinchapi.concourse.model.Revision;

/**
 * A service that can read from a store.
 * 
 * @author jnelson
 */
public interface ReadService {

	/**
	 * Load an {@link Entity} from the store based on the <code>id</code>.
	 * 
	 * @param id
	 * @return the {@link Entity}.
	 */
	public Entity read(Id id);

	/**
	 * Load a modification from the store based on the <code>lookup</code>.
	 * 
	 * @param lookup
	 * @return the {@link Revision}.
	 */
	public Revision<?> read(String lookup);

}
