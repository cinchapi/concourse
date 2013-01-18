package com.cinchapi.concourse.store;

/**
 * A store that serves as an intermediary before being flushed to a
 * {@link PersistentStore}.
 * 
 * @author jnelson
 */
public abstract class TemporaryStore extends AbstractStore {

	/**
	 * Flush the contents to a {@link PersistentStore}.
	 * 
	 * @param store
	 */
	public abstract void flush(PersistentStore store);

}
