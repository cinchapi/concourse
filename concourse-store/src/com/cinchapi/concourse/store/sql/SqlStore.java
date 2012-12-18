package com.cinchapi.concourse.store.sql;

import com.cinchapi.concourse.store.Store;

/**
 * A relational database management system store that uses JDBC.
 * @author jnelson
 *
 */
public abstract class SqlStore implements Store{

	/* (non-Javadoc)
	 * @see com.cinchapi.concourse.model.base.store.StorageService#requestStorageItem()
	 */
	@Override
	public abstract SqlStorageItem<Long> requestStorageItem();
	
	

}
