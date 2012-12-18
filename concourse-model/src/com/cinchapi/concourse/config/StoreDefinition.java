package com.cinchapi.concourse.config;

import com.cinchapi.commons.annotations.NoDocumentation;
import com.cinchapi.concourse.store.Store;

/**
 * Contains a static {@link #STORE} field that defines the underlying data store.
 * @author jnelson
 *
 */
public class StoreDefinition {
	
	@NoDocumentation
	private StoreDefinition() { }
	
	/**
	 * The {@link Store} that holds the data.
	 */
	public static Store STORE = null;

}
