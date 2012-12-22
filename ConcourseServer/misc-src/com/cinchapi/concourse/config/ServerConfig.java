package com.cinchapi.concourse.config;

import com.cinchapi.concourse.store.ConcourseStore;
import com.cinchapi.concourse.store.csv.CSVStore;

public class ServerConfig {
	
	/**
	 * The underlying persistence store.
	 */
	public static final ConcourseStore STORE = new CSVStore();
	
	private ServerConfig(){ /*Non-Initializable*/ }

}
