package com.cinchapi.concourse;

import com.cinchapi.concourse.store.AbstractStore;
import com.cinchapi.concourse.store.CsvFileStore;

public class ServerConstants {
	
	/**
	 * The underlying persistence store.
	 */
	public static final AbstractStore<?> STORE = new CsvFileStore();
	
	private ServerConstants(){ /*Non-Initializable*/ }

}
