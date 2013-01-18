package com.cinchapi.concourse.store.csv;

import java.util.ArrayList;
import java.util.List;

import com.cinchapi.concourse.model.Revision;
import com.cinchapi.concourse.store.ConcourseStoreTransaction;

/**
 * A {@link Transaction} for use with a {@link CSVStore}.
 * 
 * @author jnelson
 */
public class CSVStoreTransaction extends ConcourseStoreTransaction {

	@Override
	protected List<Revision<?>> createEmptyModificationList(){
		return new ArrayList<Revision<?>>();
	}

}
