package com.cinchapi.concourse.store;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link Transaction} for use with a {@link CsvFileStore}.
 * @author jnelson
 *
 */
public class CsvFileStoreTransaction extends AbstractTransaction<Sha1LocatorMutation>{

	@Override
	protected List<Sha1LocatorMutation> createEmptyMutationsList() {
		return new ArrayList<Sha1LocatorMutation>();
	}
	

}
