package com.cinchapi.concourse.store;

import com.cinchapi.concourse.store.api.QueryService;
import com.cinchapi.concourse.store.api.ReadService;
import com.cinchapi.concourse.store.api.WriteService;

/**
 * Abstract provider of storage services.
 * 
 * @author jnelson
 */
public abstract class AbstractStore implements QueryService, ReadService,
		WriteService {

}
