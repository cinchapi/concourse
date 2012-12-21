package com.cinchapi.concourse.store;

import com.cinchapi.concourse.store.api.QueryService;
import com.cinchapi.concourse.store.api.ReadService;
import com.cinchapi.concourse.store.api.Transaction;
import com.cinchapi.concourse.store.api.WriteService;

/**
 * An abstract provider of storage services.
 * @author jnelson
 *
 * @param <T> - the {@link Transaction} type used in the store
 */
public abstract class AbstractStore<T extends Transaction<?>> implements QueryService, ReadService, WriteService<T>{
	

}
