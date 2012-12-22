package com.cinchapi.concourse.store;

import java.util.Iterator;
import java.util.List;

import com.cinchapi.commons.annotations.NoDocumentation;
import com.cinchapi.concourse.model.api.Modification;
import com.cinchapi.concourse.store.api.StoreTransaction;

/**
 * Abstract implementation of the {@link StoreTransaction} interface. Holds a {@link Modification} list.
 * @author jnelson
 *
 * @param <M> - the value {@link Mutation} type
 */
public abstract class AbstractStoreTransaction implements StoreTransaction, Iterable<Modification<?>> {
	
	private List<Modification<?>> modifications;
	
	@NoDocumentation
	public AbstractStoreTransaction(){
		this.modifications = createEmptyModificationList();
	}

	@Override
	public StoreTransaction add(Modification<?> mod) {
		if(!modifications.contains(mod)){
			modifications.add(mod);
		}
		return this;
	}
	
	/**
	 * Return iterator of the <code>modifications</code>.
	 */
	@Override
	public Iterator<Modification<?>> iterator() {
		return modifications.iterator();
	}
	
	/**
	 * Create an empty list for <code>modifications</code>.
	 * @return empty list
	 */
	protected abstract List<Modification<?>> createEmptyModificationList();

}
