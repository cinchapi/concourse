package com.cinchapi.concourse.store;

import java.util.Iterator;
import java.util.List;

import com.cinchapi.commons.annotations.NoDocumentation;
import com.cinchapi.concourse.store.api.Mutation;
import com.cinchapi.concourse.store.api.Transaction;

/**
 * Abstract implementation of the {@link Transaction} interface. Holds a {@link Mutation} list.
 * @author jnelson
 *
 * @param <M> - the value {@link Mutation} type
 */
public abstract class AbstractTransaction<M extends Mutation> implements Transaction<M>, Iterable<M> {
	
	private List<M> mutations;
	
	@NoDocumentation
	public AbstractTransaction(){
		this.mutations = createEmptyMutationsList();
	}

	@Override
	public Transaction<M> record(M mutation) {
		mutations.add(mutation);
		return this;
	}
	
	/**
	 * Return iterator of the <code>mutations</code>.
	 */
	@Override
	public Iterator<M> iterator() {
		return mutations.iterator();
	}
	
	/**
	 * Create an empty list for <code>mutations</code>.
	 * @return empty list
	 */
	protected abstract List<M> createEmptyMutationsList();

}
