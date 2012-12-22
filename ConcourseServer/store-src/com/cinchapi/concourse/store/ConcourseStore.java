package com.cinchapi.concourse.store;

import java.util.HashMap;
import java.util.Map;

import com.cinchapi.concourse.id.Id;
import com.cinchapi.concourse.model.api.Entity;

/**
 * Any store used by an {@link ConcourseServer} should be a subclass of {@link ConcourseStore}. This class provides
 * entity caching to prevent unnecessary store lookups. 
 * @author jnelson
 *
 */
public abstract class ConcourseStore extends AbstractStore<ConcourseStoreTransaction>{ 
	
	/**
	 * Cache of loaded entities.
	 */
	private Map<Id, Entity> entityCache;
	
	public ConcourseStore(){
		entityCache = new HashMap<Id,Entity>();
	}

	/**
	 * Load the {@link Entity} from the cache, if possible, otherwise perform a {@link #lookup(Id)}.
	 */
	@Override
	public Entity load(Id id) {
		if(entityCache.containsKey(id)){
			return entityCache.get(id);
		}
		else{
			return lookup(id);
		}
	}
	
	/**
	 * Lookup the records corresponding to the <code>id</code> and return the proper {@link Entity}.
	 * @param id
	 * @return the <code>entity</code>.
	 */
	protected abstract Entity lookup(Id id);
	
}
