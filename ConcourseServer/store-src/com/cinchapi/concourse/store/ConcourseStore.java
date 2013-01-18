package com.cinchapi.concourse.store;

import java.util.concurrent.ConcurrentMap;

import org.github.jamm.MemoryMeter;

import com.cinchapi.concourse.model.Entity;
import com.cinchapi.concourse.model.Id;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EntryWeigher;

/**
 * Any store used by a {@link ConcourseServer} should be a subclass of
 * {@link ConcourseStore}. This class provides entity caching to prevent
 * unnecessary store lookups.
 * 
 * @author jnelson
 */
public abstract class ConcourseStore extends PersistentStore {

	private ConcurrentMap<Id, Entity> cache; // entity cache
	private static final EntryWeigher<Id, Entity> memoryUsageWeigher;

	/* Non-Initializable */
	public ConcourseStore() {
		cache = new ConcurrentLinkedHashMap.Builder<Id, Entity>()
				.maximumWeightedCapacity(1024*1024) // 1MB
				.weigher(memoryUsageWeigher).build();
	}

	/**
	 * Load the {@link Entity} from the cache, if possible, otherwise perform a
	 * {@link #lookup(Id)}.
	 */
	@Override
	public Entity read(Id id){
		if(cache.containsKey(id)){
			return cache.get(id);
		}
		else{
			return lookup(id);
		}
	}

	static{
		memoryUsageWeigher = new EntryWeigher<Id, Entity>() {

			final MemoryMeter memory = new MemoryMeter();

			@Override
			public int weightOf(Id key, Entity value){
				long bytes = memory.measure(key)+memory.measure(value);
				return (int) (Math.min(bytes,Integer.MAX_VALUE));
			}

		};
	}

	/**
	 * Lookup the records corresponding to the <code>id</code> and return the
	 * proper {@link Entity}.
	 * 
	 * @param id
	 * @return the <code>entity</code>.
	 */
	protected abstract Entity lookup(Id id);

}
