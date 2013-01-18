package com.cinchapi.concourse.services;

import java.util.Set;

import com.cinchapi.concourse.ConcoursePreferences;
import com.cinchapi.concourse.ConcourseServer;
import com.cinchapi.concourse.model.Entity;
import com.cinchapi.concourse.model.Id;
import com.cinchapi.concourse.model.LongProperty;
import com.cinchapi.concourse.model.Revision;
import com.cinchapi.concourse.model.Property;
import com.cinchapi.concourse.model.StringProperty;
import com.cinchapi.concourse.store.ConcourseStore;
import com.cinchapi.concourse.store.ModificationLog;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

/**
 * Interacts with a {@link ModificationLog} and {@link ConcourseStore} on behalf of a {@link ConcourseServer}.
 * 
 * @author jnelson
 */
public class ConcourseService implements CoreService, SearchService {

	private static final ConcoursePreferences prefs = new ConcoursePreferences();

	private ModificationLog log;
	private ConcourseStore store;
	private ConcurrentLinkedHashMap<Id, Entity> cache;

	public ConcourseService(ModificationLog log, ConcourseStore store) {
		this.log = log;
		this.store = store;
		//TODO create cache
	}

	@Override
	public boolean add(String key, Object value, Id id){
		Entity entity = entity(id);
		Property<?> property = property(key, value);
		Revision<?> mod = entity.add(property);
		return this.write(mod);
	}

	@Override
	public Id create(String classifier, String title){
		return null;
	}

	@Override
	public boolean delete(Id id){
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean exists(Id id){
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Set<Object> get(String key, Id id){
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<Id> list(String classifier){
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean remove(String key, Object value, Id id){
		Entity entity = entity(id);
		Property<?> property = property(key, value);
		
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Set<Id> select(String classifier, String criteria){
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean set(String key, Object value, Id id){
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deindex(String key, Id id){
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean index(String key, Id id){
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Set<Id> search(String classifier, String key, String query){
		// TODO Auto-generated method stub
		return null;
	}

	/*Private*/
	private Entity entity(Id id){
		Entity entity = cache.get(id);
		if(entity==null){
			entity = store.read(id);
			cache.put(id,entity);
		}
		return entity;
	}
	
	/*Private*/
	private Property<?> property(String key, Object value){
		Property<?> p;
		if(value instanceof Long){
			p = new LongProperty(key,(Long) value);
		}
		else{
			p = new StringProperty(key,value.toString());
		}
		return p;
	}
	
	private boolean write(Revision<?> mod){
		if(mod != null){
			if(log.sizeMB() > prefs.getCommitLogFileMaxSizeMb()){ // check if log should be flushed
				log.flush(store);
			}
			log.write(mod);
			return true;
		}
		else{
			return false;
		}
	}
}
