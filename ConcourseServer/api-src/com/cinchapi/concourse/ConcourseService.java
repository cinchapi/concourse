package com.cinchapi.concourse;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.cinchapi.concourse.id.Id;
import com.cinchapi.concourse.model.api.Entity;
import com.cinchapi.concourse.model.api.Modification;
import com.cinchapi.concourse.property.LongProperty;
import com.cinchapi.concourse.property.StringProperty;
import com.cinchapi.concourse.property.api.Property;
import com.cinchapi.concourse.store.ConcourseStoreTransaction;

import static com.cinchapi.concourse.config.ServerConfig.*;

/**
 * Provides services to a {@link ConcourseServer}.
 * @author jnelson
 *
 */
public class ConcourseService implements CoreService, SearchService{
	
	private List<ConcourseStoreTransaction> transactionLog;
	
	public ConcourseService(){
		transactionLog = new ArrayList<ConcourseStoreTransaction>();
	}

	@Override
	public boolean add(String key, Object value, Id id) {
		Entity entity = STORE.load(id);
		Property<?> property = createProperty(key, value);
		Modification<?> mod = entity.add(property);
		if(mod != null){
			ConcourseStoreTransaction transaction = STORE.startTransaction();
			transaction.add(mod);
			return transactionLog.add(transaction);
		}
		else{
			return false;
		}
	}

	private Property<?> createProperty(String key, Object value){
		Property<?> p;
		if(value instanceof Long){
			p = new LongProperty(key, (Long) value);
		}
		else{
			p = new StringProperty(key, value.toString());
		}
		return p;
	}
	
	public void flushTransactionLog(){
		Iterator<ConcourseStoreTransaction> it = transactionLog.iterator();
		while(it.hasNext()){
			STORE.writeTransaction(it.next());
		}
	}
	
	@Override
	public Id create(String classifier, String title) {
		return null;
	}

	@Override
	public boolean delete(Id id) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean exists(Id id) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Set<Object> get(String key, Id id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<Id> list(String classifier) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean remove(String key, Object value, Id id) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Set<Id> select(String classifier, String criteria) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean set(String key, Object value, Id id) {
		// TODO Auto-generated method stub
		return false;
	}
	
	@Override
	public boolean deindex(String key, Id id) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean index(String key, Id id) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Set<Id> search(String classifier, String key, String query) {
		// TODO Auto-generated method stub
		return null;
	}

}
