
package com.cinchapi.concourse.model.storable;

import java.util.Iterator;
import java.util.Set;

import com.cinchapi.commons.annotations.NoDocumentation;
import com.cinchapi.concourse.model.Entity;
import com.cinchapi.concourse.model.Metadata;
import com.cinchapi.concourse.model.Property;
import com.cinchapi.concourse.model.PropertyRecord;
import com.cinchapi.concourse.model.mock.GUID;
import com.cinchapi.concourse.model.mock.ID;

/**
 * A proxy for a {@link StorableEntity} that delays loading property <code>records</code> until necessary;
 * 
 * @author jnelson
 *
 */
public class StorableEntityProxy implements Entity{
	
	private StorableEntity entity;
	private GUID id;
	private StorableMetadataRecord metadata;
	
	public StorableEntityProxy(GUID id){
		this.id = id;
		this.entity = null;
		this.metadata = null; //TODO load this
	}

	@Override
	public ID getId() {
		return id;
	}

	@Override
	public <T> PropertyRecord add(Property<T> property){
		return entity().add(property);
	}

	@Override
	public <T> boolean contains(Property<T> property) {
		return entity().contains(property);
	}

	@Override
	public <T> Set<Property<T>> get(String key) {
		return entity().get(key);
	}

	@Override
	public <T> PropertyRecord remove(Property<T> property) {
		return entity().remove(property);
	}
	
	@NoDocumentation
	private StorableEntity entity(){
		if(entity == null){
			entity = StorableEntity.load(id);
		}
		return entity;
	}

	@Override
	public Iterator<String> iterator() {
		return entity().iterator();
	}

	@Override
	public Metadata getMetadata() {
		return entity().getMetadata();
	}

	@Override
	public boolean setTitle(String title) {
		// TODO Auto-generated method stub
		return false;
	}

}
