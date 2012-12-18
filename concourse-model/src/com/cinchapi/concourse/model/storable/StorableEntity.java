package com.cinchapi.concourse.model.storable;

import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;

import com.cinchapi.commons.annotations.NoDocumentation;
import com.cinchapi.concourse.exceptions.StorageException;
import com.cinchapi.concourse.model.Entity;
import com.cinchapi.concourse.model.Property;
import com.cinchapi.concourse.model.mock.GUID;
import com.cinchapi.concourse.model.simple.SimpleEntity;

/**
 * A {@link SimpleEntity} that contains {@link Storable} data.
 * @author jnelson
 *
 */
public class StorableEntity extends SimpleEntity{
	
	protected StorableMetadataRecord metadata;
	protected Map<String,Set<StorablePropertyRecord>> data;
	
	/**
	 * Create a new {@link StorableEntity} with the <code>classifier</code> and <code>title</code>.
	 * @param classifier
	 * @param title
	 * @throws StorageException 
	 */
	public StorableEntity(String classifier, String title) throws StorageException{
		super(classifier, title);
		metadata.store();
	}
	
	@NoDocumentation
	protected StorableEntity(GUID id, StorableMetadataRecord metadata, Map<String,Set<StorablePropertyRecord>> data){
		this.id = id;
		this.metadata = metadata;
		this.data = data;
	}
	
	/**
	 * Load an existing <code>entity</code>.
	 * @param id
	 * @return the <code>entity</code>.
	 */
	public static StorableEntity load(GUID id){
		Entity entity = new StorableEntityProxy(id);
		String classifier = null; //load
		String title = null; //load
		DateTime created = null; //load
		
		StorableMetadataRecord metadata = new StorableMetadataRecord(entity, classifier, title, created);
		
		Map<String,Set<StorablePropertyRecord>> data = null; //TODO go through all the data and add it
		return new StorableEntity(id, metadata, data);
	}

	/* (non-Javadoc)
	 * @see com.cinchapi.concourse.model.base.BaseEntity#add(com.cinchapi.concourse.model.Property)
	 */
	@Override
	public <T> StorablePropertyRecord add(Property<T> property){
		StorablePropertyRecord record = new StorablePropertyRecord(this, property); 
		try{
			record.store();
		}
		catch (StorageException e) {
			this.data.get(record.getProperty().getKey()).remove(record);
			throw new RuntimeException("Error occured while trying to store a record"); //TODO get more specific
		}
		return record;
	}
}
