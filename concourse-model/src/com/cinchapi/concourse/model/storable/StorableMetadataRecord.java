package com.cinchapi.concourse.model.storable;

import org.joda.time.DateTime;

import com.cinchapi.concourse.exceptions.StorageException;
import com.cinchapi.concourse.model.Entity;
import com.cinchapi.concourse.model.MetaProperty;
import com.cinchapi.concourse.model.simple.SimpleMetadata;
import com.cinchapi.concourse.store.StorageItem;
import com.cinchapi.concourse.store.StoredItem;
import static com.cinchapi.concourse.config.StoreDefinition.*;


/**
 * A {@link SimpleMetadata} that is can be store itself.
 * @author jnelson
 *
 */
public class StorableMetadataRecord extends SimpleMetadata implements Storable{

	private long storageId = 0; //from store.requestStorageItem().getId()
	
	/**
	 * Create a new {@link StorableMetadataRecord}.
	 * @param entity
	 * @param classifier
	 * @param title
	 * @param created
	 */
	public StorableMetadataRecord(Entity entity, String classifier, String title, DateTime created){
		super(entity, classifier, title, created);
	}

	/**
	 * Store a {@link StorageItem} that contains attributes for the <code>classifier</code>, <code>title</code> and
	 * <code>created</code> fields.
	 */
	@Override
	public boolean store() throws StorageException {
		StorageItem<Long> item = STORE.requestStorageItem();
		
		storageId = item.getId();
		
		item.addAttribute(StorableMetadataRecord.CLASS_KEY, get(StorableMetadataRecord.CLASS_KEY));
		item.addAttribute(StorableMetadataRecord.TITLE_KEY, get(StorableMetadataRecord.TITLE_KEY));
		item.addAttribute(StorableMetadataRecord.CREATED_KEY, get(StorableMetadataRecord.CREATED_KEY));
	
		return STORE.insert(item);
	}

	/* (non-Javadoc)
	 * @see com.cinchapi.concourse.model.base.BaseMetadataRecord#set(java.lang.String, java.lang.Object)
	 */
	@Override
	@SuppressWarnings("rawtypes")
	public <T> MetaProperty set(String key, T value) throws UnsupportedOperationException {
		MetaProperty property =  super.set(key, value);
		StoredItem<Long> item = STORE.retrieve(storageId);
		STORE.update(item, key, value);
		return property;
	}
	
	
	

}
