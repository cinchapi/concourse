package com.cinchapi.concourse.model.storable;

import org.joda.time.DateTime;

import com.cinchapi.concourse.exceptions.StorageException;
import com.cinchapi.concourse.model.Entity;
import com.cinchapi.concourse.model.Property;
import com.cinchapi.concourse.model.simple.SimplePropertyRecord;
import com.cinchapi.concourse.store.StorageItem;
import com.cinchapi.concourse.store.StoredItem;
import static com.cinchapi.concourse.config.StoreDefinition.*;

/**
 * A {@link SimplePropertyRecord} that can be stored.
 * @author jnelson
 *
 */
public class StorablePropertyRecord extends SimplePropertyRecord implements Storable{

	private long storageId = 0; // from store.requestStorageItem().getId()
	
	/*{@link StorageItem} attributes */
	private static final String ENTITY_ATTRIBUTE = "entity";
	private static final String KEY_ATTRIBUTE = "key";
	private static final String VALUE_ATTRIBUTE = "value";
	private static final String ADDED_ATTRIBUTE = "added";
	private static final String REMOVED_ATTRIBUTE = "removed";
	

	/**
	 * Create a new {@link StorablePropertyRecord}.
	 * @param entity
	 * @param property
	 */
	@SuppressWarnings("rawtypes")
	public StorablePropertyRecord(Entity entity, Property property) {
		super(entity, property);
	}

	/**
	 * Store a {@link StorageItem} that contains attributes for the <code>entity</code>, <code>key</code>, <code>value</code>
	 * <code>added</code> and <code>removed</code> field.
	 */
	@Override
	public boolean store() throws StorageException {
		StorageItem<Long> item = STORE.requestStorageItem();
		
		storageId = item.getId();
		
		item.addAttribute(ENTITY_ATTRIBUTE, entity);
		item.addAttribute(KEY_ATTRIBUTE, property.getKey());
		item.addAttribute(VALUE_ATTRIBUTE, property.getValue());
		item.addAttribute(ADDED_ATTRIBUTE, added);
		item.addAttribute(REMOVED_ATTRIBUTE, removed);
		return STORE.insert(item);
	}

	/* (non-Javadoc)
	 * @see com.cinchapi.concourse.model.base.BasePropertyRecord#markAsRemoved()
	 */
	@Override
	public DateTime markAsRemoved() {
		super.markAsRemoved();
		StoredItem<Long> item = STORE.retrieve(storageId);
		STORE.update(item, REMOVED_ATTRIBUTE, removed);
		return removed;
	}

}
